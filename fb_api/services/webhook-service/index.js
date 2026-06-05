const express = require('express');
const crypto = require('crypto');
const { Kafka, Partitioners } = require('kafkajs');
const path = require('path');

// Load environment variables from root .env first, then local environment
require('dotenv').config({ path: path.resolve(__dirname, '../../.env') });
require('dotenv').config();

const app = express();
const port = process.env.PORT || 3001;
const appSecret = process.env.APP_SECRET;
const verifyToken = process.env.VERIFY_TOKEN || 'my_verify_token_123';

// Express middleware to capture raw body for HMAC signature verification
app.use(
  express.json({
    verify: (req, res, buf) => {
      req.rawBody = buf;
    },
  })
);

// Health check endpoint
app.get('/health', (req, res) => res.send('OK'));

// Kafka Setup
const kafkaBrokers = process.env.KAFKA_BROKERS ? process.env.KAFKA_BROKERS.split(',') : ['localhost:9092'];
const kafka = new Kafka({
  clientId: 'webhook-service',
  brokers: kafkaBrokers,
  retry: {
    initialRetryTime: 300,
    retries: 10,
  },
});

const producer = kafka.producer({
  createPartitioner: Partitioners.LegacyPartitioner,
});

let isProducerConnected = false;
const failedEventsQueue = []; // In-memory queue for fallback/retry

async function connectKafka() {
  try {
    await producer.connect();
    isProducerConnected = true;
    console.log('Kafka Producer connected successfully to', kafkaBrokers);
  } catch (error) {
    isProducerConnected = false;
    console.error('Failed to connect to Kafka, retrying in 5 seconds...', error.message);
    setTimeout(connectKafka, 5000);
  }
}

connectKafka();

// Retry interval for failed events (runs every 10 seconds)
setInterval(async () => {
  if (isProducerConnected && failedEventsQueue.length > 0) {
    console.log(`[RETRY_QUEUE] Attempting to publish ${failedEventsQueue.length} queued events...`);
    const eventsToRetry = [...failedEventsQueue];
    failedEventsQueue.length = 0; // Clear queue for processing
    
    for (const event of eventsToRetry) {
      try {
        await producer.send({
          topic: 'raw_events',
          messages: [{ key: event.event_id, value: JSON.stringify(event) }],
        });
        console.log(`[RETRY_SUCCESS] Published queued event ${event.event_id} to topic 'raw_events'`);
      } catch (err) {
         console.error(`[RETRY_ERROR] Failed to publish queued event ${event.event_id}:`, err.message);
         failedEventsQueue.push(event); // Re-queue if it still fails
      }
    }
  }
}, 10000);

// Verification middleware for HMAC-SHA256 signature from Facebook
function verifyFacebookSignature(req, res, next) {
  if (!appSecret) {
    console.warn('WARNING: APP_SECRET is not configured. Skipping HMAC signature validation.');
    return next();
  }

  const signature = req.headers['x-hub-signature-256'];
  if (!signature) {
    console.error('Unauthorized request: X-Hub-Signature-256 header is missing.');
    return res.status(403).send('Signature missing');
  }

  const parts = signature.split('=');
  if (parts.length !== 2 || parts[0] !== 'sha256') {
    console.error('Unauthorized request: Invalid signature format.');
    return res.status(403).send('Invalid signature format');
  }

  const signatureHash = parts[1];
  const expectedHash = crypto
    .createHmac('sha256', appSecret)
    .update(req.rawBody || '')
    .digest('hex');

  if (signatureHash !== expectedHash) {
    console.error('Unauthorized request: HMAC signature verification failed.');
    return res.status(403).send('Signature mismatch');
  }

  next();
}

// 1. Webhook Verification (GET /webhook)
app.get('/webhook', (req, res) => {
  const mode = req.query['hub.mode'];
  const token = req.query['hub.verify_token'];
  const challenge = req.query['hub.challenge'];

  if (mode === 'subscribe' && token === verifyToken) {
    console.log(`Webhook verified successfully with token: ${token}`);
    res.status(200).send(challenge);
  } else {
    console.error(`Webhook verification failed. Mode: ${mode}, Token: ${token}`);
    res.sendStatus(403);
  }
});

// 2. Webhook Event Handler (POST /webhook)
app.post('/webhook', verifyFacebookSignature, (req, res) => {
  const body = req.body;

  // Log incoming webhook payload structure for debugging
  console.log('Received Webhook POST payload:', JSON.stringify(body, null, 2));

  const events = [];

  if (body.object === 'page' && Array.isArray(body.entry)) {
    for (const entry of body.entry) {
      // Extract page comment events (from changes array)
      if (Array.isArray(entry.changes)) {
        for (const change of entry.changes) {
          if (change.field === 'feed') {
            const val = change.value;
            // Process if it's a comment event (add verb or comment item)
            if (val && (val.item === 'comment' || val.comment_id)) {
              // Extract comment text. Note: message can be empty if it's media-only
              const commentText = val.message || '';
              const senderId = val.from ? val.from.id : (val.sender_id || val.from_id || 'unknown');
              
              if (String(senderId) === String(entry.id)) {
                console.log(`[IGNORE] Ignored self-comment from page ${entry.id}`);
                continue;
              }

              events.push({
                event_id: val.comment_id || `${entry.id}_comment_${Date.now()}`,
                source: 'comment',
                sender_id: senderId,
                message: commentText,
                timestamp: val.created_time ? val.created_time * 1000 : Date.now(),
                status: 'received'
              });
            }
          }
        }
      }

      // Extract Messenger events (from messaging array)
      if (Array.isArray(entry.messaging)) {
        for (const msg of entry.messaging) {
          if (msg.message) {
            const messageText = msg.message.text || '';
            const senderId = msg.sender ? msg.sender.id : 'unknown';
            
            if (String(senderId) === String(entry.id)) {
              console.log(`[IGNORE] Ignored self-message from page ${entry.id}`);
              continue;
            }

            events.push({
              event_id: msg.message.mid || `${entry.id}_msg_${Date.now()}`,
              source: 'message',
              sender_id: senderId,
              message: messageText,
              timestamp: msg.timestamp || Date.now(),
              status: 'received'
            });
          }
        }
      }
    }
  }

  // CRITICAL REQUIREMENT: Respond with 200 OK immediately to avoid FB timeouts/retries
  res.status(200).send('EVENT_RECEIVED');

  // Publish normalized events to Kafka asynchronously in the background
  if (events.length > 0) {
    events.forEach(async (event) => {
      console.log(`[EVENT_RECEIVED] Normalized Event: ${JSON.stringify(event)}`);
      
      if (!isProducerConnected) {
        console.warn(`[KAFKA_WARN] Producer not connected. Queuing event ${event.event_id} for later retry.`);
        failedEventsQueue.push(event);
        return;
      }

      try {
        await producer.send({
          topic: 'raw_events',
          messages: [
            {
              key: event.event_id,
              value: JSON.stringify(event),
            },
          ],
        });
        console.log(`[KAFKA_SUCCESS] Published event ${event.event_id} to topic 'raw_events'`);
      } catch (error) {
        console.error(`[KAFKA_ERROR] Failed to publish event ${event.event_id} to topic 'raw_events':`, error.message);
        failedEventsQueue.push(event); // Queue for retry
      }
    });
  }
});

// Start Express server
app.listen(port, () => {
  console.log(`Webhook Service listening on port ${port}`);
  console.log(`Verify Token: ${verifyToken}`);
  console.log(`App Secret configured: ${appSecret ? 'Yes' : 'No'}`);
});
