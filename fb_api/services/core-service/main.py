import os
import sys
import time
import json
import logging
import threading
from collections import defaultdict
from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse, JSONResponse
from confluent_kafka import Consumer, Producer, KafkaError
import psycopg2
from psycopg2.extras import RealDictCursor
import google.generativeai as genai
from dotenv import load_dotenv

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger("core-service")

# Load environment variables
# core-service is located at fb_api/services/core-service, root .env is at ../../../.env
current_dir = os.path.dirname(os.path.abspath(__file__))
parent_env_path = os.path.abspath(os.path.join(current_dir, "../../../.env"))
if os.path.exists(parent_env_path):
    load_dotenv(parent_env_path)
    logger.info(f"Loaded environment variables from: {parent_env_path}")
else:
    load_dotenv()
    logger.info("Loaded system or local directory environment variables.")

# Initialize FastAPI App
app = FastAPI(
    title="Facebook Core Service",
    description="Processes raw webhook events using Gemini AI & Automation Rules, publishing back to reply commands.",
    version="1.0.0"
)

# Custom Circuit Breaker for downstream AI API calls
class CircuitBreaker:
    def __init__(self, failure_threshold=3, recovery_time=30):
        self.failure_threshold = failure_threshold
        self.recovery_time = recovery_time
        self.state = "CLOSED"  # CLOSED, OPEN, HALF-OPEN
        self.failure_count = 0
        self.last_state_change = time.time()

    def record_success(self):
        if self.state != "CLOSED":
            logger.info(f"[CIRCUIT_BREAKER] State changed to CLOSED from {self.state}")
        self.failure_count = 0
        self.state = "CLOSED"

    def record_failure(self):
        self.failure_count += 1
        logger.warning(f"[CIRCUIT_BREAKER] Failure recorded: {self.failure_count}/{self.failure_threshold}")
        if self.failure_count >= self.failure_threshold and self.state != "OPEN":
            self.state = "OPEN"
            self.last_state_change = time.time()
            logger.error(f"[CIRCUIT_BREAKER] State changed to OPEN! Failing fast for next {self.recovery_time}s.")

    def can_execute(self) -> bool:
        if self.state == "CLOSED":
            return True
        if self.state == "OPEN":
            if time.time() - self.last_state_change > self.recovery_time:
                self.state = "HALF-OPEN"
                logger.info("[CIRCUIT_BREAKER] Testing state: HALF-OPEN. Attempting next call.")
                return True
            return False
        if self.state == "HALF-OPEN":
            return True
        return False

circuit_breaker = CircuitBreaker(failure_threshold=3, recovery_time=30)

# Global status and stats counters for monitoring dashboard
stats = {
    "total_processed": 0,
    "duplicates_ignored": 0,
    "ai_fallbacks": 0,
    "rate_limited": 0,
    "db_connection_status": "disconnected",
    "kafka_consumer_status": "disconnected",
}

# PostgreSQL connection helpers
def get_db_connection():
    return psycopg2.connect(
        host=os.getenv("DB_HOST", "postgres"),
        port=os.getenv("DB_PORT", "5432"),
        database=os.getenv("DB_NAME", "fb_api_db"),
        user=os.getenv("DB_USER", "fb_api_user"),
        password=os.getenv("DB_PASSWORD", "fb_api_password")
    )

def init_db():
    attempts = 10
    for attempt in range(attempts):
        try:
            conn = get_db_connection()
            cur = conn.cursor()
            cur.execute("""
                CREATE TABLE IF NOT EXISTS idempotency_keys (
                    command_id VARCHAR(100) PRIMARY KEY,
                    processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    status VARCHAR(20) NOT NULL
                );
            """)
            cur.execute("""
                CREATE TABLE IF NOT EXISTS comments (
                    id SERIAL PRIMARY KEY,
                    comment_id VARCHAR(100) UNIQUE NOT NULL,
                    post_id VARCHAR(100) DEFAULT 'unknown',
                    message TEXT,
                    intent VARCHAR(50),
                    sentiment VARCHAR(20),
                    status VARCHAR(20) DEFAULT 'received',
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                );
            """)
            conn.commit()
            cur.close()
            conn.close()
            stats["db_connection_status"] = "connected"
            logger.info("PostgreSQL Database tables initialized/verified.")
            return True
        except Exception as e:
            stats["db_connection_status"] = f"error: {str(e)}"
            logger.error(f"Database connection attempt {attempt + 1}/{attempts} failed: {e}. Retrying in 3 seconds...")
            time.sleep(3)
    return False

def check_event_exists(event_id: str) -> bool:
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        cur.execute("SELECT 1 FROM comments WHERE comment_id = %s", (event_id,))
        res = cur.fetchone()
        cur.close()
        conn.close()
        return res is not None
    except Exception as e:
        logger.error(f"Database error checking event_id {event_id}: {e}")
        return False

def insert_comment(event_id: str, message: str, status: str = 'received', post_id: str = 'unknown'):
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        cur.execute(
            """
            INSERT INTO comments (comment_id, post_id, message, status)
            VALUES (%s, %s, %s, %s)
            ON CONFLICT (comment_id) DO NOTHING
            """,
            (event_id, post_id, message, status)
        )
        conn.commit()
        cur.close()
        conn.close()
    except Exception as e:
        logger.error(f"Database error inserting comment {event_id}: {e}")

def update_comment_ai_results(event_id: str, intent: str, sentiment: str, status: str):
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        cur.execute(
            """
            UPDATE comments
            SET intent = %s, sentiment = %s, status = %s
            WHERE comment_id = %s
            """,
            (intent, sentiment, status, event_id)
        )
        conn.commit()
        cur.close()
        conn.close()
    except Exception as e:
        logger.error(f"Database error updating comment {event_id}: {e}")

def get_latest_comments(limit=15):
    try:
        conn = get_db_connection()
        cur = conn.cursor(cursor_factory=RealDictCursor)
        cur.execute(
            """
            SELECT comment_id, post_id, message, intent, sentiment, status, created_at
            FROM comments
            ORDER BY created_at DESC
            LIMIT %s
            """,
            (limit,)
        )
        rows = cur.fetchall()
        cur.close()
        conn.close()
        return rows
    except Exception as e:
        logger.error(f"Database error retrieving comments: {e}")
        return []

# Rate Limiter
sender_message_history = defaultdict(list)
rate_limit_lock = threading.Lock()

def check_rate_limit(sender_id: str) -> bool:
    """
    Returns True if sender has sent > 20 messages in the last 60 seconds.
    """
    now = time.time()
    with rate_limit_lock:
        timestamps = sender_message_history[sender_id]
        # Keep timestamps only within the last 60 seconds
        filtered = [t for t in timestamps if now - t <= 60]
        filtered.append(now)
        sender_message_history[sender_id] = filtered
        if len(filtered) > 20:
            return True
        return False

# Google Gemini Setup
api_key = os.getenv("GEMINI_API_KEY") or os.getenv("GOOGLE_API_KEY")
if api_key:
    genai.configure(api_key=api_key)
    logger.info("Google Gemini client successfully configured.")
else:
    logger.warning("No GEMINI_API_KEY found. Google Gemini integration is disabled unless configured.")

def analyze_message_with_ai(message: str) -> dict:
    """
    Calls Google Gemini API to identify intent and sentiment of the message text.
    Returns: {"intent": str, "sentiment": str}
    """
    if not api_key:
        raise ValueError("GEMINI_API_KEY environment variable is not configured")

    if not circuit_breaker.can_execute():
        raise RuntimeError("Gemini API calls are blocked by the active Circuit Breaker (State: OPEN).")

    try:
        # Using recommended model
        model_name = os.getenv("GEMINI_MODEL", "gemini-3.5-flash")
        model = genai.GenerativeModel(model_name)
        
        prompt = (
            "Bạn là một AI phân tích tin nhắn và bình luận của khách hàng trên Facebook Page bằng tiếng Việt.\n"
            "Hãy phân tích nội dung sau và trả về DUY NHẤT một chuỗi JSON hợp lệ (không kèm theo block markdown ```json hay giải thích nào khác).\n"
            "JSON schema:\n"
            "{\n"
            "  \"intent\": \"hỏi giá\" | \"khiếu nại\" | \"spam\" | \"hỏi thông tin khác\" | \"chửi bới\" | \"mua hàng\" | \"khen\",\n"
            "  \"sentiment\": \"positive\" | \"neutral\" | \"negative\"\n"
            "}\n\n"
            "Quy tắc chọn intent:\n"
            "- \"hỏi giá\": hỏi về giá cả, chi phí, bao nhiêu tiền, bảng giá...\n"
            "- \"khiếu nại\": phàn nàn về dịch vụ, sản phẩm lỗi, giao hàng chậm, hàng hỏng...\n"
            "- \"spam\": quảng cáo, link bậy, nội dung lặp vô nghĩa...\n"
            "- \"chửi bới\": dùng từ tục tĩu, xúc phạm...\n"
            "- \"mua hàng\": bày tỏ muốn mua, xin tư vấn mua hàng, đặt hàng...\n"
            "- \"khen\": khen ngợi sản phẩm, dịch vụ, nội dung bài đăng...\n"
            "- \"hỏi thông tin khác\": câu hỏi chung chung không thuộc các loại trên.\n\n"
            f"Nội dung cần phân tích: \"{message}\""
        )

        generation_config = {}
        if "1.5" in model_name or "2." in model_name or "3." in model_name:
            generation_config = {"response_mime_type": "application/json"}

        response = model.generate_content(
            prompt,
            generation_config=generation_config
        )
        
        response_text = response.text.strip()
        
        # Strip potential Markdown wrap
        if response_text.startswith("```"):
            if response_text.startswith("```json"):
                response_text = response_text[7:]
            else:
                response_text = response_text[3:]
            if response_text.endswith("```"):
                response_text = response_text[:-3]
            response_text = response_text.strip()

        result = json.loads(response_text)
        
        # Align with task requirements
        valid_intents = ["hỏi giá", "khiếu nại", "spam", "hỏi thông tin khác", "chửi bới", "mua hàng", "khen"]
        valid_sentiments = ["positive", "neutral", "negative"]

        intent = result.get("intent", "hỏi thông tin khác")
        if intent not in valid_intents:
            intent = "hỏi thông tin khác"

        sentiment = result.get("sentiment", "neutral")
        if sentiment not in valid_sentiments:
            sentiment = "neutral"

        # Success - reset circuit breaker
        circuit_breaker.record_success()
        return {"intent": intent, "sentiment": sentiment}
        
    except Exception as e:
        circuit_breaker.record_failure()
        logger.error(f"Error during Gemini AI call: {e}")
        raise e

# Kafka Producer for publish output commands
def publish_reply_command(event_id: str, source: str, sender_id: str, action: str, reply_message: str = None, intent: str = None, sentiment: str = None):
    kafka_brokers = os.getenv("KAFKA_BROKERS", "localhost:9092")
    try:
        producer_conf = {
            'bootstrap.servers': kafka_brokers,
            'client.id': 'core-service-producer'
        }
        p = Producer(producer_conf)
        
        payload = {
            "event_id": event_id,
            "source": source,
            "sender_id": sender_id,
            "action": action,
            "reply_message": reply_message,
            "intent": intent,
            "sentiment": sentiment,
            "timestamp": int(time.time() * 1000)
        }
        
        def delivery_report(err, msg):
            if err is not None:
                logger.error(f"[KAFKA_PRODUCER] Message delivery failed: {err}")
            else:
                logger.info(f"[KAFKA_PRODUCER] Message delivered to {msg.topic()} [{msg.partition()}]")
                
        p.produce(
            'reply_commands', 
            key=event_id.encode('utf-8'), 
            value=json.dumps(payload).encode('utf-8'), 
            callback=delivery_report
        )
        p.flush()
        logger.info(f"[KAFKA_PRODUCER] Published reply command: {payload}")
    except Exception as e:
        logger.error(f"[KAFKA_PRODUCER] Failed to publish message: {e}")

# Kafka Consumer Loop runs in a background thread
def kafka_consumer_loop(stop_event):
    kafka_brokers = os.getenv("KAFKA_BROKERS", "localhost:9092")
    conf = {
        'bootstrap.servers': kafka_brokers,
        'group.id': 'core-service-group',
        'auto.offset.reset': 'earliest',
        'enable.auto.commit': False
    }

    consumer = None
    while not stop_event.is_set():
        try:
            consumer = Consumer(conf)
            consumer.subscribe(['raw_events'])
            stats["kafka_consumer_status"] = "connected"
            logger.info("Kafka Consumer successfully subscribed to 'raw_events'.")
            break
        except Exception as e:
            stats["kafka_consumer_status"] = f"error: {str(e)}"
            logger.error(f"Failed to start Kafka consumer: {e}. Retrying in 5 seconds...")
            time.sleep(5)

    if not consumer:
        return

    while not stop_event.is_set():
        try:
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                else:
                    logger.error(f"[KAFKA_CONSUMER] Consumer error: {msg.error()}")
                    stats["kafka_consumer_status"] = f"error: {msg.error()}"
                    time.sleep(2)
                    continue

            stats["kafka_consumer_status"] = "connected"

            # Parse event message
            try:
                raw_value = msg.value().decode('utf-8')
                event = json.loads(raw_value)
                logger.info(f"[KAFKA_CONSUMER] Consumed event: {event}")

                event_id = event.get("event_id")
                source = event.get("source", "comment")
                sender_id = event.get("sender_id", "unknown")
                message = event.get("message", "")

                if not event_id:
                    logger.warning("[KAFKA_CONSUMER] Event missing event_id, skipping.")
                    consumer.commit(msg, asynchronous=False)
                    continue

                # 0. Failsafe: Ignore if message is our own automated reply
                auto_replies = [
                    "Cảm ơn bạn! Bạn check inbox Page để shop gửi thông tin và tư vấn chi tiết cho mình nhé!",
                    "Cảm ơn bạn đã phản hồi tích cực! Shop rất vui được phục vụ bạn.",
                    "Dạ shop rất xin lỗi bạn vì trải nghiệm chưa tốt này. Shop sẽ liên hệ hỗ trợ bạn ngay lập tức ạ!"
                ]
                if message and any(reply in message for reply in auto_replies):
                    logger.info(f"[FAILSAFE] Ignored event {event_id} because it matches our auto-reply templates (caught old queue loop).")
                    consumer.commit(msg, asynchronous=False)
                    continue


                # 1. Idempotency Check
                if check_event_exists(event_id):
                    logger.info(f"[KAFKA_CONSUMER] Deduplication triggered. Event {event_id} already processed. Skipping.")
                    stats["duplicates_ignored"] += 1
                    consumer.commit(msg, asynchronous=False)
                    continue

                # Parse post_id (FB usually maps postid_commentid)
                post_id = 'unknown'
                if '_' in event_id and source == 'comment':
                    post_id = event_id.split('_')[0]

                # Insert raw event to DB (status: 'received')
                insert_comment(event_id, message, 'received', post_id)
                stats["total_processed"] += 1

                # 2. Rate Limiting Check
                if check_rate_limit(sender_id):
                    logger.warning(f"[RATE_LIMIT] Sender {sender_id} exceeded limit (>20/min). Auto fallback to PENDING_REVIEW.")
                    stats["rate_limited"] += 1
                    update_comment_ai_results(event_id, intent="rate_limited", sentiment="neutral", status="pending_review")
                    publish_reply_command(
                        event_id=event_id,
                        source=source,
                        sender_id=sender_id,
                        action="PENDING_REVIEW",
                        reply_message=None,
                        intent="rate_limited",
                        sentiment="neutral"
                    )
                    consumer.commit(msg, asynchronous=False)
                    continue

                # 3. Automation Rule: Check link spam beforehand
                is_link_spam = "http://" in message or "https://" in message or "www." in message
                if is_link_spam:
                    logger.info(f"[SPAM_FILTER] Event {event_id} contains link. Action: HIDE_COMMENT.")
                    intent = "spam"
                    sentiment = "negative"
                    action = "HIDE_COMMENT"
                    update_comment_ai_results(event_id, intent=intent, sentiment=sentiment, status="processed")
                    publish_reply_command(
                        event_id=event_id,
                        source=source,
                        sender_id=sender_id,
                        action=action,
                        reply_message=None,
                        intent=intent,
                        sentiment=sentiment
                    )
                    consumer.commit(msg, asynchronous=False)
                    continue

                # 4. AI classification & Automation Rules Engine
                intent = "hỏi thông tin khác"
                sentiment = "neutral"
                action = "PENDING_REVIEW"
                reply_message = None
                db_status = "pending_review"

                try:
                    # AI Call
                    ai_result = analyze_message_with_ai(message)
                    intent = ai_result["intent"]
                    sentiment = ai_result["sentiment"]
                    logger.info(f"[AI_SUCCESS] Event {event_id} classification: Intent='{intent}', Sentiment='{sentiment}'")

                    # Apply Rules
                    if intent == 'spam':
                        action = "HIDE_COMMENT"
                        reply_message = None
                        db_status = "processed"
                    elif intent in ["hỏi giá", "mua hàng"] and sentiment == "positive":
                        action = "AUTO_REPLY"
                        reply_message = "Cảm ơn bạn! Bạn check inbox Page để shop gửi thông tin và tư vấn chi tiết cho mình nhé!"
                        db_status = "processed"
                    elif sentiment == "positive":
                        action = "AUTO_REPLY"
                        reply_message = "Cảm ơn bạn đã phản hồi tích cực! Shop rất vui được phục vụ bạn."
                        db_status = "processed"
                    elif sentiment == "negative":
                        action = "AUTO_REPLY"
                        reply_message = "Dạ shop rất xin lỗi bạn vì trải nghiệm chưa tốt này. Shop sẽ liên hệ hỗ trợ bạn ngay lập tức ạ!"
                        db_status = "pending_review"  # Đẩy trạng thái về PENDING_REVIEW
                    else:
                        action = "PENDING_REVIEW"
                        reply_message = None
                        db_status = "pending_review"

                except Exception as ai_err:
                    logger.error(f"[AI_ERROR] API Exception: {ai_err}. Triggering Fallback Action: PENDING_REVIEW.")
                    stats["ai_fallbacks"] += 1
                    intent = "fallback_error"
                    sentiment = "neutral"
                    action = "PENDING_REVIEW"
                    reply_message = None
                    db_status = "pending_review"

                # Update database comments table
                update_comment_ai_results(event_id, intent=intent, sentiment=sentiment, status=db_status)

                # Publish reply command to topic
                publish_reply_command(
                    event_id=event_id,
                    source=source,
                    sender_id=sender_id,
                    action=action,
                    reply_message=reply_message,
                    intent=intent,
                    sentiment=sentiment
                )

            except Exception as parse_err:
                logger.error(f"Error handling event parsing: {parse_err}")

            # Commit message offset to broker
            consumer.commit(msg, asynchronous=False)

        except Exception as loop_err:
            logger.error(f"[KAFKA_CONSUMER] Exception in loop: {loop_err}")
            time.sleep(2)

    try:
        consumer.close()
        logger.info("Kafka consumer loop ended and connection closed.")
    except Exception as e:
        logger.error(f"Error closing Kafka consumer: {e}")

# Global variables for background thread management
consumer_thread = None
stop_event = threading.Event()

@app.on_event("startup")
def startup_event():
    global consumer_thread
    stop_event.clear()
    
    # Initialize DB (Auto-creating tables if not exists)
    init_db()

    # Launch Kafka consumer worker thread
    consumer_thread = threading.Thread(target=kafka_consumer_loop, args=(stop_event,))
    consumer_thread.daemon = True
    consumer_thread.start()
    logger.info("Background Kafka consumer thread started.")

@app.on_event("shutdown")
def shutdown_event():
    logger.info("Stopping background tasks...")
    stop_event.set()
    if consumer_thread:
        consumer_thread.join(timeout=5)
    logger.info("Shutdown completed.")

# API Routes
@app.get("/health")
def health_check():
    return {
        "status": "ok",
        "database": stats["db_connection_status"],
        "kafka": stats["kafka_consumer_status"],
        "circuit_breaker": circuit_breaker.state,
        "gemini_api_key_configured": api_key is not None
    }

@app.get("/", response_class=HTMLResponse)
def get_dashboard(request: Request):
    latest_comments = get_latest_comments(15)
    
    # Simple count aggregates
    intent_counts = {}
    sentiment_counts = {}
    for c in latest_comments:
        i = c.get("intent") or "chưa phân loại"
        s = c.get("sentiment") or "chưa phân loại"
        intent_counts[i] = intent_counts.get(i, 0) + 1
        sentiment_counts[s] = sentiment_counts.get(s, 0) + 1

    # Render beautiful dashboard
    cb_state = circuit_breaker.state
    html_content = f"""
    <!DOCTYPE html>
    <html lang="vi">
    <head>
        <meta charset="UTF-8">
        <meta name="viewport" content="width=device-width, initial-scale=1.0">
        <title>Core AI Service - Console</title>
        <link href="https://fonts.googleapis.com/css2?family=Plus+Jakarta+Sans:wght@300;400;500;600;700&display=swap" rel="stylesheet">
        <style>
            :root {{
                --bg-color: #0b0f19;
                --card-bg: rgba(22, 28, 45, 0.7);
                --border-color: rgba(255, 255, 255, 0.08);
                --text-primary: #f3f4f6;
                --text-secondary: #9ca3af;
                --accent-primary: #6366f1;
                --success: #10b981;
                --warning: #f59e0b;
                --danger: #ef4444;
            }}
            * {{
                box-sizing: border-box;
                margin: 0;
                padding: 0;
                font-family: 'Plus Jakarta Sans', sans-serif;
            }}
            body {{
                background-color: var(--bg-color);
                color: var(--text-primary);
                min-height: 100vh;
                padding: 2rem;
                background-image: radial-gradient(circle at 10% 20%, rgba(99, 102, 241, 0.05) 0%, transparent 40%),
                                  radial-gradient(circle at 90% 80%, rgba(16, 185, 129, 0.03) 0%, transparent 40%);
                background-attachment: fixed;
            }}
            .container {{
                max-width: 1200px;
                margin: 0 auto;
            }}
            header {{
                display: flex;
                justify-content: space-between;
                align-items: center;
                margin-bottom: 2rem;
                border-bottom: 1px solid var(--border-color);
                padding-bottom: 1.5rem;
            }}
            h1 {{
                font-size: 1.75rem;
                font-weight: 700;
                background: linear-gradient(135deg, #fff 0%, #a5b4fc 100%);
                -webkit-background-clip: text;
                -webkit-text-fill-color: transparent;
            }}
            .sys-badges {{
                display: flex;
                gap: 1rem;
                align-items: center;
            }}
            .status-pill {{
                display: inline-flex;
                align-items: center;
                gap: 0.5rem;
                padding: 0.35rem 0.85rem;
                border-radius: 9999px;
                font-size: 0.8rem;
                font-weight: 600;
                border: 1px solid rgba(255, 255, 255, 0.05);
            }}
            .status-pill::before {{
                content: '';
                width: 8px;
                height: 8px;
                border-radius: 50%;
                display: inline-block;
            }}
            .status-online {{
                background-color: rgba(16, 185, 129, 0.1);
                color: #34d399;
            }}
            .status-online::before {{
                background-color: var(--success);
                box-shadow: 0 0 8px var(--success);
            }}
            .status-offline {{
                background-color: rgba(239, 68, 68, 0.1);
                color: #f87171;
            }}
            .status-offline::before {{
                background-color: var(--danger);
                box-shadow: 0 0 8px var(--danger);
            }}
            .status-warn {{
                background-color: rgba(245, 158, 11, 0.1);
                color: #fbbf24;
            }}
            .status-warn::before {{
                background-color: var(--warning);
                box-shadow: 0 0 8px var(--warning);
            }}
            .grid-stats {{
                display: grid;
                grid-template-columns: repeat(auto-fit, minmax(220px, 1fr));
                gap: 1.5rem;
                margin-bottom: 2rem;
            }}
            .card-stat {{
                background: var(--card-bg);
                border: 1px solid var(--border-color);
                border-radius: 12px;
                padding: 1.5rem;
                backdrop-filter: blur(10px);
                box-shadow: 0 4px 20px rgba(0, 0, 0, 0.2);
            }}
            .stat-label {{
                font-size: 0.85rem;
                color: var(--text-secondary);
                text-transform: uppercase;
                letter-spacing: 0.05em;
                margin-bottom: 0.5rem;
            }}
            .stat-value {{
                font-size: 2.2rem;
                font-weight: 700;
                color: #fff;
            }}
            .grid-main {{
                display: grid;
                grid-template-columns: 2.5fr 1fr;
                gap: 1.5rem;
            }}
            @media(max-width: 900px) {{
                .grid-main {{
                    grid-template-columns: 1fr;
                }}
            }}
            .card-main {{
                background: var(--card-bg);
                border: 1px solid var(--border-color);
                border-radius: 16px;
                padding: 1.5rem;
                backdrop-filter: blur(10px);
            }}
            .card-title {{
                font-size: 1.15rem;
                font-weight: 600;
                margin-bottom: 1.25rem;
                color: #fff;
                border-bottom: 1px solid rgba(255, 255, 255, 0.05);
                padding-bottom: 0.75rem;
            }}
            table {{
                width: 100%;
                border-collapse: collapse;
                text-align: left;
            }}
            th {{
                padding: 0.75rem 1rem;
                font-size: 0.75rem;
                color: var(--text-secondary);
                text-transform: uppercase;
                border-bottom: 1px solid var(--border-color);
                font-weight: 600;
            }}
            td {{
                padding: 1rem;
                font-size: 0.85rem;
                border-bottom: 1px solid rgba(255, 255, 255, 0.03);
            }}
            .intent-badge {{
                display: inline-block;
                padding: 0.2rem 0.5rem;
                border-radius: 6px;
                font-size: 0.75rem;
                font-weight: 600;
            }}
            .intent-gia {{ background: rgba(99, 102, 241, 0.15); color: #818cf8; }}
            .intent-spam {{ background: rgba(239, 68, 68, 0.15); color: #f87171; }}
            .intent-khen {{ background: rgba(16, 185, 129, 0.15); color: #34d399; }}
            .intent-other {{ background: rgba(156, 163, 175, 0.15); color: #d1d5db; }}
            .intent-buying {{ background: rgba(236, 72, 153, 0.15); color: #f472b6; }}
            
            .sentiment-badge {{
                display: inline-block;
                padding: 0.2rem 0.5rem;
                border-radius: 6px;
                font-size: 0.75rem;
                font-weight: 600;
            }}
            .sent-positive {{ background: rgba(16, 185, 129, 0.15); color: #34d399; }}
            .sent-neutral {{ background: rgba(245, 158, 11, 0.15); color: #fbbf24; }}
            .sent-negative {{ background: rgba(239, 68, 68, 0.15); color: #f87171; }}
            
            .status-badge {{
                display: inline-block;
                padding: 0.2rem 0.4rem;
                border-radius: 4px;
                font-size: 0.7rem;
                font-weight: 600;
            }}
            .status-processed {{ background: rgba(16, 185, 129, 0.1); color: #34d399; border: 1px solid rgba(16, 185, 129, 0.2); }}
            .status-pending {{ background: rgba(245, 158, 11, 0.1); color: #fbbf24; border: 1px solid rgba(245, 158, 11, 0.2); }}
            .status-received {{ background: rgba(156, 163, 175, 0.1); color: #9ca3af; border: 1px solid rgba(156, 163, 175, 0.2); }}
            
            .dist-row {{
                display: flex;
                justify-content: space-between;
                align-items: center;
                padding: 0.5rem 0;
                font-size: 0.85rem;
                border-bottom: 1px solid rgba(255, 255, 255, 0.02);
            }}
            .dist-label {{
                color: var(--text-secondary);
                text-transform: capitalize;
            }}
            .dist-count {{
                font-weight: 600;
                color: #fff;
                background: rgba(255, 255, 255, 0.05);
                padding: 0.1rem 0.4rem;
                border-radius: 4px;
            }}
        </style>
    </head>
    <body>
        <div class="container">
            <header>
                <div>
                    <h1>Core AI Service Console</h1>
                    <p style="font-size: 0.85rem; color: var(--text-secondary); margin-top: 0.25rem;">Hệ thống phân tích Intent & Sentiment thời gian thực</p>
                </div>
                <div class="sys-badges">
                    <span class="status-pill {"status-online" if "connected" in stats["db_connection_status"] else "status-offline"}">
                        Database
                    </span>
                    <span class="status-pill {"status-online" if "connected" in stats["kafka_consumer_status"] else "status-offline"}">
                        Kafka
                    </span>
                    <span class="status-pill {"status-online" if cb_state == "CLOSED" else ("status-warn" if cb_state == "HALF-OPEN" else "status-offline")}">
                        Circuit Breaker: {cb_state}
                    </span>
                </div>
            </header>

            <div class="grid-stats">
                <div class="card-stat">
                    <div class="stat-label">Tổng Event Đã Nhận</div>
                    <div class="stat-value" style="color: var(--accent-primary);">{stats["total_processed"]}</div>
                </div>
                <div class="card-stat">
                    <div class="stat-label">Trùng Lặp (Idempotent)</div>
                    <div class="stat-value">{stats["duplicates_ignored"]}</div>
                </div>
                <div class="card-stat">
                    <div class="stat-label">AI Fallbacks</div>
                    <div class="stat-value" style="color: var(--danger);">{stats["ai_fallbacks"]}</div>
                </div>
                <div class="card-stat">
                    <div class="stat-label">Bị Rate Limit (Sender)</div>
                    <div class="stat-value" style="color: var(--warning);">{stats["rate_limited"]}</div>
                </div>
            </div>

            <div class="grid-main">
                <div class="card-main">
                    <div class="card-title">Bình luận được xử lý gần đây</div>
                    <div style="overflow-x: auto;">
                        <table>
                            <thead>
                                <tr>
                                    <th>Comment ID</th>
                                    <th>Post ID</th>
                                    <th>Nội Dung</th>
                                    <th>Intent</th>
                                    <th>Sentiment</th>
                                    <th>Trạng Thái</th>
                                    <th>Thời Gian</th>
                                </tr>
                            </thead>
                            <tbody>
    """
    
    if not latest_comments:
        html_content += """
            <tr>
                <td colspan="7" style="text-align: center; color: var(--text-secondary); padding: 2rem;">Chưa có bình luận nào được ghi nhận. Hãy gửi bình luận trên page!</td>
            </tr>
        """
    else:
        for c in latest_comments:
            comment_id = c.get("comment_id", "")
            post_id = c.get("post_id", "unknown")
            message = c.get("message", "")
            intent = c.get("intent") or "n/a"
            sentiment = c.get("sentiment") or "n/a"
            status = c.get("status") or "received"
            created_at = c.get("created_at")
            
            formatted_time = created_at.strftime("%H:%M:%S %d/%m") if created_at else "n/a"
            
            # intent classes
            intent_class = "intent-other"
            if "giá" in intent: intent_class = "intent-gia"
            elif "spam" in intent: intent_class = "intent-spam"
            elif "khen" in intent: intent_class = "intent-khen"
            elif "mua" in intent: intent_class = "intent-buying"
            
            # sentiment classes
            sent_class = f"sent-{sentiment}"
            
            # status classes
            status_class = f"status-{status}"
            
            html_content += f"""
                <tr>
                    <td style="font-family: monospace; font-size: 0.75rem; color: var(--text-secondary);">{comment_id}</td>
                    <td style="font-family: monospace; font-size: 0.75rem; color: var(--text-secondary);">{post_id}</td>
                    <td style="max-width: 250px; overflow: hidden; text-overflow: ellipsis; white-space: nowrap;">{message}</td>
                    <td><span class="intent-badge {intent_class}">{intent}</span></td>
                    <td><span class="sentiment-badge {sent_class}">{sentiment}</span></td>
                    <td><span class="status-badge {status_class}">{status}</span></td>
                    <td style="color: var(--text-secondary); font-size: 0.75rem;">{formatted_time}</td>
                </tr>
            """
            
    html_content += f"""
                            </tbody>
                        </table>
                    </div>
                </div>

                <div style="display: flex; flex-direction: column; gap: 1.5rem;">
                    <div class="card-main">
                        <div class="card-title">Phân Phối Intent</div>
    """
    
    if not intent_counts:
        html_content += '<div style="color: var(--text-secondary); font-size: 0.85rem;">Chưa có dữ liệu</div>'
    else:
        for it, cnt in intent_counts.items():
            html_content += f"""
                <div class="dist-row">
                    <span class="dist-label">{it}</span>
                    <span class="dist-count">{cnt}</span>
                </div>
            """
            
    html_content += """
                    </div>

                    <div class="card-main">
                        <div class="card-title">Phân Phối Sentiment</div>
    """
    
    if not sentiment_counts:
        html_content += '<div style="color: var(--text-secondary); font-size: 0.85rem;">Chưa có dữ liệu</div>'
    else:
        for st, cnt in sentiment_counts.items():
            html_content += f"""
                <div class="dist-row">
                    <span class="dist-label">{st}</span>
                    <span class="dist-count">{cnt}</span>
                </div>
            """
            
    html_content += """
                    </div>
                </div>
            </div>
        </div>
    </body>
    </html>
    """
    return HTMLResponse(content=html_content, status_code=200)

