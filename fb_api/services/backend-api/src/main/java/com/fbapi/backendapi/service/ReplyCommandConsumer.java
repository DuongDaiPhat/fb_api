package com.fbapi.backendapi.service;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fbapi.backendapi.entity.IdempotencyKey;
import com.fbapi.backendapi.repository.IdempotencyKeyRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import java.time.LocalDateTime;

@Service
@RequiredArgsConstructor
@Slf4j
public class ReplyCommandConsumer {

    private final FacebookApiService facebookApiService;
    private final ObjectMapper objectMapper;
    private final IdempotencyKeyRepository idempotencyKeyRepository;
    private final KafkaTemplate<String, String> kafkaTemplate;

    @KafkaListener(topics = {"reply_commands", "send_retry"}, groupId = "backend-api-group")
    public void consumeReplyCommand(String message) {
        log.info("Received command from Kafka: {}", message);
        try {
            JsonNode commandNode = objectMapper.readTree(message);
            String eventId = commandNode.has("event_id") ? commandNode.get("event_id").asText() : "";
            
            if (eventId.isEmpty()) {
                log.warn("No event_id found in message: {}", message);
                return;
            }

            // 1. Idempotency Check
            if (idempotencyKeyRepository.existsById(eventId)) {
                log.info("Event {} already processed. Skipping to ensure idempotency.", eventId);
                return;
            }

            String action = commandNode.has("action") ? commandNode.get("action").asText() : "";
            String source = commandNode.has("source") ? commandNode.get("source").asText() : "";
            String senderId = commandNode.has("sender_id") ? commandNode.get("sender_id").asText() : "";
            String replyMessage = commandNode.has("reply_message") && !commandNode.get("reply_message").isNull() ? commandNode.get("reply_message").asText() : "";

            if ("PENDING_REVIEW".equals(action)) {
                log.info("Action is PENDING_REVIEW. No automatic reply sent for event: {}", eventId);
                return;
            }

            // 2. Save to Idempotency DB before API call
            idempotencyKeyRepository.save(new IdempotencyKey(eventId, "PROCESSED", LocalDateTime.now()));
            log.info("Saved idempotency key for event: {}", eventId);

            try {
                // 3. Call Facebook API (wrapped with CircuitBreaker)
                if ("HIDE_COMMENT".equals(action)) {
                    if ("comment".equals(source)) {
                        facebookApiService.hideComment(eventId);
                    } else {
                        log.warn("HIDE_COMMENT action is not supported for source: {}", source);
                        return;
                    }
                } else if ("AUTO_REPLY".equals(action)) {
                    if ("comment".equals(source)) {
                        facebookApiService.replyToComment(eventId, replyMessage);
                    } else if ("message".equals(source)) {
                        facebookApiService.sendPrivateMessage(senderId, replyMessage);
                    } else {
                        log.warn("AUTO_REPLY action is not supported for source: {}", source);
                        return;
                    }
                } else {
                    log.warn("Unknown action received: {}", action);
                    return;
                }

                log.info("Successfully processed event: {}", eventId);

            } catch (Exception apiException) {
                log.error("Facebook API call failed for event {}. Publishing to send_failed...", eventId, apiException);
                
                // 4. Publish to send_failed with retry_count
                ObjectNode failedNode = (ObjectNode) commandNode;
                int retryCount = commandNode.has("retry_count") ? commandNode.get("retry_count").asInt() : 0;
                failedNode.put("retry_count", retryCount);
                failedNode.put("error", apiException.getMessage());
                
                kafkaTemplate.send("send_failed", failedNode.toString());
            }

        } catch (Exception e) {
            log.error("Failed to parse and process command from Kafka", e);
        }
    }
}
