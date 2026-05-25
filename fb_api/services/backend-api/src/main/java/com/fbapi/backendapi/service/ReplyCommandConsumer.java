package com.fbapi.backendapi.service;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

@Service
@RequiredArgsConstructor
@Slf4j
public class ReplyCommandConsumer {

    private final FacebookApiService facebookApiService;
    private final ObjectMapper objectMapper;

    @KafkaListener(topics = "reply_commands", groupId = "backend-api-group")
    public void consumeReplyCommand(String message) {
        log.info("Received reply command from Kafka: {}", message);
        try {
            JsonNode commandNode = objectMapper.readTree(message);
            String action = commandNode.has("action") ? commandNode.get("action").asText() : "";
            String eventId = commandNode.has("event_id") ? commandNode.get("event_id").asText() : "";
            String source = commandNode.has("source") ? commandNode.get("source").asText() : "";
            String senderId = commandNode.has("sender_id") ? commandNode.get("sender_id").asText() : "";
            String replyMessage = commandNode.has("reply_message") && !commandNode.get("reply_message").isNull() ? commandNode.get("reply_message").asText() : "";

            if ("PENDING_REVIEW".equals(action)) {
                log.info("Action is PENDING_REVIEW. No automatic reply sent for event: {}", eventId);
                return;
            }

            if ("HIDE_COMMENT".equals(action)) {
                if ("comment".equals(source)) {
                    facebookApiService.hideComment(eventId);
                } else {
                    log.warn("HIDE_COMMENT action is not supported for source: {}", source);
                }
                return;
            }

            if ("AUTO_REPLY".equals(action)) {
                if ("comment".equals(source)) {
                    facebookApiService.replyToComment(eventId, replyMessage);
                } else if ("message".equals(source)) {
                    facebookApiService.sendPrivateMessage(senderId, replyMessage);
                }
                return;
            }
            
            log.warn("Unknown action received: {}", action);

        } catch (Exception e) {
            log.error("Failed to process reply command from Kafka", e);
        }
    }
}
