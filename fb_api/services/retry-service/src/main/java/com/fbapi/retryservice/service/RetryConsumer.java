package com.fbapi.retryservice.service;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

@Service
@RequiredArgsConstructor
@Slf4j
public class RetryConsumer {

    private final KafkaTemplate<String, String> kafkaTemplate;
    private final ObjectMapper objectMapper;
    private final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(10);

    @KafkaListener(topics = "send_failed", groupId = "retry-service-group")
    public void consumeFailedMessage(String message) {
        log.info("Received failed message: {}", message);
        try {
            JsonNode messageNode = objectMapper.readTree(message);
            int retryCount = messageNode.has("retry_count") ? messageNode.get("retry_count").asInt() : 0;

            if (retryCount >= 3) {
                log.warn("Max retry count reached ({}). Moving to dead_letter topic: {}", retryCount, message);
                kafkaTemplate.send("dead_letter", message);
                return;
            }

            // Calculate backoff: 2^retry_count seconds
            long delaySeconds = (long) Math.pow(2, retryCount);
            int newRetryCount = retryCount + 1;

            ((ObjectNode) messageNode).put("retry_count", newRetryCount);
            String updatedMessage = messageNode.toString();

            log.info("Scheduling retry {} in {} seconds for message: {}", newRetryCount, delaySeconds, updatedMessage);

            scheduler.schedule(() -> {
                log.info("Executing scheduled retry {}, publishing to send_retry topic", newRetryCount);
                kafkaTemplate.send("send_retry", updatedMessage);
            }, delaySeconds, TimeUnit.SECONDS);

        } catch (Exception e) {
            log.error("Failed to process message in retry service", e);
        }
    }
}
