package com.example.kafka_consumer_dedup.kafka.producer;

import java.util.concurrent.CompletableFuture;

import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Component;

import com.example.kafka_consumer_dedup.model.OrderSyncMessage;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@Component("orderedOrderSyncProducer")
@RequiredArgsConstructor
public class OrderedOrderSyncProducer implements OrderSyncProducerStrategy {

    private final KafkaTemplate<String, Object> kafkaTemplate;

    @Override
    public void send(String topicName, OrderSyncMessage message) {
        String recordKey = message.getEntityId();

        CompletableFuture<SendResult<String, Object>> future = kafkaTemplate.send(topicName, recordKey, message);

        future.whenComplete((result, ex) -> {
            if (ex == null) {
                log.info("[ORDERED PRODUCER SUCCESS] Topic: {} | Key: {} | Partition: {} | Offset: {} | EntityId: {} | Version: {}",
                        topicName,
                        recordKey,
                        result.getRecordMetadata().partition(),
                        result.getRecordMetadata().offset(),
                        message.getEntityId(),
                        message.getVersion());
            } else {
                log.error("[ORDERED PRODUCER ERROR] Failed to send Key: {} to Topic: {}",
                        recordKey, topicName, ex);
            }
        });
    }
}