package com.example.kafka_consumer_dedup.kafka.producer;

import java.util.concurrent.CompletableFuture;

import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Component;

import com.example.kafka_consumer_dedup.model.OrderSyncMessage;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;



@Slf4j
@Component("naiveOrderSyncProducer")
@RequiredArgsConstructor
public class NaiveOrderSyncProducer implements OrderSyncProducerStrategy {

    private final KafkaTemplate<String, Object> kafkaTemplate;

    @Override
    public void send(String topicName, OrderSyncMessage message) {
        
       CompletableFuture<SendResult<String, Object>> future = kafkaTemplate.send(topicName, null, message);

        future.whenComplete((result, ex) -> {
            if (ex == null) {
                log.info("[NAIVE PRODUCER SUCCESS] Topic: {} | Partition: {} | Offset: {} | EntityId: {} | Version: {}",
                        topicName,
                        result.getRecordMetadata().partition(),
                        result.getRecordMetadata().offset(),
                        message.getEntityId(),
                        message.getVersion());
            } else {
                log.error("[NAIVE PRODUCER ERROR] Failed to send EntityId: {} to Topic: {}",
                        message.getEntityId(), topicName, ex);
            }
        });
    }
}