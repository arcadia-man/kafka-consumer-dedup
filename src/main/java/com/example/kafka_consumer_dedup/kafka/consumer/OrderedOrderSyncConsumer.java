package com.example.kafka_consumer_dedup.kafka.consumer;

import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Component;

import com.example.kafka_consumer_dedup.model.OrderSyncMessage;

import tools.jackson.databind.ObjectMapper;

@Component
public class OrderedOrderSyncConsumer extends AbstractOrderSyncConsumer {

    public OrderedOrderSyncConsumer(ObjectMapper objectMapper) {
        super(objectMapper);
    }

    @Override
    protected void listen(
            @Header(KafkaHeaders.GROUP_ID) String groupId,
            @Payload String rawJson,
            @Header(KafkaHeaders.RECEIVED_PARTITION) int partition,
            @Header(KafkaHeaders.OFFSET) long offset,
            @Header(KafkaHeaders.RECEIVED_KEY) String key) {
        consumeRaw(groupId, rawJson, groupId, partition, offset);
    }

    @Override
    protected void process(String consumerName, OrderSyncMessage payload, String key, int partition, long offset) {
        System.out.printf("[%s] Key: %s | Partition: %d | Offset: %d | EntityId: %s | Version: %s%n",
                consumerName, key, partition, offset, payload.getEntityId(), payload.getVersion());
    }
}
