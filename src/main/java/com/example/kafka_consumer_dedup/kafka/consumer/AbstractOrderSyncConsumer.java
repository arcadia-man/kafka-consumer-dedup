package com.example.kafka_consumer_dedup.kafka.consumer;

import com.example.kafka_consumer_dedup.model.OrderSyncMessage;

import tools.jackson.databind.ObjectMapper;

public abstract class AbstractOrderSyncConsumer extends AbstractKafkaConsumer<OrderSyncMessage> {
    protected AbstractOrderSyncConsumer(ObjectMapper objectMapper) {
        super(objectMapper, OrderSyncMessage.class);
    }
}