package com.example.kafka_consumer_dedup.kafka.producer;

import com.example.kafka_consumer_dedup.model.OrderSyncMessage;

public interface OrderSyncProducerStrategy extends ProducerStrategy<OrderSyncMessage> {
    // Inherits void send(String topicName, OrderSyncMessage payload);
}
