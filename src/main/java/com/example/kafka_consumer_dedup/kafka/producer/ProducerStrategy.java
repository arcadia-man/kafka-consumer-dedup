package com.example.kafka_consumer_dedup.kafka.producer;

public interface ProducerStrategy<T> {
    void send(String topicName, T payload);
}
