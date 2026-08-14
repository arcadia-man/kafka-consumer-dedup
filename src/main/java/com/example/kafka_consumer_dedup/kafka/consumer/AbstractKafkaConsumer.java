package com.example.kafka_consumer_dedup.kafka.consumer;

import org.springframework.boot.json.JsonParseException;

import tools.jackson.databind.ObjectMapper;

public abstract class AbstractKafkaConsumer<T> {
    protected final ObjectMapper objectMapper;
    protected final Class<T> targetType;

    protected AbstractKafkaConsumer(ObjectMapper objectMapper, Class<T> targetType) {
        this.objectMapper = objectMapper;
        this.targetType = targetType;
    }

    protected void consumeRaw(String consumerName, String rawJson, String key, int partition, long offset) {
        try {
            T payload = objectMapper.readValue(rawJson, targetType);
            process(consumerName, payload, key, partition, offset);
        } catch (JsonParseException e) {
            System.err.printf("[%s] Deserialization error on Partition: %d | Offset: %d. Error: %s%n",
                    consumerName, partition, offset, e.getMessage());
        }
    }
    
    protected abstract void listen (String groupId, String rawJson, int partition, long offset, String key);

    protected abstract void process(String consumerName, T payload, String key, int partition, long offset);
}
