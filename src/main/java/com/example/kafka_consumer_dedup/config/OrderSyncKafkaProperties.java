package com.example.kafka_consumer_dedup.config;

import java.util.Map;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;

import lombok.Data;

@Data
@Configuration
@ConfigurationProperties(prefix = "kafka.connections.order-sync")
public class OrderSyncKafkaProperties {

    private String bootstrapServers;
    private ProducerProperties producer;
    private Map<String, TopicProperties> topics;

    @Data
    public static class ProducerProperties {
        private String keySerializer;
        private String valueSerializer;
    }

    @Data
    public static class TopicProperties {
        private String name;
        private int partitions;
        private short replicas;
        private Map<String, ConsumerProperties> consumers;
    }

    @Data
    public static class ConsumerProperties {
        private String groupId;
        private String autoOffsetReset;
        private int concurrency;
        private String keyDeserializer;
        private String valueDeserializer;
        private String trustedPackages;
    }
}