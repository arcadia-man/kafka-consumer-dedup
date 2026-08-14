package com.example.kafka_consumer_dedup.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * The Kafka payload published by {@link com.example.kafka_consumer_dedup.job.OrderSyncDiscoveryJob}.
 * 
 * {@code version} is ERP's version/timestamp to distinguish newer updates from out-of-order older updates.
 * {@code eventType} specifies the action (e.g., CREATE, UPDATE, DELETE).
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class OrderSyncMessage {

    private String entityId;
    private long version;
    private String eventType;
    private Object data;
}