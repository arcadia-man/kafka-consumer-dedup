package com.example.kafka_consumer_dedup.entity;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.Instant;

/**
 * State C — written by the OrderedOrderSyncConsumer.
 * Partition routing by entityId guarantees one-thread-per-entity order,
 * so this always converges to the correct final version.
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@Entity
@Table(name = "ordered_state")
public class OrderedState {

    @Id
    @Column(name = "entity_id", nullable = false, length = 64)
    private String entityId;

    @Column(name = "version", nullable = false)
    private long version;

    @Column(name = "event_type", nullable = false, length = 32)
    private String eventType;

    @Column(name = "data", columnDefinition = "TEXT")
    private String data;

    @Column(name = "updated_at", nullable = false)
    private Instant updatedAt;
}
