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
 * State B — written by the NaiveOrderSyncConsumer.
 * Multiple concurrent threads write here without ordering guarantees,
 * so older versions can overwrite newer ones (visible corruption).
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@Entity
@Table(name = "naive_state")
public class NaiveState {

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
