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
 * Ground-truth dataset A.
 * Seeded once with 5,000 records at version=1.
 * The scheduler job updates version here as it publishes each round.
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@Entity
@Table(name = "seed_data")
public class SeedData {

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
