package com.example.kafka_consumer_dedup.repository;

import com.example.kafka_consumer_dedup.entity.NaiveState;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

@Repository
public interface NaiveStateRepository extends JpaRepository<NaiveState, String> {

    /**
     * Blind upsert — no version guard.
     * Any thread can overwrite any version, demonstrating the race condition.
     */
    @Modifying
    @Query(value = """
            INSERT INTO naive_state (entity_id, version, event_type, data, updated_at)
            VALUES (:entityId, :version, :eventType, :data, :updatedAt)
            ON DUPLICATE KEY UPDATE
                version    = VALUES(version),
                event_type = VALUES(event_type),
                data       = VALUES(data),
                updated_at = VALUES(updated_at)
            """, nativeQuery = true)
    void blindUpsert(
            @Param("entityId") String entityId,
            @Param("version") long version,
            @Param("eventType") String eventType,
            @Param("data") String data,
            @Param("updatedAt") java.time.Instant updatedAt);
}
