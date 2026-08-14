package com.example.kafka_consumer_dedup.repository;

import com.example.kafka_consumer_dedup.entity.OrderedState;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

@Repository
public interface OrderedStateRepository extends JpaRepository<OrderedState, String> {

    /**
     * Version-guarded upsert — only writes if incoming version is newer.
     * Because partition routing ensures ordered delivery per entityId,
     * this always converges to the final version without corruption.
     */
    @Modifying
    @Query(value = """
            INSERT INTO ordered_state (entity_id, version, event_type, data, updated_at)
            VALUES (:entityId, :version, :eventType, :data, :updatedAt)
            ON DUPLICATE KEY UPDATE
                version    = IF(VALUES(version) > version, VALUES(version), version),
                event_type = IF(VALUES(version) > version, VALUES(event_type), event_type),
                data       = IF(VALUES(version) > version, VALUES(data), data),
                updated_at = IF(VALUES(version) > version, VALUES(updated_at), updated_at)
            """, nativeQuery = true)
    void guardedUpsert(
            @Param("entityId") String entityId,
            @Param("version") long version,
            @Param("eventType") String eventType,
            @Param("data") String data,
            @Param("updatedAt") java.time.Instant updatedAt);
}
