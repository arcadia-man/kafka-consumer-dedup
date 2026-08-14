package com.example.kafka_consumer_dedup.kafka.consumer;

import java.time.Instant;

import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import com.example.kafka_consumer_dedup.model.OrderSyncMessage;
import com.example.kafka_consumer_dedup.repository.OrderedStateRepository;

import lombok.extern.slf4j.Slf4j;
import tools.jackson.databind.ObjectMapper;

/**
 * Ordered consumer — keyed by entityId.
 *
 * The same 50ms sleep is applied here to make the comparison fair —
 * same processing latency, same back pressure, only difference is the key.
 *
 * Because hash(entityId) always maps to the same partition, ALL versions
 * of a given entity are consumed by the SAME thread in the order they were
 * produced. Even with 50ms latency, no two threads ever race on the same
 * entity. The version-guarded upsert is a safety net but in practice
 * messages already arrive in order per partition.
 */
@Slf4j
@Component
public class OrderedOrderSyncConsumer extends AbstractOrderSyncConsumer {

    private static final long PROCESSING_DELAY_MS = 50;

    private final OrderedStateRepository orderedStateRepository;

    public OrderedOrderSyncConsumer(ObjectMapper objectMapper,
                                    OrderedStateRepository orderedStateRepository) {
        super(objectMapper);
        this.orderedStateRepository = orderedStateRepository;
    }

    @Override
    @KafkaListener(
            topics = "${kafka.connections.order-sync.topics.order-sync.name}",
            groupId = "${kafka.connections.order-sync.topics.order-sync.consumers.execution-consumer.group-id}",
            concurrency = "${kafka.connections.order-sync.topics.order-sync.consumers.execution-consumer.concurrency}",
            containerFactory = "kafkaListenerContainerFactory")
    public void listen(
            @Header(KafkaHeaders.GROUP_ID) String groupId,
            @Payload String rawJson,
            @Header(KafkaHeaders.RECEIVED_PARTITION) int partition,
            @Header(KafkaHeaders.OFFSET) long offset,
            @Header(name = KafkaHeaders.RECEIVED_KEY, required = false) String key) {
        consumeRaw(groupId, rawJson, key, partition, offset);
    }

    @Override
    @Transactional
    protected void process(String consumerName, OrderSyncMessage payload,
                           String key, int partition, long offset) {
        log.info("[ORDERED] Key: {} | Partition: {} | Offset: {} | EntityId: {} | Version: {} | Thread: {}",
                key, partition, offset, payload.getEntityId(), payload.getVersion(),
                Thread.currentThread().getName());

        // Same latency as naive consumer — fair comparison.
        // Even with this delay, keyed partitioning ensures only one thread
        // ever processes a given entityId at a time.
        try {
            Thread.sleep(PROCESSING_DELAY_MS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }

        // Version-guarded upsert — only writes if incoming version > stored.
        // Combined with ordered delivery, this always lands at the correct final version.
        orderedStateRepository.guardedUpsert(
                payload.getEntityId(),
                payload.getVersion(),
                payload.getEventType(),
                payload.getData() != null ? payload.getData().toString() : null,
                Instant.now());
    }
}
