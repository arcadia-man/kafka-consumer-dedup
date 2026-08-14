package com.example.kafka_consumer_dedup.kafka.consumer;

import com.example.kafka_consumer_dedup.model.OrderSyncMessage;
import com.example.kafka_consumer_dedup.repository.OrderedStateRepository;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import tools.jackson.databind.ObjectMapper;

import java.time.Instant;

/**
 * Ordered consumer — keyed by entityId, partition routing guarantees
 * that all messages for the same entity arrive on the same partition
 * in order. Uses a version-guarded upsert so only newer versions win.
 * Result is always correct final state (State C).
 */
@Slf4j
@Component
public class OrderedOrderSyncConsumer extends AbstractOrderSyncConsumer {

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
            @Header(KafkaHeaders.RECEIVED_KEY) String key) {
        consumeRaw(groupId, rawJson, key, partition, offset);
    }

    @Override
    @Transactional
    protected void process(String consumerName, OrderSyncMessage payload,
                           String key, int partition, long offset) {
        log.info("[ORDERED] Key: {} | Partition: {} | Offset: {} | EntityId: {} | Version: {}",
                key, partition, offset, payload.getEntityId(), payload.getVersion());

        // Version-guarded upsert — only writes if incoming version > stored version
        orderedStateRepository.guardedUpsert(
                payload.getEntityId(),
                payload.getVersion(),
                payload.getEventType(),
                payload.getData() != null ? payload.getData().toString() : null,
                Instant.now());
    }
}
