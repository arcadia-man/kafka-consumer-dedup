package com.example.kafka_consumer_dedup.kafka.consumer;

import com.example.kafka_consumer_dedup.model.OrderSyncMessage;
import com.example.kafka_consumer_dedup.repository.NaiveStateRepository;
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
 * Naive consumer — no key, no ordering guarantee.
 * Multiple concurrent threads (concurrency=3) race to write naive_state.
 * Uses a blind upsert: any thread can overwrite any version, so older
 * messages arriving late silently corrupt the final state (State B).
 */
@Slf4j
@Component
public class NaiveOrderSyncConsumer extends AbstractOrderSyncConsumer {

    private final NaiveStateRepository naiveStateRepository;

    public NaiveOrderSyncConsumer(ObjectMapper objectMapper,
                                  NaiveStateRepository naiveStateRepository) {
        super(objectMapper);
        this.naiveStateRepository = naiveStateRepository;
    }

    @Override
    @KafkaListener(
            topics = "${kafka.connections.order-sync.topics.navice-order-sync.name}",
            groupId = "${kafka.connections.order-sync.topics.navice-order-sync.consumers.audit-consumer.group-id}",
            concurrency = "${kafka.connections.order-sync.topics.navice-order-sync.consumers.audit-consumer.concurrency}",
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
        log.info("[NAIVE] Partition: {} | Offset: {} | EntityId: {} | Version: {}",
                partition, offset, payload.getEntityId(), payload.getVersion());

        // Blind upsert — no version check, last writer wins (demonstrates race condition)
        naiveStateRepository.blindUpsert(
                payload.getEntityId(),
                payload.getVersion(),
                payload.getEventType(),
                payload.getData() != null ? payload.getData().toString() : null,
                Instant.now());
    }
}
