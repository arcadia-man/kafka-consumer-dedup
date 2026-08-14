package com.example.kafka_consumer_dedup.kafka.consumer;

import java.time.Instant;

import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Component;

import com.example.kafka_consumer_dedup.model.OrderSyncMessage;
import com.example.kafka_consumer_dedup.repository.NaiveStateRepository;

import lombok.extern.slf4j.Slf4j;
import tools.jackson.databind.ObjectMapper;

/**
 * Naive consumer — no key, no ordering guarantee.
 *
 * The 50ms sleep simulates real processing latency (e.g. slow DB write,
 * downstream HTTP call). Under back pressure, messages for the same entity
 * can be inflight on different threads simultaneously. Because there is no
 * key, Kafka distributes messages round-robin across the 3 partitions, so
 * version 2 and version 3 of the SAME entity can land on different partitions
 * and be processed by different threads at the same time.
 *
 * Thread A picks up version 3, sleeps 50ms.
 * Thread B picks up version 2 (delayed), sleeps 50ms.
 * Thread B finishes first → writes version 2.
 * Thread A finishes → overwrites with version 3.  (correct by accident)
 * OR
 * Thread A picks up version 2, sleeps 50ms.
 * Thread B picks up version 3, sleeps 50ms.
 * Thread B writes version 3 first.
 * Thread A writes version 2 last → CORRUPTION: version 2 overwrites version 3.
 *
 * The blind upsert has no version guard so whichever thread writes last wins.
 */
@Slf4j
@Component
public class NaiveOrderSyncConsumer extends AbstractOrderSyncConsumer {

    private static final long PROCESSING_DELAY_MS = 50;

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
            @Header(name = KafkaHeaders.RECEIVED_KEY, required = false) String key) {
        consumeRaw(groupId, rawJson, key, partition, offset);
    }

    @Override
    protected void process(String consumerName, OrderSyncMessage payload,
                           String key, int partition, long offset) {
        log.info("[NAIVE] Partition: {} | Offset: {} | EntityId: {} | Version: {} | Thread: {}",
                partition, offset, payload.getEntityId(), payload.getVersion(),
                Thread.currentThread().getName());

        // Simulate back pressure / processing latency.
        // This is the critical window where another thread can process a
        // different version of the same entity and write to DB first.
        try {
            Thread.sleep(PROCESSING_DELAY_MS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }

        // Blind upsert — no version check. Last thread to finish wins.
        // Under back pressure, this is NOT guaranteed to be the highest version.
        naiveStateRepository.blindUpsert(
                payload.getEntityId(),
                payload.getVersion(),
                payload.getEventType(),
                payload.getData() != null ? payload.getData().toString() : null,
                Instant.now());
    }
}
