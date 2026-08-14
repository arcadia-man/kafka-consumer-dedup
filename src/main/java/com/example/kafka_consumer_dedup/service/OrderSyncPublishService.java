package com.example.kafka_consumer_dedup.service;

import com.example.kafka_consumer_dedup.entity.SeedData;
import com.example.kafka_consumer_dedup.kafka.producer.OrderSyncProducerStrategy;
import com.example.kafka_consumer_dedup.model.OrderSyncMessage;
import com.example.kafka_consumer_dedup.repository.SeedDataRepository;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.time.Instant;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Core publish logic extracted from the job so it can be triggered
 * either by the scheduler OR manually via the /demo REST endpoints.
 */
@Slf4j
@Service
public class OrderSyncPublishService {

    public static final long MAX_VERSION = 5L;

    private final SeedDataRepository seedDataRepository;
    private final OrderSyncProducerStrategy naiveProducer;
    private final OrderSyncProducerStrategy orderedProducer;

    @Value("${kafka.connections.order-sync.topics.navice-order-sync.name}")
    private String naiveTopic;

    @Value("${kafka.connections.order-sync.topics.order-sync.name}")
    private String orderedTopic;

    private final AtomicBoolean completed = new AtomicBoolean(false);

    public OrderSyncPublishService(
            SeedDataRepository seedDataRepository,
            @Qualifier("naiveOrderSyncProducer") OrderSyncProducerStrategy naiveProducer,
            @Qualifier("orderedOrderSyncProducer") OrderSyncProducerStrategy orderedProducer) {
        this.seedDataRepository = seedDataRepository;
        this.naiveProducer = naiveProducer;
        this.orderedProducer = orderedProducer;
    }

    /**
     * Runs a single version-increment round.
     * @return summary string describing what happened
     */
    public String runOneRound() {
        List<SeedData> entities = seedDataRepository.findAll();
        if (entities.isEmpty()) {
            return "seed_data is empty — run POST /demo/seed first.";
        }

        long currentVersion = entities.get(0).getVersion();
        if (currentVersion >= MAX_VERSION) {
            completed.set(true);
            return String.format("Already at max version %d. No more rounds to run. " +
                    "Run POST /demo/reset to start over.", MAX_VERSION);
        }

        long nextVersion = currentVersion + 1;
        log.info("[PUBLISH] Round: version {} → {} for {} entities", currentVersion, nextVersion, entities.size());

        for (SeedData entity : entities) {
            OrderSyncMessage message = OrderSyncMessage.builder()
                    .entityId(entity.getEntityId())
                    .version(nextVersion)
                    .eventType("UPDATE")
                    .data(entity.getData())
                    .build();

            naiveProducer.send(naiveTopic, message);
            orderedProducer.send(orderedTopic, message);

            entity.setVersion(nextVersion);
            entity.setEventType("UPDATE");
            entity.setUpdatedAt(Instant.now());
        }

        seedDataRepository.saveAll(entities);

        String summary = String.format("Round complete: published version %d for %d entities. " +
                        "Rounds remaining: %d",
                nextVersion, entities.size(), MAX_VERSION - nextVersion);
        log.info("[PUBLISH] {}", summary);

        if (nextVersion >= MAX_VERSION) {
            completed.set(true);
        }
        return summary;
    }

    /**
     * Runs all remaining rounds back to back.
     * @return summary of all rounds executed
     */
    public String runAllRounds() {
        StringBuilder sb = new StringBuilder();
        List<SeedData> entities = seedDataRepository.findAll();
        if (entities.isEmpty()) {
            return "seed_data is empty — run POST /demo/seed first.";
        }

        long currentVersion = entities.get(0).getVersion();
        if (currentVersion >= MAX_VERSION) {
            return String.format("Already at max version %d. Run POST /demo/reset to start over.", MAX_VERSION);
        }

        long roundsToRun = MAX_VERSION - currentVersion;
        sb.append(String.format("Running %d remaining round(s)...\n", roundsToRun));
        for (int i = 0; i < roundsToRun; i++) {
            sb.append("  ").append(runOneRound()).append("\n");
        }
        return sb.toString().trim();
    }

    /** Resets the completed flag so rounds can be re-run after a reset. */
    public void resetCompletedFlag() {
        completed.set(false);
    }

    public boolean isCompleted() {
        return completed.get();
    }
}
