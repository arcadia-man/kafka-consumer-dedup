package com.example.kafka_consumer_dedup.service;

import com.example.kafka_consumer_dedup.entity.SeedData;
import com.example.kafka_consumer_dedup.repository.SeedDataRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

/**
 * Manages the ground-truth dataset (seed_data / State A).
 * Called manually via POST /demo/seed — no auto-run on startup.
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class SeederService {

    private static final int SEED_COUNT = 5_000;

    private final SeedDataRepository seedDataRepository;

    /**
     * Inserts 5,000 records at version=1 into seed_data.
     * Idempotent: skips if already seeded.
     */
    public String seed() {
        long existing = seedDataRepository.count();
        if (existing >= SEED_COUNT) {
            String msg = String.format("seed_data already contains %d records — skipping. " +
                    "Run POST /demo/reset first to re-seed.", existing);
            log.info("[SEEDER] {}", msg);
            return msg;
        }

        log.info("[SEEDER] Seeding {} records into seed_data ...", SEED_COUNT);
        List<SeedData> batch = new ArrayList<>(SEED_COUNT);
        for (int i = 0; i < SEED_COUNT; i++) {
            batch.add(SeedData.builder()
                    .entityId(UUID.randomUUID().toString())
                    .version(1L)
                    .eventType("CREATE")
                    .data("{\"index\":" + i + "}")
                    .updatedAt(Instant.now())
                    .build());
        }
        seedDataRepository.saveAll(batch);

        String msg = String.format("Done. %d records seeded at version=1.", SEED_COUNT);
        log.info("[SEEDER] {}", msg);
        return msg;
    }
}
