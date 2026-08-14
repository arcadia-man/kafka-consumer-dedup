package com.example.kafka_consumer_dedup.controller;

import java.util.LinkedHashMap;
import java.util.Map;

import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import com.example.kafka_consumer_dedup.repository.NaiveStateRepository;
import com.example.kafka_consumer_dedup.repository.OrderedStateRepository;
import com.example.kafka_consumer_dedup.repository.SeedDataRepository;
import com.example.kafka_consumer_dedup.service.OrderSyncPublishService;
import com.example.kafka_consumer_dedup.service.SeederService;

import lombok.RequiredArgsConstructor;

/**
 * Manual control endpoints for the demo pipeline.
 *
 *  POST /demo/seed      — inserts 5,000 records into seed_data (ground truth A)
 *  POST /demo/reset     — clears naive_state + ordered_state, resets version counter
 *  POST /demo/run       — publishes one version-increment round to both Kafka topics
 *  POST /demo/run-all   — publishes all remaining rounds back to back
 */
@RestController
@RequestMapping("/demo")
@RequiredArgsConstructor
public class DemoController {

    private final SeederService seederService;
    private final OrderSyncPublishService publishService;
    private final SeedDataRepository seedDataRepository;
    private final NaiveStateRepository naiveStateRepository;
    private final OrderedStateRepository orderedStateRepository;

    /** Step 1 — seed ground truth */
    @PostMapping("/seed")
    public ResponseEntity<Map<String, Object>> seed() {
        String result = seederService.seed();
        return ResponseEntity.ok(response("seed", result));
    }

    /**
     * Step 2 (optional between runs) — wipe consumer states and reset version
     * so you can demo the same 5,000 entities going through rounds again
     * without re-seeding (entityIds stay the same).
     */
    @PostMapping("/reset")
    public ResponseEntity<Map<String, Object>> reset() {
        naiveStateRepository.deleteAll();
        orderedStateRepository.deleteAll();

        // Reset seed_data versions back to 1 so rounds start fresh
        seedDataRepository.findAll().forEach(entity -> {
            entity.setVersion(1L);
            entity.setEventType("CREATE");
        });
        seedDataRepository.saveAll(seedDataRepository.findAll()
                .stream()
                .peek(e -> {
                    e.setVersion(1L);
                    e.setEventType("CREATE");
                })
                .toList());

        publishService.resetCompletedFlag();

        return ResponseEntity.ok(response("reset",
                "naive_state and ordered_state cleared. seed_data reset to version=1. Ready for a new run."));
    }

    /** Step 3a — run one round at a time (good for live demo, step by step) */
    @PostMapping("/run")
    public ResponseEntity<Map<String, Object>> runOneRound() {
        String result = publishService.runOneRound();
        return ResponseEntity.ok(response("run", result));
    }

    /** Step 3b — run all remaining rounds immediately */
    @PostMapping("/run-all")
    public ResponseEntity<Map<String, Object>> runAll() {
        String result = publishService.runAllRounds();
        return ResponseEntity.ok(response("run-all", result));
    }

    /**
     * Chaos mode — deliberately sends versions out-of-order to the naive topic.
     * Sends v5, v2, v4, v3 per entity to naive topic (last write = v3 → corrupt).
     * Sends v2, v3, v4, v5 per entity to ordered topic (guarded upsert → always v5).
     * This guarantees visible corruption in naive_state without relying on thread timing.
     */
    @PostMapping("/chaos")
    public ResponseEntity<Map<String, Object>> chaos() {
        String result = publishService.runChaos();
        return ResponseEntity.ok(response("chaos", result));
    }

    private Map<String, Object> response(String action, String message) {
        Map<String, Object> map = new LinkedHashMap<>();
        map.put("action", action);
        map.put("message", message);
        return map;
    }
}
