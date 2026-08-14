package com.example.kafka_consumer_dedup.controller;

import com.example.kafka_consumer_dedup.entity.NaiveState;
import com.example.kafka_consumer_dedup.entity.OrderedState;
import com.example.kafka_consumer_dedup.entity.SeedData;
import com.example.kafka_consumer_dedup.repository.NaiveStateRepository;
import com.example.kafka_consumer_dedup.repository.OrderedStateRepository;
import com.example.kafka_consumer_dedup.repository.SeedDataRepository;
import lombok.RequiredArgsConstructor;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * Hit GET /verify after all job rounds complete to see the comparison report.
 *
 * Response shape:
 * {
 *   "groundTruth"  : { "totalEntities": 5000, "expectedVersion": 5 },
 *   "naiveState"   : { "totalProcessed": ..., "correct": ..., "corrupted": ..., "missing": ..., "corruptionRate": "x.xx%" },
 *   "orderedState" : { "totalProcessed": ..., "correct": ..., "corrupted": ..., "missing": ..., "corruptionRate": "x.xx%" }
 * }
 */
@RestController
@RequestMapping("/verify")
@RequiredArgsConstructor
public class VerificationController {

    private final SeedDataRepository seedDataRepository;
    private final NaiveStateRepository naiveStateRepository;
    private final OrderedStateRepository orderedStateRepository;

    @GetMapping
    public ResponseEntity<Map<String, Object>> verify() {

        // Ground truth A
        List<SeedData> seedList = seedDataRepository.findAll();
        Map<String, Long> seedMap = seedList.stream()
                .collect(Collectors.toMap(SeedData::getEntityId, SeedData::getVersion));

        long expectedVersion = seedMap.values().stream().mapToLong(Long::longValue).max().orElse(0);

        // State B — Naive
        Map<String, Long> naiveMap = naiveStateRepository.findAll().stream()
                .collect(Collectors.toMap(NaiveState::getEntityId, NaiveState::getVersion));

        // State C — Ordered
        Map<String, Long> orderedMap = orderedStateRepository.findAll().stream()
                .collect(Collectors.toMap(OrderedState::getEntityId, OrderedState::getVersion));

        Map<String, Object> response = new LinkedHashMap<>();
        response.put("groundTruth", Map.of(
                "totalEntities", seedMap.size(),
                "expectedVersion", expectedVersion));
        response.put("naiveState", buildReport(seedMap, naiveMap));
        response.put("orderedState", buildReport(seedMap, orderedMap));

        return ResponseEntity.ok(response);
    }

    private Map<String, Object> buildReport(Map<String, Long> expected,
                                            Map<String, Long> actual) {
        long correct = 0;
        long corrupted = 0;
        long missing = 0;

        for (Map.Entry<String, Long> entry : expected.entrySet()) {
            String entityId = entry.getKey();
            long expectedVer = entry.getValue();

            if (!actual.containsKey(entityId)) {
                missing++;
            } else if (actual.get(entityId).equals(expectedVer)) {
                correct++;
            } else {
                corrupted++;
            }
        }

        long total = correct + corrupted + missing;
        double corruptionRate = total == 0 ? 0.0 : (corrupted + missing) * 100.0 / total;

        Map<String, Object> report = new LinkedHashMap<>();
        report.put("totalProcessed", actual.size());
        report.put("correct", correct);
        report.put("corrupted", corrupted);
        report.put("missing", missing);
        report.put("corruptionRate", String.format("%.2f%%", corruptionRate));
        return report;
    }
}
