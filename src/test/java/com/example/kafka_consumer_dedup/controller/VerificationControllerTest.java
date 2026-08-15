package com.example.kafka_consumer_dedup.controller;

import java.time.Instant;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import static org.mockito.Mockito.when;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.http.ResponseEntity;

import com.example.kafka_consumer_dedup.entity.NaiveState;
import com.example.kafka_consumer_dedup.entity.OrderedState;
import com.example.kafka_consumer_dedup.entity.SeedData;
import com.example.kafka_consumer_dedup.repository.NaiveStateRepository;
import com.example.kafka_consumer_dedup.repository.OrderedStateRepository;
import com.example.kafka_consumer_dedup.repository.SeedDataRepository;

@ExtendWith(MockitoExtension.class)
class VerificationControllerTest {

    @Mock private SeedDataRepository seedDataRepository;
    @Mock private NaiveStateRepository naiveStateRepository;
    @Mock private OrderedStateRepository orderedStateRepository;

    @InjectMocks
    private VerificationController controller;

    @Test
    void verify_returns200() {
        when(seedDataRepository.findAll()).thenReturn(List.of());
        when(naiveStateRepository.findAll()).thenReturn(List.of());
        when(orderedStateRepository.findAll()).thenReturn(List.of());

        ResponseEntity<Map<String, Object>> response = controller.verify();

        assertThat(response.getStatusCode().value()).isEqualTo(200);
    }

    @Test
    void verify_groundTruth_reflectsMaxVersion() {
        when(seedDataRepository.findAll()).thenReturn(List.of(
                seed("e1", 5L), seed("e2", 5L)));
        when(naiveStateRepository.findAll()).thenReturn(List.of());
        when(orderedStateRepository.findAll()).thenReturn(List.of());

        Map<String, Object> body = controller.verify().getBody();
        Map<?, ?> groundTruth = (Map<?, ?>) body.get("groundTruth");

        assertThat(groundTruth.get("totalEntities")).isEqualTo(2);
        assertThat(groundTruth.get("expectedVersion")).isEqualTo(5L);
    }

    @Test
    void verify_orderedState_shows0PercentCorruption_whenAllVersionsMatch() {
        when(seedDataRepository.findAll()).thenReturn(List.of(
                seed("e1", 5L), seed("e2", 5L)));
        when(naiveStateRepository.findAll()).thenReturn(List.of());
        when(orderedStateRepository.findAll()).thenReturn(List.of(
                orderedState("e1", 5L), orderedState("e2", 5L)));

        Map<String, Object> body = controller.verify().getBody();
        Map<?, ?> report = (Map<?, ?>) body.get("orderedState");

        assertThat(report.get("correct")).isEqualTo(2L);
        assertThat(report.get("corrupted")).isEqualTo(0L);
        assertThat(report.get("missing")).isEqualTo(0L);
        assertThat(report.get("corruptionRate")).isEqualTo("0.00%");
    }

    @Test
    void verify_naiveState_showsCorruption_whenVersionsMismatch() {
        when(seedDataRepository.findAll()).thenReturn(List.of(
                seed("e1", 5L), seed("e2", 5L)));
        when(naiveStateRepository.findAll()).thenReturn(List.of(
                naiveState("e1", 3L),   // corrupted — version 3 instead of 5
                naiveState("e2", 5L))); // correct
        when(orderedStateRepository.findAll()).thenReturn(List.of());

        Map<String, Object> body = controller.verify().getBody();
        Map<?, ?> report = (Map<?, ?>) body.get("naiveState");

        assertThat(report.get("correct")).isEqualTo(1L);
        assertThat(report.get("corrupted")).isEqualTo(1L);
        assertThat(report.get("missing")).isEqualTo(0L);
        assertThat(report.get("corruptionRate")).isEqualTo("50.00%");
    }

    @Test
    void verify_naiveState_countsMissingEntities() {
        when(seedDataRepository.findAll()).thenReturn(List.of(
                seed("e1", 5L), seed("e2", 5L), seed("e3", 5L)));
        when(naiveStateRepository.findAll()).thenReturn(List.of(
                naiveState("e1", 5L))); // e2 and e3 never processed
        when(orderedStateRepository.findAll()).thenReturn(List.of());

        Map<String, Object> body = controller.verify().getBody();
        Map<?, ?> report = (Map<?, ?>) body.get("naiveState");

        assertThat(report.get("correct")).isEqualTo(1L);
        assertThat(report.get("missing")).isEqualTo(2L);
        assertThat(report.get("corruptionRate")).isEqualTo("66.67%");
    }

    @Test
    void verify_reportsZeroTotals_whenAllTablesAreEmpty() {
        when(seedDataRepository.findAll()).thenReturn(List.of());
        when(naiveStateRepository.findAll()).thenReturn(List.of());
        when(orderedStateRepository.findAll()).thenReturn(List.of());

        Map<String, Object> body = controller.verify().getBody();
        Map<?, ?> naiveReport = (Map<?, ?>) body.get("naiveState");
        Map<?, ?> orderedReport = (Map<?, ?>) body.get("orderedState");

        assertThat(naiveReport.get("corruptionRate")).isEqualTo("0.00%");
        assertThat(orderedReport.get("corruptionRate")).isEqualTo("0.00%");
    }

    @Test
    void verify_corruptionRate_isCalculatedOverTotalSeedEntities_notActualProcessed() {
        // 4 seed entities, naive processed 2 (e1 correct, e2 corrupted), e3+e4 missing
        when(seedDataRepository.findAll()).thenReturn(List.of(
                seed("e1", 5L), seed("e2", 5L), seed("e3", 5L), seed("e4", 5L)));
        when(naiveStateRepository.findAll()).thenReturn(List.of(
                naiveState("e1", 5L),
                naiveState("e2", 2L)));
        when(orderedStateRepository.findAll()).thenReturn(List.of());

        Map<String, Object> body = controller.verify().getBody();
        Map<?, ?> report = (Map<?, ?>) body.get("naiveState");

        // correct=1, corrupted=1, missing=2, total=4 → rate = (1+2)/4 = 75%
        assertThat(report.get("correct")).isEqualTo(1L);
        assertThat(report.get("corrupted")).isEqualTo(1L);
        assertThat(report.get("missing")).isEqualTo(2L);
        assertThat(report.get("corruptionRate")).isEqualTo("75.00%");
    }

    // ── helpers ──────────────────────────────────────────────────────────────

    private SeedData seed(String entityId, long version) {
        return SeedData.builder().entityId(entityId).version(version)
                .eventType("UPDATE").data("{}").updatedAt(Instant.now()).build();
    }

    private NaiveState naiveState(String entityId, long version) {
        return NaiveState.builder().entityId(entityId).version(version)
                .eventType("UPDATE").data("{}").updatedAt(Instant.now()).build();
    }

    private OrderedState orderedState(String entityId, long version) {
        return OrderedState.builder().entityId(entityId).version(version)
                .eventType("UPDATE").data("{}").updatedAt(Instant.now()).build();
    }
}
