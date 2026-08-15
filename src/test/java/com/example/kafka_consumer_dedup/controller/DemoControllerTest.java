package com.example.kafka_consumer_dedup.controller;

import java.time.Instant;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.argThat;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.http.ResponseEntity;

import com.example.kafka_consumer_dedup.entity.SeedData;
import com.example.kafka_consumer_dedup.repository.NaiveStateRepository;
import com.example.kafka_consumer_dedup.repository.OrderedStateRepository;
import com.example.kafka_consumer_dedup.repository.SeedDataRepository;
import com.example.kafka_consumer_dedup.service.OrderSyncPublishService;
import com.example.kafka_consumer_dedup.service.SeederService;

@ExtendWith(MockitoExtension.class)
class DemoControllerTest {

    @Mock private SeederService seederService;
    @Mock private OrderSyncPublishService publishService;
    @Mock private SeedDataRepository seedDataRepository;
    @Mock private NaiveStateRepository naiveStateRepository;
    @Mock private OrderedStateRepository orderedStateRepository;

    @InjectMocks
    private DemoController controller;

    @Test
    void seed_delegatesToSeederService_andReturns200() {
        when(seederService.seed()).thenReturn("Done. 5000 records seeded at version=1.");

        ResponseEntity<Map<String, Object>> response = controller.seed();

        assertThat(response.getStatusCode().value()).isEqualTo(200);
        assertThat(response.getBody()).containsEntry("action", "seed");
        verify(seederService).seed();
    }

    @Test
    void run_delegatesToPublishService_andReturns200() {
        when(publishService.runOneRound()).thenReturn("Round complete: published version 2 for 5000 entities.");

        ResponseEntity<Map<String, Object>> response = controller.runOneRound();

        assertThat(response.getStatusCode().value()).isEqualTo(200);
        assertThat(response.getBody()).containsEntry("action", "run");
        verify(publishService).runOneRound();
    }

    @Test
    void runAll_delegatesToPublishService_andReturns200() {
        when(publishService.runAllRounds()).thenReturn("Running 4 rounds...");

        ResponseEntity<Map<String, Object>> response = controller.runAll();

        assertThat(response.getStatusCode().value()).isEqualTo(200);
        assertThat(response.getBody()).containsEntry("action", "run-all");
        verify(publishService).runAllRounds();
    }

    @Test
    void reset_clearsNaiveAndOrderedState_andResetsSeedVersionTo1() {
        SeedData entity = SeedData.builder()
                .entityId("e1").version(5L).eventType("UPDATE")
                .data("{}").updatedAt(Instant.now()).build();
        when(seedDataRepository.findAll()).thenReturn(List.of(entity));
        when(seedDataRepository.saveAll(anyList())).thenAnswer(inv -> inv.getArgument(0));

        ResponseEntity<Map<String, Object>> response = controller.reset();

        assertThat(response.getStatusCode().value()).isEqualTo(200);
        verify(naiveStateRepository).deleteAll();
        verify(orderedStateRepository).deleteAll();
        verify(publishService).resetCompletedFlag();
        assertThat(response.getBody().get("action")).isEqualTo("reset");
    }

    @Test
    void reset_resetsSeedDataVersionBackTo1() {
        SeedData entity = SeedData.builder()
                .entityId("e1").version(5L).eventType("UPDATE")
                .data("{}").updatedAt(Instant.now()).build();
        when(seedDataRepository.findAll()).thenReturn(List.of(entity));
        when(seedDataRepository.saveAll(anyList())).thenAnswer(inv -> inv.getArgument(0));

        controller.reset();

        // Verify saved entities have version=1 and eventType=CREATE
        verify(seedDataRepository, atLeastOnce()).saveAll(argThat(list -> {
            List<SeedData> saved = (List<SeedData>) list;
            return saved.stream().allMatch(e -> e.getVersion() == 1L && "CREATE".equals(e.getEventType()));
        }));
    }

    @Test
    void seed_responseContainsMessageFromService() {
        String serviceMsg = "Done. 5000 records seeded at version=1.";
        when(seederService.seed()).thenReturn(serviceMsg);

        Map<String, Object> body = controller.seed().getBody();

        assertThat(body.get("message")).isEqualTo(serviceMsg);
    }
}
