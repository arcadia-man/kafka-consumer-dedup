package com.example.kafka_consumer_dedup.service;

import java.time.Instant;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import org.mockito.Mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.test.util.ReflectionTestUtils;

import com.example.kafka_consumer_dedup.entity.SeedData;
import com.example.kafka_consumer_dedup.kafka.producer.OrderSyncProducerStrategy;
import com.example.kafka_consumer_dedup.model.OrderSyncMessage;
import com.example.kafka_consumer_dedup.repository.SeedDataRepository;

@ExtendWith(MockitoExtension.class)
class OrderSyncPublishServiceTest {

    @Mock private SeedDataRepository seedDataRepository;
    @Mock private OrderSyncProducerStrategy naiveProducer;
    @Mock private OrderSyncProducerStrategy orderedProducer;

    private OrderSyncPublishService service;

    @BeforeEach
    void setUp() {
        service = new OrderSyncPublishService(seedDataRepository, naiveProducer, orderedProducer);
        ReflectionTestUtils.setField(service, "naiveTopic", "naviceOrderSync");
        ReflectionTestUtils.setField(service, "orderedTopic", "orderSync");
    }

    // ── runOneRound ──────────────────────────────────────────────────────────

    @Test
    void runOneRound_returnsEmptyMessage_whenSeedDataIsEmpty() {
        when(seedDataRepository.findAll()).thenReturn(List.of());

        String result = service.runOneRound();

        assertThat(result).contains("empty");
        verifyNoInteractions(naiveProducer, orderedProducer);
    }

    @Test
    void runOneRound_incrementsVersionBy1() {
        when(seedDataRepository.findAll()).thenReturn(List.of(seedEntity("entity-1", 1L)));
        when(seedDataRepository.saveAll(anyList())).thenAnswer(inv -> inv.getArgument(0));

        service.runOneRound();

        ArgumentCaptor<OrderSyncMessage> naiveCaptor = ArgumentCaptor.forClass(OrderSyncMessage.class);
        verify(naiveProducer).send(eq("naviceOrderSync"), naiveCaptor.capture());
        assertThat(naiveCaptor.getValue().getVersion()).isEqualTo(2L);
    }

    @Test
    void runOneRound_sendsToBoththTopics_forEachEntity() {
        List<SeedData> entities = List.of(
                seedEntity("e1", 1L),
                seedEntity("e2", 1L),
                seedEntity("e3", 1L));
        when(seedDataRepository.findAll()).thenReturn(entities);
        when(seedDataRepository.saveAll(anyList())).thenAnswer(inv -> inv.getArgument(0));

        service.runOneRound();

        verify(naiveProducer, times(3)).send(eq("naviceOrderSync"), any());
        verify(orderedProducer, times(3)).send(eq("orderSync"), any());
    }

    @Test
    void runOneRound_messagesCarryCorrectEntityId() {
        when(seedDataRepository.findAll()).thenReturn(List.of(seedEntity("entity-abc", 2L)));
        when(seedDataRepository.saveAll(anyList())).thenAnswer(inv -> inv.getArgument(0));

        service.runOneRound();

        ArgumentCaptor<OrderSyncMessage> captor = ArgumentCaptor.forClass(OrderSyncMessage.class);
        verify(orderedProducer).send(anyString(), captor.capture());
        assertThat(captor.getValue().getEntityId()).isEqualTo("entity-abc");
        assertThat(captor.getValue().getVersion()).isEqualTo(3L);
        assertThat(captor.getValue().getEventType()).isEqualTo("UPDATE");
    }

    @Test
    void runOneRound_updatesSeedDataVersionInDb() {
        when(seedDataRepository.findAll()).thenReturn(List.of(seedEntity("e1", 1L)));
        when(seedDataRepository.saveAll(anyList())).thenAnswer(inv -> inv.getArgument(0));

        service.runOneRound();

        ArgumentCaptor<List<SeedData>> captor = ArgumentCaptor.forClass(List.class);
        verify(seedDataRepository).saveAll(captor.capture());
        assertThat(captor.getValue().get(0).getVersion()).isEqualTo(2L);
    }

    @Test
    void runOneRound_setsCompletedTrue_whenMaxVersionReached() {
        when(seedDataRepository.findAll()).thenReturn(List.of(seedEntity("e1", 4L)));
        when(seedDataRepository.saveAll(anyList())).thenAnswer(inv -> inv.getArgument(0));

        service.runOneRound();

        assertThat(service.isCompleted()).isTrue();
    }

    @Test
    void runOneRound_returnsAlreadyMaxMessage_whenAtMaxVersion() {
        when(seedDataRepository.findAll()).thenReturn(List.of(seedEntity("e1", 5L)));

        String result = service.runOneRound();

        assertThat(result).contains("max version");
        verifyNoInteractions(naiveProducer, orderedProducer);
    }

    // ── runAllRounds ─────────────────────────────────────────────────────────

    @Test
    void runAllRounds_runsExactly4Rounds_fromVersion1() {
        // runAllRounds calls findAll once at the start to determine rounds,
        // then each runOneRound call also calls findAll internally.
        // We stub findAll to return increasing versions to simulate state change.
        when(seedDataRepository.findAll())
                .thenReturn(List.of(seedEntity("e1", 1L)))  // initial check in runAllRounds
                .thenReturn(List.of(seedEntity("e1", 1L)))  // runOneRound call 1
                .thenReturn(List.of(seedEntity("e1", 2L)))  // runOneRound call 2
                .thenReturn(List.of(seedEntity("e1", 3L)))  // runOneRound call 3
                .thenReturn(List.of(seedEntity("e1", 4L))); // runOneRound call 4
        when(seedDataRepository.saveAll(anyList())).thenAnswer(inv -> inv.getArgument(0));

        service.runAllRounds();

        // 4 rounds × 1 entity × 2 topics = 8 sends total
        verify(naiveProducer, times(4)).send(anyString(), any());
        verify(orderedProducer, times(4)).send(anyString(), any());
    }

    @Test
    void runAllRounds_returnsMaxMessage_whenAlreadyAtMaxVersion() {
        when(seedDataRepository.findAll()).thenReturn(List.of(seedEntity("e1", 5L)));

        String result = service.runAllRounds();

        assertThat(result).contains("max version");
    }

    // ── resetCompletedFlag ───────────────────────────────────────────────────

    @Test
    void resetCompletedFlag_allowsRunAfterCompletion() {
        when(seedDataRepository.findAll()).thenReturn(List.of(seedEntity("e1", 4L)));
        when(seedDataRepository.saveAll(anyList())).thenAnswer(inv -> inv.getArgument(0));

        service.runOneRound(); // reaches MAX_VERSION, sets completed=true
        assertThat(service.isCompleted()).isTrue();

        service.resetCompletedFlag();
        assertThat(service.isCompleted()).isFalse();
    }

    // ── helpers ──────────────────────────────────────────────────────────────

    private SeedData seedEntity(String entityId, long version) {
        return SeedData.builder()
                .entityId(entityId)
                .version(version)
                .eventType("CREATE")
                .data("{\"index\":0}")
                .updatedAt(Instant.now())
                .build();
    }
}
