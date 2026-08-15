package com.example.kafka_consumer_dedup.service;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import static org.mockito.ArgumentMatchers.anyList;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import org.mockito.junit.jupiter.MockitoExtension;

import com.example.kafka_consumer_dedup.entity.SeedData;
import com.example.kafka_consumer_dedup.repository.SeedDataRepository;

@ExtendWith(MockitoExtension.class)
class SeederServiceTest {

    @Mock
    private SeedDataRepository seedDataRepository;

    @InjectMocks
    private SeederService seederService;

    @Test
    void seed_insertsExactly5000Records_whenTableIsEmpty() {
        when(seedDataRepository.count()).thenReturn(0L);
        when(seedDataRepository.saveAll(anyList())).thenAnswer(inv -> inv.getArgument(0));

        String result = seederService.seed();

        ArgumentCaptor<List<SeedData>> captor = ArgumentCaptor.forClass(List.class);
        verify(seedDataRepository).saveAll(captor.capture());
        assertThat(captor.getValue()).hasSize(5_000);
        assertThat(result).contains("5000");
    }

    @Test
    void seed_allRecordsHaveVersion1AndCreateEventType() {
        when(seedDataRepository.count()).thenReturn(0L);
        when(seedDataRepository.saveAll(anyList())).thenAnswer(inv -> inv.getArgument(0));

        seederService.seed();

        ArgumentCaptor<List<SeedData>> captor = ArgumentCaptor.forClass(List.class);
        verify(seedDataRepository).saveAll(captor.capture());

        List<SeedData> saved = captor.getValue();
        assertThat(saved).allSatisfy(record -> {
            assertThat(record.getVersion()).isEqualTo(1L);
            assertThat(record.getEventType()).isEqualTo("CREATE");
            assertThat(record.getEntityId()).isNotBlank();
            assertThat(record.getData()).isNotBlank();
            assertThat(record.getUpdatedAt()).isNotNull();
        });
    }

    @Test
    void seed_allEntityIdsAreUnique() {
        when(seedDataRepository.count()).thenReturn(0L);
        when(seedDataRepository.saveAll(anyList())).thenAnswer(inv -> inv.getArgument(0));

        seederService.seed();

        ArgumentCaptor<List<SeedData>> captor = ArgumentCaptor.forClass(List.class);
        verify(seedDataRepository).saveAll(captor.capture());

        List<String> ids = captor.getValue().stream()
                .map(SeedData::getEntityId)
                .toList();
        assertThat(ids).doesNotHaveDuplicates();
    }

    @Test
    void seed_skipsInsert_whenTableAlreadyHas5000Records() {
        when(seedDataRepository.count()).thenReturn(5_000L);

        String result = seederService.seed();

        verify(seedDataRepository, never()).saveAll(anyList());
        assertThat(result).contains("skipping");
    }

    @Test
    void seed_skipsInsert_whenTableHasMoreThan5000Records() {
        when(seedDataRepository.count()).thenReturn(6_000L);

        seederService.seed();

        verify(seedDataRepository, never()).saveAll(anyList());
    }

    @Test
    void seed_insertsRecords_whenTableIsPartiallyFilled() {
        when(seedDataRepository.count()).thenReturn(100L);
        when(seedDataRepository.saveAll(anyList())).thenAnswer(inv -> inv.getArgument(0));

        seederService.seed();

        verify(seedDataRepository).saveAll(anyList());
    }
}
