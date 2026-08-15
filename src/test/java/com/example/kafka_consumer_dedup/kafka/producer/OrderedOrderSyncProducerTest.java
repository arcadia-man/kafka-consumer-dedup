package com.example.kafka_consumer_dedup.kafka.producer;

import java.util.concurrent.CompletableFuture;

import static org.assertj.core.api.Assertions.assertThat;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;

import com.example.kafka_consumer_dedup.model.OrderSyncMessage;

@ExtendWith(MockitoExtension.class)
class OrderedOrderSyncProducerTest {

    @Mock private KafkaTemplate<String, Object> kafkaTemplate;
    @InjectMocks private OrderedOrderSyncProducer producer;

    @Test
    void send_usesEntityIdAsKey() {
        when(kafkaTemplate.send(anyString(), anyString(), any()))
                .thenReturn(CompletableFuture.completedFuture(mock(SendResult.class)));

        OrderSyncMessage msg = message("entity-99", 3L);
        producer.send("orderSync", msg);

        // key must be entityId — that's what ensures partition routing
        verify(kafkaTemplate).send(eq("orderSync"), eq("entity-99"), eq(msg));
    }

    @Test
    void send_keyMatchesEntityId_forDifferentEntities() {
        when(kafkaTemplate.send(anyString(), anyString(), any()))
                .thenReturn(CompletableFuture.completedFuture(mock(SendResult.class)));

        producer.send("orderSync", message("abc-123", 1L));

        ArgumentCaptor<String> keyCaptor = ArgumentCaptor.forClass(String.class);
        verify(kafkaTemplate).send(anyString(), keyCaptor.capture(), any());
        assertThat(keyCaptor.getValue()).isEqualTo("abc-123");
    }

    @Test
    void send_naiveProducerKey_isNull_butOrderedKey_isNotNull() {
        // Contrast test: ordered always has a non-null key
        when(kafkaTemplate.send(anyString(), anyString(), any()))
                .thenReturn(CompletableFuture.completedFuture(mock(SendResult.class)));

        producer.send("orderSync", message("some-entity", 2L));

        ArgumentCaptor<String> keyCaptor = ArgumentCaptor.forClass(String.class);
        verify(kafkaTemplate).send(anyString(), keyCaptor.capture(), any());
        assertThat(keyCaptor.getValue()).isNotNull().isNotBlank();
    }

    private OrderSyncMessage message(String entityId, long version) {
        return OrderSyncMessage.builder()
                .entityId(entityId).version(version)
                .eventType("UPDATE").data("{}").build();
    }

    private <T> T mock(Class<T> clazz) {
        return org.mockito.Mockito.mock(clazz);
    }
}
