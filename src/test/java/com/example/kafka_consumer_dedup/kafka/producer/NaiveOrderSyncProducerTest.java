package com.example.kafka_consumer_dedup.kafka.producer;

import java.util.concurrent.CompletableFuture;

import static org.assertj.core.api.Assertions.assertThat;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isNull;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;

import com.example.kafka_consumer_dedup.model.OrderSyncMessage;

@ExtendWith(MockitoExtension.class)
class NaiveOrderSyncProducerTest {

    @Mock private KafkaTemplate<String, Object> kafkaTemplate;
    @InjectMocks private NaiveOrderSyncProducer producer;

    @Test
    void send_publishesWithNullKey() {
        when(kafkaTemplate.send(anyString(), isNull(), any()))
                .thenReturn(CompletableFuture.completedFuture(mock(SendResult.class)));

        OrderSyncMessage msg = message("entity-1", 2L);
        producer.send("naviceOrderSync", msg);

        // key must be null — that's what makes it naive (round-robin)
        verify(kafkaTemplate).send(eq("naviceOrderSync"), isNull(), eq(msg));
    }

    @Test
    void send_usesCorrectTopic() {
        when(kafkaTemplate.send(anyString(), isNull(), any()))
                .thenReturn(CompletableFuture.completedFuture(mock(SendResult.class)));

        producer.send("my-topic", message("e1", 1L));

        ArgumentCaptor<String> topicCaptor = ArgumentCaptor.forClass(String.class);
        verify(kafkaTemplate).send(topicCaptor.capture(), isNull(), any());
        assertThat(topicCaptor.getValue()).isEqualTo("my-topic");
    }

    private OrderSyncMessage message(String entityId, long version) {
        return OrderSyncMessage.builder()
                .entityId(entityId).version(version)
                .eventType("UPDATE").data("{}").build();
    }

    // Mockito helper — avoids static import conflict
    private <T> T mock(Class<T> clazz) {
        return org.mockito.Mockito.mock(clazz);
    }
}
