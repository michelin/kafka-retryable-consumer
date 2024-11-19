package com.michelin.kafka.test.unit;

import com.michelin.kafka.avro.GenericErrorModel;
import com.michelin.kafka.configuration.DeadLetterProducerConfiguration;
import com.michelin.kafka.error.DeadLetterProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.util.Properties;
import java.util.concurrent.CompletableFuture;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

public class DeadLetterProducerTest {

    @Mock
    private static Producer<String, GenericErrorModel> mockKafkaProducer;

    private DeadLetterProducer deadLetterProducer;

    @BeforeEach
    public void setUp() {
        MockitoAnnotations.openMocks(this);
        String dlTopic = "dlTopic";
        deadLetterProducer = new DeadLetterProducer(mockKafkaProducer, dlTopic);
    }


    @Test
    public void shouldSendMessageSuccessfully() throws Exception {
        // Given
        String key = "key";
        GenericErrorModel errorObject = new GenericErrorModel();
        RecordMetadata metadata = new RecordMetadata(new TopicPartition("dlTopic", 1), 1, 1, 1, Long.valueOf(1), 1, 1);
        CompletableFuture<RecordMetadata> future = CompletableFuture.completedFuture(metadata);
        when(mockKafkaProducer.send(any(ProducerRecord.class), any())).thenReturn(future);

        // When
        deadLetterProducer.send(key, errorObject);

        // Then
        verify(mockKafkaProducer, times(1)).send(any(ProducerRecord.class), any());
    }

    @Test
    public void shouldHandleExceptionWhenSendingMessage() throws Exception {
        // Given
        String key = "key";
        GenericErrorModel errorObject = new GenericErrorModel();
        CompletableFuture<RecordMetadata> future = new CompletableFuture<>();
        future.completeExceptionally(new RuntimeException("Test exception"));
        when(mockKafkaProducer.send(any(ProducerRecord.class), any())).thenReturn(future);

        // When
        deadLetterProducer.send(key, errorObject);

        // Then
        verify(mockKafkaProducer, times(1)).send(any(ProducerRecord.class), any());
    }
}
