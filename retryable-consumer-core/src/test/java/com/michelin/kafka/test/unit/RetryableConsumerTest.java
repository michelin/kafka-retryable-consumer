package com.michelin.kafka.test.unit;

import com.michelin.kafka.RecordProcessor;
import com.michelin.kafka.RetryableConsumer;
import com.michelin.kafka.RetryableConsumerRebalanceListener;
import com.michelin.kafka.configuration.KafkaRetryableConfiguration;
import com.michelin.kafka.configuration.RetryableConsumerConfiguration;
import com.michelin.kafka.error.RetryableConsumerErrorHandler;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.RecordDeserializationException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.util.Collections;

import static org.mockito.Mockito.*;

class RetryableConsumerTest {
    @Mock
    private KafkaConsumer<String, String> kafkaConsumer;
    @Mock
    private KafkaRetryableConfiguration retryableConfiguration;
    @Mock
    private RetryableConsumerErrorHandler<String, String> errorHandler;
    @Mock
    private RetryableConsumer<String, String> retryableConsumer;
    @Mock
    private RetryableConsumerRebalanceListener rebalanceListener;
    @Mock
    RetryableConsumerConfiguration consumerConfiguration;
    @Mock
    RecordProcessor<ConsumerRecord<String, String>, Exception> recordProcessorNoError;
    @Mock
    RecordProcessor<ConsumerRecord<String, String>, Exception> recordProcessorDeserializationError;

    private final String topic = "topic";
    private final int record1_partition = 1;
    private final long record1_offset = 1L;
    private final TopicPartition record1_topicPartition = new TopicPartition(topic, record1_partition);

    private final int record2_partition = 1;
    private final long record2_offset = 2L;
    private final TopicPartition record2_topicPartition = new TopicPartition(topic, record2_partition);

    @BeforeEach
    void setUp() throws Exception {
        MockitoAnnotations.openMocks(this);

        when(errorHandler.isExceptionRetryable(CustomRetryableException.class)).thenReturn(true);
        when(errorHandler.isExceptionRetryable(CustomNotRetryableException.class)).thenReturn(false);

        when(retryableConfiguration.getName()).thenReturn("Test Consumer");
        when(retryableConfiguration.getConsumer()).thenReturn(consumerConfiguration);
        when(consumerConfiguration.getTopics()).thenReturn(Collections.singletonList(topic));

        doNothing().when(recordProcessorNoError).processRecord(any());
        doThrow(
                new RecordDeserializationException(
                        record1_topicPartition,
                        record1_offset,
                        "Fake DeSer Error",
                        new Exception()
                )
        ).when(recordProcessorDeserializationError).processRecord(any());

        retryableConsumer = new RetryableConsumer<>(
                retryableConfiguration,
                kafkaConsumer,
                errorHandler,
                rebalanceListener
        );
    }

    @AfterEach
    public void teardown() {
        if(retryableConsumer != null) {
            retryableConsumer.close();
        }
    }

    @Test
    void listenAsync_shouldProcessRecords() throws Exception {
        ConsumerRecord<String, String> record =
                new ConsumerRecord<>(topic, record1_partition, record1_offset, "key", "value");

        when(kafkaConsumer.poll(any()))
                .thenReturn(
                        new ConsumerRecords<>(
                                Collections.singletonMap(
                                        record1_topicPartition,
                                        Collections.singletonList(record)
                                )
                        )
                ).thenReturn(new ConsumerRecords<>(Collections.emptyMap()));

        retryableConsumer.listenAsync(r -> recordProcessorNoError.processRecord(r));
        verify(kafkaConsumer, timeout(5000).atLeast(1)).poll(any());
        verify(recordProcessorNoError, timeout(5000).times(1)).processRecord(any());

        Assertions.assertEquals(
                retryableConsumer.getCurrentOffset(record1_topicPartition).offset(), record1_offset + 1
        );
    }

    @Test
    void listenAsync_shouldHandleDeserializationException() throws Exception {
        ConsumerRecord<String, String> record =
                new ConsumerRecord<>(topic, record1_partition, record1_offset, "key", "value");

        when(kafkaConsumer.poll(any()))
                .thenReturn( //First poll return one record
                        new ConsumerRecords<>(
                                Collections.singletonMap(record1_topicPartition, Collections.singletonList(record))
                        )
                )
                .thenReturn(new ConsumerRecords<>(Collections.emptyMap())); //all subsequent calls return empty record list

        doThrow(
                new RecordDeserializationException(
                        record1_topicPartition,
                        record1_offset,
                        "Fake DeSer Error",
                        new Exception()
                )
        ).when(recordProcessorNoError).processRecord(any());

        retryableConsumer.listenAsync(r -> recordProcessorNoError.processRecord(r));
        verify(kafkaConsumer, timeout(5000).atLeast(2)).poll(any());

        //Check the record is sent to DLQ
        verify(errorHandler, timeout(5000).times(1)).handleConsumerDeserializationError(any());

        //Check we have correctly skipped the record
        Assertions.assertNotNull(retryableConsumer.getCurrentOffset(record1_topicPartition));
        Assertions.assertEquals(
                retryableConsumer.getCurrentOffset(record1_topicPartition).offset(), record1_offset + 1
        );
    }

    @Test
    void listenAsync_shouldHandleNotRetryableError() throws Exception {
        ConsumerRecord<String, String> record1 =
                new ConsumerRecord<>(topic, record1_partition, record1_offset, "key1", "value1");

        ConsumerRecord<String, String> record2 =
                new ConsumerRecord<>(topic, record2_partition, record2_offset, "key2", "value2");

        when(kafkaConsumer.poll(any()))
                .thenReturn( //First poll return one record
                        new ConsumerRecords<>(
                                Collections.singletonMap(record1_topicPartition, Collections.singletonList(record1))
                        )
                ).thenReturn(
                        new ConsumerRecords<>(
                                Collections.singletonMap(record2_topicPartition, Collections.singletonList(record2))
                        )
                ).thenReturn(new ConsumerRecords<>(Collections.emptyMap())); //all subsequent calls return empty record list

        doThrow(new CustomNotRetryableException()).when(recordProcessorNoError).processRecord(record2);

        retryableConsumer.listenAsync(r -> recordProcessorNoError.processRecord(r));
        verify(kafkaConsumer, timeout(5000).atLeastOnce()).poll(any());
        verify(errorHandler, timeout(5000).times(1)).handleError(any(), any());

        //Not retryable error : Check we have correctly skipped the record
        Assertions.assertNotNull(retryableConsumer.getCurrentOffset(record1_topicPartition));
        Assertions.assertEquals(
                retryableConsumer.getCurrentOffset(record1_topicPartition).offset(), record2_offset + 1
        );
    }

    @Test
    void listenAsync_shouldHandleInfiniteRetryableError() throws Exception {
        ConsumerRecord<String, String> record1 =
                new ConsumerRecord<>(topic, record1_partition, record1_offset, "key1", "value1");

        ConsumerRecord<String, String> record2 =
                new ConsumerRecord<>(topic, record2_partition, record2_offset, "key2", "value2");

        when(kafkaConsumer.poll(any()))
                .thenReturn( //First poll return one record
                        new ConsumerRecords<>(
                                Collections.singletonMap(record1_topicPartition, Collections.singletonList(record1))
                        )
                ).thenReturn(
                        new ConsumerRecords<>(
                                Collections.singletonMap(record2_topicPartition, Collections.singletonList(record2))
                        )
                ).thenReturn(new ConsumerRecords<>(Collections.emptyMap())); //all subsequent calls return empty record list

        doThrow(new CustomRetryableException()).when(recordProcessorNoError).processRecord(record2);

        retryableConsumer.listenAsync(r -> recordProcessorNoError.processRecord(r));

        //Check we continuously call poll
        verify(kafkaConsumer, timeout(5000).atLeast(3)).poll(any());

        //check we do not send anything in DLQ because of infinite retry
        verify(errorHandler, timeout(5000).times(0)).handleError(any(), any());
        verify(errorHandler, timeout(5000).times(0)).handleError(any(), any(), any());


        //Retryable error : Check we store correctly the offset of second record only
        Assertions.assertNotNull(retryableConsumer.getCurrentOffset(record1_topicPartition));
        Assertions.assertEquals(
                retryableConsumer.getCurrentOffset(record1_topicPartition).offset(), record2_offset
        );
    }

    static class CustomRetryableException extends Exception {}
    static class CustomNotRetryableException extends Exception {}
}
