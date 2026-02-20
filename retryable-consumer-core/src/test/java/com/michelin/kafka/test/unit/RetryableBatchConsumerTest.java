/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package com.michelin.kafka.test.unit;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

import com.michelin.kafka.BatchRecordProcessor;
import com.michelin.kafka.ErrorProcessor;
import com.michelin.kafka.RetryableBatchConsumer;
import com.michelin.kafka.RetryableConsumerRebalanceListener;
import com.michelin.kafka.configuration.KafkaConfigurationException;
import com.michelin.kafka.configuration.KafkaRetryableConfiguration;
import com.michelin.kafka.configuration.RetryableConsumerConfiguration;
import com.michelin.kafka.error.RetryableConsumerErrorHandler;
import java.nio.ByteBuffer;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.RecordDeserializationException;
import org.apache.kafka.common.record.TimestampType;
import org.junit.jupiter.api.*;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

@Slf4j
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class RetryableBatchConsumerTest {
    private RetryableBatchConsumer<String, String> retryableBatchConsumer;

    @Mock
    private KafkaConsumer<String, String> kafkaConsumer;

    @Mock
    private KafkaRetryableConfiguration retryableConfiguration;

    @Mock
    private KafkaRetryableConfiguration retryableConfigurationStopOnError;

    @Mock
    private RetryableConsumerErrorHandler<String, String> errorHandler;

    @Mock
    private RetryableConsumerRebalanceListener rebalanceListener;

    @Mock
    RetryableConsumerConfiguration consumerConfiguration;

    @Mock
    RetryableConsumerConfiguration consumerConfigurationStopOnError;

    @Mock
    BatchRecordProcessor<String, String, Exception> batchProcessorNoError;

    private AutoCloseable closeableMocks;

    private final String topic = "retryable-batch-consumer-test-topic";
    private final int record1Partition = 1;
    private final long record1Offset = 1L;
    private final TopicPartition record1TopicPartition = new TopicPartition(topic, record1Partition);

    private final int record2Partition = 1;
    private final long record2Offset = 2L;
    private final TopicPartition record2TopicPartition = new TopicPartition(topic, record2Partition);

    @BeforeEach
    void setUp(TestInfo testInfo) throws Exception {
        log.info("Setting up test : {}", testInfo.getDisplayName());
        closeableMocks = MockitoAnnotations.openMocks(this);
        log.info("Mocks initialized!");

        when(errorHandler.isExceptionRetryable(CustomRetryableException.class)).thenReturn(true);
        when(errorHandler.isExceptionRetryable(CustomNotRetryableException.class))
                .thenReturn(false);

        when(retryableConfiguration.getName()).thenReturn("Test Batch Consumer");
        when(retryableConfiguration.getConsumer()).thenReturn(consumerConfiguration);
        when(consumerConfiguration.getTopics()).thenReturn(Collections.singletonList(topic));
        when(consumerConfiguration.getNotRetryableExceptions())
                .thenReturn(Collections.singletonList(CustomNotRetryableException.class.getName()));

        when(retryableConfigurationStopOnError.getName()).thenReturn("Test Batch Consumer with stop on error config");
        when(retryableConfigurationStopOnError.getConsumer()).thenReturn(consumerConfigurationStopOnError);
        when(consumerConfigurationStopOnError.getTopics()).thenReturn(Collections.singletonList(topic));
        when(consumerConfigurationStopOnError.getNotRetryableExceptions())
                .thenReturn(Collections.singletonList(CustomNotRetryableException.class.getName()));
        when(consumerConfigurationStopOnError.getStopOnError()).thenReturn(true);

        doNothing().when(batchProcessorNoError).processRecords(any());

        retryableBatchConsumer =
                new RetryableBatchConsumer<>(retryableConfiguration, kafkaConsumer, errorHandler, rebalanceListener);

        log.info("Test setup completed for test {} !", testInfo.getDisplayName());
    }

    @AfterEach
    void teardown(TestInfo testInfo) throws Exception {
        log.info("Tearing down test : {} ...", testInfo.getDisplayName());
        if (retryableBatchConsumer != null) {
            retryableBatchConsumer.close();
        }
        if (closeableMocks != null) {
            closeableMocks.close();
            log.info("Mocks closed");
        }
        log.info("Test tear down completed for test {} !", testInfo.getDisplayName());
    }

    @Test
    @Order(1)
    void listenAsync_shouldProcessBatchOfRecords() throws Exception {
        ConsumerRecord<String, String> consumerRecord1 =
                new ConsumerRecord<>(topic, record1Partition, record1Offset, "key1", "value1");
        ConsumerRecord<String, String> consumerRecord2 =
                new ConsumerRecord<>(topic, record2Partition, record2Offset, "key2", "value2");

        List<ConsumerRecord<String, String>> recordList = List.of(consumerRecord1, consumerRecord2);

        when(kafkaConsumer.poll(any()))
                .thenReturn(new ConsumerRecords<>(
                        Collections.singletonMap(record1TopicPartition, recordList),
                        Collections.singletonMap(record1TopicPartition, new OffsetAndMetadata(1L))))
                .thenReturn(new ConsumerRecords<>(
                        Collections.emptyMap(),
                        Collections.singletonMap(record1TopicPartition, new OffsetAndMetadata(1L))));

        retryableBatchConsumer.listenAsync(records -> batchProcessorNoError.processRecords(records));
        verify(kafkaConsumer, timeout(5000).atLeast(1)).poll(any());
        verify(batchProcessorNoError, timeout(5000).times(1)).processRecords(any());

        // Check offsets updated to after the last record in the batch
        assertEquals(
                retryableBatchConsumer.getCurrentOffset(record1TopicPartition).offset(), record2Offset + 1);
    }

    @Test
    @Order(2)
    void listenAsync_shouldHandleNotRetryableErrorInBatch() throws Exception {
        ConsumerRecord<String, String> consumerRecord1 =
                new ConsumerRecord<>(topic, record1Partition, record1Offset, "key1", "value1");
        ConsumerRecord<String, String> consumerRecord2 =
                new ConsumerRecord<>(topic, record2Partition, record2Offset, "key2", "value2");

        List<ConsumerRecord<String, String>> recordList = List.of(consumerRecord1, consumerRecord2);

        when(kafkaConsumer.poll(any()))
                .thenReturn(new ConsumerRecords<>(
                        Collections.singletonMap(record1TopicPartition, recordList),
                        Collections.singletonMap(record1TopicPartition, new OffsetAndMetadata(1L))))
                .thenReturn(new ConsumerRecords<>(
                        Collections.emptyMap(),
                        Collections.singletonMap(record1TopicPartition, new OffsetAndMetadata(1L))));

        doThrow(new CustomNotRetryableException()).when(batchProcessorNoError).processRecords(any());

        retryableBatchConsumer.listenAsync(records -> batchProcessorNoError.processRecords(records));
        verify(kafkaConsumer, timeout(5000).atLeastOnce()).poll(any());

        // Check the batch error is sent to DLQ (with null record since it's batch mode)
        verify(errorHandler, timeout(5000).times(1)).handleError(any(), isNull());
    }

    @Test
    @Order(3)
    void listenAsync_shouldHandleInfiniteRetryableErrorInBatch() throws Exception {
        ConsumerRecord<String, String> consumerRecord1 =
                new ConsumerRecord<>(topic, record1Partition, record1Offset, "key1", "value1");
        ConsumerRecord<String, String> consumerRecord2 =
                new ConsumerRecord<>(topic, record2Partition, record2Offset, "key2", "value2");

        List<ConsumerRecord<String, String>> recordList = List.of(consumerRecord1, consumerRecord2);

        when(kafkaConsumer.poll(any()))
                .thenReturn(new ConsumerRecords<>(
                        Collections.singletonMap(record1TopicPartition, recordList),
                        Collections.singletonMap(record1TopicPartition, new OffsetAndMetadata(1L))))
                .thenReturn(new ConsumerRecords<>(
                        Collections.emptyMap(),
                        Collections.singletonMap(record1TopicPartition, new OffsetAndMetadata(1L))));

        doThrow(new CustomRetryableException()).when(batchProcessorNoError).processRecords(any());

        retryableBatchConsumer.listenAsync(records -> batchProcessorNoError.processRecords(records));

        // Check we continuously call poll (infinite retry)
        verify(kafkaConsumer, timeout(5000).atLeast(3)).poll(any());

        // Check we do not send anything in DLQ because of infinite retry
        verify(errorHandler, timeout(5000).times(0)).handleError(any(), any());
        verify(errorHandler, timeout(5000).times(0)).handleError(any(), any(), any());
    }

    @Test
    @Order(4)
    void listenAsync_shouldHandleDeserializationExceptionInBatch() throws Exception {
        ConsumerRecord<String, String> consumerRecord =
                new ConsumerRecord<>(topic, record1Partition, record1Offset, "key", "value");

        when(kafkaConsumer.poll(any()))
                .thenReturn(new ConsumerRecords<>(
                        Collections.singletonMap(record1TopicPartition, Collections.singletonList(consumerRecord)),
                        Collections.singletonMap(record1TopicPartition, new OffsetAndMetadata(1L))))
                .thenReturn(new ConsumerRecords<>(
                        Collections.emptyMap(),
                        Collections.singletonMap(record1TopicPartition, new OffsetAndMetadata(1L))));

        doThrow(new RecordDeserializationException(
                        RecordDeserializationException.DeserializationExceptionOrigin.VALUE,
                        record1TopicPartition,
                        record1Offset,
                        Instant.now().toEpochMilli(),
                        TimestampType.NO_TIMESTAMP_TYPE,
                        ByteBuffer.wrap("Test Key".getBytes()),
                        ByteBuffer.wrap("Test Value".getBytes()),
                        null,
                        "Fake DeSer Error",
                        new Exception()))
                .when(batchProcessorNoError)
                .processRecords(any());

        retryableBatchConsumer.listenAsync(records -> batchProcessorNoError.processRecords(records));
        verify(kafkaConsumer, timeout(5000).atLeast(2)).poll(any());

        // Check the record is sent to DLQ
        verify(errorHandler, timeout(5000).times(1)).handleError(any(), any());

        // Check we have correctly skipped the record
        assertNotNull(retryableBatchConsumer.getCurrentOffset(record1TopicPartition));
        assertEquals(
                retryableBatchConsumer.getCurrentOffset(record1TopicPartition).offset(), record1Offset + 1);
    }

    @Test
    @Order(5)
    void listenAsync_shouldFailWithStopOnErrorConfigInBatch() throws Exception {
        RetryableBatchConsumer<String, String> retryableBatchConsumerStopOnError = new RetryableBatchConsumer<>(
                retryableConfigurationStopOnError, kafkaConsumer, errorHandler, rebalanceListener);

        ConsumerRecord<String, String> consumerRecord =
                new ConsumerRecord<>(topic, record1Partition, record1Offset, "key", "value");

        when(kafkaConsumer.poll(any()))
                .thenReturn(new ConsumerRecords<>(
                        Collections.singletonMap(record1TopicPartition, Collections.singletonList(consumerRecord)),
                        Collections.singletonMap(record1TopicPartition, new OffsetAndMetadata(1L))))
                .thenReturn(new ConsumerRecords<>(
                        Collections.emptyMap(),
                        Collections.singletonMap(record1TopicPartition, new OffsetAndMetadata(1L))));

        doThrow(new CustomNotRetryableException()).when(batchProcessorNoError).processRecords(any());

        retryableBatchConsumerStopOnError.listenAsync(records -> batchProcessorNoError.processRecords(records));
        verify(kafkaConsumer, timeout(5000).atLeast(1)).poll(any());
        verify(errorHandler, timeout(5000).times(1)).handleError(any(), isNull());

        // Check the consumer is stopped
        assertTrue(retryableBatchConsumerStopOnError.isStopped());
    }

    @Test
    @Order(6)
    void testBatchRetryableWithCustomErrorProcessor() throws Exception {
        CustomBatchErrorProcessor customErrorProcessor = new CustomBatchErrorProcessor();
        ConsumerRecord<String, String> record1 =
                new ConsumerRecord<>(topic, record1Partition, record1Offset, "key1", "value1");
        ConsumerRecord<String, String> record2 =
                new ConsumerRecord<>(topic, record2Partition, record2Offset, "key2", "value2");

        List<ConsumerRecord<String, String>> recordList = List.of(record1, record2);

        try (RetryableBatchConsumer<String, String> retryableBatchConsumerCustomError =
                new RetryableBatchConsumer<>(retryableConfigurationStopOnError, kafkaConsumer, customErrorProcessor)) {
            when(kafkaConsumer.poll(any()))
                    .thenReturn(new ConsumerRecords<>(
                            Collections.singletonMap(record1TopicPartition, recordList),
                            Collections.singletonMap(record1TopicPartition, new OffsetAndMetadata(1L))))
                    .thenReturn(new ConsumerRecords<>(
                            Collections.emptyMap(),
                            Collections.singletonMap(record1TopicPartition, new OffsetAndMetadata(1L))));

            doThrow(new CustomNotRetryableException("Test Batch Custom Error Processor"))
                    .when(batchProcessorNoError)
                    .processRecords(any());

            retryableBatchConsumerCustomError.listenAsync(records -> batchProcessorNoError.processRecords(records));
            verify(kafkaConsumer, timeout(5000).atLeastOnce()).poll(any());

            // Wait for error to be processed
            Thread.sleep(1000);
            assertEquals(1, customErrorProcessor.getErrors().size());
            assertEquals(
                    "Test Batch Custom Error Processor",
                    customErrorProcessor.getErrors().get(0));
        }
    }

    @Test
    @Order(7)
    void listenAsync_shouldProcessMultipleBatchesSequentially() throws Exception {
        ConsumerRecord<String, String> batch1Record =
                new ConsumerRecord<>(topic, record1Partition, record1Offset, "key1", "value1");
        ConsumerRecord<String, String> batch2Record =
                new ConsumerRecord<>(topic, record2Partition, record2Offset, "key2", "value2");

        when(kafkaConsumer.poll(any()))
                .thenReturn(new ConsumerRecords<>(
                        Collections.singletonMap(record1TopicPartition, Collections.singletonList(batch1Record)),
                        Collections.singletonMap(record1TopicPartition, new OffsetAndMetadata(1L))))
                .thenReturn(new ConsumerRecords<>(
                        Collections.singletonMap(record2TopicPartition, Collections.singletonList(batch2Record)),
                        Collections.singletonMap(record2TopicPartition, new OffsetAndMetadata(2L))))
                .thenReturn(new ConsumerRecords<>(
                        Collections.emptyMap(),
                        Collections.singletonMap(record1TopicPartition, new OffsetAndMetadata(2L))));

        retryableBatchConsumer.listenAsync(records -> batchProcessorNoError.processRecords(records));
        verify(kafkaConsumer, timeout(5000).atLeast(2)).poll(any());
        verify(batchProcessorNoError, timeout(5000).atLeast(2)).processRecords(any());

        // Check that offsets reflect the last batch processed
        assertEquals(
                retryableBatchConsumer.getCurrentOffset(record1TopicPartition).offset(), record2Offset + 1);
    }

    @Test
    @Order(8)
    void testBatchRetryableConstructors() throws KafkaConfigurationException {
        CustomBatchErrorProcessor customErrorProcessor = new CustomBatchErrorProcessor();

        try (RetryableBatchConsumer<String, String> batchConsumer1 = new RetryableBatchConsumer<>("test")) {
            assertNotNull(batchConsumer1);
        } catch (Exception e) {
            assertEquals(KafkaException.class, e.getClass());
        }

        KafkaRetryableConfiguration config = KafkaRetryableConfiguration.load();
        try (RetryableBatchConsumer<String, String> batchConsumer2 =
                new RetryableBatchConsumer<>(config, customErrorProcessor)) {
            assertNotNull(batchConsumer2);
        } catch (Exception e) {
            assertEquals(KafkaException.class, e.getClass());
        }

        try (RetryableBatchConsumer<String, String> batchConsumer3 =
                new RetryableBatchConsumer<>(retryableConfigurationStopOnError, kafkaConsumer, customErrorProcessor)) {
            assertNotNull(batchConsumer3);
        }

        try (RetryableBatchConsumer<String, String> batchConsumer4 = new RetryableBatchConsumer<>(
                retryableConfigurationStopOnError, kafkaConsumer, customErrorProcessor, rebalanceListener)) {
            assertNotNull(batchConsumer4);
        }
    }

    @Getter
    static class CustomBatchErrorProcessor implements ErrorProcessor<ConsumerRecord<String, String>> {
        List<String> errors = new ArrayList<>();

        @Override
        public void processError(Throwable throwable, ConsumerRecord<String, String> record, Long retryCount) {
            log.error(
                    "Error processing batch. Record: {}, Retry count: {}, Error: {}",
                    record,
                    retryCount,
                    throwable.getMessage());
            errors.add(throwable.getMessage());
        }
    }

    static class CustomRetryableException extends Exception {}

    static class CustomNotRetryableException extends Exception {
        public CustomNotRetryableException() {
            super();
        }

        public CustomNotRetryableException(String message) {
            super(message);
        }
    }
}
