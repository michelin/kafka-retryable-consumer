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

import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

import com.michelin.kafka.ErrorProcessor;
import com.michelin.kafka.RecordProcessor;
import com.michelin.kafka.RetryableConsumer;
import com.michelin.kafka.RetryableConsumerRebalanceListener;
import com.michelin.kafka.configuration.KafkaConfigurationException;
import com.michelin.kafka.configuration.KafkaRetryableConfiguration;
import com.michelin.kafka.configuration.RetryableConsumerConfiguration;
import com.michelin.kafka.error.RetryableConsumerErrorHandler;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.time.Instant;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.RecordDeserializationException;
import org.apache.kafka.common.record.TimestampType;
import org.junit.jupiter.api.*;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

@Slf4j
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class RetryableConsumerTest {
    private RetryableConsumer<String, String> retryableConsumer; // The tested class

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
    RecordProcessor<ConsumerRecord<String, String>, Exception> recordProcessorNoError;

    private AutoCloseable closeableMocks;

    private final String topic = "retryable-consumer-test-topic";
    private final int record1Partition = 1;
    private final long record1Offset = 1L;
    private final TopicPartition record1TopicPartition = new TopicPartition(topic, record1Partition);

    private final int record2Partition = 1;
    private final long record2Offset = 2L;
    private final TopicPartition record2TopicPartition = new TopicPartition(topic, record2Partition);

    /** Budget left to the consumer thread to catch up before an assertion is considered failed. */
    private static final Duration AWAIT_TIMEOUT = Duration.ofSeconds(10);

    /**
     * Null-safe read of the consumer internal offset position. The offsets map is fed by the consumer thread while the
     * test thread reads it, so callers must poll this value through Awaitility rather than reading it once.
     *
     * @return the current offset for the given partition, or -1 if the partition is not tracked yet
     */
    private static long currentOffsetOf(RetryableConsumer<String, String> consumer, TopicPartition topicPartition) {
        OffsetAndMetadata offsetAndMetadata = consumer.getCurrentOffset(topicPartition);
        return offsetAndMetadata == null ? -1L : offsetAndMetadata.offset();
    }

    @BeforeEach
    void setUp(TestInfo testInfo) throws Exception {
        log.info("Setting up test : {}", testInfo.getDisplayName());
        closeableMocks = MockitoAnnotations.openMocks(this);
        log.info("Mocks initialized!");

        when(errorHandler.isExceptionRetryable(CustomRetryableException.class)).thenReturn(true);
        when(errorHandler.isExceptionRetryable(CustomNotRetryableException.class))
                .thenReturn(false);

        when(retryableConfiguration.getName()).thenReturn("Test Consumer");
        when(retryableConfiguration.getConsumer()).thenReturn(consumerConfiguration);
        when(consumerConfiguration.getTopics()).thenReturn(Collections.singletonList(topic));
        when(consumerConfiguration.getNotRetryableExceptions())
                .thenReturn(Collections.singletonList(CustomNotRetryableException.class.getName()));

        when(retryableConfigurationStopOnError.getName()).thenReturn("Test Consumer with stop on error config");
        when(retryableConfigurationStopOnError.getConsumer()).thenReturn(consumerConfigurationStopOnError);
        when(consumerConfigurationStopOnError.getTopics()).thenReturn(Collections.singletonList(topic));
        when(consumerConfigurationStopOnError.getNotRetryableExceptions())
                .thenReturn(Collections.singletonList(CustomNotRetryableException.class.getName()));
        when(consumerConfigurationStopOnError.getStopOnError()).thenReturn(true);

        doNothing().when(recordProcessorNoError).processRecord(any());

        retryableConsumer =
                new RetryableConsumer<>(retryableConfiguration, kafkaConsumer, errorHandler, rebalanceListener);

        log.info("Test setup completed for test {} !", testInfo.getDisplayName());
    }

    @AfterEach
    void teardown(TestInfo testInfo) throws Exception {
        log.info("Tearing down test : {} ...", testInfo.getDisplayName());
        if (retryableConsumer != null) {
            retryableConsumer.close();
        }
        if (closeableMocks != null) {
            closeableMocks.close();
            log.info("Mocks closed");
        }

        log.info("Test tear down completed for test {} !", testInfo.getDisplayName());
    }

    @Test
    @Order(1)
    void listenAsync_shouldProcessRecords() throws Exception {
        ConsumerRecord<String, String> consumerRecord =
                new ConsumerRecord<>(topic, record1Partition, record1Offset, "key", "value");

        when(kafkaConsumer.poll(any()))
                .thenReturn(new ConsumerRecords<>(
                        Collections.singletonMap(record1TopicPartition, Collections.singletonList(consumerRecord)),
                        Collections.singletonMap(record1TopicPartition, new OffsetAndMetadata(1L)) // next records
                        ))
                .thenReturn(new ConsumerRecords<>(
                        Collections.emptyMap(),
                        Collections.singletonMap(record1TopicPartition, new OffsetAndMetadata(1L))));

        retryableConsumer.listenAsync(r -> recordProcessorNoError.processRecord(r));
        verify(kafkaConsumer, timeout(5000).atLeast(1)).poll(any());
        verify(recordProcessorNoError, timeout(5000).times(1)).processRecord(any());

        // The internal offset is updated *after* processRecord() returns, so we must wait for it
        await().atMost(AWAIT_TIMEOUT)
                .untilAsserted(() ->
                        assertEquals(record1Offset + 1, currentOffsetOf(retryableConsumer, record1TopicPartition)));
    }

    @Test
    @Order(2)
    void listenAsync_shouldHandleNotRetryableError() throws Exception {
        ConsumerRecord<String, String> record1 =
                new ConsumerRecord<>(topic, record1Partition, record1Offset, "key1", "value1");

        ConsumerRecord<String, String> record2 =
                new ConsumerRecord<>(topic, record2Partition, record2Offset, "key2", "value2");

        when(kafkaConsumer.poll(any()))
                .thenReturn( // First poll return one record
                        new ConsumerRecords<>(
                                Collections.singletonMap(record1TopicPartition, Collections.singletonList(record1)),
                                Collections.singletonMap(
                                        record1TopicPartition, new OffsetAndMetadata(1L)) // next records
                                ))
                .thenReturn(new ConsumerRecords<>(
                        Collections.singletonMap(record2TopicPartition, Collections.singletonList(record2)),
                        Collections.singletonMap(record1TopicPartition, new OffsetAndMetadata(1L)) // next records
                        ))
                .thenReturn(new ConsumerRecords<>(
                        Collections.emptyMap(),
                        Collections.singletonMap(record1TopicPartition, new OffsetAndMetadata(1L)) // next records
                        )); // all subsequent calls return empty record list

        doThrow(new RetryableConsumerTest.CustomNotRetryableException())
                .when(recordProcessorNoError)
                .processRecord(record2);

        retryableConsumer.listenAsync(r -> recordProcessorNoError.processRecord(r));
        verify(kafkaConsumer, timeout(5000).atLeastOnce()).poll(any());
        verify(errorHandler, timeout(5000).times(1)).handleError(any(), any());

        // Not retryable error : Check we have correctly skipped the record.
        // The offset is committed after handleError() returns, hence the wait.
        await().atMost(AWAIT_TIMEOUT)
                .untilAsserted(() ->
                        assertEquals(record2Offset + 1, currentOffsetOf(retryableConsumer, record1TopicPartition)));
    }

    @Test
    @Order(3)
    void listenAsync_shouldHandleInfiniteRetryableError() throws Exception {
        ConsumerRecord<String, String> record1 =
                new ConsumerRecord<>(topic, record1Partition, record1Offset, "key1", "value1");

        ConsumerRecord<String, String> record2 =
                new ConsumerRecord<>(topic, record2Partition, record2Offset, "key2", "value2");

        when(kafkaConsumer.poll(any()))
                .thenReturn( // First poll return one record
                        new ConsumerRecords<>(
                                Collections.singletonMap(record1TopicPartition, Collections.singletonList(record1)),
                                Collections.singletonMap(
                                        record1TopicPartition, new OffsetAndMetadata(1L)) // next records
                                ))
                .thenReturn(new ConsumerRecords<>(
                        Collections.singletonMap(record2TopicPartition, Collections.singletonList(record2)),
                        Collections.singletonMap(record2TopicPartition, new OffsetAndMetadata(1L)) // next records
                        ))
                .thenReturn(new ConsumerRecords<>(
                        Collections.emptyMap(),
                        Collections.singletonMap(record1TopicPartition, new OffsetAndMetadata(1L)) // next record
                        )); // all subsequent calls return empty record list

        doThrow(new RetryableConsumerTest.CustomRetryableException())
                .when(recordProcessorNoError)
                .processRecord(record2);

        retryableConsumer.listenAsync(r -> recordProcessorNoError.processRecord(r));

        // Check we continuously call poll
        verify(kafkaConsumer, timeout(5000).atLeast(3)).poll(any());

        // check we do not send anything in DLQ because of infinite retry.
        // after().never() actually waits for the delay, unlike timeout().times(0) which returns immediately.
        verify(errorHandler, after(500).never()).handleError(any(), any());
        verify(errorHandler, after(500).never()).handleError(any(), any(), any());

        // Retryable error : Check we store correctly the offset of second record only
        await().atMost(AWAIT_TIMEOUT)
                .untilAsserted(
                        () -> assertEquals(record2Offset, currentOffsetOf(retryableConsumer, record1TopicPartition)));
    }

    @Test
    @Order(4)
    void listenAsync_shouldHandleDeserializationException() throws Exception {
        ConsumerRecord<String, String> consumerRecord =
                new ConsumerRecord<>(topic, record1Partition, record1Offset, "key", "value");

        when(kafkaConsumer.poll(any()))
                .thenReturn( // First poll return one record
                        new ConsumerRecords<>(
                                Collections.singletonMap(
                                        record1TopicPartition, Collections.singletonList(consumerRecord)),
                                Collections.singletonMap(
                                        record1TopicPartition, new OffsetAndMetadata(1L)) // next records
                                ))
                .thenReturn(new ConsumerRecords<>(
                        Collections.emptyMap(),
                        Collections.singletonMap(record1TopicPartition, new OffsetAndMetadata(1L)) // next records
                        )); // all subsequent calls return empty record list

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
                .when(recordProcessorNoError)
                .processRecord(any());

        retryableConsumer.listenAsync(r -> recordProcessorNoError.processRecord(r));
        verify(kafkaConsumer, timeout(5000).atLeast(2)).poll(any());

        // Check the record is sent to DLQ
        verify(errorHandler, timeout(5000).times(1)).handleError(any(), any());

        // Check we have correctly skipped the record
        await().atMost(AWAIT_TIMEOUT)
                .untilAsserted(() ->
                        assertEquals(record1Offset + 1, currentOffsetOf(retryableConsumer, record1TopicPartition)));
    }

    @Test
    @Order(5)
    void listenAsync_shouldFailWithStopOnErrorConfig() throws Exception {
        try (RetryableConsumer<String, String> retryableConsumerStopOnError = new RetryableConsumer<>(
                retryableConfigurationStopOnError, kafkaConsumer, errorHandler, rebalanceListener)) {

            ConsumerRecord<String, String> consumerRecord =
                    new ConsumerRecord<>(topic, record1Partition, record1Offset, "key", "value");

            when(kafkaConsumer.poll(any()))
                    .thenReturn( // First poll return one record
                            new ConsumerRecords<>(
                                    Collections.singletonMap(
                                            record1TopicPartition, Collections.singletonList(consumerRecord)),
                                    Collections.singletonMap(
                                            record1TopicPartition, new OffsetAndMetadata(1L)) // next records
                                    ))
                    .thenReturn(new ConsumerRecords<>(
                            Collections.emptyMap(),
                            Collections.singletonMap(record1TopicPartition, new OffsetAndMetadata(1L)) // next records
                            )); // all subsequent calls return empty record list

            doThrow(new CustomNotRetryableException())
                    .when(recordProcessorNoError)
                    .processRecord(any());

            retryableConsumerStopOnError.listenAsync(r -> recordProcessorNoError.processRecord(r));
            verify(kafkaConsumer, timeout(5000).atLeast(1)).poll(any());
            verify(errorHandler, timeout(5000).times(1)).handleError(any(), any()); // Check the record is sent to DLQ

            // Check the consumer is stopped. stop() is called *after* handleError() returns, so we must wait
            // for it instead of asserting straight away.
            await().atMost(AWAIT_TIMEOUT).until(retryableConsumerStopOnError::isStopped);
        }
    }

    @Test
    @Order(6)
    void testRetryableWithCustomErrorProcessor() throws Exception {
        CustomErrorProcessor customErrorProcessor = new CustomErrorProcessor();
        ConsumerRecord<String, String> record1 =
                new ConsumerRecord<>(topic, record1Partition, record1Offset, "key1", "value1");
        ConsumerRecord<String, String> record2 =
                new ConsumerRecord<>(topic, record2Partition, record2Offset, "key2", "value2");

        try (RetryableConsumer<String, String> retryableConsumerCustomError =
                new RetryableConsumer<>(retryableConfigurationStopOnError, kafkaConsumer, customErrorProcessor)) {
            when(kafkaConsumer.poll(any()))
                    .thenReturn( // First poll return one record
                            new ConsumerRecords<>(
                                    Collections.singletonMap(record1TopicPartition, Collections.singletonList(record1)),
                                    Collections.singletonMap(
                                            record1TopicPartition, new OffsetAndMetadata(1L)) // next records
                                    ))
                    .thenReturn(new ConsumerRecords<>(
                            Collections.singletonMap(record2TopicPartition, Collections.singletonList(record2)),
                            Collections.singletonMap(record1TopicPartition, new OffsetAndMetadata(1L)) // next records
                            ))
                    .thenReturn(new ConsumerRecords<>(
                            Collections.emptyMap(),
                            Collections.singletonMap(record1TopicPartition, new OffsetAndMetadata(1L)) // next records
                            )); // all subsequent calls return empty record list

            doThrow(new RetryableConsumerTest.CustomNotRetryableException("Test Custom Error Processor"))
                    .when(recordProcessorNoError)
                    .processRecord(record2);

            retryableConsumerCustomError.listenAsync(r -> recordProcessorNoError.processRecord(r));
            verify(kafkaConsumer, timeout(5000).atLeastOnce()).poll(any());

            // The failing record is the one of the *second* poll, so waiting for the first poll proves nothing:
            // wait for the custom processor to actually record the error.
            await().atMost(AWAIT_TIMEOUT)
                    .untilAsserted(() ->
                            assertEquals(1, customErrorProcessor.getErrors().size()));
            assertEquals(
                    "Test Custom Error Processor",
                    customErrorProcessor.getErrors().get(0));
        }
    }

    @Test
    @Order(7)
    void testRetryableConstructors() throws KafkaConfigurationException {
        CustomErrorProcessor customErrorProcessor = new CustomErrorProcessor();

        try (RetryableConsumer<String, String> retryableConsumer1 = new RetryableConsumer<>("test")) {
            assertNotNull(retryableConsumer1);
        } catch (Exception e) {
            assertInstanceOf(org.apache.kafka.common.KafkaException.class, e);
        }

        KafkaRetryableConfiguration config = KafkaRetryableConfiguration.load();
        try (RetryableConsumer<String, String> retryableConsumer2 =
                new RetryableConsumer<>(config, customErrorProcessor)) {
            assertNotNull(retryableConsumer2);
        } catch (Exception e) {
            assertInstanceOf(org.apache.kafka.common.KafkaException.class, e);
        }

        try (RetryableConsumer<String, String> retryableConsumerCustomError =
                new RetryableConsumer<>(retryableConfigurationStopOnError, kafkaConsumer, customErrorProcessor)) {
            assertNotNull(retryableConsumerCustomError);
        }

        try (RetryableConsumer<String, String> retryableConsumerCustomError = new RetryableConsumer<>(
                retryableConfigurationStopOnError, kafkaConsumer, customErrorProcessor, rebalanceListener)) {
            assertNotNull(retryableConsumerCustomError);
        }
    }

    @Getter
    static class CustomErrorProcessor implements ErrorProcessor<ConsumerRecord<String, String>> {
        // Written by the consumer thread, read by the test thread
        List<String> errors = new CopyOnWriteArrayList<>();

        @Override
        public void processError(Throwable throwable, ConsumerRecord<String, String> record, Long retryCount) {
            // Custom error processing logic
            log.error(
                    "Error processing record with key {} and value {}. Retry count: {}. Error: {}",
                    record.key(),
                    record.value(),
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
