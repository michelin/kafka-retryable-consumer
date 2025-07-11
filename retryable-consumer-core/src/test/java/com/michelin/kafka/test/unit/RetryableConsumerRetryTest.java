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

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

import com.michelin.kafka.RecordProcessor;
import com.michelin.kafka.RetryableConsumer;
import com.michelin.kafka.RetryableConsumerRebalanceListener;
import com.michelin.kafka.configuration.KafkaRetryableConfiguration;
import com.michelin.kafka.configuration.RetryableConsumerConfiguration;
import com.michelin.kafka.error.RetryableConsumerErrorHandler;
import java.nio.ByteBuffer;
import java.time.Instant;
import java.util.Collections;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.RecordDeserializationException;
import org.apache.kafka.common.record.TimestampType;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

public class RetryableConsumerRetryTest {

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

    private AutoCloseable closeable;

    private final String topic = "topic";
    private final int record1Partition = 1;
    private final long record1Offset = 1L;
    private final TopicPartition record1TopicPartition = new TopicPartition(topic, record1Partition);

    private final int record2Partition = 1;
    private final long record2Offset = 2L;
    private final TopicPartition record2TopicPartition = new TopicPartition(topic, record2Partition);

    @BeforeEach
    void setUp() throws Exception {
        closeable = MockitoAnnotations.openMocks(this);

        when(errorHandler.isExceptionRetryable(RetryableConsumerTest.CustomRetryableException.class))
                .thenReturn(true);
        when(errorHandler.isExceptionRetryable(RetryableConsumerTest.CustomNotRetryableException.class))
                .thenReturn(false);

        when(retryableConfiguration.getName()).thenReturn("Test Consumer");
        when(retryableConfiguration.getConsumer()).thenReturn(consumerConfiguration);
        when(consumerConfiguration.getTopics()).thenReturn(Collections.singletonList(topic));

        doNothing().when(recordProcessorNoError).processRecord(any());
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
                .when(recordProcessorDeserializationError)
                .processRecord(any());

        retryableConsumer =
                new RetryableConsumer<>(retryableConfiguration, kafkaConsumer, errorHandler, rebalanceListener);
    }

    @AfterEach
    void teardown() throws Exception {
        if (retryableConsumer != null) {
            retryableConsumer.close();
        }
        closeable.close();
    }

    @Test
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

        // Not retryable error : Check we have correctly skipped the record
        Assertions.assertNotNull(retryableConsumer.getCurrentOffset(record1TopicPartition));
        Assertions.assertEquals(
                retryableConsumer.getCurrentOffset(record1TopicPartition).offset(), record2Offset + 1);
    }

    @Test
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

        // check we do not send anything in DLQ because of infinite retry
        verify(errorHandler, timeout(5000).times(0)).handleError(any(), any());
        verify(errorHandler, timeout(5000).times(0)).handleError(any(), any(), any());

        // Retryable error : Check we store correctly the offset of second record only
        Assertions.assertNotNull(retryableConsumer.getCurrentOffset(record1TopicPartition));
        Assertions.assertEquals(
                retryableConsumer.getCurrentOffset(record1TopicPartition).offset(), record2Offset);
    }

    @Test
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
        verify(errorHandler, timeout(5000).times(1)).handleConsumerDeserializationError(any());

        // Check we have correctly skipped the record
        Assertions.assertNotNull(retryableConsumer.getCurrentOffset(record1TopicPartition));
        Assertions.assertEquals(
                retryableConsumer.getCurrentOffset(record1TopicPartition).offset(), record1Offset + 1);
    }
}
