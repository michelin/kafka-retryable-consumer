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
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.RecordDeserializationException;
import org.apache.kafka.common.record.TimestampType;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

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

    private AutoCloseable closeable;

    private final String topic = "topic";
    private final int record1Partition = 1;
    private final long record1Offset = 1L;
    private final TopicPartition record1TopicPartition = new TopicPartition(topic, record1Partition);

    @BeforeEach
    void setUp() throws Exception {
        closeable = MockitoAnnotations.openMocks(this);

        when(errorHandler.isExceptionRetryable(CustomRetryableException.class)).thenReturn(true);
        when(errorHandler.isExceptionRetryable(CustomNotRetryableException.class))
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
        retryableConsumer.stop();
        Assertions.assertEquals(
                retryableConsumer.getCurrentOffset(record1TopicPartition).offset(), record1Offset + 1);
    }

    static class CustomRetryableException extends Exception {}

    static class CustomNotRetryableException extends Exception {}
}
