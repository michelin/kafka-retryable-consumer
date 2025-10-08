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

import com.michelin.kafka.avro.GenericErrorModel;
import com.michelin.kafka.error.DeadLetterProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.io.Closeable;
import java.util.concurrent.CompletableFuture;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

class DeadLetterProducerTest {

    @Mock
    private static Producer<String, GenericErrorModel> mockKafkaProducer;

    private DeadLetterProducer deadLetterProducer;

    AutoCloseable mockCloseable = mock(Closeable.class);

    @BeforeEach
    void setUp() {
        mockCloseable = MockitoAnnotations.openMocks(this);
        String dlTopic = "dlTopic";
        deadLetterProducer = new DeadLetterProducer(mockKafkaProducer, dlTopic);
    }

    @AfterEach
    void tearDown() throws Exception {
        if (mockCloseable != null) {
            mockCloseable.close();
        }
    }

    @Test
    void shouldSendMessageSuccessfully() {
        // Given
        String key = "key";
        GenericErrorModel errorObject = new GenericErrorModel();
        RecordMetadata metadata = new RecordMetadata(new TopicPartition("dlTopic", 1), 1, 1, 1, 1, 1);
        CompletableFuture<RecordMetadata> future = CompletableFuture.completedFuture(metadata);
        when(mockKafkaProducer.send(any(ProducerRecord.class), any())).thenReturn(future);

        // When
        deadLetterProducer.send(key, errorObject);

        // Then
        verify(mockKafkaProducer, times(1)).send(any(ProducerRecord.class), any());
    }

    @Test
    void shouldHandleExceptionWhenSendingMessage() {
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
