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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

import com.michelin.kafka.ErrorProcessor;
import com.michelin.kafka.avro.GenericErrorModel;
import com.michelin.kafka.error.DeadLetterProducer;
import com.michelin.kafka.error.DefaultErrorProcessor;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.nio.ByteBuffer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.RecordDeserializationException;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.record.TimestampType;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

public class DefaultErrorProcessorTest {
    @Captor
    private ArgumentCaptor<String> keyCaptor;

    @Captor
    private ArgumentCaptor<GenericErrorModel> valueCaptor;

    @Mock
    private static DeadLetterProducer mockDeadLetterProducer;

    private ErrorProcessor<ConsumerRecord<String, String>> errorProcessor;

    AutoCloseable mockCloseable;

    @BeforeEach
    void setUp() {
        mockCloseable = MockitoAnnotations.openMocks(this);
        errorProcessor = new DefaultErrorProcessor<>(mockDeadLetterProducer);
        doNothing().when(mockDeadLetterProducer).send(any(), any());
    }

    @Test
    void testToByteBuffer() throws IOException, ClassNotFoundException {
        // Arrange
        String testString = "Test string";

        // Act
        ByteBuffer buffer = DefaultErrorProcessor.toByteBuffer(testString);

        // Convert ByteBuffer back to String
        byte[] bytes = new byte[buffer.remaining()];
        buffer.get(bytes);
        ByteArrayInputStream bais = new ByteArrayInputStream(bytes);
        ObjectInputStream ois = new ObjectInputStream(bais);
        String resultString = (String) ois.readObject();

        // Assert
        assertEquals(testString, resultString);
    }

    @Test
    void shouldHandleErrorWithThrowableAndRecord() {
        // Given
        String cause = "Exception message";
        Exception e = new Exception(cause);

        ConsumerRecord<String, String> record = new ConsumerRecord<>("testTopic", 12, 13458L, "key", "value");

        // When
        errorProcessor.processError(e, record, 32L);

        // Then
        verify(mockDeadLetterProducer, times(1)).send(keyCaptor.capture(), valueCaptor.capture());

        GenericErrorModel capturedErrorModel = valueCaptor.getValue();

        assertEquals(cause, capturedErrorModel.getCause());
        assertEquals("Error processing record after 32 retry(ies).", capturedErrorModel.getContextMessage());
        assertEquals(record.topic(), capturedErrorModel.getTopic());
        assertEquals(record.partition(), capturedErrorModel.getPartition());
        assertEquals(record.offset(), capturedErrorModel.getOffset());
        assertEquals(record.key(), capturedErrorModel.getKey());
        assertEquals(record.value(), capturedErrorModel.getValue());
    }

    @Test
    void shouldHandleErrorWithNullRecord() {
        // Given
        String cause = "Exception message";
        Exception e = new Exception(cause);

        // When
        errorProcessor.processError(e, null, 32L);

        // Then
        verify(mockDeadLetterProducer, times(1)).send(keyCaptor.capture(), valueCaptor.capture());

        GenericErrorModel capturedErrorModel = valueCaptor.getValue();

        assertEquals(cause, capturedErrorModel.getCause());
        assertEquals("Error processing an unknown record after 32 retry(ies).", capturedErrorModel.getContextMessage());
    }

    @Test
    void shouldHandleErrorWithNullThrowable() {
        // Given
        ConsumerRecord<String, String> record = new ConsumerRecord<>("testTopic", 12, 13458L, "key", "value");

        // When
        errorProcessor.processError(null, record, 32L);

        // Then
        verify(mockDeadLetterProducer, times(1)).send(keyCaptor.capture(), valueCaptor.capture());

        GenericErrorModel capturedErrorModel = valueCaptor.getValue();

        assertEquals("Undefined error", capturedErrorModel.getCause());
        assertEquals("Error processing record after 32 retry(ies).", capturedErrorModel.getContextMessage());
        assertEquals(record.topic(), capturedErrorModel.getTopic());
        assertEquals(record.partition(), capturedErrorModel.getPartition());
        assertEquals(record.offset(), capturedErrorModel.getOffset());
        assertEquals(record.key(), capturedErrorModel.getKey());
        assertEquals(record.value(), capturedErrorModel.getValue());
    }

    @Test
    void shouldHandleErrorWithRecordDeserializationException() throws IOException {
        // Given
        ConsumerRecord<String, String> record = new ConsumerRecord<>("testTopic", 12, 13458L, "key", "value");
        RecordDeserializationException rde =
                new RecordDeserializationException(
                        RecordDeserializationException.DeserializationExceptionOrigin.KEY,
                        new TopicPartition(record.topic(),record.partition()),
                        record.offset(),
                        1764603801,
                        TimestampType.CREATE_TIME,
                        DefaultErrorProcessor.toByteBuffer("MyKey"),
                        DefaultErrorProcessor.toByteBuffer("MyValue"),
                        null,
                        "Record deserialization error",
                        null);

        // When
        errorProcessor.processError(rde, record, 32L);
        verify(mockDeadLetterProducer, times(1)).send(keyCaptor.capture(), valueCaptor.capture());

        GenericErrorModel capturedErrorModel = valueCaptor.getValue();

        assertEquals("Record deserialization error", capturedErrorModel.getCause());
        assertEquals(record.topic(), capturedErrorModel.getTopic());
    }
}
