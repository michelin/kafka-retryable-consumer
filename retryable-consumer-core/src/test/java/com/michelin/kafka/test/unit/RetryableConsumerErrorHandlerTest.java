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
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

import com.michelin.kafka.avro.GenericErrorModel;
import com.michelin.kafka.error.DeadLetterProducer;
import com.michelin.kafka.error.RetryableConsumerErrorHandler;
import java.io.*;
import java.nio.ByteBuffer;
import java.time.Instant;
import java.util.Arrays;
import java.util.List;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.RecordDeserializationException;
import org.apache.kafka.common.record.TimestampType;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

class RetryableConsumerErrorHandlerTest {

    @Captor
    private ArgumentCaptor<String> keyCaptor;

    @Captor
    private ArgumentCaptor<GenericErrorModel> valueCaptor;
    /** Shared test Dead Letter Topic consumer configuration */
    @Mock
    private static DeadLetterProducer mockDeadLetterProducer;

    private RetryableConsumerErrorHandler<String, String> errorHandler;
    private RetryableConsumerErrorHandler<String, SerializableObject> serializableObjectErrorHandler;
    private RetryableConsumerErrorHandler<SerializableObject, String> serializableObjectKeyErrorHandler;

    @BeforeEach
    void setUp() {
        MockitoAnnotations.openMocks(this);
        List<String> notRetryableExceptions =
                Arrays.asList("java.lang.IllegalArgumentException", "java.lang.NullPointerException");
        doNothing().when(mockDeadLetterProducer).send(any(), any());
        errorHandler = new RetryableConsumerErrorHandler<>(mockDeadLetterProducer, notRetryableExceptions);
        serializableObjectErrorHandler =
                new RetryableConsumerErrorHandler<>(mockDeadLetterProducer, notRetryableExceptions);
        serializableObjectKeyErrorHandler =
                new RetryableConsumerErrorHandler<>(mockDeadLetterProducer, notRetryableExceptions);
    }

    @Test
    void shouldIdentifyRetryableExceptions() {
        assertTrue(errorHandler.isExceptionRetryable(RuntimeException.class));
    }

    @Test
    void shouldIdentifyNonRetryableExceptions() {
        assertFalse(errorHandler.isExceptionRetryable(IllegalArgumentException.class));
        assertFalse(errorHandler.isExceptionRetryable(NullPointerException.class));
    }

    @Test
    void shouldAddNotRetryableExceptions() {
        errorHandler.addNotRetryableExceptions(IOException.class, FileNotFoundException.class);
        assertFalse(errorHandler.isExceptionRetryable(IOException.class));
        assertFalse(errorHandler.isExceptionRetryable(FileNotFoundException.class));
    }

    @Test
    void shouldNotAddRetryableExceptions() {
        errorHandler.addNotRetryableExceptions(IOException.class, FileNotFoundException.class);
        assertTrue(errorHandler.isExceptionRetryable(RuntimeException.class));
    }

    @Test
    void shouldInitializeWithDefaultNotRetryableExceptions() {
        assertFalse(errorHandler.isExceptionRetryable(RecordDeserializationException.class));
        assertFalse(errorHandler.isExceptionRetryable(NoSuchMethodException.class));
        assertFalse(errorHandler.isExceptionRetryable(ClassCastException.class));
    }

    @Test
    void testHandleConsumerDeserializationErrorExecution() {
        TopicPartition topicPartition = new TopicPartition("ExampleTopic", 1);
        RecordDeserializationException exception = new RecordDeserializationException(
                RecordDeserializationException.DeserializationExceptionOrigin.VALUE,
                topicPartition,
                1L,
                Instant.now().toEpochMilli(),
                TimestampType.NO_TIMESTAMP_TYPE,
                ByteBuffer.wrap("Test Key".getBytes()),
                ByteBuffer.wrap("Test Value".getBytes()),
                null,
                "Test message",
                null);
        assertDoesNotThrow(() -> errorHandler.handleConsumerDeserializationError(exception));
    }

    @Test
    void testHandleConsumerDeserializationWithErrorNullTopic() {
        RecordDeserializationException exception = new RecordDeserializationException(
                RecordDeserializationException.DeserializationExceptionOrigin.VALUE,
                null,
                1L,
                Instant.now().toEpochMilli(),
                TimestampType.NO_TIMESTAMP_TYPE,
                ByteBuffer.wrap("Test Key".getBytes()),
                ByteBuffer.wrap("Test Value".getBytes()),
                null,
                "Test message",
                null);

        assertThrows(NullPointerException.class, () -> errorHandler.handleConsumerDeserializationError(exception));
    }

    @Test
    void shouldHandleErrorWhenAllParametersAreNotNull() {
        // Given
        String cause = "cause";
        String context = "context";
        Long offset = 1L;
        Integer partition = 1;
        String topic = "topic";
        Throwable exception = new RuntimeException("Test exception");
        String key = "key";
        String value = "value";
        doNothing().when(mockDeadLetterProducer).send(any(), any());

        // When
        errorHandler.handleError(cause, context, offset, partition, topic, exception, key, value);

        // Then
        verify(mockDeadLetterProducer, times(1)).send(keyCaptor.capture(), valueCaptor.capture());

        GenericErrorModel capturedErrorModel = valueCaptor.getValue();

        assertEquals(cause, capturedErrorModel.getCause());
        assertEquals(context, capturedErrorModel.getContextMessage());
        assertEquals(offset, capturedErrorModel.getOffset());
        assertEquals(partition, capturedErrorModel.getPartition());
        assertEquals(topic, capturedErrorModel.getTopic());
    }

    @Test
    void shouldHandleErrorWhenExceptionIsNull() {
        // Given
        String cause = "cause";
        String context = "context";
        Long offset = 1L;
        Integer partition = 1;
        String topic = "topic";
        String key = "key";
        String value = "value";

        // When
        errorHandler.handleError(cause, context, offset, partition, topic, null, key, value);

        // Then
        verify(mockDeadLetterProducer, times(1)).send(keyCaptor.capture(), valueCaptor.capture());

        GenericErrorModel capturedErrorModel = valueCaptor.getValue();

        assertEquals(cause, capturedErrorModel.getCause());
        assertEquals(context, capturedErrorModel.getContextMessage());
        assertEquals(offset, capturedErrorModel.getOffset());
        assertEquals(partition, capturedErrorModel.getPartition());
        assertEquals(topic, capturedErrorModel.getTopic());
        assertNull(capturedErrorModel.getStack()); // Check that Stack is null when exception is null
    }

    public static class SerializableObject implements Serializable {
        @Serial
        private static final long serialVersionUID = 1L;
    }

    @Test
    void shouldHandleErrorWhenValueIsNotString() {
        // Given
        String cause = "cause";
        String context = "context";
        Long offset = 1L;
        Integer partition = 1;
        String topic = "topic";
        Throwable exception = new RuntimeException("Test exception");
        String key = "key";
        SerializableObject value = new SerializableObject();
        doNothing().when(mockDeadLetterProducer).send(any(), any());

        // When
        serializableObjectErrorHandler.handleError(cause, context, offset, partition, topic, exception, key, value);

        // Then
        verify(mockDeadLetterProducer, times(1)).send(keyCaptor.capture(), valueCaptor.capture());

        GenericErrorModel capturedErrorModel = valueCaptor.getValue();

        assertEquals(cause, capturedErrorModel.getCause());
        assertEquals(context, capturedErrorModel.getContextMessage());
        assertEquals(offset, capturedErrorModel.getOffset());
        assertEquals(partition, capturedErrorModel.getPartition());
        assertEquals(topic, capturedErrorModel.getTopic());
        assertNotNull(capturedErrorModel.getByteValue()); // Check that ByteValue is not null when value is not a String
    }

    @Test
    void shouldHandleErrorWhenKeyIsNotString() {
        // Given
        String cause = "cause";
        String context = "context";
        Long offset = 1L;
        Integer partition = 1;
        String topic = "topic";
        Throwable exception = new RuntimeException("Test exception");
        SerializableObject key = new SerializableObject();
        String value = "value";
        doNothing().when(mockDeadLetterProducer).send(any(), any());

        // When
        serializableObjectKeyErrorHandler.handleError(cause, context, offset, partition, topic, exception, key, value);

        // Then
        verify(mockDeadLetterProducer, times(1)).send(keyCaptor.capture(), valueCaptor.capture());

        GenericErrorModel capturedErrorModel = valueCaptor.getValue();

        assertEquals(cause, capturedErrorModel.getCause());
        assertEquals(context, capturedErrorModel.getContextMessage());
        assertEquals(offset, capturedErrorModel.getOffset());
        assertEquals(partition, capturedErrorModel.getPartition());
        assertEquals(topic, capturedErrorModel.getTopic());
        assertNotNull(capturedErrorModel.getByteKey()); // Check that ByteKey is not null when key is not a String
    }

    @Test
    void testToByteBuffer() throws IOException, ClassNotFoundException {
        // Arrange
        String testString = "Test string";

        // Act
        ByteBuffer buffer = RetryableConsumerErrorHandler.toByteBuffer(testString);

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
    void testConvertStringToExceptionWithValidExceptionNames() {

        List<String> exceptionNames = Arrays.asList("java.lang.Exception", "java.io.IOException");

        List<Class<? extends Exception>> exceptions =
                RetryableConsumerErrorHandler.convertStringToException(exceptionNames);

        assertEquals(2, exceptions.size());
        assertTrue(exceptions.contains(Exception.class));
        assertTrue(exceptions.contains(IOException.class));
    }

    @Test
    void testConvertStringToExceptionWithInvalidClassName() {

        List<String> exceptionNames = List.of("java.lang.NonExistentClass");

        assertThrows(
                IllegalArgumentException.class,
                () -> RetryableConsumerErrorHandler.convertStringToException(exceptionNames));
    }

    @Test
    void testConvertStringToExceptionWithNonExceptionClassName() {

        List<String> exceptionNames = List.of("java.lang.String");

        assertThrows(
                IllegalArgumentException.class,
                () -> RetryableConsumerErrorHandler.convertStringToException(exceptionNames));
    }
}
