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

import com.michelin.kafka.ErrorProcessor;
import com.michelin.kafka.avro.GenericErrorModel;
import com.michelin.kafka.configuration.KafkaRetryableConfiguration;
import com.michelin.kafka.configuration.RetryableConsumerConfiguration;
import com.michelin.kafka.error.DeadLetterProducer;
import com.michelin.kafka.error.DefaultErrorProcessor;
import com.michelin.kafka.error.RetryableConsumerErrorHandler;
import java.io.*;
import java.util.Arrays;
import java.util.List;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.errors.RecordDeserializationException;
import org.junit.jupiter.api.AfterEach;
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

    @Captor
    private ArgumentCaptor<Throwable> throwableCaptor;

    @Captor
    private ArgumentCaptor<ConsumerRecord<String, String>> recordCaptor;

    @Captor
    private ArgumentCaptor<Long> retryCountCaptor;

    /** Shared test Dead Letter Topic consumer configuration */
    @Mock
    private static DeadLetterProducer mockDeadLetterProducer;

    @Mock
    private KafkaRetryableConfiguration retryableConfiguration;

    @Mock
    private RetryableConsumerConfiguration consumerConfiguration;

    private RetryableConsumerErrorHandler<String, String> errorHandler;
    private RetryableConsumerErrorHandler<String, SerializableObject> serializableObjectErrorHandler;
    private RetryableConsumerErrorHandler<SerializableObject, String> serializableObjectKeyErrorHandler;

    @Mock
    ErrorProcessor<ConsumerRecord<String, String>> customErrorProcessor;

    AutoCloseable mockCloseable;

    @BeforeEach
    void setUp() {
        mockCloseable = MockitoAnnotations.openMocks(this);
        List<String> notRetryableExceptions =
                Arrays.asList("java.lang.IllegalArgumentException", "java.lang.NullPointerException");
        when(retryableConfiguration.getConsumer()).thenReturn(consumerConfiguration);
        when(consumerConfiguration.getNotRetryableExceptions()).thenReturn(notRetryableExceptions);

        keyCaptor = ArgumentCaptor.forClass(String.class);
        valueCaptor = ArgumentCaptor.forClass(GenericErrorModel.class);

        doNothing().when(mockDeadLetterProducer).send(any(), any());
        ErrorProcessor<ConsumerRecord<String, String>> errorProcessor =
                new DefaultErrorProcessor<>(mockDeadLetterProducer);
        errorHandler = new RetryableConsumerErrorHandler<>(retryableConfiguration, errorProcessor);

        ErrorProcessor<ConsumerRecord<String, SerializableObject>> serializableObjectErrorProcessor =
                new DefaultErrorProcessor<>(mockDeadLetterProducer);
        serializableObjectErrorHandler =
                new RetryableConsumerErrorHandler<>(retryableConfiguration, serializableObjectErrorProcessor);

        ErrorProcessor<ConsumerRecord<SerializableObject, String>> serializableObjectKeyErrorProcessor =
                new DefaultErrorProcessor<>(mockDeadLetterProducer);
        serializableObjectKeyErrorHandler =
                new RetryableConsumerErrorHandler<>(retryableConfiguration, serializableObjectKeyErrorProcessor);
    }

    @AfterEach
    void tearDown() throws Exception {
        if (mockCloseable != null) {
            mockCloseable.close();
        }
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
    void shouldHandleCustomErrorProcessor() {
        // Given
        RetryableConsumerErrorHandler<String, String> errorHandlerWithCustomErrorProcessor =
                new RetryableConsumerErrorHandler<>(retryableConfiguration, customErrorProcessor);

        String cause = "Test exception message";
        Long offset = 1L;
        Integer partition = 1;
        String topic = "topic";
        Throwable exception = new RuntimeException(cause);
        String key = "key";
        String value = "value";
        doNothing().when(mockDeadLetterProducer).send(any(), any());
        ConsumerRecord<String, String> record = new ConsumerRecord<>(topic, partition, offset, key, value);

        // When
        errorHandlerWithCustomErrorProcessor.handleError(exception, record, 2L);

        // Then
        verify(mockDeadLetterProducer, times(0)).send(any(), any()); // Verify the default error processor is not used
        verify(customErrorProcessor, times(1)) // Verify the custom error processor is used
                .processError(throwableCaptor.capture(), recordCaptor.capture(), retryCountCaptor.capture());

        assertEquals(exception.getMessage(), throwableCaptor.getValue().getMessage());
        assertEquals(record.key(), recordCaptor.getValue().key());
        assertEquals(record.value(), recordCaptor.getValue().value());
        assertEquals(record.topic(), recordCaptor.getValue().topic());
        assertEquals(record.offset(), recordCaptor.getValue().offset());
        assertEquals(record.partition(), recordCaptor.getValue().partition());
        assertEquals(2L, retryCountCaptor.getValue());
    }

    public static class SerializableObject implements Serializable {
        @Serial
        private static final long serialVersionUID = 1L;
    }

    @Test
    void shouldHandleErrorWhenValueIsNotString() {
        // Given
        String cause = "Test exception message";
        Long offset = 1L;
        Integer partition = 1;
        String topic = "topic";
        Throwable exception = new RuntimeException(cause);
        String key = "key";
        SerializableObject value = new SerializableObject();
        doNothing().when(mockDeadLetterProducer).send(any(), any());

        ConsumerRecord<String, SerializableObject> record = new ConsumerRecord<>(topic, partition, offset, key, value);

        // When
        serializableObjectErrorHandler.handleError(exception, record);

        // Then
        verify(mockDeadLetterProducer, times(1)).send(keyCaptor.capture(), valueCaptor.capture());

        GenericErrorModel capturedErrorModel = valueCaptor.getValue();

        assertEquals(cause, capturedErrorModel.getCause());
        assertNotNull(capturedErrorModel.getContextMessage());
        assertEquals(offset, capturedErrorModel.getOffset());
        assertEquals(partition, capturedErrorModel.getPartition());
        assertEquals(topic, capturedErrorModel.getTopic());
        assertNotNull(capturedErrorModel.getByteValue()); // Check that ByteValue is not null when value is not a String
    }

    @Test
    void shouldHandleErrorWhenKeyIsNotString() {
        // Given
        String cause = "Test exception message";
        Long offset = 1L;
        Integer partition = 1;
        String topic = "topic";

        Throwable exception = new RuntimeException(cause);
        SerializableObject key = new SerializableObject();
        String value = "value";
        doNothing().when(mockDeadLetterProducer).send(any(), any());

        ConsumerRecord<SerializableObject, String> record = new ConsumerRecord<>(topic, partition, offset, key, value);

        // When
        serializableObjectKeyErrorHandler.handleError(exception, record);

        // Then
        verify(mockDeadLetterProducer, times(1)).send(keyCaptor.capture(), valueCaptor.capture());

        GenericErrorModel capturedErrorModel = valueCaptor.getValue();

        assertEquals(cause, capturedErrorModel.getCause());
        assertNotNull(capturedErrorModel.getContextMessage());
        assertEquals(offset, capturedErrorModel.getOffset());
        assertEquals(partition, capturedErrorModel.getPartition());
        assertEquals(topic, capturedErrorModel.getTopic());
        assertNotNull(capturedErrorModel.getByteKey()); // Check that ByteKey is not null when key is not a String
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
