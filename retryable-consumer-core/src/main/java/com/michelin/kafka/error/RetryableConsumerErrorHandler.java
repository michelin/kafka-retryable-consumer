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
package com.michelin.kafka.error;

import com.michelin.kafka.avro.GenericErrorModel;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.errors.RecordDeserializationException;

import java.io.*;
import java.nio.ByteBuffer;
import java.util.*;

@Slf4j
@AllArgsConstructor
public class RetryableConsumerErrorHandler<K, V> {

    private final DeadLetterProducer deadLetterProducer;

    private final List<Class<? extends Exception>> notRetryableExceptions = new ArrayList<>();

    public RetryableConsumerErrorHandler(
            DeadLetterProducer deadLetterProducer, Collection<String> notRetryableExceptions) {
        this.deadLetterProducer = deadLetterProducer;
        convertStringToException(notRetryableExceptions).forEach(this::addNotRetryableExceptions);
        this.initDefaultRetryableExceptions();
    }

    private void initDefaultRetryableExceptions() {
        this.addNotRetryableExceptions(
                RecordDeserializationException.class, NoSuchMethodException.class, ClassCastException.class);
    }

    @SafeVarargs
    public final void addNotRetryableExceptions(Class<? extends Exception>... exceptionTypes) {
        notRetryableExceptions.addAll(Arrays.asList(exceptionTypes));
    }

    public boolean isExceptionRetryable(Class<? extends Exception> exceptionType) {
        return !notRetryableExceptions.contains(exceptionType);
    }

    public void handleConsumerDeserializationError(RecordDeserializationException e) {
        if (e != null) {
            this.handleError(
                    e.getMessage(),
                    null,
                    e.offset(),
                    e.topicPartition().partition(),
                    e.topicPartition().topic(),
                    e,
                    null,
                    null);
        }
    }

    public void handleError(Throwable throwable, ConsumerRecord<K, V> consumerRecord, Long retryNumber) {
        if (throwable != null) {
            this.handleError(
                    throwable.getMessage(),
                    "Error processing record after" + retryNumber + " retry(ies).",
                    consumerRecord.offset(),
                    consumerRecord.partition(),
                    consumerRecord.topic(),
                    throwable,
                    consumerRecord.key(),
                    consumerRecord.value());
        }
    }

    public void handleError(Throwable throwable, ConsumerRecord<K, V> consumerRecord) {
        handleError(throwable, consumerRecord, 0L);
    }

    public void handleError(
            String cause,
            String context,
            Long offset,
            Integer partition,
            String topic,
            Throwable exception,
            K key,
            V value) {
        try {
            GenericErrorModel errorAvroObject = GenericErrorModel.newBuilder()
                    .setCause(cause)
                    .setContextMessage(context)
                    .setOffset(offset)
                    .setPartition(partition)
                    .setTopic(topic)
                    .build();
            if (exception != null) {
                StringWriter sw = new StringWriter();
                PrintWriter pw = new PrintWriter(sw);
                exception.printStackTrace(pw);
                errorAvroObject.setStack(sw.toString());
            }
            if (value != null) {
                if (value instanceof String s) {
                    errorAvroObject.setValue(s);
                } else {
                    errorAvroObject.setByteValue(toByteBuffer((Serializable) value));
                }
            }
            if (key != null) {
                if (key instanceof String s) {
                    errorAvroObject.setKey(s);
                } else {
                    errorAvroObject.setByteKey(toByteBuffer((Serializable) key));
                }
            }

            // Build the producer and send message
            deadLetterProducer.send(UUID.randomUUID().toString(), errorAvroObject);

        } catch (Exception e) {

            log.error("An error occurred during error management... good luck with that!", e);
        }
    }

    public static ByteBuffer toByteBuffer(Serializable obj) throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        ObjectOutputStream oos = new ObjectOutputStream(baos);
        oos.writeObject(obj);
        oos.flush();
        ByteBuffer buffer = ByteBuffer.wrap(baos.toByteArray());
        oos.close();
        baos.close();
        return buffer;
    }

    public static List<Class<? extends Exception>> convertStringToException(Collection<String> exceptionNames) {
        List<Class<? extends Exception>> exceptionClasses = new ArrayList<>();
        for (String exceptionName : exceptionNames) {
            try {
                Class<?> clazz = Class.forName(exceptionName);
                if (Exception.class.isAssignableFrom(clazz)) {
                    //noinspection unchecked
                    exceptionClasses.add((Class<? extends Exception>) clazz);
                } else {
                    throw new IllegalArgumentException(
                            "Parameter 'not-retryable-exceptions' must be a list of exception classes");
                }
            } catch (Exception e) {
                log.error("An error occurred while converting string to exception", e);
                throw new IllegalArgumentException(
                        "Parameter 'not-retryable-exceptions' must be a list of exception classes");
            }
        }
        return exceptionClasses;
    }
}
