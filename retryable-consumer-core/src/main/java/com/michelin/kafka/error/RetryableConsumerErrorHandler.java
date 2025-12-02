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

import com.michelin.kafka.ErrorProcessor;
import com.michelin.kafka.configuration.KafkaRetryableConfiguration;
import java.io.*;
import java.util.*;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.errors.RecordDeserializationException;

@Slf4j
@AllArgsConstructor
public class RetryableConsumerErrorHandler<K, V> {

    private final ErrorProcessor<ConsumerRecord<K, V>> errorProcessor;

    private final List<Class<? extends Exception>> notRetryableExceptions = new ArrayList<>();

    public RetryableConsumerErrorHandler(
            KafkaRetryableConfiguration kafkaRetryableConfiguration,
            ErrorProcessor<ConsumerRecord<K, V>> errorProcessor) {
        this.errorProcessor = errorProcessor;
        convertStringToException(kafkaRetryableConfiguration.getConsumer().getNotRetryableExceptions())
                .forEach(this::addNotRetryableExceptions);
        this.initDefaultRetryableExceptions();
    }

    public RetryableConsumerErrorHandler(KafkaRetryableConfiguration kafkaRetryableConfiguration) {
        this(kafkaRetryableConfiguration, new DefaultErrorProcessor<>(kafkaRetryableConfiguration));
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

    public void handleError(Throwable throwable, ConsumerRecord<K, V> consumerRecord, Long retryCount) {
        this.errorProcessor.processError(throwable, consumerRecord, retryCount);
    }

    public void handleError(Throwable throwable, ConsumerRecord<K, V> consumerRecord) {
        this.errorProcessor.processError(throwable, consumerRecord, 0L);
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
