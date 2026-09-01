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
 */package com.michelin.kafka.example.core;

import com.michelin.kafka.RetryableConsumer;
import com.michelin.kafka.configuration.KafkaConfigurationException;
import com.michelin.kafka.configuration.KafkaRetryableConfiguration;
import java.io.Closeable;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;

/**
 * Declare the exceptions that must never be retried.
 *
 * <p>Retrying is only useful when the cause is transient. A functional error, such as a malformed payload, will fail
 * exactly the same way on every attempt: retrying it only delays the inevitable and blocks the partition meanwhile.
 *
 * <p>Listing the exception in {@code not-retryable-exceptions} short circuits the retry mechanism entirely: the record
 * goes to the dead letter topic on the very first failure, whatever the value of {@code retry.max}. Exceptions are
 * declared by class name, which is what makes them expressible in {@code non-retryable-exception-example.yml}.
 *
 * <p>{@code RecordDeserializationException}, {@code NoSuchMethodException} and {@code ClassCastException} are treated
 * as non retryable out of the box, no configuration needed.
 */
@Slf4j
public class NonRetryableExceptionExample implements Closeable {

    /** Configuration of this example, loaded from the classpath. */
    public static final String CONFIG_FILE = "non-retryable-exception-example.yml";

    private final RetryableConsumer<String, String> consumer;
    private final AtomicInteger attempts = new AtomicInteger();

    /** Build the example from its own configuration file. */
    public NonRetryableExceptionExample() throws KafkaConfigurationException {
        this(KafkaRetryableConfiguration.load(CONFIG_FILE));
    }

    /** Build the example from an already loaded configuration, see {@link SimpleConsumerExample}. */
    public NonRetryableExceptionExample(KafkaRetryableConfiguration configuration) {
        this.consumer = new RetryableConsumer<>(configuration);
    }

    public Future<Void> start() {
        return consumer.listenAsync(this::process);
    }

    private void process(ConsumerRecord<String, String> record) throws InvalidOrderException {
        attempts.incrementAndGet();
        log.info("Processing record key={}", record.key());
        // Replaying this record would fail identically: straight to the dead letter topic
        throw new InvalidOrderException("Order " + record.key() + " is not valid");
    }

    public int getAttempts() {
        return attempts.get();
    }

    @Override
    public void close() {
        consumer.close();
    }

    /** A business error: the record is wrong, no amount of retrying will fix it. */
    public static class InvalidOrderException extends Exception {
        public InvalidOrderException(String message) {
            super(message);
        }
    }

    public static void main(String[] args) throws Exception {
        try (NonRetryableExceptionExample example = new NonRetryableExceptionExample()) {
            example.start().get();
        }
    }
}
