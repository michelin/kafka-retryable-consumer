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
 * Retry a bounded number of times, then give up and route the record to the dead letter topic.
 *
 * <p>This is the safety net for a record that will never be processed successfully, typically because its content is
 * wrong. Retrying it forever would block the partition, so after {@code retry.max} attempts the library publishes the
 * record and its error to the dead letter topic, commits the offset and moves on to the next record.
 *
 * <p>{@code limited-retry-example.yml} sets {@code retry.max: 2}, so the business code is called three times: the
 * initial attempt plus two retries.
 *
 * <p>The dead letter record is an Avro payload, so the dead letter producer needs a schema registry.
 */
@Slf4j
public class LimitedRetryToDeadLetterExample implements Closeable {

    /** Configuration of this example, loaded from the classpath. */
    public static final String CONFIG_FILE = "limited-retry-example.yml";

    private final RetryableConsumer<String, String> consumer;
    private final AtomicInteger attempts = new AtomicInteger();

    /** Build the example from its own configuration file. */
    public LimitedRetryToDeadLetterExample() throws KafkaConfigurationException {
        this(KafkaRetryableConfiguration.load(CONFIG_FILE));
    }

    /** Build the example from an already loaded configuration, see {@link SimpleConsumerExample}. */
    public LimitedRetryToDeadLetterExample(KafkaRetryableConfiguration configuration) {
        this.consumer = new RetryableConsumer<>(configuration);
    }

    public Future<Void> start() {
        return consumer.listenAsync(this::process);
    }

    private void process(ConsumerRecord<String, String> record) {
        int attempt = attempts.incrementAndGet();
        log.info("Processing record key={}, attempt {}", record.key(), attempt);
        // This record will never be processed successfully: it ends up in the dead letter topic
        throw new IllegalStateException("Record " + record.key() + " cannot be processed");
    }

    public int getAttempts() {
        return attempts.get();
    }

    @Override
    public void close() {
        consumer.close();
    }

    public static void main(String[] args) throws Exception {
        try (LimitedRetryToDeadLetterExample example = new LimitedRetryToDeadLetterExample()) {
            example.start().get();
        }
    }
}
