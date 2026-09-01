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
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Future;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;

/**
 * The simplest possible usage: consume records one by one and process them.
 *
 * <p>Nothing here is specific to the retry mechanism. It shows the baseline every other example builds on: the consumer
 * is given a configuration and a processing function, and calls that function once per record. Offsets are committed by
 * the library only once the processing returned normally, so a record is never acknowledged before it has been handled.
 *
 * <p>Everything is declared in {@code simple-consumer-example.yml}, nothing is configured in Java.
 */
@Slf4j
public class SimpleConsumerExample implements Closeable {

    /** Configuration of this example, loaded from the classpath. */
    public static final String CONFIG_FILE = "simple-consumer-example.yml";

    private final RetryableConsumer<String, String> consumer;

    /** Records handed over to the business code, exposed so that the integration test can assert on them. */
    @Getter
    private final List<String> processedValues = new CopyOnWriteArrayList<>();

    /** Build the example from its own configuration file. */
    public SimpleConsumerExample() throws KafkaConfigurationException {
        this(KafkaRetryableConfiguration.load(CONFIG_FILE));
    }

    /**
     * Build the example from an already loaded configuration.
     *
     * <p>Used by the integration test, which loads {@link #CONFIG_FILE} then overrides the broker coordinates to point
     * at an embedded cluster whose address is only known at runtime.
     */
    public SimpleConsumerExample(KafkaRetryableConfiguration configuration) {
        this.consumer = new RetryableConsumer<>(configuration);
    }

    /**
     * Start consuming in a background thread.
     *
     * <p>{@code listenAsync} is used rather than {@code listen} so that the caller keeps the hand and can stop the
     * consumer. {@code listen} does exactly the same thing but blocks the calling thread.
     *
     * @return a future completing when the consumer stops
     */
    public Future<Void> start() {
        return consumer.listenAsync(this::process);
    }

    /** The business code. Called once per record, by the consumer thread. */
    private void process(ConsumerRecord<String, String> record) {
        log.info("Processing record key={} value={}", record.key(), record.value());
        processedValues.add(record.value());
    }

    @Override
    public void close() {
        consumer.close();
    }

    public static void main(String[] args) throws Exception {
        try (SimpleConsumerExample example = new SimpleConsumerExample()) {
            // Blocks until the consumer is stopped
            example.start().get();
        }
    }
}
