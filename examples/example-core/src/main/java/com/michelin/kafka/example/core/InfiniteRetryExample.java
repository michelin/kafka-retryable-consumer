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
import java.util.concurrent.atomic.AtomicInteger;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;

/**
 * Retry a failing record forever, until it finally succeeds.
 *
 * <p>This is the behaviour you want when the failure comes from an unavailable downstream system: losing the record or
 * pushing it to a dead letter topic would be wrong, the only sensible thing to do is to wait and try again.
 *
 * <p>It is enabled by {@code retry.max: 0} in {@code infinite-retry-example.yml}, which is also the default value. The
 * offset is never advanced, so the record is replayed until the processing returns normally. Nothing is ever sent to
 * the dead letter topic.
 */
@Slf4j
public class InfiniteRetryExample implements Closeable {

    /** Configuration of this example, loaded from the classpath. */
    public static final String CONFIG_FILE = "infinite-retry-example.yml";

    private final RetryableConsumer<String, String> consumer;
    private final int failureCount;

    /** Number of times the business code has been called, retries included. */
    private final AtomicInteger attempts = new AtomicInteger();

    @Getter
    private final List<String> processedValues = new CopyOnWriteArrayList<>();

    /**
     * Build the example from its own configuration file.
     *
     * @param failureCount how many times the simulated downstream system fails before recovering
     */
    public InfiniteRetryExample(int failureCount) throws KafkaConfigurationException {
        this(KafkaRetryableConfiguration.load(CONFIG_FILE), failureCount);
    }

    /** Build the example from an already loaded configuration, see {@link SimpleConsumerExample}. */
    public InfiniteRetryExample(KafkaRetryableConfiguration configuration, int failureCount) {
        this.consumer = new RetryableConsumer<>(configuration);
        this.failureCount = failureCount;
    }

    public Future<Void> start() {
        return consumer.listenAsync(this::process);
    }

    private void process(ConsumerRecord<String, String> record) {
        int attempt = attempts.incrementAndGet();
        if (attempt <= failureCount) {
            // Any exception which is not declared as non retryable triggers a retry
            throw new IllegalStateException("Downstream system unavailable, attempt " + attempt);
        }
        log.info("Record key={} finally processed after {} attempts", record.key(), attempt);
        processedValues.add(record.value());
    }

    public int getAttempts() {
        return attempts.get();
    }

    @Override
    public void close() {
        consumer.close();
    }

    public static void main(String[] args) throws Exception {
        try (InfiniteRetryExample example = new InfiniteRetryExample(3)) {
            example.start().get();
        }
    }
}
