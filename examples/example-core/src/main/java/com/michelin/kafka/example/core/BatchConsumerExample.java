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

import com.michelin.kafka.RetryableBatchConsumer;
import com.michelin.kafka.configuration.KafkaConfigurationException;
import com.michelin.kafka.configuration.KafkaRetryableConfiguration;
import java.io.Closeable;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecords;

/**
 * Consume records by batch instead of one by one.
 *
 * <p>{@link RetryableBatchConsumer} hands over the whole result of a poll in a single call, which is what you want when
 * the processing has a fixed cost per call: a bulk database insert, a single HTTP request carrying many items, etc.
 *
 * <p>The trade-off is the granularity of the error handling: a failure concerns the batch, not a specific record, so
 * the retry replays the whole batch. Batches are sized by the standard {@code max.poll.records} consumer property, set
 * in {@code batch-consumer-example.yml}.
 */
@Slf4j
public class BatchConsumerExample implements Closeable {

    /** Configuration of this example, loaded from the classpath. */
    public static final String CONFIG_FILE = "batch-consumer-example.yml";

    private final RetryableBatchConsumer<String, String> consumer;

    /** Size of every batch received, exposed so that the integration test can assert records came in grouped. */
    @Getter
    private final List<Integer> batchSizes = new CopyOnWriteArrayList<>();

    private final AtomicInteger processedRecordCount = new AtomicInteger();

    /** Build the example from its own configuration file. */
    public BatchConsumerExample() throws KafkaConfigurationException {
        this(KafkaRetryableConfiguration.load(CONFIG_FILE));
    }

    /** Build the example from an already loaded configuration, see {@link SimpleConsumerExample}. */
    public BatchConsumerExample(KafkaRetryableConfiguration configuration) {
        this.consumer = new RetryableBatchConsumer<>(configuration);
    }

    public Future<Void> start() {
        return consumer.listenAsync(this::processBatch);
    }

    /** The business code. Called once per poll, with every record it returned. */
    private void processBatch(ConsumerRecords<String, String> records) {
        if (records.isEmpty()) {
            return;
        }
        log.info("Processing a batch of {} records", records.count());
        batchSizes.add(records.count());
        records.forEach(record -> processedRecordCount.incrementAndGet());
    }

    public int getProcessedRecordCount() {
        return processedRecordCount.get();
    }

    @Override
    public void close() {
        consumer.close();
    }

    public static void main(String[] args) throws Exception {
        try (BatchConsumerExample example = new BatchConsumerExample()) {
            example.start().get();
        }
    }
}
