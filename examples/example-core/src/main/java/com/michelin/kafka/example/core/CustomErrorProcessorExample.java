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
package com.michelin.kafka.example.core;

import com.michelin.kafka.ErrorProcessor;
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
 * Replace the dead letter topic by your own error handling.
 *
 * <p>When the retry budget is exhausted the library delegates to an {@link ErrorProcessor}. The default one publishes
 * the record to the dead letter topic; providing your own replaces that behaviour entirely, which is how you plug in an
 * alerting system, a relational database or an audit log instead of, or in addition to, a Kafka topic.
 *
 * <p>Because the default processor is replaced, nothing is written to the dead letter topic any more. This is the one
 * capability that cannot be expressed in a configuration file: {@code custom-error-processor-example.yml} only carries
 * the retry policy, the error handling itself is the Java method below.
 *
 * <p>Note that the processor also receives the retry count, which is handy to tell a first failure from a definitive
 * one.
 */
@Slf4j
public class CustomErrorProcessorExample implements Closeable {

    /** Configuration of this example, loaded from the classpath. */
    public static final String CONFIG_FILE = "custom-error-processor-example.yml";

    private final RetryableConsumer<String, String> consumer;

    /** Errors collected by the custom processor, in place of the dead letter topic. */
    @Getter
    private final List<String> collectedErrors = new CopyOnWriteArrayList<>();

    /** Build the example from its own configuration file. */
    public CustomErrorProcessorExample() throws KafkaConfigurationException {
        this(KafkaRetryableConfiguration.load(CONFIG_FILE));
    }

    /** Build the example from an already loaded configuration, see {@link SimpleConsumerExample}. */
    public CustomErrorProcessorExample(KafkaRetryableConfiguration configuration) {
        this.consumer = new RetryableConsumer<>(configuration, this::handleError);
    }

    public Future<Void> start() {
        return consumer.listenAsync(this::process);
    }

    private void process(ConsumerRecord<String, String> record) {
        log.info("Processing record key={}", record.key());
        throw new IllegalStateException("Processing failed for record " + record.key());
    }

    /** Called instead of the dead letter production, once the record is declared unrecoverable. */
    private void handleError(Throwable throwable, ConsumerRecord<String, String> record, Long retryCount) {
        log.error("Record key={} definitively failed after {} retries", record.key(), retryCount, throwable);
        collectedErrors.add(record.key() + " -> " + throwable.getMessage());
    }

    @Override
    public void close() {
        consumer.close();
    }

    public static void main(String[] args) throws Exception {
        try (CustomErrorProcessorExample example = new CustomErrorProcessorExample()) {
            example.start().get();
        }
    }
}
