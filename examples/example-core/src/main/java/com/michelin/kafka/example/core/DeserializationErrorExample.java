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
 * Survive a poison pill: a record the consumer cannot even deserialize.
 *
 * <p>A malformed record is a dead end that plain Kafka clients handle badly: {@code poll} keeps throwing on the same
 * offset, and the consumer is stuck forever on a record it will never be able to read.
 *
 * <p>The library treats {@code RecordDeserializationException} as non retryable by default. The faulty offset is
 * skipped, the error is routed to the dead letter topic, and the consumer resumes with the next record. Note that the
 * dead letter entry carries no record: by definition its content could not be decoded.
 *
 * <p>{@code deserialization-error-example.yml} declares an {@code IntegerDeserializer} for the values, so any non
 * numeric payload published on the topic is a poison pill.
 */
@Slf4j
public class DeserializationErrorExample implements Closeable {

    /** Configuration of this example, loaded from the classpath. */
    public static final String CONFIG_FILE = "deserialization-error-example.yml";

    private final RetryableConsumer<String, Integer> consumer;

    @Getter
    private final List<Integer> processedValues = new CopyOnWriteArrayList<>();

    /** Build the example from its own configuration file. */
    public DeserializationErrorExample() throws KafkaConfigurationException {
        this(KafkaRetryableConfiguration.load(CONFIG_FILE));
    }

    /** Build the example from an already loaded configuration, see {@link SimpleConsumerExample}. */
    public DeserializationErrorExample(KafkaRetryableConfiguration configuration) {
        this.consumer = new RetryableConsumer<>(configuration);
    }

    public Future<Void> start() {
        return consumer.listenAsync(this::process);
    }

    private void process(ConsumerRecord<String, Integer> record) {
        log.info("Processing record key={} value={}", record.key(), record.value());
        processedValues.add(record.value());
    }

    @Override
    public void close() {
        consumer.close();
    }

    public static void main(String[] args) throws Exception {
        try (DeserializationErrorExample example = new DeserializationErrorExample()) {
            example.start().get();
        }
    }
}
