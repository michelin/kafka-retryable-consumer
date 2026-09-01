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
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Future;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;

/**
 * Let the library find its configuration on its own.
 *
 * <p>Every other example of this module names the file it loads, because they all live in the same classpath and each
 * one needs its own settings. A real application usually has a single configuration and does not need to name it: this
 * is the shape your own code will most likely take.
 *
 * <p>{@link KafkaRetryableConfiguration#load()} looks for {@code application.yaml}, {@code application.yml} then
 * {@code application.properties} in the classpath, in that order. The {@link RetryableConsumer} constructor taking only
 * a name does exactly that under the hood, so dropping the file in {@code src/main/resources} is all it takes: see
 * {@code src/main/resources/application.yml}.
 */
@Slf4j
public class DefaultConfigurationFileExample implements Example {

    private final RetryableConsumer<String, String> consumer;

    /**
     * Records handed over to the business code, exposed so that the integration test can assert on them.
     *
     * <p>Test hook only, do not copy this into a real consumer: accumulating every record in memory grows without bound
     * and eventually exhausts the heap. Real business code should hand the record over to its destination and keep
     * nothing. {@link CopyOnWriteArrayList} is deliberate, as the test thread iterates this list while the consumer
     * thread writes to it, which a plain synchronized list could not support safely.
     */
    @Getter
    private final List<String> processedValues = new CopyOnWriteArrayList<>();

    /**
     * Build the example without naming any configuration file.
     *
     * <p>This single constructor call is the whole point of the example: the consumer discovers {@code application.yml}
     * by itself.
     *
     * @throws KafkaConfigurationException if no configuration file can be found in the classpath
     */
    public DefaultConfigurationFileExample() throws KafkaConfigurationException {
        this.consumer = new RetryableConsumer<>("default-configuration-example");
    }

    /** Build the example from an already loaded configuration, see {@link SimpleConsumerExample}. */
    public DefaultConfigurationFileExample(KafkaRetryableConfiguration configuration) {
        this.consumer = new RetryableConsumer<>(configuration);
    }

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

    public static void main(String[] args) {
        // Topics, retry policy, dead letter topic and Kafka properties all come from application.yml
        System.exit(ExampleRunner.run(DefaultConfigurationFileExample::new));
    }
}
