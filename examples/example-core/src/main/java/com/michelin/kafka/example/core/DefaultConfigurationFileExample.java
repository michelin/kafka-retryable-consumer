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
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;

/**
 * Let the library find its configuration on its own.
 *
 * <p>Every other example of this module names the file it loads, because they all live in the same classpath and each
 * one needs its own settings. A real application usually has a single configuration, and does not need to name it at
 * all.
 *
 * <p>{@link KafkaRetryableConfiguration#load()} looks for {@code application.yaml}, {@code application.yml} then
 * {@code application.properties} in the classpath, in that order. The {@link RetryableConsumer} constructor taking only
 * a name does exactly that under the hood, so dropping the file in {@code src/main/resources} is all it takes: see
 * {@code src/main/resources/application.yml}.
 */
@Slf4j
public class DefaultConfigurationFileExample {

    private DefaultConfigurationFileExample() {
        // Only a main method: the point of this example is the configuration file
    }

    /**
     * Load the configuration bundled in the classpath, without naming it.
     *
     * @return the configuration declared in {@code application.yml}
     * @throws KafkaConfigurationException if no configuration file can be found
     */
    public static KafkaRetryableConfiguration loadConfiguration() throws KafkaConfigurationException {
        return KafkaRetryableConfiguration.load();
    }

    private static void process(ConsumerRecord<String, String> record) {
        log.info("Processing record key={} value={}", record.key(), record.value());
    }

    public static void main(String[] args) throws Exception {
        // Topics, retry policy, dead letter topic and Kafka properties all come from application.yml
        try (RetryableConsumer<String, String> consumer = new RetryableConsumer<>("default-configuration-example")) {
            consumer.listen(DefaultConfigurationFileExample::process);
        }
    }
}
