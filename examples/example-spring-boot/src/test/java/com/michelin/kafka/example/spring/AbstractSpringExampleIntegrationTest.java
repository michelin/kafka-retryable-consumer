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
package com.michelin.kafka.example.spring;

import com.michelin.kafka.test.integration.AbstractKafkaIntegrationTest;
import java.util.ArrayList;
import java.util.List;
import org.springframework.context.ConfigurableApplicationContext;

/**
 * Boots a Spring Boot example against the embedded broker.
 *
 * <p>Examples are booted through their own {@code run} method, the one their {@code main} delegates to, so that the
 * test cannot drift from the entry point a user actually runs: the auto-configuration, the selection of the example
 * configuration file, the property binding and the {@code ApplicationRunner} beans are all exercised.
 *
 * <p>Only what cannot be known in advance is overridden, as command line arguments so that they win over the
 * configuration file: the address of the embedded broker, the schema registry, and the topic names, which must be
 * unique to keep the tests isolated.
 */
abstract class AbstractSpringExampleIntegrationTest extends AbstractKafkaIntegrationTest {

    /** The {@code run} method of an example, the entry point its {@code main} delegates to. */
    @FunctionalInterface
    protected interface ExampleLauncher {
        ConfigurableApplicationContext run(String... args);
    }

    /**
     * Start a Spring Boot example through its own entry point.
     *
     * @param launcher the {@code run} method of the example, such as {@code SimpleConsumerRunner::run}
     * @param topic the topic the consumer must listen to
     * @param deadLetterTopic the dead letter topic
     * @param extraArguments example specific command line arguments
     * @return the running application context, to be closed by the caller
     */
    protected ConfigurableApplicationContext startExample(
            ExampleLauncher launcher, String topic, String deadLetterTopic, String... extraArguments) {

        List<String> arguments = new ArrayList<>(List.of(
                "--kafka.retryable.consumer.topics=" + topic,
                "--kafka.retryable.consumer.properties.bootstrap.servers=" + bootstrapServers(),
                "--kafka.retryable.consumer.properties.group.id=" + uniqueName("group-" + topic),
                "--kafka.retryable.dead-letter.producer.topic=" + deadLetterTopic,
                "--kafka.retryable.dead-letter.producer.properties.bootstrap.servers=" + bootstrapServers(),
                "--kafka.retryable.dead-letter.producer.properties.schema.registry.url=" + SCHEMA_REGISTRY_URL));
        arguments.addAll(List.of(extraArguments));

        return launcher.run(arguments.toArray(new String[0]));
    }
}
