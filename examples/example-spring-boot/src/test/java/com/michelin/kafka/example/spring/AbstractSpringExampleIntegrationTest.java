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

import com.michelin.kafka.configuration.KafkaRetryableConfiguration;
import com.michelin.kafka.test.integration.AbstractKafkaIntegrationTest;
import java.util.ArrayList;
import java.util.List;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.springframework.boot.WebApplicationType;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.context.ConfigurableApplicationContext;

/**
 * Boots a Spring Boot example against the embedded broker.
 *
 * <p>The examples are started exactly as they would be in production, through {@link SpringApplicationBuilder}: the
 * auto-configuration, the property binding and the {@code ApplicationRunner} beans are all exercised. Only the broker
 * addresses are overridden, as command line arguments so that they win over {@code application.yml}.
 */
abstract class AbstractSpringExampleIntegrationTest extends AbstractKafkaIntegrationTest {

    /**
     * Start the Spring Boot application declared by {@code exampleClass}.
     *
     * @param exampleClass the example to boot
     * @param topic the topic the consumer must listen to
     * @param deadLetterTopic the dead letter topic
     * @param extraArguments example specific command line arguments
     * @return the running application context, to be closed by the caller
     */
    protected ConfigurableApplicationContext startExample(
            Class<?> exampleClass, String topic, String deadLetterTopic, String... extraArguments) {

        // Reuse the helper of the core harness to get the address of the embedded broker
        KafkaRetryableConfiguration configuration = newConfiguration(topic, deadLetterTopic);
        String bootstrapServers =
                (String) configuration.getConsumer().getProperties().get(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG);

        List<String> arguments = new ArrayList<>(List.of(
                "--kafka.retryable.consumer.topics=" + topic,
                "--kafka.retryable.consumer.properties.bootstrap.servers=" + bootstrapServers,
                "--kafka.retryable.consumer.properties.group.id=" + uniqueName("group-" + topic),
                "--kafka.retryable.dead-letter.producer.topic=" + deadLetterTopic,
                "--kafka.retryable.dead-letter.producer.properties.bootstrap.servers=" + bootstrapServers,
                "--kafka.retryable.dead-letter.producer.properties.schema.registry.url=" + SCHEMA_REGISTRY_URL));
        arguments.addAll(List.of(extraArguments));

        return new SpringApplicationBuilder(exampleClass)
                .web(WebApplicationType.NONE)
                .run(arguments.toArray(new String[0]));
    }
}
