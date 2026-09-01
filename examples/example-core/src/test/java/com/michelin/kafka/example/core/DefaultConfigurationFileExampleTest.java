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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.michelin.kafka.configuration.KafkaConfigurationException;
import com.michelin.kafka.configuration.KafkaRetryableConfiguration;
import java.util.List;
import org.junit.jupiter.api.Test;

/**
 * Checks that the {@code application.yml} shipped with this module is actually understood by the library.
 *
 * <p>No broker is needed: the point is the mapping between the file and the configuration object, which is exactly what
 * silently breaks when a property is misspelled.
 */
class DefaultConfigurationFileExampleTest {

    @Test
    void shouldLoadEveryDocumentedPropertyFromTheClasspath() throws KafkaConfigurationException {
        KafkaRetryableConfiguration configuration = DefaultConfigurationFileExample.loadConfiguration();

        assertEquals(
                List.of("default-configuration-example"),
                configuration.getConsumer().getTopics());
        assertEquals(3L, configuration.getConsumer().getRetryMax());
        assertEquals(500L, configuration.getConsumer().getRetryBackoffMs());
        assertEquals(1000L, configuration.getConsumer().getPollBackoffMs());
        assertFalse(configuration.getConsumer().getStopOnError());
        assertEquals(
                List.of("java.lang.IllegalArgumentException"),
                configuration.getConsumer().getNotRetryableExceptions());
        assertEquals(
                "localhost:9092", configuration.getConsumer().getProperties().get("bootstrap.servers"));

        assertEquals(
                "default-configuration-example-dlt", configuration.getDeadLetter().getTopic());
        assertTrue(configuration.getDeadLetter().getProperties().containsKey("schema.registry.url"));
    }
}
