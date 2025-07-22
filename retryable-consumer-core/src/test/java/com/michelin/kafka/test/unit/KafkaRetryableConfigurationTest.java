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
package com.michelin.kafka.test.unit;

import static org.junit.jupiter.api.Assertions.*;

import com.michelin.kafka.configuration.DeadLetterProducerConfiguration;
import com.michelin.kafka.configuration.KafkaConfigurationException;
import com.michelin.kafka.configuration.KafkaRetryableConfiguration;
import com.michelin.kafka.configuration.RetryableConsumerConfiguration;
import java.util.Properties;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

@Slf4j
class KafkaRetryableConfigurationTest {

    @BeforeAll
    static void setup() {
        log.info("Starting configuration tests...");
    }

    @AfterAll
    static void teardown() {
        log.info("Configuration test completed");
    }

    @Test
    void loadDefaultConfigurationTest() throws KafkaConfigurationException {
        KafkaRetryableConfiguration config = KafkaRetryableConfiguration.load();
        configurationAssertions(config);
    }

    @Test
    void loadPropertiesConfigurationTest() throws KafkaConfigurationException {
        KafkaRetryableConfiguration config = KafkaRetryableConfiguration.load("application.properties");
        configurationAssertions(config);
    }

    @Test
    void loadYmlConfigurationTest() throws KafkaConfigurationException {
        KafkaRetryableConfiguration config = KafkaRetryableConfiguration.load("application.yml");
        configurationAssertions(config);
    }

    private void configurationAssertions(KafkaRetryableConfiguration config) {
        assertNotNull(config, "KafkaRetryableConfiguration config not loaded");
        assertNotNull(config.getConsumer(), "Consumer configuration not loaded");
        assertNotNull(config.getDeadLetter(), "DeadLetter producer configuration not loaded");

        // Dead Letter producer config
        assertEquals("DL_TOPIC", config.getDeadLetter().getTopic());
        assertFalse(config.getDeadLetter().getProperties().isEmpty(), "DeadLetter Producer properties not loaded");
        Properties producerProps = config.getDeadLetter().getProperties();
        assertEquals("dl-producer-test", producerProps.get("application.id"));
        assertEquals("fake.server:9092", producerProps.get("bootstrap.servers"));

        // Consumer config
        assertTrue(
                config.getConsumer().getTopics().contains("TOPIC"), "Consumer topics does not contains expected topic");
        assertEquals(2345L, config.getConsumer().getPollBackoffMs());
        assertTrue(config.getConsumer().getStopOnError(), "stop-on-error should be true");

        Properties kafkaProps = config.getConsumer().getProperties();
        assertNotNull(kafkaProps, "kafka.properties not loaded");
        assertEquals("retryable-consumer-test", kafkaProps.get("application.id"));
        assertEquals("fake.server:9092", kafkaProps.get("bootstrap.servers"));
        assertTrue(kafkaProps.get("specific.avro.reader").equals(true)
                || kafkaProps.get("specific.avro.reader").equals("true"));

        assertEquals(2345L, config.getConsumer().getPollBackoffMs());
        assertEquals(2, config.getConsumer().getNotRetryableExceptions().size());
        assertTrue(config.getConsumer().getNotRetryableExceptions().contains("java.lang.NullPointerException"));
        assertTrue(config.getConsumer().getNotRetryableExceptions().contains("java.lang.IllegalArgumentException"));

        assertEquals(10L, config.getConsumer().getRetryMax());
        assertEquals(1000L, config.getConsumer().getRetryBackoffMs());
    }

    @Test
    void loadNotExistingFileTest() {
        Exception exception =
                assertThrows(KafkaConfigurationException.class, () -> KafkaRetryableConfiguration.load("test.yml"));
        assertTrue(exception.getMessage().contains("Cannot find configuration file 'test.yml'"));
    }

    @Test
    void loadNotKnownExtensionFileTest() {
        Exception exception = assertThrows(
                KafkaConfigurationException.class,
                () -> KafkaRetryableConfiguration.load("application-wrong-extension.app"));
        assertTrue(
                exception
                        .getMessage()
                        .contains(
                                "Unknown configuration file type 'application-wrong-extension.app'. Properties or a Yaml file expected"));
    }

    @Test
    void loadConfigFileWithoutKafkaConfigTest() {
        Exception exception = assertThrows(
                RuntimeException.class,
                () -> KafkaRetryableConfiguration.load("application-NoKafkaConsumerConfig.yml"));
        assertTrue(exception
                .getMessage()
                .contains("No 'kafka.retryable.consumer' configuration found in configuration file"));

        exception = assertThrows(
                RuntimeException.class,
                () -> KafkaRetryableConfiguration.load("application-NoKafkaDeadLetterProducerConfig.yml"));
        assertTrue(exception
                .getMessage()
                .contains("No 'kafka.retryable.dead-letter.producer' configuration found in configuration file"));
    }

    @Test
    void testBuilder() {
        // Given
        String expectedName = "testName";
        RetryableConsumerConfiguration expectedConsumer = new RetryableConsumerConfiguration();
        DeadLetterProducerConfiguration expectedDeadLetter = new DeadLetterProducerConfiguration();

        // When
        KafkaRetryableConfiguration config = KafkaRetryableConfiguration.builder()
                .name(expectedName)
                .consumer(expectedConsumer)
                .deadLetter(expectedDeadLetter)
                .build();

        // Then
        assertEquals(expectedName, config.getName());
        assertEquals(expectedConsumer, config.getConsumer());
        assertEquals(expectedDeadLetter, config.getDeadLetter());
    }

    @Test
    void testAllArgsConstructor() {
        // Given
        String expectedName = "testName";
        RetryableConsumerConfiguration expectedConsumer = new RetryableConsumerConfiguration();
        DeadLetterProducerConfiguration expectedDeadLetter = new DeadLetterProducerConfiguration();

        // When
        KafkaRetryableConfiguration config =
                new KafkaRetryableConfiguration(expectedName, expectedConsumer, expectedDeadLetter);

        // Then
        assertEquals(expectedName, config.getName());
        assertEquals(expectedConsumer, config.getConsumer());
        assertEquals(expectedDeadLetter, config.getDeadLetter());
    }

    @Test
    void testSetName() {
        // Given
        String expectedName = "testName";

        // When
        KafkaRetryableConfiguration config = new KafkaRetryableConfiguration();
        config.setName(expectedName);

        // Then
        assertEquals(expectedName, config.getName());
    }

    @Test
    void testSetConsumer() {
        // Given
        RetryableConsumerConfiguration expectedConsumer = new RetryableConsumerConfiguration();

        // When
        KafkaRetryableConfiguration config = new KafkaRetryableConfiguration();
        config.setConsumer(expectedConsumer);

        // Then
        assertEquals(expectedConsumer, config.getConsumer());
    }

    @Test
    void testSetDeadLetter() {
        // Given
        DeadLetterProducerConfiguration expectedDeadLetter = new DeadLetterProducerConfiguration();

        // When
        KafkaRetryableConfiguration config = new KafkaRetryableConfiguration();
        config.setDeadLetter(expectedDeadLetter);

        // Then
        assertEquals(expectedDeadLetter, config.getDeadLetter());
    }
}
