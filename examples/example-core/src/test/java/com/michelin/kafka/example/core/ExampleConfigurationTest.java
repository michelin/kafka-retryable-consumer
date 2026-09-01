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
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.params.provider.Arguments.arguments;

import com.michelin.kafka.configuration.KafkaConfigurationException;
import com.michelin.kafka.configuration.KafkaRetryableConfiguration;
import com.michelin.kafka.configuration.RetryableConsumerConfiguration;
import java.io.Closeable;
import java.util.List;
import java.util.stream.Stream;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * Checks the configuration files shipped with the examples, without any broker.
 *
 * <p>The integration tests prove the examples behave as documented, but they retarget the configuration at an embedded
 * cluster. This class covers the other half: that each file is found, parses, and really declares the capability its
 * example claims to demonstrate. Those two things are exactly what silently breaks when a property is misspelled, since
 * an unknown key binds nothing and falls back on a default.
 *
 * <p>No broker is needed: the library defers the creation of the Kafka client until the first poll, so an example can
 * be built and closed without any connection.
 */
class ExampleConfigurationTest {

    /** Builds an example through its no-argument constructor, the one a user copies. */
    @FunctionalInterface
    private interface ExampleFactory {
        Closeable create() throws KafkaConfigurationException;
    }

    private static Stream<Arguments> examples() {
        return Stream.of(
                arguments("SimpleConsumerExample", (ExampleFactory) SimpleConsumerExample::new),
                arguments("BatchConsumerExample", (ExampleFactory) BatchConsumerExample::new),
                arguments("InfiniteRetryExample", (ExampleFactory) () -> new InfiniteRetryExample(3)),
                arguments("LimitedRetryToDeadLetterExample", (ExampleFactory) LimitedRetryToDeadLetterExample::new),
                arguments("NonRetryableExceptionExample", (ExampleFactory) NonRetryableExceptionExample::new),
                arguments("StopOnErrorExample", (ExampleFactory) StopOnErrorExample::new),
                arguments("CustomErrorProcessorExample", (ExampleFactory) CustomErrorProcessorExample::new),
                arguments("DeserializationErrorExample", (ExampleFactory) DeserializationErrorExample::new),
                arguments("DefaultConfigurationFileExample", (ExampleFactory) DefaultConfigurationFileExample::new));
    }

    /**
     * Every example must be buildable exactly as its {@code main} method does it, with no Java configuration at all. A
     * missing file, a malformed one or a missing mandatory property fails here.
     */
    @ParameterizedTest(name = "{0}")
    @MethodSource("examples")
    void shouldBuildEveryExampleFromItsOwnConfigurationFile(String name, ExampleFactory factory) throws Exception {
        try (Closeable example = factory.create()) {
            assertNotNull(example, name + " should be buildable from its configuration file alone");
        }
    }

    @Test
    void simpleConsumerExampleShouldKeepEveryDefault() throws KafkaConfigurationException {
        RetryableConsumerConfiguration consumer = consumerConfigurationOf(SimpleConsumerExample.CONFIG_FILE);

        assertEquals(List.of("simple-consumer-example"), consumer.getTopics());
        // The baseline of the module: no retry tuning, so the defaults must be left untouched
        assertEquals(0L, consumer.getRetryMax());
        assertFalse(consumer.getStopOnError());
    }

    @Test
    void batchConsumerExampleShouldBoundTheBatchSize() throws KafkaConfigurationException {
        RetryableConsumerConfiguration consumer = consumerConfigurationOf(BatchConsumerExample.CONFIG_FILE);

        // max.poll.records is what sizes the batches handed over to the business code
        assertEquals("10", String.valueOf(consumer.getProperties().get(ConsumerConfig.MAX_POLL_RECORDS_CONFIG)));
    }

    @Test
    void infiniteRetryExampleShouldNeverGiveUp() throws KafkaConfigurationException {
        RetryableConsumerConfiguration consumer = consumerConfigurationOf(InfiniteRetryExample.CONFIG_FILE);

        // 0 is what enables the infinite retry, and nothing must ever reach the dead letter topic
        assertEquals(0L, consumer.getRetryMax());
    }

    @Test
    void limitedRetryExampleShouldDeclareABoundedBudget() throws KafkaConfigurationException {
        RetryableConsumerConfiguration consumer = consumerConfigurationOf(LimitedRetryToDeadLetterExample.CONFIG_FILE);

        // 2 retries means the business code is called three times, as the integration test asserts
        assertEquals(2L, consumer.getRetryMax());
        assertEquals(100L, consumer.getRetryBackoffMs());
    }

    @Test
    void nonRetryableExceptionExampleShouldListItsBusinessException() throws KafkaConfigurationException {
        RetryableConsumerConfiguration consumer = consumerConfigurationOf(NonRetryableExceptionExample.CONFIG_FILE);

        // The exception is declared by name: a typo here would silently retry it instead
        assertEquals(
                List.of(NonRetryableExceptionExample.InvalidOrderException.class.getName()),
                consumer.getNotRetryableExceptions());
        // A generous budget, deliberately: it proves the exception above really bypasses it
        assertEquals(10L, consumer.getRetryMax());
    }

    @Test
    void stopOnErrorExampleShouldEnableStopOnError() throws KafkaConfigurationException {
        RetryableConsumerConfiguration consumer = consumerConfigurationOf(StopOnErrorExample.CONFIG_FILE);

        assertTrue(consumer.getStopOnError());
    }

    @Test
    void deserializationErrorExampleShouldExpectIntegerValues() throws KafkaConfigurationException {
        RetryableConsumerConfiguration consumer = consumerConfigurationOf(DeserializationErrorExample.CONFIG_FILE);

        // Without this deserializer there is no poison pill to demonstrate
        assertEquals(
                "org.apache.kafka.common.serialization.IntegerDeserializer",
                consumer.getProperties().get(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG));
    }

    /**
     * The dead letter payload is an Avro record, so every file declaring a dead letter topic needs a schema registry
     * and the Avro serializer. Falling back on the default string serializer fails at runtime only.
     */
    @ParameterizedTest
    @MethodSource("configurationFiles")
    void everyConfigurationFileShouldMakeItsDeadLetterProducerUsable(String configFile)
            throws KafkaConfigurationException {
        KafkaRetryableConfiguration configuration = KafkaRetryableConfiguration.load(configFile);

        assertEquals(
                "io.confluent.kafka.serializers.KafkaAvroSerializer",
                configuration.getDeadLetter().getProperties().get("value.serializer"));
        assertNotNull(configuration.getDeadLetter().getProperties().get("schema.registry.url"));
    }

    private static Stream<String> configurationFiles() {
        return Stream.of(
                SimpleConsumerExample.CONFIG_FILE,
                BatchConsumerExample.CONFIG_FILE,
                InfiniteRetryExample.CONFIG_FILE,
                LimitedRetryToDeadLetterExample.CONFIG_FILE,
                NonRetryableExceptionExample.CONFIG_FILE,
                StopOnErrorExample.CONFIG_FILE,
                CustomErrorProcessorExample.CONFIG_FILE,
                DeserializationErrorExample.CONFIG_FILE,
                "application.yml");
    }

    private static RetryableConsumerConfiguration consumerConfigurationOf(String configFile)
            throws KafkaConfigurationException {
        return KafkaRetryableConfiguration.load(configFile).getConsumer();
    }
}
