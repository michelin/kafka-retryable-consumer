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

import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertEquals;

import com.michelin.kafka.configuration.KafkaRetryableConfiguration;
import com.michelin.kafka.configuration.KafkaConfigurationException;
import java.time.Duration;
import java.util.List;
import java.util.Properties;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.Test;

/** Runs {@link DeserializationErrorExample} against an embedded broker. */
class DeserializationErrorExampleIntegrationTest extends AbstractExampleIntegrationTest {

    @Test
    void shouldSkipThePoisonPillAndKeepConsuming() throws KafkaConfigurationException {
        String topic = uniqueName("poison-pill");
        String deadLetterTopic = uniqueName("poison-pill-dlt");
        createTopic(topic, 1);
        createTopic(deadLetterTopic, 1);

        KafkaRetryableConfiguration configuration = loadExampleConfiguration(DeserializationErrorExample.CONFIG_FILE, topic, deadLetterTopic);
        // A record the consumer will never be able to decode, surrounded by two valid ones
        produceIntegerRecord(bootstrapServers(), topic, "k0", 1);
        producePoisonPill(bootstrapServers(), topic, "k1");
        produceIntegerRecord(bootstrapServers(), topic, "k2", 2);

        try (DeserializationErrorExample example = new DeserializationErrorExample(configuration)) {
            example.start();

            await("the two decodable records to be processed")
                    .atMost(CLUSTER_OPERATION_TIMEOUT)
                    .pollInterval(Duration.ofMillis(100))
                    .until(() -> example.getProcessedValues().size() == 2);

            // The poison pill did not block the partition
            assertEquals(List.of(1, 2), example.getProcessedValues());
            assertEquals(1, awaitDeadLetterRecords(deadLetterTopic, 1).size());
        }
    }

    private static void produceIntegerRecord(String bootstrapServers, String topic, String key, int value) {
        Properties config = producerConfig(bootstrapServers, IntegerSerializer.class.getName());
        try (KafkaProducer<String, Integer> producer = new KafkaProducer<>(config)) {
            producer.send(new ProducerRecord<>(topic, key, value));
            producer.flush();
        }
    }

    /** Publishes a payload that the integer deserializer of the example cannot decode. */
    private static void producePoisonPill(String bootstrapServers, String topic, String key) {
        Properties config = producerConfig(bootstrapServers, ByteArraySerializer.class.getName());
        try (KafkaProducer<String, byte[]> producer = new KafkaProducer<>(config)) {
            producer.send(new ProducerRecord<>(topic, key, "not-an-integer".getBytes()));
            producer.flush();
        }
    }

    private static Properties producerConfig(String bootstrapServers, String valueSerializer) {
        Properties config = new Properties();
        config.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        config.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        config.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, valueSerializer);
        config.put(ProducerConfig.ACKS_CONFIG, "all");
        return config;
    }
}
