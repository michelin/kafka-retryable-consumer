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
package com.michelin.kafka.test.utils;

import com.github.javafaker.Faker;
import com.michelin.kafka.test.ChuckFact;
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.integration.utils.EmbeddedKafkaCluster;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

@Slf4j
public class EmbeddedClusterTest {

    private static final String TOPIC1 = "CHUCK_FACTS";

    private static final Properties clusterCommonConfig = new Properties();
    public static EmbeddedKafkaCluster embeddedCluster = new EmbeddedKafkaCluster(1);

    @BeforeAll
    static void setup() throws Exception {
        log.info("Starting embedded kafka cluster...");
        embeddedCluster.start();
        embeddedCluster.createTopic(TOPIC1, 3, (short) 1);

        // Setup cluster common configuration properties
        clusterCommonConfig.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, embeddedCluster.bootstrapServers());
        clusterCommonConfig.put(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, "mock://url");
    }

    @AfterAll
     static void teardown() {
        embeddedCluster.stop();
    }

    @Test
    void runBasicProducerConsumerTest() throws ExecutionException, InterruptedException {
        // GIVEN
        // Setup producer configuration
        Properties producerConfig = new Properties();
        producerConfig.putAll(clusterCommonConfig);
        producerConfig.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        producerConfig.put(
                ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, io.confluent.kafka.serializers.KafkaAvroSerializer.class);

        try (KafkaProducer<String, ChuckFact> producer = new KafkaProducer<>(producerConfig)) {
            Faker faker = new Faker();
            RecordMetadata rm1 = sendFact(producer, faker.chuckNorris().fact()).get();
            Assertions.assertTrue(rm1.hasOffset());
            Assertions.assertTrue(rm1.hasTimestamp());
            log.info("Record sent to topic {} in partition {}, offset {}", rm1.topic(), rm1.partition(), rm1.offset());

            RecordMetadata rm2 = sendFact(producer, faker.chuckNorris().fact()).get();
            Assertions.assertTrue(rm2.hasOffset());
            Assertions.assertTrue(rm2.hasTimestamp());
            log.info("Record sent to topic {} in partition {}, offset {}", rm2.topic(), rm2.partition(), rm2.offset());

            RecordMetadata rm3 = sendFact(producer, faker.chuckNorris().fact()).get();
            Assertions.assertTrue(rm3.hasOffset());
            Assertions.assertTrue(rm3.hasTimestamp());
            log.info("Record sent to topic {} in partition {}, offset {}", rm3.topic(), rm3.partition(), rm3.offset());
        }

        // WHEN
        // Setup consumer configuration
        Properties consumerConfig = new Properties();
        consumerConfig.putAll(clusterCommonConfig);
        consumerConfig.put(ConsumerConfig.GROUP_ID_CONFIG, "BasicConsumerTest");
        consumerConfig.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        consumerConfig.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        consumerConfig.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 10);
        consumerConfig.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        consumerConfig.put(
                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                io.confluent.kafka.serializers.KafkaAvroDeserializer.class);

        ConsumerRecords<String, ChuckFact> records;
        try (KafkaConsumer<String, ChuckFact> consumer = new KafkaConsumer<>(consumerConfig)) {
            consumer.subscribe(Collections.singleton(TOPIC1));
            records = consumer.poll(Duration.ofMillis(5000));
        }

        // THEN
        Assertions.assertEquals(3, records.count(), "Wrong record count in the topic");
        records.forEach(r -> {
            log.info("Record received from topic {} in partition {}, offset {}", r.topic(), r.partition(), r.offset());
            Assertions.assertEquals(TOPIC1, r.topic());
            Assertions.assertNotNull(r.key());
        });
    }

    private Future<RecordMetadata> sendFact(KafkaProducer<String, ChuckFact> producer, String fact) {
        return producer.send(new ProducerRecord<>(
                TOPIC1,
                UUID.randomUUID().toString(),
                ChuckFact.newBuilder()
                        .setId(UUID.randomUUID().toString())
                        .setFact(fact)
                        .build()));
    }
}
