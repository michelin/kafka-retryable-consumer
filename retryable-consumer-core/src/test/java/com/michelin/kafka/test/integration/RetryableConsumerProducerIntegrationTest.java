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
package com.michelin.kafka.test.integration;

import com.michelin.kafka.RetryableConsumerProducer;
import com.michelin.kafka.avro.GenericErrorModel;
import com.michelin.kafka.configuration.KafkaRetryableConfiguration;
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroDeserializerConfig;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.integration.utils.EmbeddedKafkaCluster;
import org.junit.jupiter.api.*;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.ExecutionException;

import static org.junit.jupiter.api.Assertions.*;

@Slf4j
@Timeout(15)
public class RetryableConsumerProducerIntegrationTest {

    /** Default timeout used by various Kafka consumer to wait for new message in a topic */
    private static final Long DEFAULT_TOPIC_CONSUMPTION_TIMEOUT_MS = 10000L;

    /** The in memory Kafka Cluster. Not mocked servers, but real ZooKeeper/SchemaRegistry/KafkaBroker instances ! */
    public static final EmbeddedKafkaCluster KAFKA_CLUSTER = new EmbeddedKafkaCluster(1);

    /** The Kafka cluster properties used by both Producers and Consumers. */
    private static final Properties clusterCommonConfig = new Properties();

    /** Shared test producer configuration used to push test data into topics */
    private static final Properties dataInjectionProducerConfig = new Properties();

    /** Shared test Dead Letter Topic consumer configuration */
    private static final Properties dlqConsumerConfig = new Properties();

    /** Shared producer configuration */
    private static final Properties producerConfig = new Properties();

    /** Shared test consumer configuration */
    private static final Properties consumerConfig = new Properties();

    /** Share */
    private static final KafkaRetryableConfiguration kafkaRetryableConfiguration = new KafkaRetryableConfiguration();

    private String dataTopic;
    private String deadLetterTopic;
    private String dataOutputTopic;

    @BeforeAll
    static void setup() throws Exception {
        log.info("Starting embedded kafka cluster...");
        KAFKA_CLUSTER.start();
        setupKafkaConfig();
    }

    @AfterAll
    static void teardown() {
        log.info("Shutting down embedded kafka cluster...");
        KAFKA_CLUSTER.stop();
    }

    public static void setupKafkaConfig() {
        // Setup cluster common configuration properties
        clusterCommonConfig.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA_CLUSTER.bootstrapServers());
        clusterCommonConfig.put(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, "mock://url");

        // Setup producer configuration
        dataInjectionProducerConfig.putAll(clusterCommonConfig);
        dataInjectionProducerConfig.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        dataInjectionProducerConfig.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // Setup DLQ consumer configuration
        dlqConsumerConfig.putAll(clusterCommonConfig);
        dlqConsumerConfig.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        dlqConsumerConfig.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaAvroDeserializer.class.getName());
        dlqConsumerConfig.put(KafkaAvroDeserializerConfig.SPECIFIC_AVRO_READER_CONFIG, true);
        dlqConsumerConfig.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        dlqConsumerConfig.put(ConsumerConfig.GROUP_ID_CONFIG, "DLQCheckerConsumer");

        // Setup retryable consumer configuration
        Properties retryableConsumerKafkaProps =
                kafkaRetryableConfiguration.getConsumer().getProperties();
        retryableConsumerKafkaProps.putAll(clusterCommonConfig);
        retryableConsumerKafkaProps.put(ConsumerConfig.GROUP_ID_CONFIG, "RetryableConsumerTest");
        retryableConsumerKafkaProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        retryableConsumerKafkaProps.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        retryableConsumerKafkaProps.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 10);
        retryableConsumerKafkaProps.put(
                ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        retryableConsumerKafkaProps.put(
                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        kafkaRetryableConfiguration.getConsumer().setPollBackoffMs(1000L);

        // Setup producer configuration
        Properties retryableProducerKafkaProps =
                kafkaRetryableConfiguration.getProducer().getProperties();
        retryableProducerKafkaProps.putAll(clusterCommonConfig);
        retryableProducerKafkaProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        retryableProducerKafkaProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());


        // Setup test Consumer configuration
        consumerConfig.putAll(clusterCommonConfig);
        consumerConfig.put(ConsumerConfig.GROUP_ID_CONFIG, "OutputCheckerConsumer");
        consumerConfig.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        consumerConfig.put(
                ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        consumerConfig.put(
                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

        // Setup Dead Letter Producer configuration
        Properties dlProducerProps = kafkaRetryableConfiguration.getDeadLetter().getProperties();
        dlProducerProps.putAll(clusterCommonConfig);
        dlProducerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        dlProducerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class.getName());
    }

    @BeforeEach
    void initTestInfo(TestInfo testInfo) throws Exception {

        log.info("Init topic for test {} ...", testInfo.getDisplayName());

        if (testInfo.getTestMethod().isPresent()) {
            final String currentTestName =
                    testInfo.getTestMethod().get().getName().toUpperCase();
            this.dataTopic = "TOPIC-" + currentTestName;
            this.deadLetterTopic = "DEAD-LETTER-" + currentTestName;
            this.dataOutputTopic = "OUTPUT-TOPIC-" + currentTestName;
        } else {
            this.dataTopic = "TOPIC-TEST";
            this.deadLetterTopic = "DEAD-LETTER-TEST";
            this.dataOutputTopic = "OUTPUT-TOPIC-TEST" ;

        }

        KAFKA_CLUSTER.createTopic(dataTopic, 3, (short) 1);
        KAFKA_CLUSTER.createTopic(deadLetterTopic, 3, (short) 1);
        KAFKA_CLUSTER.createTopic(dataOutputTopic, 3, (short) 1);

        // Setup specific dead letter queue for each test
        kafkaRetryableConfiguration.getDeadLetter().setTopic(deadLetterTopic);
    }

    @Test
    @Timeout(25)
    void happyPath() throws ExecutionException, InterruptedException {
        // GIVEN
        /* Produce records into topic */
        final int numberOfRecordToProduce = 30;
        try (KafkaProducer<String, String> producer = new KafkaProducer<>(dataInjectionProducerConfig)) {
            for (int i = 0; i < numberOfRecordToProduce; i++) {
                RecordMetadata recordMeta = producer.send(new ProducerRecord<>(dataTopic, "k" + i, "value" + i))
                        .get();
                log.info(
                        "Record sent to topic {} in partition {}, offset {}",
                        recordMeta.topic(),
                        recordMeta.partition(),
                        recordMeta.offset());
            }
        }

        // WHEN
        try (RetryableConsumerProducer<String, String, String, String> retryableConsumer =
                new RetryableConsumerProducer<>(kafkaRetryableConfiguration)) {
            // Call the record processor on each received record
            List<ConsumerRecord<String, String>> consumedRecords = new ArrayList<>();
            retryableConsumer.listenAsync(
                    Collections.singleton(dataTopic),
                    consumerRecord -> { // Build a record process that fails on purpose only one time
                        log.info(
                                "[TEST] record {} received from partition {} ...",
                                consumerRecord.key(),
                                consumerRecord.partition());
                        consumedRecords.add(consumerRecord);
                        List<ProducerRecord<String, String>> results = new ArrayList<>();
                        results.add(new ProducerRecord<>(dataOutputTopic, "k" + consumerRecord.key(), "value" + consumerRecord.value()));
                        return results;
                    });

            // THEN
            /* Check number of received records */
            int timeoutSecond = 5;
            assertTimeout(
                    Duration.ofSeconds(timeoutSecond),
                    () -> waitForListSize(consumedRecords, numberOfRecordToProduce),
                    "Expected number of records (" + numberOfRecordToProduce + ") " + "were not received within "
                            + timeoutSecond + " seconds");
        }

        /* Verify records in output topic */
        List<ConsumerRecord<String, String>> records = this.getOutputContent(dataOutputTopic);
        assertEquals(30, records.size());

        /* Verify Dead Letter topic does not contain anything */
        List<ConsumerRecord<String, GenericErrorModel>> deadLetterRecords = this.getDeadLetterContent(deadLetterTopic);
        assertTrue(
                deadLetterRecords.isEmpty(), deadLetterRecords.size() + " record(s) found inside dead letter topic!");
    }

    @Test
    @Timeout(25)
    void happyPathHeavyDuty() throws ExecutionException, InterruptedException {
        // GIVEN
        /* Produce records into topic */
        final int numberOfRecordToProduce = 200;
        try (KafkaProducer<String, String> producer = new KafkaProducer<>(dataInjectionProducerConfig)) {
            for (int i = 0; i < numberOfRecordToProduce; i++) {
                RecordMetadata recordMeta = producer.send(new ProducerRecord<>(dataTopic, "k" + i, "value" + i))
                        .get();
                log.info(
                        "Record sent to topic {} in partition {}, offset {}",
                        recordMeta.topic(),
                        recordMeta.partition(),
                        recordMeta.offset());
            }
        }

        // WHEN
        try (RetryableConsumerProducer<String, String, String, String> retryableConsumer =
                new RetryableConsumerProducer<>(kafkaRetryableConfiguration)) {
            // Call the record processor on each received record
            List<ConsumerRecord<String, String>> consumedRecords = new ArrayList<>();
            retryableConsumer.listenAsync(
                    Collections.singleton(dataTopic),
                    consumerRecord -> { // Build a record process that fails on purpose only one time
                        log.info(
                                "[TEST] record {} received from partition {} ...",
                                consumerRecord.key(),
                                consumerRecord.partition());
                        consumedRecords.add(consumerRecord);
                        List<ProducerRecord<String, String>> results = new ArrayList<>();
                        results.add(new ProducerRecord<>(dataOutputTopic, "k" + consumerRecord.key(), "value" + consumerRecord.value()));
                        return results;
                    });

            // THEN
            /* Check number of received records */
            int timeoutSecond = 15;
            assertTimeout(
                    Duration.ofSeconds(timeoutSecond),
                    () -> waitForListSize(consumedRecords, numberOfRecordToProduce),
                    "Expected number of records (" + numberOfRecordToProduce + ") " + "were not received within "
                            + timeoutSecond + " seconds");
        }

        /* Verify records in output topic */
        List<ConsumerRecord<String, String>> records = this.getOutputContent(dataOutputTopic);
        assertEquals(200, records.size());

        /* Verify Dead Letter topic does not contain anything */
        List<ConsumerRecord<String, GenericErrorModel>> deadLetterRecords = this.getDeadLetterContent(deadLetterTopic);
        assertTrue(
                deadLetterRecords.isEmpty(), deadLetterRecords.size() + " record(s) found inside dead letter topic!");
    }

    private List<ConsumerRecord<String, GenericErrorModel>> getDeadLetterContent(String deadLetterTopic) {
        return this.getDeadLetterContent(deadLetterTopic, Duration.ofMillis(DEFAULT_TOPIC_CONSUMPTION_TIMEOUT_MS));
    }

    private List<ConsumerRecord<String, String>> getOutputContent(String topic) {
        return this.getOutputContent(topic, Duration.ofMillis(DEFAULT_TOPIC_CONSUMPTION_TIMEOUT_MS));
    }

    /**
     * This method
     *
     * @param deadLetterTopic the dead letter topic to consume record
     * @param consumerTimeout the duration to wait for new records
     * @return The record list from the dead letter queue
     */
    private List<ConsumerRecord<String, GenericErrorModel>> getDeadLetterContent(
            String deadLetterTopic, Duration consumerTimeout) {
        List<ConsumerRecord<String, GenericErrorModel>> recordsList = new ArrayList<>();
        try (KafkaConsumer<String, GenericErrorModel> dlqConsumer = new KafkaConsumer<>(dlqConsumerConfig)) {
            dlqConsumer.subscribe(Collections.singleton(deadLetterTopic));

            long start = System.currentTimeMillis();
            long elapsedTime;
            while (true) {
                dlqConsumer.poll(Duration.ofMillis(10L)).forEach(recordsList::add);
                elapsedTime = System.currentTimeMillis() - start;
                if (Duration.ofMillis(elapsedTime).compareTo(consumerTimeout) > 0) return recordsList;
            }
        }
    }

    private List<ConsumerRecord<String, String>> getOutputContent(
            String topic, Duration consumerTimeout) {
        List<ConsumerRecord<String, String>> recordsList = new ArrayList<>();
        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(consumerConfig)) {
            consumer.subscribe(Collections.singleton(topic));

            long start = System.currentTimeMillis();
            long elapsedTime;
            while (true) {
                consumer.poll(Duration.ofMillis(10L)).forEach(recordsList::add);
                elapsedTime = System.currentTimeMillis() - start;
                if (Duration.ofMillis(elapsedTime).compareTo(consumerTimeout) > 0) return recordsList;
            }
        }
    }

    /**
     * This method wait until the given list contains the expected number of entry
     *
     * @param list the list to check
     * @param expectedListSize the expected number of entry expected in the records list
     * @throws InterruptedException Thread exception
     */
    private <T> void waitForListSize(Collection<T> list, int expectedListSize) throws InterruptedException {
        boolean recordCountReached = false;
        while (!recordCountReached) {
            if (list != null) {
                log.info("[TEST ASSERT] current list size {}, expected size {}", list.size(), expectedListSize);
                if (list.size() == expectedListSize) recordCountReached = true;
            }
            Thread.sleep(200);
        }
    }

    static class NotRetryableCustomException extends Exception {
        public NotRetryableCustomException() {
            super();
        }

        public NotRetryableCustomException(String message) {
            super(message);
        }

        public NotRetryableCustomException(String message, Throwable cause) {
            super(message, cause);
        }

        public NotRetryableCustomException(Throwable cause) {
            super(cause);
        }

        protected NotRetryableCustomException(
                String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
            super(message, cause, enableSuppression, writableStackTrace);
        }
    }
}
