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

import static org.junit.jupiter.api.Assertions.*;

import com.michelin.kafka.RetryableConsumer;
import com.michelin.kafka.avro.GenericErrorModel;
import com.michelin.kafka.configuration.KafkaRetryableConfiguration;
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroDeserializerConfig;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.integration.utils.EmbeddedKafkaCluster;
import org.junit.jupiter.api.*;

@Slf4j
@Timeout(15)
public class RetryableConsumerIT {

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

    /** Share */
    private static final KafkaRetryableConfiguration kafkaRetryableConfiguration = new KafkaRetryableConfiguration();

    private String dataTopic;
    private String deadLetterTopic;

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
        } else {
            this.dataTopic = "TOPIC-TEST";
            this.deadLetterTopic = "DEAD-LETTER-TEST";
        }

        KAFKA_CLUSTER.createTopic(dataTopic, 3, (short) 1);
        KAFKA_CLUSTER.createTopic(deadLetterTopic, 3, (short) 1);

        // Setup specific dead letter queue for each test
        kafkaRetryableConfiguration.getDeadLetter().setTopic(deadLetterTopic);
    }

    @Test
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
        try (RetryableConsumer<String, String> retryableConsumer =
                new RetryableConsumer<>(kafkaRetryableConfiguration)) {
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
                    });

            // THEN
            /* Check number of received records */
            int timeoutSecond = 10;
            assertTimeout(
                    Duration.ofSeconds(timeoutSecond),
                    () -> waitForListSize(consumedRecords, numberOfRecordToProduce),
                    "Expected number of records (" + numberOfRecordToProduce + ") " + "were not received within "
                            + timeoutSecond + " seconds");
        }

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
        try (RetryableConsumer<String, String> retryableConsumer =
                new RetryableConsumer<>(kafkaRetryableConfiguration)) {
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

        /* Verify Dead Letter topic does not contain anything */
        List<ConsumerRecord<String, GenericErrorModel>> deadLetterRecords = this.getDeadLetterContent(deadLetterTopic);
        assertTrue(
                deadLetterRecords.isEmpty(), deadLetterRecords.size() + " record(s) found inside dead letter topic!");
    }

    @Test
    void retryableError() throws ExecutionException, InterruptedException {
        // GIVEN
        /* Produce records into topic */
        final int numberOfRecordToProduce = 50;
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
        try (RetryableConsumer<String, String> retryableConsumer =
                new RetryableConsumer<>(kafkaRetryableConfiguration)) {

            List<ConsumerRecord<String, String>> consumedRecords = new ArrayList<>();
            AtomicInteger receivedRecordCounter = new AtomicInteger();
            retryableConsumer.listenAsync(
                    Collections.singleton(dataTopic),
                    consumerRecord -> { // Build a record process that fails on purpose only one time
                        receivedRecordCounter.getAndIncrement();
                        if (receivedRecordCounter.get() == 1) {
                            // Let's provoque a retryable failure on the 2nd record processing
                            throw new Exception(
                                    "Fake retryable exception"); // By default, "Exception" type is retryable
                        } else {
                            // Run the normal business process
                            consumedRecords.add(consumerRecord);
                        }
                    });

            // THEN
            /* While consumer is running Check number of processed records */
            int timeoutSecond = 10;
            assertTimeout(
                    Duration.ofSeconds(timeoutSecond),
                    // We de expect to receive all the 50 records,
                    // even the one that failed one time during business processing
                    () -> waitForListSize(consumedRecords, numberOfRecordToProduce),
                    "Expected number of records (" + numberOfRecordToProduce + ") " + "were not received within "
                            + timeoutSecond + " seconds");

            /* Verify Dead Letter topic does not contain anything */
            List<ConsumerRecord<String, GenericErrorModel>> deadLetterRecords =
                    this.getDeadLetterContent(deadLetterTopic);
            assertTrue(
                    deadLetterRecords.isEmpty(),
                    deadLetterRecords.size() + " record(s) found inside dead letter topic!");
        }
    }

    @Test
    void notRetryableError() throws ExecutionException, InterruptedException {
        // GIVEN
        /* Produce records into topic */
        final int numberOfRecordToProduce = 50;
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
        // Run the retryable consumer
        AtomicReference<ConsumerRecord<String, String>> errorRecord = new AtomicReference<>();
        kafkaRetryableConfiguration
                .getConsumer()
                .setNotRetryableExceptions(Collections.singletonList(NotRetryableCustomException.class.getName()));
        try (RetryableConsumer<String, String> retryableConsumer =
                new RetryableConsumer<>(kafkaRetryableConfiguration)) {
            System.out.println(kafkaRetryableConfiguration.getConsumer().getProperties());
            List<ConsumerRecord<String, String>> consumedRecords = new ArrayList<>();
            AtomicInteger receivedRecordCounter = new AtomicInteger(0);

            retryableConsumer.listenAsync(Collections.singleton(dataTopic), consumerRecord -> {
                // Build a record process that fails on purpose only one time on the 23rd record
                if (receivedRecordCounter.getAndIncrement() == 23) {
                    // Let's generate a non retryable failure
                    errorRecord.set(consumerRecord);

                    // Throw a custom non retryable custom exception
                    throw new NotRetryableCustomException("Fake error");
                } else {
                    // Run the normal (successful) business process
                    consumedRecords.add(consumerRecord);
                }
            });

            // THEN
            /* While retryable consumer is running, verify the number of processed records */
            int expectedRecordNumber = numberOfRecordToProduce - 1; // One record must have failed
            Assertions.assertTimeoutPreemptively(
                    Duration.ofSeconds(10), // Wait for 10 seconds to have the expected number of record processed
                    () -> waitForListSize(consumedRecords, expectedRecordNumber),
                    "Wrong number of record processed (" + consumedRecords.size() + ") " + "from topic " + dataTopic
                            + " within 10 seconds, expected " + expectedRecordNumber);
            /* Verify poison pills is not present in consumed record */
            consumedRecords.forEach(r -> {
                if (r.key().equals(errorRecord.get().key()))
                    Assertions.fail("Not retryable exception should not be present in consumed message");
            });

            /* Verify poison pill has been sent into DL Topic */
            try (KafkaConsumer<String, GenericErrorModel> dlqConsumer = new KafkaConsumer<>(dlqConsumerConfig)) {
                dlqConsumer.subscribe(Collections.singleton(deadLetterTopic));

                final int EXPECTED_DL_RECORD_NUMBER = 1;
                List<ConsumerRecord<String, GenericErrorModel>> dlqRecords = new ArrayList<>();
                Assertions.assertTimeoutPreemptively(
                        Duration.ofSeconds(
                                15), // Wait for 15 seconds max to have the expected number of record in DL topic
                        () -> {
                            while (true) {
                                dlqConsumer.poll(Duration.ofMillis(1000L)).forEach(dlqRecords::add);
                                if (dlqRecords.size() == EXPECTED_DL_RECORD_NUMBER) return;
                            }
                        },
                        "Wrong number of record found (" + dlqRecords.size() + ") in Dead Letter topic "
                                + deadLetterTopic + " within 10 seconds, expected " + EXPECTED_DL_RECORD_NUMBER);

                GenericErrorModel dlqError = dlqRecords.get(0).value();
                Assertions.assertEquals(
                        dataTopic, dlqError.getTopic(), "Wrong topic name defined in the DL topic message");
                Assertions.assertEquals(
                        errorRecord.get().key(), dlqError.getKey(), "Wrong Key defined in the DL topic message");
                Assertions.assertEquals(
                        errorRecord.get().value(), dlqError.getValue(), "Wrong Value defined in the DL topic message");
                Assertions.assertTrue(
                        dlqError.getStack().contains("NotRetryableCustomException"), "Wrong exception sent to DLQ");
            }
        }
    }

    @Test
    @Timeout(15)
    void limitedRetryAndSuccess() throws ExecutionException, InterruptedException {
        // GIVEN
        /* Produce records into topic */
        final int numberOfRecordToProduce = 10;
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
        // Run the retryable consumer with limited retry
        kafkaRetryableConfiguration.getConsumer().setRetryMax(10L);
        kafkaRetryableConfiguration.getConsumer().setRetryBackoffMs(100L);

        AtomicReference<ConsumerRecord<String, String>> errorRecord = new AtomicReference<>();
        try (RetryableConsumer<String, String> retryableConsumer =
                new RetryableConsumer<>(kafkaRetryableConfiguration)) {
            List<ConsumerRecord<String, String>> consumedRecords = new ArrayList<>();
            AtomicInteger failureCounter = new AtomicInteger(0);
            AtomicInteger keyInErrorRecordReceptionCounter = new AtomicInteger();
            String keyInError = "k7";

            retryableConsumer.listenAsync(Collections.singleton(dataTopic), consumerRecord -> {
                log.info(
                        "[TEST] record {} received from partition {} ...",
                        consumerRecord.key(),
                        consumerRecord.partition());
                if (consumerRecord.key().equals(keyInError)) {
                    keyInErrorRecordReceptionCounter.getAndIncrement();
                }
                // let's fail 5 times when we receive a specific record,
                // on the 6th loop, the record will be process successfully
                if (consumerRecord.key().equals(keyInError) && failureCounter.getAndIncrement() < 5) {
                    // Let's generate a non retryable failure
                    errorRecord.set(consumerRecord);

                    // Throw a custom non retryable custom exception
                    throw new Exception("Retryable Error");
                } else {
                    // Run the normal (successful) business process
                    consumedRecords.add(consumerRecord);
                    log.info("[TEST] record {} added to list", consumerRecord.key());
                }
            });

            // THEN
            /* While retryable consumer is running, verify the number of processed records */
            Assertions.assertTimeoutPreemptively(
                    Duration.ofSeconds(10), // Wait for 10 seconds to have the expected number of record processed
                    () -> waitForListSize(consumedRecords, numberOfRecordToProduce),
                    "Wrong number of record processed (" + consumedRecords.size() + ") " + "from topic " + dataTopic
                            + " within 10 seconds, expected " + numberOfRecordToProduce);

            /* k3 record has been in error for sometimes */

            Assertions.assertEquals(keyInError, errorRecord.get().key());

            /* Verify Dead Letter topic does not contain anything */
            List<ConsumerRecord<String, GenericErrorModel>> deadLetterRecords =
                    this.getDeadLetterContent(deadLetterTopic);
            assertTrue(
                    deadLetterRecords.isEmpty(),
                    deadLetterRecords.size() + " record(s) found inside dead letter topic!");
        }
    }

    @Test
    void limitedRetryAndError() throws ExecutionException, InterruptedException {
        // GIVEN
        /* Produce records into topic */
        final int numberOfRecordToProduce = 10;
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
        // Run the retryable consumer with limited retry
        kafkaRetryableConfiguration.getConsumer().setRetryMax(10L);
        kafkaRetryableConfiguration.getConsumer().setRetryBackoffMs(100L);

        AtomicReference<ConsumerRecord<String, String>> errorRecord = new AtomicReference<>();
        try (RetryableConsumer<String, String> retryableConsumer =
                new RetryableConsumer<>(kafkaRetryableConfiguration)) {
            List<ConsumerRecord<String, String>> consumedRecords = new ArrayList<>();
            AtomicInteger keyInErrorRecordReceptionCounter = new AtomicInteger();
            String keyInError = "k7";

            retryableConsumer.listenAsync(Collections.singleton(dataTopic), consumerRecord -> {
                log.info("[TEST] record {} received ...", consumerRecord.key());
                if (consumerRecord.key().equals(keyInError)) {
                    keyInErrorRecordReceptionCounter.getAndIncrement();
                }

                // let's fail each time we receive k9 record, after 10 retries,
                // the record should be pushed to DLQ
                if (consumerRecord.key().equals(keyInError)) {
                    // Let's generate a non retryable failure
                    errorRecord.set(consumerRecord);

                    // Throw a custom non retryable custom exception
                    throw new Exception("Retryable Error");
                } else {
                    // Run the normal (successful) business process
                    consumedRecords.add(consumerRecord);
                    log.info("[TEST] record {} added to consumed record list", consumerRecord.key());
                }
            });

            // THEN
            /* While retryable consumer is running, verify the number of processed records */
            int expectedRecordNumber = numberOfRecordToProduce - 1; // record in error should not be consumed correctly
            Assertions.assertTimeoutPreemptively(
                    Duration.ofSeconds(10), // Wait for 10 seconds to have the expected number of record processed
                    () -> waitForListSize(consumedRecords, expectedRecordNumber),
                    "Wrong number of record processed (" + consumedRecords.size() + ") " + "from topic " + dataTopic
                            + " within 10 seconds, expected " + expectedRecordNumber);

            /* k3 record has been in error for sometimes */
            Assertions.assertEquals(keyInError, errorRecord.get().key());
            Assertions.assertTrue(keyInErrorRecordReceptionCounter.get() > 10, "More than 10 retry should occur");

            /* Verify poison pill has been sent into DL Topic */

            /* Verify Dead Letter topic does not contain anything */
            List<ConsumerRecord<String, GenericErrorModel>> deadLetterRecords =
                    this.getDeadLetterContent(deadLetterTopic);
            assertFalse(
                    deadLetterRecords.isEmpty(),
                    deadLetterRecords.size() + " record(s) found inside dead letter topic!");
        }
    }

    private List<ConsumerRecord<String, GenericErrorModel>> getDeadLetterContent(String deadLetterTopic) {
        return this.getDeadLetterContent(deadLetterTopic, Duration.ofMillis(DEFAULT_TOPIC_CONSUMPTION_TIMEOUT_MS));
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
