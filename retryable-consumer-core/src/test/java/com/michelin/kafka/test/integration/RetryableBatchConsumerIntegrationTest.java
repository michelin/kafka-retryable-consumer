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

import com.michelin.kafka.RetryableBatchConsumer;
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
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.integration.utils.EmbeddedKafkaCluster;
import org.junit.jupiter.api.*;

@Slf4j
@Timeout(15)
public class RetryableBatchConsumerIntegrationTest {

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

    /** Shared Kafka retryable configuration */
    private static final KafkaRetryableConfiguration kafkaRetryableConfiguration = new KafkaRetryableConfiguration();

    private String dataTopic;
    private String deadLetterTopic;
    private static int dlqGroupIdCounter = 0;

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
        dlqConsumerConfig.put(ConsumerConfig.GROUP_ID_CONFIG, "DLQCheckerBatchConsumer");

        // Setup retryable consumer configuration
        Properties retryableConsumerKafkaProps =
                kafkaRetryableConfiguration.getConsumer().getProperties();
        retryableConsumerKafkaProps.putAll(clusterCommonConfig);
        retryableConsumerKafkaProps.put(ConsumerConfig.GROUP_ID_CONFIG, "RetryableBatchConsumerTest");
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
            this.dataTopic = "BATCH-TOPIC-" + currentTestName;
            this.deadLetterTopic = "BATCH-DEAD-LETTER-" + currentTestName;
        } else {
            this.dataTopic = "BATCH-TOPIC-TEST";
            this.deadLetterTopic = "BATCH-DEAD-LETTER-TEST";
        }

        KAFKA_CLUSTER.createTopic(dataTopic, 3, (short) 1);
        KAFKA_CLUSTER.createTopic(deadLetterTopic, 3, (short) 1);

        // Setup specific dead letter queue for each test
        kafkaRetryableConfiguration.getDeadLetter().setTopic(deadLetterTopic);

        // Reset retry config to defaults for each test
        kafkaRetryableConfiguration.getConsumer().setRetryMax(0L);
        kafkaRetryableConfiguration.getConsumer().setRetryBackoffMs(0L);
        kafkaRetryableConfiguration.getConsumer().setNotRetryableExceptions(Collections.emptyList());
    }

    @Test
    void happyPath() throws ExecutionException, InterruptedException {
        // GIVEN
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
        try (RetryableBatchConsumer<String, String> retryableBatchConsumer =
                new RetryableBatchConsumer<>(kafkaRetryableConfiguration)) {
            List<ConsumerRecord<String, String>> consumedRecords = Collections.synchronizedList(new ArrayList<>());
            retryableBatchConsumer.listenAsync(Collections.singleton(dataTopic), records -> {
                log.info("[TEST] Batch of {} records received", records.count());
                for (ConsumerRecord<String, String> record : records) {
                    log.info("[TEST] record {} received from partition {} ...", record.key(), record.partition());
                    consumedRecords.add(record);
                }
            });

            // THEN
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
        try (RetryableBatchConsumer<String, String> retryableBatchConsumer =
                new RetryableBatchConsumer<>(kafkaRetryableConfiguration)) {
            List<ConsumerRecord<String, String>> consumedRecords = Collections.synchronizedList(new ArrayList<>());
            retryableBatchConsumer.listenAsync(Collections.singleton(dataTopic), records -> {
                log.info("[TEST] Batch of {} records received", records.count());
                for (ConsumerRecord<String, String> record : records) {
                    log.info("[TEST] record {} received from partition {} ...", record.key(), record.partition());
                    consumedRecords.add(record);
                }
            });

            // THEN
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
        try (RetryableBatchConsumer<String, String> retryableBatchConsumer =
                new RetryableBatchConsumer<>(kafkaRetryableConfiguration)) {

            List<ConsumerRecord<String, String>> consumedRecords = Collections.synchronizedList(new ArrayList<>());
            AtomicInteger batchCallCounter = new AtomicInteger(0);
            retryableBatchConsumer.listenAsync(Collections.singleton(dataTopic), records -> {
                int callNumber = batchCallCounter.getAndIncrement();
                if (callNumber == 0) {
                    // Let's provoke a retryable failure on the first batch processing
                    throw new Exception("Fake retryable exception on first batch");
                } else {
                    // Run the normal business process
                    for (ConsumerRecord<String, String> record : records) {
                        consumedRecords.add(record);
                    }
                }
            });

            // THEN
            /* We expect to receive all records because the batch is retried */
            int timeoutSecond = 10;
            assertTimeout(
                    Duration.ofSeconds(timeoutSecond),
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
        kafkaRetryableConfiguration
                .getConsumer()
                .setNotRetryableExceptions(Collections.singletonList(NotRetryableCustomException.class.getName()));
        try (RetryableBatchConsumer<String, String> retryableBatchConsumer =
                new RetryableBatchConsumer<>(kafkaRetryableConfiguration)) {

            List<ConsumerRecord<String, String>> consumedRecords = Collections.synchronizedList(new ArrayList<>());
            AtomicInteger batchCallCounter = new AtomicInteger(0);

            retryableBatchConsumer.listenAsync(Collections.singleton(dataTopic), records -> {
                int callNumber = batchCallCounter.getAndIncrement();
                if (callNumber == 0) {
                    // First batch fails with a non-retryable exception
                    throw new NotRetryableCustomException("Fake non-retryable batch error");
                } else {
                    // Subsequent batches are processed normally
                    for (ConsumerRecord<String, String> record : records) {
                        consumedRecords.add(record);
                    }
                }
            });

            // THEN
            /* We expect the remaining records to be consumed after the first batch is sent to DLQ */
            int timeoutSecond = 10;
            Assertions.assertTimeoutPreemptively(
                    Duration.ofSeconds(timeoutSecond),
                    () -> waitForMinListSize(consumedRecords, 1),
                    "No records were consumed within " + timeoutSecond + " seconds");

            /* Verify poison pill has been sent into DL Topic */
            Properties dedicatedDlqConfig = new Properties();
            dedicatedDlqConfig.putAll(dlqConsumerConfig);
            dedicatedDlqConfig.put(ConsumerConfig.GROUP_ID_CONFIG, "DLQBatchNotRetryable-" + (++dlqGroupIdCounter));

            try (KafkaConsumer<String, GenericErrorModel> dlqConsumer = new KafkaConsumer<>(dedicatedDlqConfig)) {
                dlqConsumer.subscribe(Collections.singleton(deadLetterTopic));

                List<ConsumerRecord<String, GenericErrorModel>> dlqRecords = new ArrayList<>();
                Assertions.assertTimeoutPreemptively(
                        Duration.ofSeconds(15),
                        () -> {
                            while (true) {
                                dlqConsumer.poll(Duration.ofMillis(1000L)).forEach(dlqRecords::add);
                                if (!dlqRecords.isEmpty()) return;
                            }
                        },
                        "No record found in Dead Letter topic " + deadLetterTopic + " within 15 seconds");

                GenericErrorModel dlqError = dlqRecords.get(0).value();
                Assertions.assertTrue(
                        dlqError.getStack().contains("NotRetryableCustomException"), "Wrong exception sent to DLQ");
            }
        }
    }

    @Test
    @Timeout(15)
    void limitedRetryAndSuccess() throws ExecutionException, InterruptedException {
        // GIVEN
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
        kafkaRetryableConfiguration.getConsumer().setRetryMax(10L);
        kafkaRetryableConfiguration.getConsumer().setRetryBackoffMs(100L);

        try (RetryableBatchConsumer<String, String> retryableBatchConsumer =
                new RetryableBatchConsumer<>(kafkaRetryableConfiguration)) {

            List<ConsumerRecord<String, String>> consumedRecords = Collections.synchronizedList(new ArrayList<>());
            AtomicInteger failureCounter = new AtomicInteger(0);

            retryableBatchConsumer.listenAsync(Collections.singleton(dataTopic), records -> {
                log.info("[TEST] Batch of {} records received", records.count());
                // Fail 3 times then succeed
                if (failureCounter.getAndIncrement() < 3) {
                    throw new Exception("Retryable batch error");
                } else {
                    for (ConsumerRecord<String, String> record : records) {
                        consumedRecords.add(record);
                        log.info("[TEST] record {} added to list", record.key());
                    }
                }
            });

            // THEN
            /* All records should eventually be processed after retries succeed */
            Assertions.assertTimeoutPreemptively(
                    Duration.ofSeconds(10),
                    () -> waitForListSize(consumedRecords, numberOfRecordToProduce),
                    "Wrong number of record processed (" + consumedRecords.size() + ") "
                            + "from topic " + dataTopic + " within 10 seconds, expected "
                            + numberOfRecordToProduce);

            /* Verify Dead Letter topic does not contain anything */
            List<ConsumerRecord<String, GenericErrorModel>> deadLetterRecords =
                    this.getDeadLetterContent(deadLetterTopic);
            assertTrue(
                    deadLetterRecords.isEmpty(),
                    deadLetterRecords.size() + " record(s) found inside dead letter topic!");
        }
    }

    @Test
    @Timeout(30)
    void limitedRetryAndError() throws ExecutionException, InterruptedException {
        // GIVEN
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
        kafkaRetryableConfiguration.getConsumer().setRetryMax(3L);
        kafkaRetryableConfiguration.getConsumer().setRetryBackoffMs(100L);

        try (RetryableBatchConsumer<String, String> retryableBatchConsumer =
                new RetryableBatchConsumer<>(kafkaRetryableConfiguration)) {

            AtomicInteger batchCallCounter = new AtomicInteger(0);

            retryableBatchConsumer.listenAsync(Collections.singleton(dataTopic), records -> {
                log.info("[TEST] Batch received (call #{}, {} records)", batchCallCounter.get(), records.count());
                int callNum = batchCallCounter.getAndIncrement();
                // Always fail: after retryMax retries, it should go to DLQ
                throw new Exception("Persistent retryable batch error #" + callNum);
            });

            // THEN
            /* After exceeding retry max, error handler should kick in and the batch should be sent to DLQ */
            /* Use a dedicated DLQ consumer with a unique group.id to avoid interference */
            Properties dedicatedDlqConfig = new Properties();
            dedicatedDlqConfig.putAll(dlqConsumerConfig);
            dedicatedDlqConfig.put(ConsumerConfig.GROUP_ID_CONFIG, "DLQBatchLimitedRetry-" + (++dlqGroupIdCounter));

            List<ConsumerRecord<String, GenericErrorModel>> deadLetterRecords = new ArrayList<>();
            try (KafkaConsumer<String, GenericErrorModel> dlqConsumer = new KafkaConsumer<>(dedicatedDlqConfig)) {
                dlqConsumer.subscribe(Collections.singleton(deadLetterTopic));

                Assertions.assertTimeoutPreemptively(
                        Duration.ofSeconds(20),
                        () -> {
                            while (deadLetterRecords.isEmpty()) {
                                dlqConsumer.poll(Duration.ofMillis(1000L)).forEach(deadLetterRecords::add);
                            }
                        },
                        "No records found in Dead Letter topic " + deadLetterTopic + " within 20 seconds");
            }

            assertFalse(deadLetterRecords.isEmpty(), "Expected at least one record in dead letter topic!");
            log.info("[TEST] Found {} records in DLQ", deadLetterRecords.size());
        }
    }

    private List<ConsumerRecord<String, GenericErrorModel>> getDeadLetterContent(String deadLetterTopic) {
        return this.getDeadLetterContent(deadLetterTopic, Duration.ofMillis(DEFAULT_TOPIC_CONSUMPTION_TIMEOUT_MS));
    }

    /**
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
     * Wait until the given list contains the expected number of entries
     *
     * @param list the list to check
     * @param expectedListSize the expected number of entries
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

    /**
     * Wait until the given list contains at least the minimum number of entries
     *
     * @param list the list to check
     * @param minSize the minimum number of entries expected
     * @throws InterruptedException Thread exception
     */
    private <T> void waitForMinListSize(Collection<T> list, int minSize) throws InterruptedException {
        while (true) {
            if (list != null && list.size() >= minSize) {
                log.info("[TEST ASSERT] list size {} reached minimum {}", list.size(), minSize);
                return;
            }
            Thread.sleep(200);
        }
    }

    static class NotRetryableCustomException extends Exception {
        public NotRetryableCustomException(String message) {
            super(message);
        }
    }
}
