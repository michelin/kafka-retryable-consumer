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

import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.michelin.kafka.BatchRecordProcessor;
import com.michelin.kafka.RetryableBatchConsumer;
import com.michelin.kafka.avro.GenericErrorModel;
import com.michelin.kafka.configuration.KafkaRetryableConfiguration;
import java.lang.reflect.Method;
import java.time.Duration;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.Timeout;

/**
 * Integration tests of {@link RetryableBatchConsumer} against a real in-memory Kafka broker.
 *
 * <p>Same stability rules as {@link RetryableConsumerIntegrationTest}: isolated topics and consumer group per test,
 * concurrent collections keyed by record key, Awaitility conditions instead of sleeps, and a deterministic shutdown of
 * the consumer before the dead letter assertions.
 */
@Slf4j
@Timeout(value = 3, unit = TimeUnit.MINUTES)
class RetryableBatchConsumerIntegrationTest extends AbstractKafkaIntegrationTest {

    /**
     * Maximum time given to the consumer under test to process the records of a test. Generous on purpose: on a healthy
     * run the wait exits as soon as the records are processed, so a large bound costs nothing and only protects against
     * a saturated CI agent.
     */
    private static final Duration PROCESSING_TIMEOUT = Duration.ofSeconds(60);

    private static final int PARTITION_COUNT = 3;

    private String dataTopic;
    private String deadLetterTopic;
    private KafkaRetryableConfiguration configuration;

    @BeforeEach
    void initTestContext(TestInfo testInfo) {
        String testName = testInfo.getTestMethod().map(Method::getName).orElse("test");

        this.dataTopic = uniqueName("batch-topic-" + testName);
        this.deadLetterTopic = uniqueName("batch-dead-letter-" + testName);

        createTopic(dataTopic, PARTITION_COUNT);
        createTopic(deadLetterTopic, 1);

        this.configuration = newConfiguration(dataTopic, deadLetterTopic);
    }

    @Test
    void happyPath() {
        // GIVEN
        final int numberOfRecordToProduce = 30;
        produceStringRecords(dataTopic, numberOfRecordToProduce);

        // WHEN
        Map<String, ConsumerRecord<String, String>> consumedRecords = new ConcurrentHashMap<>();
        consumeUntil(
                records -> records.forEach(record -> consumedRecords.put(record.key(), record)),
                () -> consumedRecords.size() >= numberOfRecordToProduce,
                "all %d records to be processed".formatted(numberOfRecordToProduce));

        // THEN
        assertEquals(expectedKeys(numberOfRecordToProduce), new LinkedHashSet<>(consumedRecords.keySet()));
        assertDeadLetterTopicIsEmpty(deadLetterTopic);
    }

    @Test
    void happyPathHeavyDuty() {
        // GIVEN
        final int numberOfRecordToProduce = 200;
        produceStringRecords(dataTopic, numberOfRecordToProduce);

        // WHEN
        Map<String, ConsumerRecord<String, String>> consumedRecords = new ConcurrentHashMap<>();
        consumeUntil(
                records -> records.forEach(record -> consumedRecords.put(record.key(), record)),
                () -> consumedRecords.size() >= numberOfRecordToProduce,
                "all %d records to be processed".formatted(numberOfRecordToProduce));

        // THEN
        assertEquals(expectedKeys(numberOfRecordToProduce), new LinkedHashSet<>(consumedRecords.keySet()));
        assertDeadLetterTopicIsEmpty(deadLetterTopic);
    }

    @Test
    void retryableError() {
        // GIVEN
        final int numberOfRecordToProduce = 50;
        produceStringRecords(dataTopic, numberOfRecordToProduce);

        // WHEN
        // The very first batch fails with a retryable exception: it must be replayed and no record must be lost
        Map<String, ConsumerRecord<String, String>> consumedRecords = new ConcurrentHashMap<>();
        AtomicInteger failureCounter = new AtomicInteger();

        consumeUntil(
                records -> {
                    if (failureCounter.get() == 0) {
                        failureCounter.incrementAndGet();
                        throw new Exception("Fake retryable exception on first batch");
                    }
                    records.forEach(record -> consumedRecords.put(record.key(), record));
                },
                () -> consumedRecords.size() >= numberOfRecordToProduce,
                "all %d records to be processed despite one retryable batch failure"
                        .formatted(numberOfRecordToProduce));

        // THEN
        assertEquals(expectedKeys(numberOfRecordToProduce), new LinkedHashSet<>(consumedRecords.keySet()));
        assertEquals(1, failureCounter.get(), "The retryable batch failure should have been triggered exactly once");
        assertDeadLetterTopicIsEmpty(deadLetterTopic);
    }

    @Test
    void notRetryableError() {
        // GIVEN
        final int numberOfRecordToProduce = 50;
        produceStringRecords(dataTopic, numberOfRecordToProduce);

        configuration
                .getConsumer()
                .setNotRetryableExceptions(Collections.singletonList(NotRetryableCustomException.class.getName()));

        // WHEN
        // The first batch fails with a non retryable exception: it is sent to the dead letter topic and skipped,
        // the following batches are processed normally
        Map<String, ConsumerRecord<String, String>> consumedRecords = new ConcurrentHashMap<>();
        AtomicInteger failureCounter = new AtomicInteger();

        consumeUntil(
                records -> {
                    if (failureCounter.get() == 0) {
                        failureCounter.incrementAndGet();
                        throw new NotRetryableCustomException("Fake non-retryable batch error");
                    }
                    records.forEach(record -> consumedRecords.put(record.key(), record));
                },
                () -> failureCounter.get() >= 1 && !consumedRecords.isEmpty(),
                "the failing batch to be handled and the following ones to be processed");

        // THEN
        List<ConsumerRecord<String, GenericErrorModel>> deadLetterRecords = awaitDeadLetterRecords(deadLetterTopic, 1);
        assertTrue(
                deadLetterRecords.get(0).value().getStack().contains(NotRetryableCustomException.class.getSimpleName()),
                "Wrong exception sent to DLQ");
    }

    @Test
    void limitedRetryAndSuccess() {
        // GIVEN
        final int numberOfRecordToProduce = 10;
        produceStringRecords(dataTopic, numberOfRecordToProduce);

        configuration.getConsumer().setRetryMax(10L);
        configuration.getConsumer().setRetryBackoffMs(100L);

        // WHEN
        // The batch processing fails 3 times then succeeds: the retry budget of 10 must be enough
        Map<String, ConsumerRecord<String, String>> consumedRecords = new ConcurrentHashMap<>();
        AtomicInteger failureCounter = new AtomicInteger();

        consumeUntil(
                records -> {
                    if (failureCounter.get() < 3) {
                        failureCounter.incrementAndGet();
                        throw new Exception("Retryable batch error");
                    }
                    records.forEach(record -> consumedRecords.put(record.key(), record));
                },
                () -> consumedRecords.size() >= numberOfRecordToProduce,
                "all %d records to be processed after the batch retries".formatted(numberOfRecordToProduce));

        // THEN
        assertEquals(expectedKeys(numberOfRecordToProduce), new LinkedHashSet<>(consumedRecords.keySet()));
        assertEquals(3, failureCounter.get(), "The batch should have failed exactly 3 times");
        assertDeadLetterTopicIsEmpty(deadLetterTopic);
    }

    @Test
    void limitedRetryAndError() {
        // GIVEN
        final int numberOfRecordToProduce = 10;
        produceStringRecords(dataTopic, numberOfRecordToProduce);

        configuration.getConsumer().setRetryMax(3L);
        configuration.getConsumer().setRetryBackoffMs(100L);

        // WHEN
        // The batch processing always fails: once the retry budget is exhausted the batch goes to the dead letter topic
        AtomicInteger batchCallCounter = new AtomicInteger();

        consumeUntil(
                records -> {
                    batchCallCounter.incrementAndGet();
                    throw new Exception("Persistent retryable batch error");
                },
                // 1 nominal call + 3 retries: stopping earlier would make the dead letter assertion racy
                () -> batchCallCounter.get() > 3,
                "the retry budget of the batch to be exhausted");

        // THEN
        List<ConsumerRecord<String, GenericErrorModel>> deadLetterRecords = awaitDeadLetterRecords(deadLetterTopic, 1);
        assertTrue(
                deadLetterRecords.get(0).value().getStack().contains("Persistent retryable batch error"),
                "Wrong exception sent to DLQ");
    }

    /**
     * Run the batch consumer under test until {@code endCondition} is satisfied, then stop it and wait for its
     * listening thread to actually terminate.
     *
     * @param processor the business processor called for every batch
     * @param endCondition the condition ending the consumption
     * @param conditionDescription human readable description used in the failure message
     */
    private void consumeUntil(
            BatchRecordProcessor<String, String, Exception> processor,
            Supplier<Boolean> endCondition,
            String conditionDescription) {

        RetryableBatchConsumer<String, String> retryableBatchConsumer = new RetryableBatchConsumer<>(configuration);
        Future<Void> listener = retryableBatchConsumer.listenAsync(Collections.singleton(dataTopic), processor);
        try {
            await(conditionDescription)
                    .atMost(PROCESSING_TIMEOUT)
                    .pollInterval(Duration.ofMillis(100))
                    .until(endCondition::get);
        } finally {
            retryableBatchConsumer.close();
            awaitListenerTermination(listener);
        }
    }

    private static void awaitListenerTermination(Future<Void> listener) {
        try {
            listener.get(CLUSTER_OPERATION_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("Interrupted while stopping the retryable batch consumer", e);
        } catch (ExecutionException | TimeoutException e) {
            throw new IllegalStateException("The retryable batch consumer did not stop properly", e);
        }
    }

    private static Set<String> expectedKeys(int recordCount) {
        return IntStream.range(0, recordCount)
                .mapToObj(index -> "k" + index)
                .collect(Collectors.toCollection(LinkedHashSet::new));
    }

    static class NotRetryableCustomException extends Exception {
        public NotRetryableCustomException(String message) {
            super(message);
        }
    }
}
