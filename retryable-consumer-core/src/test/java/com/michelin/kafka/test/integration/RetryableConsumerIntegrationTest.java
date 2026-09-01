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
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.michelin.kafka.RecordProcessor;
import com.michelin.kafka.RetryableConsumer;
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
import java.util.concurrent.atomic.AtomicBoolean;
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
 * Integration tests of {@link RetryableConsumer} against a real in-memory Kafka broker.
 *
 * <p>Stability rules applied here:
 *
 * <ul>
 *   <li>every test owns its topics, its consumer group and its {@link KafkaRetryableConfiguration}, so no state leaks
 *       from one test to another and the tests are order independent;
 *   <li>records processed by the consumer thread are collected in a concurrent map keyed by the record key, so the
 *       assertions are immune to the at-least-once duplicates Kafka is allowed to deliver;
 *   <li>every wait is an Awaitility condition that returns as soon as it is satisfied and fails with an explicit
 *       message otherwise, instead of a fixed sleep or an unbounded polling loop;
 *   <li>the consumer is always stopped and joined before the assertions on the dead letter topic, so no background
 *       thread can still be committing offsets while the next test runs.
 * </ul>
 *
 * <p>The class level timeout is only a safety net for a deadlock: the Awaitility timeouts are much shorter and always
 * fire first with an actionable message.
 */
@Slf4j
@Timeout(value = 3, unit = TimeUnit.MINUTES)
class RetryableConsumerIntegrationTest extends AbstractKafkaIntegrationTest {

    /**
     * Maximum time given to the consumer under test to process the records of a test. Generous on purpose: on a healthy
     * run the wait exits as soon as the records are processed, so a large bound costs nothing and only protects against
     * a saturated CI agent.
     */
    private static final Duration PROCESSING_TIMEOUT = Duration.ofSeconds(60);

    /**
     * Number of partitions of the data topic. More than one partition is required by the retry tests: a blocking record
     * must not prevent the records of the other partitions from being processed.
     */
    private static final int PARTITION_COUNT = 3;

    private String dataTopic;
    private String deadLetterTopic;
    private KafkaRetryableConfiguration configuration;

    @BeforeEach
    void initTestContext(TestInfo testInfo) {
        String testName = testInfo.getTestMethod().map(Method::getName).orElse("test");

        this.dataTopic = uniqueName("topic-" + testName);
        this.deadLetterTopic = uniqueName("dead-letter-" + testName);

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
                consumerRecord -> consumedRecords.put(consumerRecord.key(), consumerRecord),
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
                consumerRecord -> consumedRecords.put(consumerRecord.key(), consumerRecord),
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
        final String keyInError = "k7";
        produceStringRecords(dataTopic, numberOfRecordToProduce);

        // WHEN
        // "k7" fails once with a retryable exception: the consumer must rewind and process it successfully
        Map<String, ConsumerRecord<String, String>> consumedRecords = new ConcurrentHashMap<>();
        AtomicBoolean firstAttemptOnFailingKey = new AtomicBoolean(true);

        consumeUntil(
                consumerRecord -> {
                    if (keyInError.equals(consumerRecord.key()) && firstAttemptOnFailingKey.getAndSet(false)) {
                        throw new Exception("Fake retryable exception"); // "Exception" is retryable by default
                    }
                    consumedRecords.put(consumerRecord.key(), consumerRecord);
                },
                () -> consumedRecords.size() >= numberOfRecordToProduce,
                "all %d records to be processed despite one retryable failure".formatted(numberOfRecordToProduce));

        // THEN
        assertEquals(expectedKeys(numberOfRecordToProduce), new LinkedHashSet<>(consumedRecords.keySet()));
        assertFalse(firstAttemptOnFailingKey.get(), "The retryable failure was never triggered");
        assertDeadLetterTopicIsEmpty(deadLetterTopic);
    }

    @Test
    void notRetryableError() {
        // GIVEN
        final int numberOfRecordToProduce = 50;
        final String keyInError = "k23";
        produceStringRecords(dataTopic, numberOfRecordToProduce);

        configuration
                .getConsumer()
                .setNotRetryableExceptions(Collections.singletonList(NotRetryableCustomException.class.getName()));

        // WHEN
        // "k23" always fails with a non retryable exception: it must be routed to the dead letter topic and skipped
        Map<String, ConsumerRecord<String, String>> consumedRecords = new ConcurrentHashMap<>();
        AtomicInteger failureCounter = new AtomicInteger();
        consumeUntil(
                consumerRecord -> {
                    if (keyInError.equals(consumerRecord.key())) {
                        failureCounter.incrementAndGet();
                        throw new NotRetryableCustomException("Fake error");
                    }
                    consumedRecords.put(consumerRecord.key(), consumerRecord);
                },
                // Stopping the consumer before the poison pill is handled would make the dead letter assertions racy
                () -> consumedRecords.size() >= numberOfRecordToProduce - 1 && failureCounter.get() >= 1,
                "the %d valid records to be processed and the poison pill to be handled"
                        .formatted(numberOfRecordToProduce - 1));

        // THEN
        Set<String> expectedKeys = expectedKeys(numberOfRecordToProduce);
        expectedKeys.remove(keyInError);
        assertEquals(expectedKeys, new LinkedHashSet<>(consumedRecords.keySet()));

        /* Verify the poison pill has been sent to the dead letter topic */
        List<ConsumerRecord<String, GenericErrorModel>> deadLetterRecords = awaitDeadLetterRecords(deadLetterTopic, 1);
        GenericErrorModel deadLetterError = deadLetterRecords.get(0).value();
        assertEquals(dataTopic, deadLetterError.getTopic(), "Wrong topic name defined in the DL topic message");
        assertEquals(keyInError, deadLetterError.getKey(), "Wrong Key defined in the DL topic message");
        assertEquals("value23", deadLetterError.getValue(), "Wrong Value defined in the DL topic message");
        assertTrue(
                deadLetterError.getStack().contains(NotRetryableCustomException.class.getSimpleName()),
                "Wrong exception sent to DLQ");
    }

    @Test
    void limitedRetryAndSuccess() {
        // GIVEN
        final int numberOfRecordToProduce = 10;
        final String keyInError = "k7";
        produceStringRecords(dataTopic, numberOfRecordToProduce);

        configuration.getConsumer().setRetryMax(10L);
        configuration.getConsumer().setRetryBackoffMs(100L);

        // WHEN
        // "k7" fails 5 times then succeeds: the retry budget of 10 must be enough
        Map<String, ConsumerRecord<String, String>> consumedRecords = new ConcurrentHashMap<>();
        AtomicInteger failureCounter = new AtomicInteger();

        consumeUntil(
                consumerRecord -> {
                    if (keyInError.equals(consumerRecord.key()) && failureCounter.get() < 5) {
                        failureCounter.incrementAndGet();
                        throw new Exception("Retryable Error");
                    }
                    consumedRecords.put(consumerRecord.key(), consumerRecord);
                },
                () -> consumedRecords.size() >= numberOfRecordToProduce,
                "all %d records to be processed after the retries of %s"
                        .formatted(numberOfRecordToProduce, keyInError));

        // THEN
        assertEquals(expectedKeys(numberOfRecordToProduce), new LinkedHashSet<>(consumedRecords.keySet()));
        assertEquals(5, failureCounter.get(), "The failing record should have been retried exactly 5 times");
        assertDeadLetterTopicIsEmpty(deadLetterTopic);
    }

    @Test
    void limitedRetryAndError() {
        // GIVEN
        final int numberOfRecordToProduce = 10;
        final String keyInError = "k7";
        produceStringRecords(dataTopic, numberOfRecordToProduce);

        configuration.getConsumer().setRetryMax(10L);
        configuration.getConsumer().setRetryBackoffMs(100L);

        // WHEN
        // "k7" always fails: once the retry budget is exhausted the record must be routed to the dead letter topic
        Map<String, ConsumerRecord<String, String>> consumedRecords = new ConcurrentHashMap<>();
        AtomicInteger failingKeyReceptionCounter = new AtomicInteger();

        consumeUntil(
                consumerRecord -> {
                    if (keyInError.equals(consumerRecord.key())) {
                        failingKeyReceptionCounter.getAndIncrement();
                        throw new Exception("Retryable Error");
                    }
                    consumedRecords.put(consumerRecord.key(), consumerRecord);
                },
                () -> consumedRecords.size() >= numberOfRecordToProduce - 1 && failingKeyReceptionCounter.get() > 10,
                "the %d valid records to be processed and the retry budget of %s to be exhausted"
                        .formatted(numberOfRecordToProduce - 1, keyInError));

        // THEN
        Set<String> expectedKeys = expectedKeys(numberOfRecordToProduce);
        expectedKeys.remove(keyInError);
        assertEquals(expectedKeys, new LinkedHashSet<>(consumedRecords.keySet()));
        assertTrue(
                failingKeyReceptionCounter.get() > 10,
                "More than 10 retries should occur, got " + failingKeyReceptionCounter.get());

        /* Verify the record is finally sent to the dead letter topic */
        List<ConsumerRecord<String, GenericErrorModel>> deadLetterRecords = awaitDeadLetterRecords(deadLetterTopic, 1);
        assertEquals(keyInError, deadLetterRecords.get(0).value().getKey());
    }

    /**
     * Run the consumer under test until {@code endCondition} is satisfied, then stop it and wait for its listening
     * thread to actually terminate. Joining the thread before returning guarantees that the consumer has left its group
     * and flushed its dead letter records, which makes the assertions that follow deterministic.
     *
     * @param processor the business processor called for every record
     * @param endCondition the condition ending the consumption
     * @param conditionDescription human readable description used in the failure message
     */
    private void consumeUntil(
            RecordProcessor<ConsumerRecord<String, String>, Exception> processor,
            Supplier<Boolean> endCondition,
            String conditionDescription) {

        RetryableConsumer<String, String> retryableConsumer = new RetryableConsumer<>(configuration);
        Future<Void> listener = retryableConsumer.listenAsync(Collections.singleton(dataTopic), processor);
        try {
            await(conditionDescription)
                    .atMost(PROCESSING_TIMEOUT)
                    .pollInterval(Duration.ofMillis(100))
                    .until(endCondition::get);
        } finally {
            retryableConsumer.close();
            awaitListenerTermination(listener);
        }
    }

    private static void awaitListenerTermination(Future<Void> listener) {
        try {
            listener.get(CLUSTER_OPERATION_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("Interrupted while stopping the retryable consumer", e);
        } catch (ExecutionException | TimeoutException e) {
            throw new IllegalStateException("The retryable consumer did not stop properly", e);
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
