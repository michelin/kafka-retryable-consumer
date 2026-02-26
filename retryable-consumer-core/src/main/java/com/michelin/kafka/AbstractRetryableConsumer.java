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
package com.michelin.kafka;

import com.michelin.kafka.configuration.KafkaConfigurationException;
import com.michelin.kafka.configuration.KafkaRetryableConfiguration;
import com.michelin.kafka.configuration.RetryableConsumerConfiguration;
import com.michelin.kafka.error.RetryableConsumerErrorHandler;
import java.io.Closeable;
import java.time.Duration;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.consumer.internals.AutoOffsetResetStrategy;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.RecordDeserializationException;
import org.apache.kafka.common.errors.WakeupException;

/**
 * Abstract base class for retryable Kafka consumers. Contains all common logic for offset management, commit, retry,
 * pause/resume, and the poll loop. Subclasses only need to implement the processor-specific logic.
 *
 * @param <K> the type of key
 * @param <V> the type of value
 * @param <P> the type of processor (e.g. RecordProcessor or BatchRecordProcessor)
 */
@Slf4j
public abstract class AbstractRetryableConsumer<K, V, P> implements Closeable {

    /** The Kafka consumer itself */
    protected final Consumer<K, V> consumer;

    /** Consumer kafka configuration */
    protected final KafkaRetryableConfiguration kafkaRetryableConfiguration;

    /** Listener for rebalancing */
    protected final ConsumerRebalanceListener rebalanceListener;

    /**
     * Map saving the current offset of TopicPartitions treated by this consumer. This offsets map is used to manage
     * rewind in case of error or retry.
     */
    protected final Map<TopicPartition, OffsetAndMetadata> offsets = new HashMap<>();

    @Getter
    protected String name;

    protected final RetryableConsumerErrorHandler<K, V> errorHandler;

    protected int retryCounter;
    protected boolean wakeUp;

    // ---- Constructors ----

    protected AbstractRetryableConsumer(String name) throws KafkaConfigurationException {
        this(KafkaRetryableConfiguration.load());
        this.name = name;
    }

    protected AbstractRetryableConsumer(String name, ErrorProcessor<ConsumerRecord<K, V>> errorProcessor)
            throws KafkaConfigurationException {
        this(KafkaRetryableConfiguration.load(), errorProcessor);
        this.name = name;
    }

    protected AbstractRetryableConsumer(
            KafkaRetryableConfiguration kafkaRetryableConfiguration,
            ErrorProcessor<ConsumerRecord<K, V>> errorProcessor) {
        this.kafkaRetryableConfiguration = kafkaRetryableConfiguration;
        this.consumer = new KafkaConsumer<>(
                this.kafkaRetryableConfiguration.getConsumer().getProperties());
        this.rebalanceListener = new RetryableConsumerRebalanceListener(consumer, offsets);
        this.errorHandler = new RetryableConsumerErrorHandler<>(this.kafkaRetryableConfiguration, errorProcessor);
    }

    protected AbstractRetryableConsumer(KafkaRetryableConfiguration kafkaRetryableConfiguration) {
        this.kafkaRetryableConfiguration = kafkaRetryableConfiguration;
        this.consumer = new KafkaConsumer<>(
                this.kafkaRetryableConfiguration.getConsumer().getProperties());
        this.errorHandler = new RetryableConsumerErrorHandler<>(this.kafkaRetryableConfiguration);
        this.rebalanceListener = new RetryableConsumerRebalanceListener(consumer, offsets);
    }

    protected AbstractRetryableConsumer(
            KafkaRetryableConfiguration kafkaRetryableConfiguration,
            KafkaConsumer<K, V> consumer,
            ErrorProcessor<ConsumerRecord<K, V>> errorProcessor,
            RetryableConsumerRebalanceListener rebalanceListener) {
        this.kafkaRetryableConfiguration = kafkaRetryableConfiguration;
        this.consumer = consumer;
        this.errorHandler = new RetryableConsumerErrorHandler<>(this.kafkaRetryableConfiguration, errorProcessor);
        this.rebalanceListener = rebalanceListener;
    }

    protected AbstractRetryableConsumer(
            KafkaRetryableConfiguration kafkaRetryableConfiguration,
            KafkaConsumer<K, V> consumer,
            ErrorProcessor<ConsumerRecord<K, V>> errorProcessor) {
        this.kafkaRetryableConfiguration = kafkaRetryableConfiguration;
        this.consumer = consumer;
        this.errorHandler = new RetryableConsumerErrorHandler<>(this.kafkaRetryableConfiguration, errorProcessor);
        this.rebalanceListener = new RetryableConsumerRebalanceListener(consumer, offsets);
    }

    protected AbstractRetryableConsumer(
            KafkaRetryableConfiguration kafkaRetryableConfiguration,
            KafkaConsumer<K, V> consumer,
            RetryableConsumerErrorHandler<K, V> errorHandler,
            RetryableConsumerRebalanceListener rebalanceListener) {
        this.kafkaRetryableConfiguration = kafkaRetryableConfiguration;
        this.consumer = consumer;
        this.errorHandler = errorHandler;
        this.rebalanceListener = rebalanceListener;
    }

    // ---- Common utility methods ----

    /**
     * Jump to the offset of the next record.
     *
     * @param topicPartition the topic partition on which we want to fast-forward to next record
     * @param currentOffset the offset that needs to be skipped
     */
    protected void skipCurrentOffset(TopicPartition topicPartition, long currentOffset) {
        offsets.put(topicPartition, new OffsetAndMetadata(currentOffset + 1));
        consumer.seek(topicPartition, currentOffset + 1);
    }

    /**
     * Check if the given Kafka consumer is paused or not.
     *
     * @return True if it is paused, false otherwise
     */
    protected boolean isConsumerPaused() {
        return !this.consumer.paused().isEmpty();
    }

    /**
     * Save the offset of the next record. Offsets are saved according to the name of its belonging topic and the number
     * of its belonging partition.
     *
     * @param consumerRecord The current record
     */
    protected void updateInternalOffsetsPosition(ConsumerRecord<?, ?> consumerRecord) {
        offsets.put(
                new TopicPartition(consumerRecord.topic(), consumerRecord.partition()),
                new OffsetAndMetadata(consumerRecord.offset() + 1));
    }

    /**
     * Sync the current offsets with kafka cluster. Handle closing down exceptions and CommitFailedExceptions in case of
     * rebalance.
     *
     * <p><a href="https://docs.confluent.io/current/clients/java.html#synchronous-commits">Synchronous Commits on
     * confluent docs</a>
     */
    protected void doCommitSync() {
        try {
            log.debug("Committing offsets {}", offsets);
            consumer.commitSync(offsets);
        } catch (WakeupException e) {
            this.doCommitSync();
            throw e;
        } catch (CommitFailedException e) {
            log.warn(
                    "Commit failed Normal : due to rebalance. If this persists there may be issues with configuration or infrastructure",
                    e);
        }
    }

    /** Seek the consumer to the latest in memory saved offsets */
    protected void seekAndCommitToLatestSuccessfulOffset() {
        consumer.assignment().forEach(tp -> {
            if (offsets.containsKey(tp)) {
                if (offsets.get(tp) != null) {
                    consumer.seek(tp, offsets.get(tp));
                    consumer.commitSync();
                    log.info("Seeked offset {}, for topic partition {}", offsets.get(tp), tp.toString());
                } else {
                    log.warn(
                            "Cannot rewind on {} to null offset, "
                                    + "this could happen if the consumer group was just created",
                            tp.toString());
                }
            } else {
                String autoOffsetReset = (String) this.kafkaRetryableConfiguration
                        .getConsumer()
                        .getProperties()
                        .getOrDefault(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, AutoOffsetResetStrategy.EARLIEST.name());
                if (autoOffsetReset.equalsIgnoreCase(AutoOffsetResetStrategy.EARLIEST.name())) {
                    consumer.seekToBeginning(Collections.singleton(tp));
                    log.info("Seeked beginning of consumer assignment {}", tp.toString());
                } else if (autoOffsetReset.equalsIgnoreCase(AutoOffsetResetStrategy.LATEST.name())) {
                    consumer.seekToEnd(Collections.singleton(tp));
                    log.info("Seeked end of consumer assignment {}", tp.toString());
                }
            }
        });
    }

    // ---- Listen / ListenAsync (Template Method pattern) ----

    public void listen(P processor, ErrorProcessor<ConsumerRecord<K, V>> errorProcessor) {
        if (kafkaRetryableConfiguration.getConsumer().getTopics() == null
                || kafkaRetryableConfiguration.getConsumer().getTopics().isEmpty()) {
            throw new IllegalArgumentException("Topic list consumer configuration is not set");
        }
        this.listen(kafkaRetryableConfiguration.getConsumer().getTopics(), processor, errorProcessor);
    }

    public void listen(P processor) {
        if (kafkaRetryableConfiguration.getConsumer().getTopics() == null
                || kafkaRetryableConfiguration.getConsumer().getTopics().isEmpty()) {
            throw new IllegalArgumentException("Topic list consumer configuration is not set");
        }
        this.listen(kafkaRetryableConfiguration.getConsumer().getTopics(), processor);
    }

    public void listen(Collection<String> topics, P processor) {
        this.listen(topics, processor, null);
    }

    public void listen(Collection<String> topics, P processor, ErrorProcessor<ConsumerRecord<K, V>> errorProcessor) {
        log.info("Starting consumer for topics {}", topics);
        try {
            consumer.subscribe(topics, this.rebalanceListener);
            this.retryCounter = 0;
            while (!this.wakeUp) {
                this.pollAndConsume(processor);
            }
        } catch (WakeupException e) {
            log.info("Wake up signal received. Getting out of the while loop", e);
        } finally {
            consumer.close();
            log.info("Consumer is closed");
        }
    }

    public Future<Void> listenAsync(P processor, ErrorProcessor<ConsumerRecord<K, V>> errorProcessor) {
        return CompletableFuture.runAsync(() -> listen(processor, errorProcessor));
    }

    public Future<Void> listenAsync(P processor) {
        return CompletableFuture.runAsync(() -> listen(processor));
    }

    public Future<Void> listenAsync(Collection<String> topics, P processor) {
        return CompletableFuture.runAsync(() -> listen(topics, processor, null));
    }

    // ---- Poll loop (common skeleton) ----

    private void pollAndConsume(P processor) {
        try {
            log.info("Polling for records ...");
            ConsumerRecords<K, V> records = consumer.poll(Duration.ofMillis(
                    this.kafkaRetryableConfiguration.getConsumer().getPollBackoffMs()));
            log.debug("Pulled {} records", records.count());
            if (records.count() > 0) {
                log.info(
                        "Processing {} records in processor {}",
                        records.count(),
                        processor.getClass().getName());
                processRecords(processor, records);
                this.doCommitSync();
            }
        } catch (WakeupException w) {
            this.wakeUp = true;
            log.info("Wake up signal received. Getting out of the while loop", w);
            throw w;
        } catch (RecordDeserializationException e) {
            log.error("It looks like we ate a poison pill, let's skip this record!", e);
            skipCurrentOffset(e.topicPartition(), e.offset());
            errorHandler.handleError(e, null);
        } catch (Exception e) {
            handleProcessingException(e);
        } finally {
            if (isConsumerPaused() && !wakeUp) {
                log.debug("Consumer was paused, resuming topic-partitions {}", consumer.assignment());
                consumer.resume(consumer.assignment());
            }
        }
    }

    // ---- Abstract methods for subclass-specific logic ----

    /**
     * Process the polled records. Called by the poll loop when records are available.
     *
     * @param processor the processor to use
     * @param records the polled records
     * @throws Exception if processing fails
     */
    protected abstract void processRecords(P processor, ConsumerRecords<K, V> records) throws Exception;

    /**
     * Handle a processing exception. Called by the poll loop when an exception occurs during record processing.
     *
     * @param e the exception that occurred
     */
    protected abstract void handleProcessingException(Exception e);

    // ---- Common helper for retry logic ----

    /**
     * Common retry/error handling logic. Subclasses call this from their {@link #handleProcessingException} to benefit
     * from the shared retry mechanism.
     *
     * @param e the exception that occurred
     * @param failedRecord the record that failed (may be null in batch mode)
     * @param nonRetryableAction action to perform after sending to DLQ when stopOnError is false (e.g. skip offset or
     *     commit)
     */
    protected void handleExceptionWithRetry(
            Exception e, ConsumerRecord<K, V> failedRecord, Runnable nonRetryableAction) {
        log.debug("Exception occurred, running error handler processing ...", e);
        consumer.pause(consumer.assignment());

        RetryableConsumerConfiguration consumerConfig = this.kafkaRetryableConfiguration.getConsumer();
        boolean isCurrentErrorRetryable = this.retryCounter < consumerConfig.getRetryMax()
                || consumerConfig.getRetryMax().equals(0L);

        if (errorHandler.isExceptionRetryable(e.getClass()) && isCurrentErrorRetryable) {
            log.warn(e.getMessage(), e);
            log.warn("Retryable exception occurred, launching retry process ... (retry number={})", retryCounter + 1);

            // Commit the latest successful offsets
            // => on next poll the record/batch will be re-consumed from the last committed position
            seekAndCommitToLatestSuccessfulOffset();

            // unlimited retry => we don't care about retryCounter
            if (!consumerConfig.getRetryMax().equals(0L)) {
                this.retryCounter++;
            }
        } else {
            // Non-recoverable error: send to Dead Letter Topic
            errorHandler.handleError(e, failedRecord);

            if (consumerConfig.getStopOnError()) {
                log.error(
                        "Non-recoverable error occurred (Not retryable, or retry limit reached). Stopping consumer after 'stop-on-error' configuration...",
                        e);
                this.stop();
            } else {
                nonRetryableAction.run();
                this.doCommitSync();
                log.debug("Committing offsets {} because of not retryable exception", offsets);

                if (this.retryCounter > 0) {
                    this.retryCounter = 0;
                }
            }
        }
    }

    // ---- Public control methods ----

    public void stop() {
        consumer.wakeup();
        this.wakeUp = true;
    }

    @SafeVarargs
    public final void addNonRetryableException(Class<? extends Exception>... exceptionTypes) {
        errorHandler.addNotRetryableExceptions(exceptionTypes);
    }

    // must be overridden by retryable consumer to control offset from a partition
    public abstract OffsetAndMetadata getCurrentOffset(TopicPartition topicPartition);

    @Override
    public void close() {
        log.info("Closing Consumer ...");
        this.stop();
    }

    public boolean isStopped() {
        return this.wakeUp;
    }
}
