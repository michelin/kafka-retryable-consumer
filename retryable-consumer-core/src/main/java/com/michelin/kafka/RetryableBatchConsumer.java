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
 * A batch-mode alternative to {@link RetryableConsumer} that processes records in batches instead of one by one, while
 * preserving the retry mechanism.
 *
 * <p>In case of a retryable error, the entire batch is retried by rewinding to the last committed offsets. In case of a
 * non-retryable error, the batch is sent to the dead-letter topic and offsets are advanced past the batch.
 *
 * @param <K> the type of key
 * @param <V> the type of value
 */
@Slf4j
public class RetryableBatchConsumer<K, V> implements Closeable {

    /** The Kafka consumer itself */
    private final Consumer<K, V> consumer;

    /** Consumer kafka configuration */
    private final KafkaRetryableConfiguration kafkaRetryableConfiguration;

    /** Listener for rebalancing */
    private final ConsumerRebalanceListener rebalanceListener;

    /**
     * Map saving the current offset of TopicPartitions treated by this consumer. This offsets map is used to manage
     * rewind in case of error or retry.
     */
    private final Map<TopicPartition, OffsetAndMetadata> offsets = new HashMap<>();

    @Getter
    private String name;

    private final RetryableConsumerErrorHandler<K, V> errorHandler;

    private int retryCounter;
    private boolean wakeUp;

    /** Default constructor with name only */
    public RetryableBatchConsumer(String name) throws KafkaConfigurationException {
        this(KafkaRetryableConfiguration.load());
        this.name = name;
    }

    /** Default constructor with name and custom error processor */
    public RetryableBatchConsumer(String name, ErrorProcessor<ConsumerRecord<K, V>> errorProcessor)
            throws KafkaConfigurationException {
        this(KafkaRetryableConfiguration.load(), errorProcessor);
        this.name = name;
    }

    /**
     * Constructor with parameters
     *
     * @param kafkaRetryableConfiguration kafka properties to set
     * @param errorProcessor custom error processor to set
     */
    public RetryableBatchConsumer(
            KafkaRetryableConfiguration kafkaRetryableConfiguration,
            ErrorProcessor<ConsumerRecord<K, V>> errorProcessor) {
        this.kafkaRetryableConfiguration = kafkaRetryableConfiguration;
        this.consumer = new KafkaConsumer<>(
                this.kafkaRetryableConfiguration.getConsumer().getProperties());
        this.rebalanceListener = new RetryableConsumerRebalanceListener(consumer, offsets);
        this.errorHandler = new RetryableConsumerErrorHandler<>(this.kafkaRetryableConfiguration, errorProcessor);
    }

    /**
     * Constructor with parameters
     *
     * @param kafkaRetryableConfiguration kafka properties to set
     */
    public RetryableBatchConsumer(KafkaRetryableConfiguration kafkaRetryableConfiguration) {
        this.kafkaRetryableConfiguration = kafkaRetryableConfiguration;
        this.consumer = new KafkaConsumer<>(
                this.kafkaRetryableConfiguration.getConsumer().getProperties());
        this.errorHandler = new RetryableConsumerErrorHandler<>(this.kafkaRetryableConfiguration);
        this.rebalanceListener = new RetryableConsumerRebalanceListener(consumer, offsets);
    }

    /**
     * Constructor with parameters
     *
     * @param kafkaRetryableConfiguration kafka properties to set
     * @param consumer kafka consumer to set
     * @param errorProcessor custom error processor to set
     * @param rebalanceListener rebalance listener to set
     */
    public RetryableBatchConsumer(
            KafkaRetryableConfiguration kafkaRetryableConfiguration,
            KafkaConsumer<K, V> consumer,
            ErrorProcessor<ConsumerRecord<K, V>> errorProcessor,
            RetryableConsumerRebalanceListener rebalanceListener) {
        this.kafkaRetryableConfiguration = kafkaRetryableConfiguration;
        this.consumer = consumer;
        this.errorHandler = new RetryableConsumerErrorHandler<>(this.kafkaRetryableConfiguration, errorProcessor);
        this.rebalanceListener = rebalanceListener;
    }

    /**
     * Constructor with parameters
     *
     * @param kafkaRetryableConfiguration kafka properties to set
     * @param consumer kafka consumer to set
     * @param errorProcessor custom error processor to set
     */
    public RetryableBatchConsumer(
            KafkaRetryableConfiguration kafkaRetryableConfiguration,
            KafkaConsumer<K, V> consumer,
            ErrorProcessor<ConsumerRecord<K, V>> errorProcessor) {
        this.kafkaRetryableConfiguration = kafkaRetryableConfiguration;
        this.consumer = consumer;
        this.errorHandler = new RetryableConsumerErrorHandler<>(this.kafkaRetryableConfiguration, errorProcessor);
        this.rebalanceListener = new RetryableConsumerRebalanceListener(consumer, offsets);
    }

    /**
     * Constructor with parameters
     *
     * @param kafkaRetryableConfiguration kafka properties to set
     * @param consumer kafka consumer to set
     * @param errorHandler error handler to set
     * @param rebalanceListener rebalance listener to set
     */
    public RetryableBatchConsumer(
            KafkaRetryableConfiguration kafkaRetryableConfiguration,
            KafkaConsumer<K, V> consumer,
            RetryableConsumerErrorHandler<K, V> errorHandler,
            RetryableConsumerRebalanceListener rebalanceListener) {
        this.kafkaRetryableConfiguration = kafkaRetryableConfiguration;
        this.consumer = consumer;
        this.errorHandler = errorHandler;
        this.rebalanceListener = rebalanceListener;
    }

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
     * Save the offset of the next record.
     *
     * @param consumerRecord The current record
     */
    protected void updateInternalOffsetsPosition(ConsumerRecord<?, ?> consumerRecord) {
        offsets.put(
                new TopicPartition(consumerRecord.topic(), consumerRecord.partition()),
                new OffsetAndMetadata(consumerRecord.offset() + 1));
    }

    /** Sync the current offsets with kafka cluster. */
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

    /**
     * This method will start a blocking Kafka consumer in batch mode. The listened topics are the ones in the
     * configuration file. Warning: this method is blocking. If you need an asynchronous consumer, use listenAsync.
     *
     * @param batchProcessor processor function to process each batch of records
     * @param errorProcessor processor function to be called whenever an unrecoverable error is detected
     */
    public void listen(
            BatchRecordProcessor<K, V, Exception> batchProcessor, ErrorProcessor<ConsumerRecord<K, V>> errorProcessor) {
        if (kafkaRetryableConfiguration.getConsumer().getTopics() == null
                || kafkaRetryableConfiguration.getConsumer().getTopics().isEmpty()) {
            throw new IllegalArgumentException("Topic list consumer configuration is not set");
        }
        this.listen(kafkaRetryableConfiguration.getConsumer().getTopics(), batchProcessor, errorProcessor);
    }

    /**
     * This method will start a blocking Kafka consumer in batch mode. The listened topics are the ones in the
     * configuration file. Warning: this method is blocking. If you need an asynchronous consumer, use listenAsync.
     *
     * @param batchProcessor processor function to process each batch of records
     */
    public void listen(BatchRecordProcessor<K, V, Exception> batchProcessor) {
        if (kafkaRetryableConfiguration.getConsumer().getTopics() == null
                || kafkaRetryableConfiguration.getConsumer().getTopics().isEmpty()) {
            throw new IllegalArgumentException("Topic list consumer configuration is not set");
        }
        this.listen(kafkaRetryableConfiguration.getConsumer().getTopics(), batchProcessor);
    }

    /**
     * This method will start a blocking Kafka consumer in batch mode. The listened topics are the ones in topics
     * parameter. Warning: this method is blocking.
     *
     * @param topics list of topics to listen to
     * @param batchProcessor processor function to process each batch of records
     */
    public void listen(Collection<String> topics, BatchRecordProcessor<K, V, Exception> batchProcessor) {
        this.listen(topics, batchProcessor, null);
    }

    /**
     * This method will start a blocking Kafka consumer in batch mode. The listened topics are the ones in topics
     * parameter. Warning: this method is blocking.
     *
     * @param topics list of topics to listen to
     * @param batchProcessor processor function to process each batch of records
     * @param errorProcessor processor function to be called whenever an unrecoverable error is detected
     */
    public void listen(
            Collection<String> topics,
            BatchRecordProcessor<K, V, Exception> batchProcessor,
            ErrorProcessor<ConsumerRecord<K, V>> errorProcessor) {
        log.info("Starting batch consumer for topics {}", topics);
        try {
            consumer.subscribe(topics, this.rebalanceListener);
            this.retryCounter = 0;
            while (!this.wakeUp) {
                this.pollAndConsumeBatch(batchProcessor);
            }
        } catch (WakeupException e) {
            log.info("Wake up signal received. Getting out of the while loop", e);
        } finally {
            consumer.close();
            log.info("Batch consumer is closed");
        }
    }

    /**
     * Start an asynchronous Kafka batch consumer.
     *
     * @param batchProcessor processor function to process each batch of records
     * @param errorProcessor processor function to be called whenever an unrecoverable error is detected
     * @return A CompletableFuture of the kafka consumer listener
     */
    public Future<Void> listenAsync(
            BatchRecordProcessor<K, V, Exception> batchProcessor, ErrorProcessor<ConsumerRecord<K, V>> errorProcessor) {
        return CompletableFuture.runAsync(() -> listen(batchProcessor, errorProcessor));
    }

    /**
     * Start an asynchronous Kafka batch consumer.
     *
     * @param batchProcessor processor function to process each batch of records
     * @return A CompletableFuture of the kafka consumer listener
     */
    public Future<Void> listenAsync(BatchRecordProcessor<K, V, Exception> batchProcessor) {
        return CompletableFuture.runAsync(() -> listen(batchProcessor));
    }

    /**
     * Start an asynchronous Kafka batch consumer with specific topics.
     *
     * @param topics list of topics to listen to
     * @param batchProcessor processor function to process each batch of records
     * @return A CompletableFuture of the kafka consumer listener
     */
    public Future<Void> listenAsync(Collection<String> topics, BatchRecordProcessor<K, V, Exception> batchProcessor) {
        return CompletableFuture.runAsync(() -> listen(topics, batchProcessor, null));
    }

    private void pollAndConsumeBatch(BatchRecordProcessor<K, V, Exception> batchProcessor) {
        try {
            log.info("Polling for records (batch mode) ...");
            ConsumerRecords<K, V> records = consumer.poll(Duration.ofMillis(
                    this.kafkaRetryableConfiguration.getConsumer().getPollBackoffMs()));
            log.debug("Pulled {} records (batch mode)", records.count());
            if (records.count() > 0) {
                log.info(
                        "Processing batch of {} records in processor {}",
                        records.count(),
                        batchProcessor.getClass().getName());
                processBatch(batchProcessor, records);
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
            handleBatchException(e);
        } finally {
            if (isConsumerPaused() && !wakeUp) {
                log.debug("Consumer was paused, resuming topic-partitions {}", consumer.assignment());
                consumer.resume(consumer.assignment());
            }
        }
    }

    private void handleBatchException(Exception e) {
        log.debug("Exception occurred during batch processing, running error handler ...", e);
        consumer.pause(consumer.assignment());

        RetryableConsumerConfiguration consumerConfig = this.kafkaRetryableConfiguration.getConsumer();
        boolean isCurrentErrorRetryable = this.retryCounter < consumerConfig.getRetryMax()
                || consumerConfig.getRetryMax().equals(0L);

        if (errorHandler.isExceptionRetryable(e.getClass()) && isCurrentErrorRetryable) {
            log.warn(e.getMessage(), e);
            log.warn(
                    "Retryable exception occurred during batch processing, launching retry process ... (retry number={})",
                    retryCounter + 1);

            // Rewind to last committed offsets => the entire batch will be re-consumed
            seekAndCommitToLatestSuccessfulOffset();

            if (!consumerConfig.getRetryMax().equals(0L)) {
                this.retryCounter++;
            }
        } else {
            // Non-recoverable error: send the first record of the batch to DLQ as representative
            // (in batch mode we cannot pinpoint which specific record caused the error)
            errorHandler.handleError(e, null);

            if (consumerConfig.getStopOnError()) {
                log.error(
                        "Non-recoverable error occurred during batch processing. Stopping consumer after 'stop-on-error' configuration...",
                        e);
                this.stop();
            } else {
                // Advance past the current batch: the offsets were already updated in processBatch
                // for successfully processed records. Commit what we have.
                this.doCommitSync();
                log.debug("Committing offsets {} because of not retryable exception in batch mode", offsets);

                if (this.retryCounter > 0) {
                    this.retryCounter = 0;
                }
            }
        }
    }

    /**
     * Process the entire batch of records. Updates internal offsets for each record after the batch processor completes
     * successfully.
     */
    private void processBatch(BatchRecordProcessor<K, V, Exception> batchProcessor, ConsumerRecords<K, V> records)
            throws Exception {
        batchProcessor.processRecords(records);

        // Batch processed successfully: update internal offsets for all records
        for (ConsumerRecord<K, V> r : records) {
            updateInternalOffsetsPosition(r);
        }

        // If retry counter was incremented, reset it since the batch succeeded
        if (this.retryCounter > 0) {
            log.info("Previously failing batch processing is now successful, retry counter set to 0");
            this.retryCounter = 0;
        }

        log.debug("Batch of {} records processed successfully", records.count());
    }

    public void stop() {
        consumer.wakeup();
        this.wakeUp = true;
    }

    @SafeVarargs
    public final void addNonRetryableException(Class<? extends Exception>... exceptionTypes) {
        errorHandler.addNotRetryableExceptions(exceptionTypes);
    }

    public OffsetAndMetadata getCurrentOffset(TopicPartition topicPartition) {
        return this.offsets.get(topicPartition);
    }

    @Override
    public void close() {
        log.info("Closing Batch Consumer ...");
        this.stop();
    }

    public boolean isStopped() {
        return this.wakeUp;
    }
}
