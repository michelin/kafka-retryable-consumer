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
import com.michelin.kafka.error.DeadLetterProducer;
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

@Slf4j
public class RetryableConsumer<K, V> implements Closeable {

    /** The Kafka consumer itself */
    private final Consumer<K, V> consumer;

    /** Consumer kafka configuration */
    private final KafkaRetryableConfiguration kafkaRetryableConfiguration;

    /** Listener for rebalancing */
    private final ConsumerRebalanceListener rebalanceListener;

    /**
     * Map saving the current offset Of TopicPartitions treated by this consumer. This offsets map is used to manage
     * rewind in case of error or retry
     */
    private final Map<TopicPartition, OffsetAndMetadata> offsets = new HashMap<>();

    private ConsumerRecord<K, V> currentProcessingRecord;

    @Getter
    private String name;

    private final RetryableConsumerErrorHandler<K, V> errorHandler;

    private int retryCounter;
    private boolean wakeUp;

    /** Default constructor with no parameters */
    public RetryableConsumer(String name) throws KafkaConfigurationException {
        this(KafkaRetryableConfiguration.load());
        this.name = name;
    }

    /**
     * Constructor with parameters
     *
     * @param kafkaRetryableConfiguration kafka properties to set
     */
    public RetryableConsumer(KafkaRetryableConfiguration kafkaRetryableConfiguration) {
        this.kafkaRetryableConfiguration = kafkaRetryableConfiguration;
        this.consumer = new KafkaConsumer<>(
                this.kafkaRetryableConfiguration.getConsumer().getProperties());
        this.errorHandler = new RetryableConsumerErrorHandler<>(
                new DeadLetterProducer(this.kafkaRetryableConfiguration.getDeadLetter()),
                this.kafkaRetryableConfiguration.getConsumer().getNotRetryableExceptions());
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
    public RetryableConsumer(
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
     * Sync the current offsets with kafka cluster Handle closing down exceptions and CommitFailedExceptions in case of
     * rebalance
     *
     * <p><a href="https://docs.confluent.io/current/clients/java.html#synchronous-commits">Synchronous Commits on
     * confluent docs</a>
     */
    protected void doCommitSync() {
        try {
            log.debug("Committing offsets {}", offsets);
            consumer.commitSync(offsets);
        } catch (WakeupException e) {
            // we're shutting down, but finish the commit first and then
            // rethrow the exception so that the main loop can exit
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
            if (offsets.containsKey(tp)) { // some offset has been processed for this partition
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
                // This assigned topic partition is not present in our in memory offsets
                // => Let's seek beginning of end of partition
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
     * This method will start a blocking Kafka the consumer that will run in background. The listened topics are the one
     * * in the configuration file. Warning : this method is blocker. If you need an asynchronous consumer, use
     * listenAsync
     *
     * @param recordProcessor processor function to process every received records
     */
    public void listen(RecordProcessor<ConsumerRecord<K, V>, Exception> recordProcessor) {
        if (kafkaRetryableConfiguration.getConsumer().getTopics() == null
                || kafkaRetryableConfiguration.getConsumer().getTopics().isEmpty()) {
            throw new IllegalArgumentException("Topic list consumer configuration is not set");
        }
        this.listen(kafkaRetryableConfiguration.getConsumer().getTopics(), recordProcessor);
    }

    /**
     * This method will start a blocking Kafka the consumer that will run in background. The listened topics are the one
     * in topics parameter. (Topics list of the configuration file is ignored). Warning : this method is blocker. If you
     * need an asynchronous consumer, use listenAsync
     *
     * @param recordProcessor processor function to process every received records
     */
    public void listen(Collection<String> topics, RecordProcessor<ConsumerRecord<K, V>, Exception> recordProcessor) {
        log.info("Starting consumer for topics {}", topics);
        try {
            consumer.subscribe(topics, this.rebalanceListener);
            this.retryCounter = 0;
            while (!this.wakeUp) {
                this.pollAndConsumeRecords(recordProcessor);
            }
        } catch (WakeupException e) {
            log.info("Wake up signal received. Getting out of the while loop", e);
        } finally {
            consumer.close();
            log.info("Consumer is closed");
        }
    }

    /**
     * This method will start an asynchronous Kafka the consumer that will run in background. The listened topics are
     * the one in the configuration file.
     *
     * @param recordProcessor processor function to process every received records
     * @return A CompletableFuture of the kafka consumer listener
     */
    public Future<Void> listenAsync(RecordProcessor<ConsumerRecord<K, V>, Exception> recordProcessor) {
        return CompletableFuture.runAsync(() -> listen(recordProcessor));
    }

    /**
     * This method will start an asynchronous Kafka the consumer that will run in background. The listened topics are
     * the one in topics parameter. (Topics list of the configuration file is ignored)
     *
     * @param topics list of topics to listen to
     * @param recordProcessor processor function to process every received records
     * @return A CompletableFuture of the kafka consumer listener
     */
    public Future<Void> listenAsync(
            Collection<String> topics, RecordProcessor<ConsumerRecord<K, V>, Exception> recordProcessor) {
        return CompletableFuture.runAsync(() -> listen(topics, recordProcessor));
    }

    private void pollAndConsumeRecords(RecordProcessor<ConsumerRecord<K, V>, Exception> recordProcessor) {
        log.info("Starting polling ...");
        try {
            ConsumerRecords<K, V> records = consumer.poll(Duration.ofMillis(
                    this.kafkaRetryableConfiguration.getConsumer().getPollBackoffMs()));
            log.debug("Pulled {} records", records.count());
            if (records.count() > 0) {
                log.info("Processing records in processor {}", recordProcessor.getClass().getName());
                processRecords(recordProcessor, records);
                this.doCommitSync();
            }
        } catch (WakeupException w) {
            this.wakeUp = true;
            log.info("Wake up signal received. Getting out of the while loop", w);
            throw w;
        } catch (RecordDeserializationException e) {
            log.error("It looks like we ate a poison pill, let's skip this record!", e);
            skipCurrentOffset(e.topicPartition(), e.offset());
            errorHandler.handleConsumerDeserializationError(e);
        } catch (Exception e) {
            handleUnknownException(e);
        } finally {
            // If the consumer has been paused for any error management during event processing
            // resume it before starting the next poll loop
            if (isConsumerPaused()) {
                log.debug("Consumer was paused, resuming topic-partitions {}", consumer.assignment());
                consumer.resume(consumer.assignment());
            }
            log.info("Polling for records finished, consumer is ready for next poll");
        }
    }

    private void handleUnknownException(Exception e) {
        log.debug("Exception occurred, running error handler processing ...", e);
        consumer.pause(consumer.assignment());
        if (this.currentProcessingRecord == null) {
            // We just received a poison pills, let's skip it
            log.error("null record received", e);
        } else {
            RetryableConsumerConfiguration consumerConfig = this.kafkaRetryableConfiguration.getConsumer();
            boolean isCurrentErrorRetryable = this.retryCounter < consumerConfig.getRetryMax()
                    || consumerConfig.getRetryMax().equals(0L);
            if (errorHandler.isExceptionRetryable(e.getClass()) && isCurrentErrorRetryable) {
                log.warn(e.getMessage(), e);
                log.warn(
                        "Retryable exception occurred, launching retry process ... (retry number={})",
                        retryCounter + 1);

                // Commit the latest successful offsets
                // => on next poll the 1st record to be consumed will be the one generating this Exception
                seekAndCommitToLatestSuccessfulOffset();

                // unlimited retry => we don't care about retryCounter
                if (!consumerConfig.getRetryMax().equals(0L)) {
                    this.retryCounter++;
                }
            } else { // not retryable : let's skip this record
                // FEATURE log&fail to be implemented here (right now only log&continue-skip- exists)

                // switch to next offset, so we do not loop on an unreadable record
                skipCurrentOffset(
                        new TopicPartition(
                                this.currentProcessingRecord.topic(), this.currentProcessingRecord.partition()),
                        this.currentProcessingRecord.offset());

                // Send message to DeadLetter Topic
                errorHandler.handleError(e, this.currentProcessingRecord);
                this.doCommitSync();
                log.debug("Committing offsets {} because of not retryable exception", offsets);
                if (this.retryCounter > 0) {
                    // if we were in retry mode, and now it is in error, it means we reached the max retry
                    this.retryCounter = 0;
                }
            }
            log.debug("Processing of record with key {} completed with error", this.currentProcessingRecord.key());
        }
    }

    private void processRecords(
            RecordProcessor<ConsumerRecord<K, V>, Exception> recordProcessor, ConsumerRecords<K, V> records)
            throws Exception {
        for (ConsumerRecord<K, V> r : records) {
            log.debug("Begin processing of record with key {} ...", r.key());
            this.currentProcessingRecord = r;
            recordProcessor.processRecord(r); // Call business processor for each polled records
            updateInternalOffsetsPosition(r); // previous processing is, ok let's update our internal offset map

            // if we get there, it means at least the 1st record of the poll has been processed
            // if the retryCounter has been incremented, it means this record was previously in error
            // but the retry has worked, and the record is now processed => Let's re init retry counter
            if (this.retryCounter > 0) {
                log.info("Previously failing record processing is now successful, retry counter set to 0");
                this.retryCounter = 0;
            }
            log.debug("Processing of record with key {} completed successfully", r.key());
            this.currentProcessingRecord = null;
        }
    }

    public void stop() {
        consumer.wakeup();
    }

    public void addNonRetryableException(Class<? extends Exception>... exceptionTypes) {
        errorHandler.addNotRetryableExceptions(exceptionTypes);
    }

    public OffsetAndMetadata getCurrentOffset(TopicPartition topicPartition) {
        return this.offsets.get(topicPartition);
    }

    @Override
    public void close() {
        log.info("Closing Consumer");
        this.stop(); // this will exit the while true loop properly before consumer.close()
    }
}
