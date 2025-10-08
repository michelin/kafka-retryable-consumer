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

import com.michelin.kafka.configuration.KafkaRetryableConfiguration;
import com.michelin.kafka.configuration.RetryableConsumerConfiguration;
import com.michelin.kafka.error.DeadLetterProducer;
import com.michelin.kafka.error.RetryableConsumerErrorHandler;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.consumer.internals.AutoOffsetResetStrategy;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.RecordDeserializationException;
import org.apache.kafka.common.errors.WakeupException;

import java.io.Closeable;
import java.time.Duration;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

@Slf4j
public abstract class AbstractRetryableConsumer<K, V> implements Closeable {

    protected final Consumer<K, V> consumer;
    protected final KafkaRetryableConfiguration kafkaRetryableConfiguration;
    protected final ConsumerRebalanceListener rebalanceListener;
    protected final Map<TopicPartition, OffsetAndMetadata> offsets = new HashMap<>();
    protected final RetryableConsumerErrorHandler<K, V> errorHandler;

    protected ConsumerRecord<K, V> currentProcessingRecord;

    @Getter
    protected String name;

    protected int retryCounter;
    protected boolean wakeUp;

    protected AbstractRetryableConsumer(KafkaRetryableConfiguration kafkaRetryableConfiguration) {
        this.kafkaRetryableConfiguration = kafkaRetryableConfiguration;
        this.consumer =
                new KafkaConsumer<>(kafkaRetryableConfiguration.getConsumer().getProperties());
        this.errorHandler = new RetryableConsumerErrorHandler<>(
                new DeadLetterProducer(kafkaRetryableConfiguration.getDeadLetter()),
                kafkaRetryableConfiguration.getConsumer().getNotRetryableExceptions());
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

    protected void skipCurrentOffset(TopicPartition topicPartition, long currentOffset) {
        offsets.put(topicPartition, new OffsetAndMetadata(currentOffset + 1));
        consumer.seek(topicPartition, currentOffset + 1);
    }

    protected boolean isConsumerPaused() {
        return !this.consumer.paused().isEmpty();
    }

    protected void updateInternalOffsetsPosition(ConsumerRecord<?, ?> consumerRecord) {
        offsets.put(
                new TopicPartition(consumerRecord.topic(), consumerRecord.partition()),
                new OffsetAndMetadata(consumerRecord.offset() + 1));
    }

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

    protected void seekAndCommitToLatestSuccessfulOffset() {
        consumer.assignment().forEach(tp -> {
            if (offsets.containsKey(tp)) {
                if (offsets.get(tp) != null) {
                    consumer.seek(tp, offsets.get(tp));
                    consumer.commitSync();
                    log.info("Seeked offset {}, for topic partition {}", offsets.get(tp), tp.toString());
                } else {
                    log.warn(
                            "Cannot rewind on {} to null offset, this could happen if the consumer group was just created",
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

    protected void pollAndConsumeRecordsInternal() {
        try {
            log.info("Polling for records ...");
            ConsumerRecords<K, V> records = consumer.poll(Duration.ofMillis(
                    this.kafkaRetryableConfiguration.getConsumer().getPollBackoffMs()));
            log.debug("Pulled {} records", records.count());

            if (records.count() > 0) {
                log.info("Processing {} records", records.count());
                processRecordsTemplate(records);
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
            if (isConsumerPaused() && !wakeUp) {
                log.debug("Consumer was paused, resuming topic-partitions {}", consumer.assignment());
                consumer.resume(consumer.assignment());
            }
        }
    }

    protected void handleUnknownException(Exception e) {
        log.debug("Exception occurred, running error handler processing ...", e);
        consumer.pause(consumer.assignment());

        if (this.currentProcessingRecord == null) {
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
                seekAndCommitToLatestSuccessfulOffset();

                if (!consumerConfig.getRetryMax().equals(0L)) {
                    this.retryCounter++;
                }
            } else {
                errorHandler.handleError(e, this.currentProcessingRecord);

                if (consumerConfig.getStopOnError()) {
                    log.error(
                            "Non-recoverable error occurred (Not retryable, or retry limit reached). Stopping consumer after 'stop-on-error' configuration...",
                            e);
                    this.stop();
                } else {
                    skipCurrentOffset(
                            new TopicPartition(
                                    this.currentProcessingRecord.topic(), this.currentProcessingRecord.partition()),
                            this.currentProcessingRecord.offset());
                    this.doCommitSync();
                    log.debug("Committing offsets {} because of not retryable exception", offsets);

                    if (this.retryCounter > 0) {
                        this.retryCounter = 0;
                    }
                }
            }
            log.debug("Processing of record with key {} completed with error", this.currentProcessingRecord.key());
        }
    }

    protected void listenInternal(Collection<String> topics) {
        log.info("Starting consumer for topics {}", topics);
        try {
            consumer.subscribe(topics, this.rebalanceListener);
            this.retryCounter = 0;
            while (!this.wakeUp) {
                this.pollAndConsumeRecordsInternal();
            }
        } catch (WakeupException e) {
            log.info("Wake up signal received. Getting out of the while loop", e);
        } finally {
            consumer.close();
            log.info("Consumer is closed");
        }
    }

    // Template method to be implemented by subclasses
    protected abstract void processRecordsTemplate(ConsumerRecords<K, V> records) throws Exception;

    protected abstract void setCurrentProcessor(Object processor);

    public void stop() {
        consumer.wakeup();
        this.wakeUp = true;
    }

    public void addNonRetryableException(Class<? extends Exception>... exceptionTypes) {
        errorHandler.addNotRetryableExceptions(exceptionTypes);
    }

    public OffsetAndMetadata getCurrentOffset(TopicPartition topicPartition) {
        return this.offsets.get(topicPartition);
    }

    @Override
    public void close() {
        log.info("Closing Consumer ...");
        this.stop();
    }

    public boolean isStopped() {
        return this.wakeUp;
    }
}
