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
import com.michelin.kafka.error.RetryableConsumerErrorHandler;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
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
public class RetryableBatchConsumer<K, V>
        extends AbstractRetryableConsumer<K, V, BatchRecordProcessor<K, V, Exception>> {

    /** Default constructor with name only */
    public RetryableBatchConsumer(String name) throws KafkaConfigurationException {
        super(name);
    }

    /** Default constructor with name and custom error processor */
    public RetryableBatchConsumer(String name, ErrorProcessor<ConsumerRecord<K, V>> errorProcessor)
            throws KafkaConfigurationException {
        super(name, errorProcessor);
    }

    public RetryableBatchConsumer(
            KafkaRetryableConfiguration kafkaRetryableConfiguration,
            ErrorProcessor<ConsumerRecord<K, V>> errorProcessor) {
        super(kafkaRetryableConfiguration, errorProcessor);
    }

    public RetryableBatchConsumer(KafkaRetryableConfiguration kafkaRetryableConfiguration) {
        super(kafkaRetryableConfiguration);
    }

    public RetryableBatchConsumer(
            KafkaRetryableConfiguration kafkaRetryableConfiguration,
            KafkaConsumer<K, V> consumer,
            ErrorProcessor<ConsumerRecord<K, V>> errorProcessor,
            RetryableConsumerRebalanceListener rebalanceListener) {
        super(kafkaRetryableConfiguration, consumer, errorProcessor, rebalanceListener);
    }

    public RetryableBatchConsumer(
            KafkaRetryableConfiguration kafkaRetryableConfiguration,
            KafkaConsumer<K, V> consumer,
            ErrorProcessor<ConsumerRecord<K, V>> errorProcessor) {
        super(kafkaRetryableConfiguration, consumer, errorProcessor);
    }

    public RetryableBatchConsumer(
            KafkaRetryableConfiguration kafkaRetryableConfiguration,
            KafkaConsumer<K, V> consumer,
            RetryableConsumerErrorHandler<K, V> errorHandler,
            RetryableConsumerRebalanceListener rebalanceListener) {
        super(kafkaRetryableConfiguration, consumer, errorHandler, rebalanceListener);
    }

    @Override
    protected void processRecords(BatchRecordProcessor<K, V, Exception> batchProcessor, ConsumerRecords<K, V> records)
            throws Exception {
        batchProcessor.processRecords(records);

        // Batch processed successfully: update internal offsets for all records
        //for (ConsumerRecord<K, V> r : records) {
            //updateInternalOffsetsPosition(r);
        //}

        // If retry counter was incremented, reset it since the batch succeeded
        if (this.retryCounter > 0) {
            log.info("Previously failing batch processing is now successful, retry counter set to 0");
            this.retryCounter = 0;
        }

        log.debug("Batch of {} records processed successfully", records.count());
    }

    @Override
    protected void doCommitSync() {
        try {
            log.debug("Committing whole batch");
            consumer.commitSync();
        } catch (WakeupException e) {
            this.doCommitSync();
            throw e;
        } catch (CommitFailedException e) {
            log.warn(
                    "Commit failed Normal : due to rebalance. If this persists there may be issues with configuration or infrastructure",
                    e);
        }
    }

    @Override
    protected void handleProcessingException(Exception e) {
        // In batch mode we cannot pinpoint which specific record caused the error, so we pass null
        handleExceptionWithRetry(e, null, () -> {
            // Log&Continue in batch mode: nothing specific to skip, just commit current offsets
        });
    }

    @Override
    public OffsetAndMetadata getCurrentOffset(TopicPartition topicPartition) {

        return this.offsets.get(topicPartition);
    }
}
