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
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;

@Slf4j
public class RetryableConsumer<K, V>
        extends AbstractRetryableConsumer<K, V, RecordProcessor<ConsumerRecord<K, V>, Exception>> {

    private ConsumerRecord<K, V> currentProcessingRecord;

    /** Default constructor with no parameters */
    public RetryableConsumer(String name) throws KafkaConfigurationException {
        super(name);
    }

    /** Default constructor with no parameters */
    public RetryableConsumer(String name, ErrorProcessor<ConsumerRecord<K, V>> errorProcessor)
            throws KafkaConfigurationException {
        super(name, errorProcessor);
    }

    public RetryableConsumer(
            KafkaRetryableConfiguration kafkaRetryableConfiguration,
            ErrorProcessor<ConsumerRecord<K, V>> errorProcessor) {
        super(kafkaRetryableConfiguration, errorProcessor);
    }

    public RetryableConsumer(KafkaRetryableConfiguration kafkaRetryableConfiguration) {
        super(kafkaRetryableConfiguration);
    }

    public RetryableConsumer(
            KafkaRetryableConfiguration kafkaRetryableConfiguration,
            KafkaConsumer<K, V> consumer,
            ErrorProcessor<ConsumerRecord<K, V>> errorProcessor,
            RetryableConsumerRebalanceListener rebalanceListener) {
        super(kafkaRetryableConfiguration, consumer, errorProcessor, rebalanceListener);
    }

    public RetryableConsumer(
            KafkaRetryableConfiguration kafkaRetryableConfiguration,
            KafkaConsumer<K, V> consumer,
            ErrorProcessor<ConsumerRecord<K, V>> errorProcessor) {
        super(kafkaRetryableConfiguration, consumer, errorProcessor);
    }

    public RetryableConsumer(
            KafkaRetryableConfiguration kafkaRetryableConfiguration,
            KafkaConsumer<K, V> consumer,
            RetryableConsumerErrorHandler<K, V> errorHandler,
            RetryableConsumerRebalanceListener rebalanceListener) {
        super(kafkaRetryableConfiguration, consumer, errorHandler, rebalanceListener);
    }

    @Override
    protected void processRecords(
            RecordProcessor<ConsumerRecord<K, V>, Exception> recordProcessor, ConsumerRecords<K, V> records)
            throws Exception {
        for (ConsumerRecord<K, V> r : records) {
            log.debug("Begin processing of record with key {} ...", r.key());
            this.currentProcessingRecord = r;
            recordProcessor.processRecord(r);
            updateInternalOffsetsPosition(r);

            if (this.retryCounter > 0) {
                log.info("Previously failing record processing is now successful, retry counter set to 0");
                this.retryCounter = 0;
            }
            log.debug("Processing of record with key {} completed successfully", r.key());
            this.currentProcessingRecord = null;
        }
    }

    @Override
    protected void handleProcessingException(Exception e) {
        if (this.currentProcessingRecord == null) {
            log.error("null record received", e);
        } else {
            handleExceptionWithRetry(e, this.currentProcessingRecord, () -> {
                // Log&Continue: skip the failing record and move to next offset
                skipCurrentOffset(
                        new TopicPartition(
                                this.currentProcessingRecord.topic(), this.currentProcessingRecord.partition()),
                        this.currentProcessingRecord.offset());
            });
            log.debug("Processing of record with key {} completed with error", this.currentProcessingRecord.key());
        }
    }

    @Override
    public OffsetAndMetadata getCurrentOffset(TopicPartition topicPartition) {
        return this.offsets.get(topicPartition);
    }
}
