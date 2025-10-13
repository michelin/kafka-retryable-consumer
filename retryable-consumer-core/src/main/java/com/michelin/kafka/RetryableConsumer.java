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
import com.michelin.kafka.processors.RecordProcessor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

@Slf4j
public class RetryableConsumer<K, V>
        extends AbstractRetryableConsumer<K, V, RecordProcessor<ConsumerRecord<K, V>, Void, Exception>> {

    public RetryableConsumer(String name) throws KafkaConfigurationException {
        super(KafkaRetryableConfiguration.load());
        this.name = name;
    }

    public RetryableConsumer(KafkaRetryableConfiguration config) {
        super(config);
    }

    public RetryableConsumer(
            KafkaRetryableConfiguration config,
            KafkaConsumer<K, V> consumer,
            RetryableConsumerErrorHandler<K, V> errorHandler,
            RetryableConsumerRebalanceListener rebalanceListener) {
        super(config, consumer, errorHandler, rebalanceListener);
    }

    @Override
    protected void processRecordsTemplate(
            ConsumerRecords<K, V> records, RecordProcessor<ConsumerRecord<K, V>, Void, Exception> processor)
            throws Exception {

        for (ConsumerRecord<K, V> record : records) {
            log.debug("Begin processing of record with key {} ...", record.key());
            this.currentProcessingRecord = record;

            recordProcessor.processRecord(record);
            updateInternalOffsetsPosition(record);

            if (this.retryCounter > 0) {
                log.info("Previously failing record processing is now successful, retry counter set to 0");
                this.retryCounter = 0;
            }

            log.debug("Processing of record with key {} completed successfully", record.key());
            this.currentProcessingRecord = null;
        }
    }
}
