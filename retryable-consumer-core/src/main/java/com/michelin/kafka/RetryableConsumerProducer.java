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
import com.michelin.kafka.processors.RecordProcessorList;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.LinkedList;
import java.util.List;

@Slf4j
public class RetryableConsumerProducer<CK, CV, PK, PV>
        extends AbstractRetryableRecordProcessor<
                CK, CV, RecordProcessorList<ConsumerRecord<CK, CV>, Exception, PK, PV>> {

    private final Producer<PK, PV> producer;

    public RetryableConsumerProducer(String name) throws KafkaConfigurationException {
        super(KafkaRetryableConfiguration.load());
        this.name = name;
        this.producer = new KafkaProducer<>(
                this.kafkaRetryableConfiguration.getProducer().getProperties());
    }

    public RetryableConsumerProducer(KafkaRetryableConfiguration config) {
        super(config);
        this.producer = new KafkaProducer<>(config.getProducer().getProperties());
    }

    public RetryableConsumerProducer(
            KafkaRetryableConfiguration config,
            KafkaConsumer<CK, CV> consumer,
            KafkaProducer<PK, PV> producer,
            RetryableConsumerErrorHandler<CK, CV> errorHandler,
            RetryableConsumerRebalanceListener rebalanceListener) {
        super(config, consumer, errorHandler, rebalanceListener);
        this.producer = producer;
    }

    @Override
    protected void processRecordsTemplate(ConsumerRecords<CK, CV> records) throws Exception {
        List<ProducerRecord<PK, PV>> toSend = new LinkedList<>();

        for (ConsumerRecord<CK, CV> record : records) {
            log.debug("Begin processing of record with key {} ...", record.key());
            this.currentProcessingRecord = record;

            List<ProducerRecord<PK, PV>> results = recordProcessor.processRecord(record);
            for (ProducerRecord<PK, PV> result : results) {
                ProducerRecord<PK, PV> copy = new ProducerRecord<>(
                        result.topic(), null, result.timestamp(), result.key(), result.value(), result.headers());
                toSend.add(copy);
            }

            updateInternalOffsetsPosition(record);

            if (this.retryCounter > 0) {
                log.info("Previously failing record processing is now successful, retry counter set to 0");
                this.retryCounter = 0;
            }

            log.debug("Processing of record with key {} completed successfully", record.key());
            this.currentProcessingRecord = null;
        }

        for (ProducerRecord<PK, PV> rec : toSend) {
            producer.send(rec);
        }
    }
}
