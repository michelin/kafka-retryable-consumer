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
package com.michelin.kafka.error;

import com.michelin.kafka.avro.GenericErrorModel;
import com.michelin.kafka.configuration.DeadLetterProducerConfiguration;
import java.util.Properties;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

/** Dlq producer */
@Slf4j
@AllArgsConstructor
public class DeadLetterProducer {

    /** Kafka producer - Key is a UUID string - Value is a GenericErrorModel object (see GenericErrorModel avro doc) */
    private final Producer<String, GenericErrorModel> kafkaProducer;

    private final String dlTopic;

    /** Constructor */
    public DeadLetterProducer(DeadLetterProducerConfiguration configuration) {
        Properties dlProducerProperties = configuration.getProperties();
        kafkaProducer = new KafkaProducer<>(dlProducerProperties);
        this.dlTopic = configuration.getTopic();
    }

    public DeadLetterProducer(
            DeadLetterProducerConfiguration configuration, Producer<String, GenericErrorModel> kafkaProducer) {
        this.kafkaProducer = kafkaProducer;
        this.dlTopic = configuration.getTopic();
    }

    /**
     * Send message to dlq by kafka
     *
     * @param key message key
     * @param errorObject object wrapping the error to send to dlq
     */
    public void send(String key, GenericErrorModel errorObject) {
        log.info("Sending error to DLQ topic = {}", dlTopic);
        ProducerRecord<String, GenericErrorModel> rec = new ProducerRecord<>(dlTopic, key, errorObject);

        this.kafkaProducer.send(rec, (recordMetadata, e) -> {
            if (e != null) {
                log.error(e.getMessage());
            } else {
                String errorRecordKey = "unknown key";
                if (rec.value().getKey() != null) {
                    errorRecordKey = rec.value().getKey();
                } else if (rec.value().getByteKey() != null) {
                    errorRecordKey = rec.value().getByteKey().toString();
                }

                log.info(
                        "Error management about issue with event key '{}' "
                                + "has been successfully traced in DLQ topic "
                                + "= {} partition = {} offset = {}",
                        errorRecordKey,
                        recordMetadata.topic(),
                        recordMetadata.partition(),
                        recordMetadata.offset());
            }
        }); // Remember that the actual sending of data to kafka cluster is done asynchronously in a separated thread
    }
}
