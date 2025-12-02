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

import com.michelin.kafka.ErrorProcessor;
import com.michelin.kafka.avro.GenericErrorModel;
import com.michelin.kafka.configuration.KafkaRetryableConfiguration;
import java.io.*;
import java.nio.ByteBuffer;
import java.util.UUID;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.errors.RecordDeserializationException;

@Slf4j
public class DefaultErrorProcessor<K, V> implements ErrorProcessor<ConsumerRecord<K, V>> {

    private final DeadLetterProducer deadLetterProducer;

    public DefaultErrorProcessor(KafkaRetryableConfiguration kafkaRetryableConfiguration) {
        this.deadLetterProducer = new DeadLetterProducer(kafkaRetryableConfiguration.getDeadLetter());
    }

    public DefaultErrorProcessor(DeadLetterProducer deadLetterProducer) {
        this.deadLetterProducer = deadLetterProducer;
    }

    @Override
    public void processError(Throwable throwable, ConsumerRecord<K, V> consumerRecord, Long retryCount) {
        if (throwable instanceof RecordDeserializationException) {
            this.handleConsumerDeserializationError((RecordDeserializationException) throwable);
        } else {
            if (retryCount == null) {
                retryCount = 0L;
            }
            if (throwable != null) {
                if (consumerRecord != null) {
                    this.handleError(
                            throwable.getMessage(),
                            "Error processing record after " + retryCount + " retry(ies).",
                            consumerRecord.offset(),
                            consumerRecord.partition(),
                            consumerRecord.topic(),
                            throwable,
                            consumerRecord.key(),
                            consumerRecord.value());
                } else {
                    this.handleError(
                            throwable.getMessage(),
                            "Error processing an unknown record after " + retryCount + " retry(ies).",
                            0L,
                            0,
                            "unknown",
                            throwable,
                            null,
                            null);
                }
            } else { // Throwable is null
                if (consumerRecord != null) {
                    this.handleError(
                            "Undefined error",
                            "Error processing record after " + retryCount + " retry(ies).",
                            consumerRecord.offset(),
                            consumerRecord.partition(),
                            consumerRecord.topic(),
                            null,
                            consumerRecord.key(),
                            consumerRecord.value());
                }
            }
        }
    }

    public void handleConsumerDeserializationError(RecordDeserializationException e) {
        if (e != null) {
            this.handleError(
                    e.getMessage(),
                    null,
                    e.offset(),
                    e.topicPartition().partition(),
                    e.topicPartition().topic(),
                    e,
                    null,
                    null);
        }
    }

    public void handleError(
            String cause,
            String context,
            Long offset,
            Integer partition,
            String topic,
            Throwable exception,
            K key,
            V value) {
        try {
            GenericErrorModel errorAvroObject = GenericErrorModel.newBuilder()
                    .setCause(cause)
                    .setContextMessage(context)
                    .setOffset(offset)
                    .setPartition(partition)
                    .setTopic(topic)
                    .build();
            if (exception != null) {
                StringWriter sw = new StringWriter();
                PrintWriter pw = new PrintWriter(sw);
                exception.printStackTrace(pw);
                errorAvroObject.setStack(sw.toString());
            }
            if (value != null) {
                if (value instanceof String s) {
                    errorAvroObject.setValue(s);
                } else {
                    errorAvroObject.setByteValue(toByteBuffer((Serializable) value));
                }
            }
            if (key != null) {
                if (key instanceof String s) {
                    errorAvroObject.setKey(s);
                } else {
                    errorAvroObject.setByteKey(toByteBuffer((Serializable) key));
                }
            }

            // Build the producer and send message
            deadLetterProducer.send(UUID.randomUUID().toString(), errorAvroObject);

        } catch (Exception e) {
            log.error("An error occurred during error management... good luck with that!", e);
        }
    }

    public static ByteBuffer toByteBuffer(Serializable obj) throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        ObjectOutputStream oos = new ObjectOutputStream(baos);
        oos.writeObject(obj);
        oos.flush();
        ByteBuffer buffer = ByteBuffer.wrap(baos.toByteArray());
        oos.close();
        baos.close();
        return buffer;
    }
}
