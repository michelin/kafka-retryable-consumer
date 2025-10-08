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
import com.michelin.kafka.error.RetryableConsumerErrorHandler;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.Collection;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;

@Slf4j
public abstract class AbstractRetryableRecordProcessor<K, V, P> extends AbstractRetryableConsumer<K, V> {

    protected P recordProcessor;

    protected AbstractRetryableRecordProcessor(KafkaRetryableConfiguration kafkaRetryableConfiguration) {
        super(kafkaRetryableConfiguration);
    }

    protected AbstractRetryableRecordProcessor(
            KafkaRetryableConfiguration kafkaRetryableConfiguration,
            KafkaConsumer<K, V> consumer,
            RetryableConsumerErrorHandler<K, V> errorHandler,
            RetryableConsumerRebalanceListener rebalanceListener) {
        super(kafkaRetryableConfiguration, consumer, errorHandler, rebalanceListener);
    }

    @Override
    @SuppressWarnings("unchecked")
    protected void setCurrentProcessor(Object processor) {
        this.recordProcessor = (P) processor;
    }

    public void listen(P processor) {
        if (kafkaRetryableConfiguration.getConsumer().getTopics() == null
                || kafkaRetryableConfiguration.getConsumer().getTopics().isEmpty()) {
            throw new IllegalArgumentException("Topic list consumer configuration is not set");
        }
        listen(kafkaRetryableConfiguration.getConsumer().getTopics(), processor);
    }

    public void listen(Collection<String> topics, P processor) {
        setCurrentProcessor(processor);
        listenInternal(topics);
    }

    public Future<Void> listenAsync(P processor) {
        return CompletableFuture.runAsync(() -> listen(processor));
    }

    public Future<Void> listenAsync(Collection<String> topics, P processor) {
        return CompletableFuture.runAsync(() -> listen(topics, processor));
    }
}
