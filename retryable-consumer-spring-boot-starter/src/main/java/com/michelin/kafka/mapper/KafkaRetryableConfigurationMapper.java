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
package com.michelin.kafka.mapper;

import com.michelin.kafka.configuration.DeadLetterProducerConfiguration;
import com.michelin.kafka.configuration.KafkaRetryableConfiguration;
import com.michelin.kafka.configuration.RetryableConsumerConfiguration;
import com.michelin.kafka.properties.KafkaRetryableSpringProperties;

public final class KafkaRetryableConfigurationMapper {

    private KafkaRetryableConfigurationMapper() {}

    public static KafkaRetryableConfiguration map(KafkaRetryableSpringProperties props) {

        KafkaRetryableConfiguration config = new KafkaRetryableConfiguration();

        // ---- Consumer ----
        RetryableConsumerConfiguration consumer = getRetryableConsumerConfiguration(props);

        // ---- Dead letter ----
        DeadLetterProducerConfiguration dlq = new DeadLetterProducerConfiguration();
        dlq.setTopic(props.getDeadLetter().getTopic());
        dlq.setProperties(props.getDeadLetter().getProperties());

        config.setConsumer(consumer);
        config.setDeadLetter(dlq);

        return config;
    }

    private static RetryableConsumerConfiguration getRetryableConsumerConfiguration(
            KafkaRetryableSpringProperties props) {
        RetryableConsumerConfiguration consumer = new RetryableConsumerConfiguration();
        consumer.setTopics(props.getConsumer().getTopics());
        consumer.setStopOnError(props.getConsumer().isStopOnError());
        consumer.setRetryMax(props.getConsumer().getRetryMax());
        consumer.setRetryBackoffMs(props.getConsumer().getRetryBackoffMs());
        consumer.setPollBackoffMs(props.getConsumer().getPollBackoffMs());
        consumer.setNotRetryableExceptions(props.getConsumer().getNotRetryableExceptions());
        consumer.setProperties(props.getConsumer().getProperties());
        return consumer;
    }
}
