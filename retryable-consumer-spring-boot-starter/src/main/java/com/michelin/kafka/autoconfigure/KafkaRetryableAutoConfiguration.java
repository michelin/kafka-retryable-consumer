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
package com.michelin.kafka.autoconfigure;

import com.michelin.kafka.RetryableConsumer;
import com.michelin.kafka.configuration.DeadLetterProducerConfiguration;
import com.michelin.kafka.configuration.KafkaRetryableConfiguration;
import com.michelin.kafka.error.DeadLetterProducer;
import com.michelin.kafka.mapper.KafkaRetryableConfigurationMapper;
import com.michelin.kafka.properties.KafkaRetryableSpringProperties;
import java.util.Properties;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Lazy;
import org.springframework.lang.Nullable;

@AutoConfiguration
@EnableConfigurationProperties(KafkaRetryableSpringProperties.class)
@ConditionalOnProperty(prefix = "kafka.retryable", name = "enabled", havingValue = "true", matchIfMissing = true)
public class KafkaRetryableAutoConfiguration {

    @Bean
    @ConditionalOnMissingBean(KafkaRetryableConfiguration.class)
    public KafkaRetryableConfiguration kafkaRetryableConfiguration(KafkaRetryableSpringProperties properties) {
        return KafkaRetryableConfigurationMapper.map(properties);
    }

    @Bean
    @Lazy
    @ConditionalOnProperty(prefix = "kafka.retryable.dead-letter.producer", name = "topic")
    public DeadLetterProducer deadLetterProducer(KafkaRetryableConfiguration configuration) {
        return new DeadLetterProducer(validatedDeadLetterConfiguration(configuration));
    }

    @Bean
    @Lazy
    public <K, V> RetryableConsumer<K, V> retryableConsumer(
            KafkaRetryableConfiguration configuration, @Nullable DeadLetterProducer deadLetterProducer) {
        // Basic validation: topics and bootstrap.servers are required to run the consumer
        if (configuration.getConsumer() == null
                || configuration.getConsumer().getTopics() == null
                || configuration.getConsumer().getTopics().isEmpty()) {
            throw new IllegalStateException(
                    "Missing required property: kafka.retryable.consumer.topics must be configured and non-empty");
        }

        Properties consumerProps = getConsumerProps(configuration);
        configuration.getConsumer().setProperties(consumerProps);

        if (deadLetterProducer != null) {
            return new RetryableConsumer<>(configuration, deadLetterProducer);
        }
        // No dedicated bean: the consumer builds its own dead-letter producer from the configuration, so the
        // dead-letter properties must be validated here too, otherwise the failure surfaces as an obscure
        // Kafka ConfigException about a missing serializer.
        validatedDeadLetterConfiguration(configuration);
        return new RetryableConsumer<>(configuration);
    }

    /**
     * Validates the dead-letter properties and applies the default serializers.
     *
     * <p>A dead-letter producer is always built by the consumer, so these properties are required even when no
     * dead-letter topic is configured.
     *
     * @return the validated dead-letter configuration
     * @throws IllegalStateException if the dead-letter bootstrap servers are missing
     */
    private static DeadLetterProducerConfiguration validatedDeadLetterConfiguration(
            KafkaRetryableConfiguration configuration) {
        DeadLetterProducerConfiguration deadLetter = configuration.getDeadLetter();
        if (deadLetter == null) {
            throw new IllegalStateException("Missing required configuration: kafka.retryable.dead-letter.producer");
        }

        Properties deadLetterProps = deadLetter.getProperties();
        String bootstrap = (String) deadLetterProps.get(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG);
        if (bootstrap == null || bootstrap.isBlank()) {
            throw new IllegalStateException(
                    "Missing required property: kafka.retryable.dead-letter.producer.properties.bootstrap.servers must be configured");
        }

        deadLetterProps.putIfAbsent("key.serializer", StringSerializer.class.getName());
        deadLetterProps.putIfAbsent("value.serializer", StringSerializer.class.getName());
        deadLetter.setProperties(deadLetterProps);
        return deadLetter;
    }

    private static Properties getConsumerProps(KafkaRetryableConfiguration configuration) {
        Properties consumerProps = configuration.getConsumer().getProperties();
        String bootstrap = (String) consumerProps.get(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG);
        if (bootstrap == null || bootstrap.isBlank()) {
            throw new IllegalStateException(
                    "Missing required property: kafka.retryable.consumer.properties.bootstrap.servers must be configured");
        }

        consumerProps.putIfAbsent(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        consumerProps.putIfAbsent(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        return consumerProps;
    }
}
