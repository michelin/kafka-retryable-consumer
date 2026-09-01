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
package com.michelin.kafka.test.spring;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.michelin.kafka.RetryableConsumer;
import com.michelin.kafka.autoconfigure.KafkaRetryableAutoConfiguration;
import com.michelin.kafka.configuration.KafkaRetryableConfiguration;
import com.michelin.kafka.error.DeadLetterProducer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.Test;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.context.annotation.ImportCandidates;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Tests the starter through the Spring Boot auto-configuration mechanism, so that the property binding, the
 * conditionals and the registration file are all exercised the way a consuming application would.
 */
class KafkaRetryableAutoConfigurationTest {

    private static final String[] MINIMAL_PROPERTIES = {
        "kafka.retryable.consumer.topics=TOPIC",
        // A resolvable address is required: the Kafka client validates it on construction (no connection is opened).
        "kafka.retryable.consumer.properties.bootstrap.servers=127.0.0.1:9092",
        // The consumer always builds a dead-letter producer, so these properties are required as well.
        "kafka.retryable.dead-letter.producer.properties.bootstrap.servers=127.0.0.1:9092"
    };

    private final ApplicationContextRunner runner = new ApplicationContextRunner()
            .withConfiguration(AutoConfigurations.of(KafkaRetryableAutoConfiguration.class));

    /**
     * Guards the public contract of the starter. Without this entry the auto-configuration is never applied in a
     * consuming application, and every other test here would still pass because they register the class explicitly.
     */
    @Test
    void shouldBeRegisteredAsAnAutoConfigurationCandidate() {
        Iterable<String> candidates =
                ImportCandidates.load(AutoConfiguration.class, getClass().getClassLoader());

        assertThat(candidates).contains(KafkaRetryableAutoConfiguration.class.getName());
    }

    @Test
    void shouldCreateConsumerWithMinimalProperties() {
        runner.withPropertyValues(MINIMAL_PROPERTIES).run(context -> {
            assertThat(context).hasNotFailed();
            // getBean() forces the instantiation of the @Lazy bean, which containsBean() would not do.
            assertThat(context.getBean(RetryableConsumer.class)).isNotNull();
            // No dead-letter topic configured, so the conditional bean must be absent.
            assertThat(context).doesNotHaveBean(DeadLetterProducer.class);
        });
    }

    @Test
    void shouldFailWithAnExplicitMessageWhenDeadLetterIsNotConfigured() {
        // The dead-letter producer is always built, so its properties are mandatory. Without this validation the
        // failure would surface as an obscure Kafka ConfigException about a null key.serializer.
        runner.withPropertyValues(
                        "kafka.retryable.consumer.topics=TOPIC",
                        "kafka.retryable.consumer.properties.bootstrap.servers=127.0.0.1:9092")
                .run(context -> assertThatThrownBy(() -> context.getBean(RetryableConsumer.class))
                        .hasMessageContaining("kafka.retryable.dead-letter.producer.properties.bootstrap.servers"));
    }

    @Test
    void shouldDefaultDeadLetterSerializers() {
        // Documented promise of the starter: serializers are optional and default to StringSerializer.
        runner.withPropertyValues(MINIMAL_PROPERTIES)
                .withPropertyValues("kafka.retryable.dead-letter.producer.topic=DL_TOPIC")
                .run(context -> {
                    KafkaRetryableConfiguration configuration = context.getBean(KafkaRetryableConfiguration.class);
                    context.getBean(DeadLetterProducer.class);
                    assertThat(configuration.getDeadLetter().getProperties())
                            .containsEntry("key.serializer", StringSerializer.class.getName())
                            .containsEntry("value.serializer", StringSerializer.class.getName());
                });
    }

    @Test
    void shouldBindRelaxedPropertyNames() {
        runner.withPropertyValues(MINIMAL_PROPERTIES)
                .withPropertyValues(
                        "kafka.retryable.consumer.retry-max=10",
                        "kafka.retryable.consumer.poll-backoff-ms=2345",
                        "kafka.retryable.consumer.stop-on-error=true")
                .run(context -> {
                    KafkaRetryableConfiguration configuration = context.getBean(KafkaRetryableConfiguration.class);
                    assertThat(configuration.getConsumer().getRetryMax()).isEqualTo(10L);
                    assertThat(configuration.getConsumer().getPollBackoffMs()).isEqualTo(2345L);
                    assertThat(configuration.getConsumer().getTopics()).containsExactly("TOPIC");
                });
    }

    @Test
    void shouldCreateDeadLetterProducerWhenTopicIsSet() {
        runner.withPropertyValues(MINIMAL_PROPERTIES)
                .withPropertyValues(
                        "kafka.retryable.dead-letter.producer.topic=DL_TOPIC",
                        "kafka.retryable.dead-letter.producer.properties.bootstrap.servers=127.0.0.1:9092")
                .run(context ->
                        assertThat(context.getBean(DeadLetterProducer.class)).isNotNull());
    }

    @Test
    void shouldBackOffWhenDisabled() {
        runner.withPropertyValues(MINIMAL_PROPERTIES)
                .withPropertyValues("kafka.retryable.enabled=false")
                .run(context -> assertThat(context).doesNotHaveBean(KafkaRetryableConfiguration.class));
    }

    @Test
    void shouldBackOffWhenUserProvidesItsOwnConfiguration() {
        runner.withPropertyValues(MINIMAL_PROPERTIES)
                .withUserConfiguration(UserConfiguration.class)
                .run(context -> {
                    assertThat(context).hasSingleBean(KafkaRetryableConfiguration.class);
                    assertThat(context.getBean(KafkaRetryableConfiguration.class))
                            .isSameAs(UserConfiguration.USER_CONFIGURATION);
                });
    }

    @Test
    void shouldFailWhenTopicsAreMissing() {
        // The consumer bean is lazy, so the failure surfaces on first access rather than on context refresh.
        runner.withPropertyValues("kafka.retryable.consumer.properties.bootstrap.servers=127.0.0.1:9092")
                .run(context -> assertThatThrownBy(() -> context.getBean(RetryableConsumer.class))
                        .hasMessageContaining("kafka.retryable.consumer.topics"));
    }

    @Test
    void shouldFailWhenBootstrapServersAreMissing() {
        runner.withPropertyValues("kafka.retryable.consumer.topics=TOPIC")
                .run(context -> assertThatThrownBy(() -> context.getBean(RetryableConsumer.class))
                        .hasMessageContaining("bootstrap.servers"));
    }

    @Configuration(proxyBeanMethods = false)
    static class UserConfiguration {

        static final KafkaRetryableConfiguration USER_CONFIGURATION = new KafkaRetryableConfiguration();

        @Bean
        KafkaRetryableConfiguration kafkaRetryableConfiguration() {
            return USER_CONFIGURATION;
        }
    }
}
