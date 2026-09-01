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
package com.michelin.kafka.example.spring;

import com.michelin.kafka.ErrorProcessor;
import com.michelin.kafka.RetryableConsumer;
import com.michelin.kafka.configuration.KafkaRetryableConfiguration;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.boot.ApplicationRunner;
import org.springframework.boot.WebApplicationType;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;

/**
 * Replace the dead letter topic by your own error handling, in a Spring Boot application.
 *
 * <p>An {@link ErrorProcessor} is what the library calls once a record is declared unrecoverable. Providing your own
 * replaces the default dead letter production entirely, which is how you plug in an alerting system, a relational
 * database or an audit log.
 *
 * <p>The starter has no dedicated extension point for this: its {@code retryableConsumer} bean does not look for an
 * {@link ErrorProcessor} bean. The way to inject one today is to declare the consumer yourself, marked {@link Primary}
 * so that it wins over the auto-configured one. Everything else, including the parsing of the {@code kafka.retryable.*}
 * properties into a {@link KafkaRetryableConfiguration}, is still provided by the starter.
 */
@Slf4j
@Configuration
@EnableAutoConfiguration
public class CustomErrorProcessorConfiguration {

    /** Configuration file of this example, selected through {@code spring.config.name}. */
    public static final String CONFIG_NAME = "custom-error-processor";

    /** Errors collected by the custom processor, in place of the dead letter topic. */
    @Getter
    private final List<String> collectedErrors = new CopyOnWriteArrayList<>();

    /** Takes precedence over the consumer built by the starter, which cannot be given an error processor. */
    @Bean
    @Primary
    RetryableConsumer<String, String> retryableConsumerWithCustomErrorProcessor(
            KafkaRetryableConfiguration configuration) {
        return new RetryableConsumer<>(configuration, this::handleError);
    }

    @Bean
    ApplicationRunner startConsumer(RetryableConsumer<String, String> consumer) {
        return args -> consumer.listenAsync(this::process);
    }

    private void process(ConsumerRecord<String, String> record) {
        log.info("Processing record key={}", record.key());
        throw new IllegalStateException("Processing failed for record " + record.key());
    }

    /** Called instead of the dead letter production, once the record is declared unrecoverable. */
    private void handleError(Throwable throwable, ConsumerRecord<String, String> record, Long retryCount) {
        log.error("Record key={} definitively failed after {} retries", record.key(), retryCount, throwable);
        collectedErrors.add(record.key() + " -> " + throwable.getMessage());
    }

    public static void main(String[] args) {
        new SpringApplicationBuilder(CustomErrorProcessorConfiguration.class)
                .web(WebApplicationType.NONE)
                // Each example of this module owns its configuration file, none of them is merged
                .properties("spring.config.name=" + CONFIG_NAME)
                .run(args);
    }
}
