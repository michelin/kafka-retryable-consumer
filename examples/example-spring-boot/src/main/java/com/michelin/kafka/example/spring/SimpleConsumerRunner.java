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

import com.michelin.kafka.RetryableConsumer;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.boot.ApplicationRunner;
import org.springframework.boot.WebApplicationType;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * The baseline Spring Boot usage: inject the auto-configured consumer and start listening.
 *
 * <p>The starter reads the {@code kafka.retryable.*} properties and exposes a ready to use {@link RetryableConsumer}
 * bean, so there is no configuration code left to write. The bean is lazy: it is only built when something injects it,
 * which is what the runner below does.
 *
 * <p>Note the deliberate absence of {@code @ComponentScan}: {@code @Configuration} plus
 * {@code @EnableAutoConfiguration} boots the starter and this class only, so every example of this module stays
 * independent from the others.
 */
@Slf4j
@Configuration
@EnableAutoConfiguration
public class SimpleConsumerRunner {

    /** Configuration file of this example, selected through {@code spring.config.name}. */
    public static final String CONFIG_NAME = "simple-consumer";

    /**
     * Records handed over to the business code, exposed so that the integration test can assert on them.
     *
     * <p>Test hook only, do not copy this into a real consumer: accumulating every record in memory grows without bound
     * and eventually exhausts the heap. Real business code should hand the record over to its destination and keep
     * nothing. {@link CopyOnWriteArrayList} is deliberate, as the test thread iterates this list while the consumer
     * thread writes to it, which a plain synchronized list could not support safely.
     */
    @Getter
    private final List<String> processedValues = new CopyOnWriteArrayList<>();

    /**
     * Start consuming as soon as the application context is ready.
     *
     * <p>{@code listenAsync} is used rather than {@code listen} so that the startup is not blocked by the poll loop.
     */
    @Bean
    ApplicationRunner startConsumer(RetryableConsumer<String, String> consumer) {
        return args -> consumer.listenAsync(this::process);
    }

    /** The business code. Called once per record, by the consumer thread. */
    private void process(ConsumerRecord<String, String> record) {
        log.info("Processing record key={} value={}", record.key(), record.value());
        processedValues.add(record.value());
    }

    public static void main(String[] args) {
        run(args);
    }

    /**
     * Boot the example.
     *
     * <p>Extracted from {@link #main} so that the integration test starts this very application, with its own
     * configuration file, instead of rebuilding a similar one and drifting from it.
     *
     * @param args command line arguments, which take precedence over the configuration file
     * @return the running application context
     */
    static ConfigurableApplicationContext run(String... args) {
        return new SpringApplicationBuilder(SimpleConsumerRunner.class)
                .web(WebApplicationType.NONE)
                // Each example of this module owns its configuration file, none of them is merged
                .properties("spring.config.name=" + CONFIG_NAME)
                .run(args);
    }
}
