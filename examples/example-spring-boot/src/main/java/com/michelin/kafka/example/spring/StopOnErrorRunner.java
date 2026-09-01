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
 * Stop the consumer instead of skipping a record that could not be processed.
 *
 * <p>Everything is driven by properties here: {@code kafka.retryable.consumer.stop-on-error=true} makes the consumer
 * give up rather than commit the offset of an unrecoverable record and move on. The offset stays uncommitted, so
 * restarting the application replays the record.
 *
 * <p>The same applies to the whole retry policy ({@code retry.max}, {@code retry.backoff.ms},
 * {@code not-retryable-exceptions}): none of it requires a line of Java in a Spring Boot application.
 */
@Slf4j
@Configuration
@EnableAutoConfiguration
public class StopOnErrorRunner {

    /** Configuration file of this example, selected through {@code spring.config.name}. */
    public static final String CONFIG_NAME = "stop-on-error";

    private RetryableConsumer<String, String> consumer;

    @Bean
    ApplicationRunner startConsumer(RetryableConsumer<String, String> retryableConsumer) {
        this.consumer = retryableConsumer;
        return args -> retryableConsumer.listenAsync(this::process);
    }

    private void process(ConsumerRecord<String, String> record) {
        log.info("Processing record key={}", record.key());
        throw new IllegalStateException("Unrecoverable error on record " + record.key());
    }

    /** Whether the consumer has been stopped by the error handling. */
    public boolean isStopped() {
        return consumer != null && consumer.isStopped();
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
        return new SpringApplicationBuilder(StopOnErrorRunner.class)
                .web(WebApplicationType.NONE)
                // Each example of this module owns its configuration file, none of them is merged
                .properties("spring.config.name=" + CONFIG_NAME)
                .run(args);
    }
}
