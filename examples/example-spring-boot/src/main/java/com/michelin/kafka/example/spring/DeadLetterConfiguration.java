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
 * Route the records that could not be processed to a dead letter topic.
 *
 * <p>Declaring {@code kafka.retryable.dead-letter.producer.topic} is enough for the starter to build a dead letter
 * producer and wire it into the consumer. Once the retry budget is exhausted, the record and its error are published
 * there, the offset is committed and the consumer moves on.
 *
 * <p>Two properties are easy to get wrong and are therefore spelled out in {@code dead-letter.yml}: the dead letter
 * payload is an Avro record, so the producer needs {@code value.serializer=io.confluent.kafka.serializers
 * .KafkaAvroSerializer} and a {@code schema.registry.url}. The serializer the starter falls back on is a plain string
 * serializer, which cannot encode that payload.
 */
@Slf4j
@Configuration
@EnableAutoConfiguration
public class DeadLetterConfiguration {

    /** Configuration file of this example, selected through {@code spring.config.name}. */
    public static final String CONFIG_NAME = "dead-letter";

    @Bean
    ApplicationRunner startConsumer(RetryableConsumer<String, String> consumer) {
        return args -> consumer.listenAsync(this::process);
    }

    private void process(ConsumerRecord<String, String> record) {
        log.info("Processing record key={}", record.key());
        // This record will never be processed successfully: it ends up in the dead letter topic
        throw new IllegalStateException("Record " + record.key() + " cannot be processed");
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
        return new SpringApplicationBuilder(DeadLetterConfiguration.class)
                .web(WebApplicationType.NONE)
                // Each example of this module owns its configuration file, none of them is merged
                .properties("spring.config.name=" + CONFIG_NAME)
                .run(args);
    }
}
