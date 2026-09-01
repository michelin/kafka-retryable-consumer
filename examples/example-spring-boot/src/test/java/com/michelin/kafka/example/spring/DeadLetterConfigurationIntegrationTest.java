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

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.Test;
import org.springframework.context.ConfigurableApplicationContext;

/** Boots {@link DeadLetterConfiguration} against an embedded broker. */
class DeadLetterConfigurationIntegrationTest extends AbstractSpringExampleIntegrationTest {

    @Test
    void shouldPublishTheUnrecoverableRecordToTheDeadLetterTopic() {
        String topic = uniqueName("spring-dlt");
        String deadLetterTopic = uniqueName("spring-dlt-dlt");
        createTopic(topic, 1);
        createTopic(deadLetterTopic, 1);

        try (ConfigurableApplicationContext context = startExample(
                DeadLetterConfiguration.class, topic, deadLetterTopic, "--kafka.retryable.consumer.retry-max=1")) {

            produceStringRecords(topic, 1);

            assertEquals(1, awaitDeadLetterRecords(deadLetterTopic, 1).size());
        }
    }
}
