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

import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.time.Duration;
import org.junit.jupiter.api.Test;
import org.springframework.context.ConfigurableApplicationContext;

/** Boots {@link CustomErrorProcessorConfiguration} against an embedded broker. */
class CustomErrorProcessorConfigurationIntegrationTest extends AbstractSpringExampleIntegrationTest {

    @Test
    void shouldReplaceTheDeadLetterProductionByTheCustomProcessor() {
        String topic = uniqueName("spring-custom-error");
        String deadLetterTopic = uniqueName("spring-custom-error-dlt");
        createTopic(topic, 1);
        createTopic(deadLetterTopic, 1);

        try (ConfigurableApplicationContext context = startExample(
                CustomErrorProcessorConfiguration.class,
                topic,
                deadLetterTopic,
                "--kafka.retryable.consumer.retry-max=1")) {

            CustomErrorProcessorConfiguration example = context.getBean(CustomErrorProcessorConfiguration.class);
            produceStringRecords(topic, 1);

            await("the custom error processor to be called")
                    .atMost(CLUSTER_OPERATION_TIMEOUT)
                    .pollInterval(Duration.ofMillis(100))
                    .until(() -> !example.getCollectedErrors().isEmpty());

            assertEquals(1, example.getCollectedErrors().size());
        }

        // The default processor has been replaced: the dead letter topic is left untouched
        assertDeadLetterTopicIsEmpty(deadLetterTopic);
    }
}
