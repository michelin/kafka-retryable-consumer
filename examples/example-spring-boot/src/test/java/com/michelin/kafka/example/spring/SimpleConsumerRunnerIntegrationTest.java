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
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Duration;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.springframework.context.ConfigurableApplicationContext;

/** Boots {@link SimpleConsumerRunner} against an embedded broker. */
class SimpleConsumerRunnerIntegrationTest extends AbstractSpringExampleIntegrationTest {

    @Test
    void shouldProcessEveryRecord() {
        String topic = uniqueName("spring-simple");
        String deadLetterTopic = uniqueName("spring-simple-dlt");
        createTopic(topic, 1);
        createTopic(deadLetterTopic, 1);

        try (ConfigurableApplicationContext context =
                startExample(SimpleConsumerRunner.class, SimpleConsumerRunner.CONFIG_NAME, topic, deadLetterTopic)) {
            SimpleConsumerRunner runner = context.getBean(SimpleConsumerRunner.class);
            produceStringRecords(topic, 3);

            await("the 3 records to be processed")
                    .atMost(CLUSTER_OPERATION_TIMEOUT)
                    .pollInterval(Duration.ofMillis(100))
                    .until(() -> runner.getProcessedValues().size() == 3);

            assertTrue(runner.getProcessedValues().containsAll(List.of("value0", "value1", "value2")));
        }

        assertDeadLetterTopicIsEmpty(deadLetterTopic);
    }
}
