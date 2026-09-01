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
package com.michelin.kafka.example.core;

import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.michelin.kafka.configuration.KafkaRetryableConfiguration;
import com.michelin.kafka.configuration.KafkaConfigurationException;
import java.time.Duration;
import java.util.List;
import org.junit.jupiter.api.Test;

/** Runs {@link SimpleConsumerExample} against an embedded broker to prove the example actually works. */
class SimpleConsumerExampleIntegrationTest extends AbstractExampleIntegrationTest {

    @Test
    void shouldProcessEveryRecord() throws KafkaConfigurationException {
        String topic = uniqueName("simple");
        String deadLetterTopic = uniqueName("simple-dlt");
        createTopic(topic, 1);
        createTopic(deadLetterTopic, 1);

        KafkaRetryableConfiguration configuration = loadExampleConfiguration(SimpleConsumerExample.CONFIG_FILE, topic, deadLetterTopic);

        try (SimpleConsumerExample example = new SimpleConsumerExample(configuration)) {
            example.start();
            produceStringRecords(topic, 3);

            await("the 3 records to be processed")
                    .atMost(CLUSTER_OPERATION_TIMEOUT)
                    .pollInterval(Duration.ofMillis(100))
                    .until(() -> example.getProcessedValues().size() == 3);

            assertTrue(example.getProcessedValues().containsAll(List.of("value0", "value1", "value2")));
        }

        assertDeadLetterTopicIsEmpty(deadLetterTopic);
    }
}
