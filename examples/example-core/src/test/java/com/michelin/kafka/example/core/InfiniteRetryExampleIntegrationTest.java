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
import static org.junit.jupiter.api.Assertions.assertEquals;

import com.michelin.kafka.configuration.KafkaConfigurationException;
import com.michelin.kafka.configuration.KafkaRetryableConfiguration;
import java.time.Duration;
import java.util.List;
import org.junit.jupiter.api.Test;

/** Runs {@link InfiniteRetryExample} against an embedded broker. */
class InfiniteRetryExampleIntegrationTest extends AbstractExampleIntegrationTest {

    @Test
    void shouldRetryUntilTheRecordIsFinallyProcessed() throws KafkaConfigurationException {
        String topic = uniqueName("infinite-retry");
        String deadLetterTopic = uniqueName("infinite-retry-dlt");
        createTopic(topic, 1);
        createTopic(deadLetterTopic, 1);

        KafkaRetryableConfiguration configuration =
                loadExampleConfiguration(InfiniteRetryExample.CONFIG_FILE, topic, deadLetterTopic);

        // The downstream system fails 3 times before recovering
        try (InfiniteRetryExample example = new InfiniteRetryExample(configuration, 3)) {
            example.start();
            produceStringRecords(topic, 1);

            await("the record to be processed once the downstream system recovers")
                    .atMost(CLUSTER_OPERATION_TIMEOUT)
                    .pollInterval(Duration.ofMillis(100))
                    .until(() -> !example.getProcessedValues().isEmpty());

            assertEquals(List.of("value0"), example.getProcessedValues());
            // 3 failed attempts, then the successful one
            assertEquals(4, example.getAttempts());
        }

        // Nothing is ever given up on with an infinite retry
        assertDeadLetterTopicIsEmpty(deadLetterTopic);
    }
}
