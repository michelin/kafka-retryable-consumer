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
import static org.junit.jupiter.api.Assertions.assertFalse;

import com.michelin.kafka.configuration.KafkaRetryableConfiguration;
import com.michelin.kafka.configuration.KafkaConfigurationException;
import java.time.Duration;
import org.junit.jupiter.api.Test;

/** Runs {@link BatchConsumerExample} against an embedded broker. */
class BatchConsumerExampleIntegrationTest extends AbstractExampleIntegrationTest {

    @Test
    void shouldProcessRecordsGroupedInBatches() throws KafkaConfigurationException {
        String topic = uniqueName("batch");
        String deadLetterTopic = uniqueName("batch-dlt");
        createTopic(topic, 1);
        createTopic(deadLetterTopic, 1);

        KafkaRetryableConfiguration configuration = loadExampleConfiguration(BatchConsumerExample.CONFIG_FILE, topic, deadLetterTopic);

        try (BatchConsumerExample example = new BatchConsumerExample(configuration)) {
            example.start();
            produceStringRecords(topic, 10);

            await("the 10 records to be processed")
                    .atMost(CLUSTER_OPERATION_TIMEOUT)
                    .pollInterval(Duration.ofMillis(100))
                    .until(() -> example.getProcessedRecordCount() == 10);

            // The point of the batch consumer: records are handed over grouped, not one by one
            assertFalse(example.getBatchSizes().isEmpty());
            assertEquals(
                    10,
                    example.getBatchSizes().stream().mapToInt(Integer::intValue).sum());
        }

        assertDeadLetterTopicIsEmpty(deadLetterTopic);
    }
}
