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

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.michelin.kafka.configuration.KafkaRetryableConfiguration;
import com.michelin.kafka.configuration.KafkaConfigurationException;
import org.junit.jupiter.api.Test;

/** Runs {@link NonRetryableExceptionExample} against an embedded broker. */
class NonRetryableExceptionExampleIntegrationTest extends AbstractExampleIntegrationTest {

    @Test
    void shouldSkipTheRetriesForADeclaredNonRetryableException() throws KafkaConfigurationException {
        String topic = uniqueName("non-retryable");
        String deadLetterTopic = uniqueName("non-retryable-dlt");
        createTopic(topic, 1);
        createTopic(deadLetterTopic, 1);

        KafkaRetryableConfiguration configuration = loadExampleConfiguration(NonRetryableExceptionExample.CONFIG_FILE, topic, deadLetterTopic);

        try (NonRetryableExceptionExample example = new NonRetryableExceptionExample(configuration)) {
            example.start();
            produceStringRecords(topic, 1);

            assertEquals(1, awaitDeadLetterRecords(deadLetterTopic, 1).size());
            // The generous retry-max of the example is bypassed: a single attempt was made
            assertEquals(1, example.getAttempts());
        }
    }
}
