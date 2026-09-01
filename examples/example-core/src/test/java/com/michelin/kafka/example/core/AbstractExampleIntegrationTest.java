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

import com.michelin.kafka.configuration.KafkaConfigurationException;
import com.michelin.kafka.configuration.KafkaRetryableConfiguration;
import com.michelin.kafka.test.integration.AbstractKafkaIntegrationTest;
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import java.util.Collections;
import java.util.Properties;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;

/**
 * Base class of the example tests, which run the examples exactly as a user would.
 *
 * <p>Each example owns a configuration file describing the capability it demonstrates. The tests load that very file,
 * so that a mistake in it breaks the build, and override only what cannot be known in advance: the address of the
 * embedded broker, the schema registry, and the topic names, which must be unique to keep the tests isolated.
 *
 * <p>Nothing related to the demonstrated behaviour, retry policy, deserializers or non retryable exceptions, is
 * overridden here. Those come from the example configuration file and from nowhere else.
 */
abstract class AbstractExampleIntegrationTest extends AbstractKafkaIntegrationTest {

    /**
     * Load the configuration file of an example and point it at the embedded cluster.
     *
     * @param configFile the classpath resource owned by the example, such as {@code simple-consumer-example.yml}
     * @param dataTopic the topic the consumer must subscribe to
     * @param deadLetterTopic the dead letter topic
     * @return the example configuration, retargeted at the test infrastructure
     * @throws KafkaConfigurationException if the example configuration file is missing or invalid
     */
    protected static KafkaRetryableConfiguration loadExampleConfiguration(
            String configFile, String dataTopic, String deadLetterTopic) throws KafkaConfigurationException {

        KafkaRetryableConfiguration configuration = KafkaRetryableConfiguration.load(configFile);

        Properties consumerProperties = configuration.getConsumer().getProperties();
        consumerProperties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers());
        consumerProperties.put(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, SCHEMA_REGISTRY_URL);
        // A dedicated group and client per test: no offset and no state can leak from one test to another
        consumerProperties.put(ConsumerConfig.GROUP_ID_CONFIG, uniqueName("group-" + dataTopic));
        consumerProperties.put(ConsumerConfig.CLIENT_ID_CONFIG, uniqueName("client-" + dataTopic));
        // Detect a dead member quickly and refresh metadata often: both shorten the recovery time on a slow CI agent
        consumerProperties.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, 10000);
        consumerProperties.put(ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG, 1000);
        consumerProperties.put(ConsumerConfig.METADATA_MAX_AGE_CONFIG, 2000);
        consumerProperties.put(ConsumerConfig.FETCH_MAX_WAIT_MS_CONFIG, 100);
        configuration.getConsumer().setPollBackoffMs(500L);
        configuration.getConsumer().setTopics(Collections.singletonList(dataTopic));

        Properties deadLetterProperties = configuration.getDeadLetter().getProperties();
        deadLetterProperties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers());
        deadLetterProperties.put(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, SCHEMA_REGISTRY_URL);
        deadLetterProperties.put(ProducerConfig.ACKS_CONFIG, "all");
        configuration.getDeadLetter().setTopic(deadLetterTopic);

        return configuration;
    }
}
