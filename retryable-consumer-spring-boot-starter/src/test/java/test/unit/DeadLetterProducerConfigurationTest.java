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
package test.unit;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.michelin.kafka.configuration.DeadLetterProducerConfiguration;
import java.util.Properties;
import org.junit.jupiter.api.Test;

class DeadLetterProducerConfigurationTest {

    @Test
    void testBuilder() {
        // Given
        Properties expectedProperties = new Properties();
        expectedProperties.put("key1", "value1");
        String expectedTopic = "testTopic";

        // When
        DeadLetterProducerConfiguration config = DeadLetterProducerConfiguration.builder()
                .properties(expectedProperties)
                .topic(expectedTopic)
                .build();

        // Then
        assertEquals(expectedProperties, config.getProperties());
        assertEquals(expectedTopic, config.getTopic());
    }

    @Test
    void testSetProperties() {
        // Given
        Properties expectedProperties = new Properties();
        expectedProperties.put("key1", "value1");

        // When
        DeadLetterProducerConfiguration config = new DeadLetterProducerConfiguration();
        config.setProperties(expectedProperties);

        // Then
        assertEquals(expectedProperties, config.getProperties());
    }

    @Test
    void testSetTopic() {
        // Given
        String expectedTopic = "testTopic";

        // When
        DeadLetterProducerConfiguration config = new DeadLetterProducerConfiguration();
        config.setTopic(expectedTopic);

        // Then
        assertEquals(expectedTopic, config.getTopic());
    }

    @Test
    void testLoadConfigProperties_withPrefix() {
        // Given
        String configurationPrefix = "testPrefix";
        Properties retryablConsumerConfigProperties = new Properties();
        retryablConsumerConfigProperties.put("testPrefix.dead-letter.producer.properties.key1", "value1");
        retryablConsumerConfigProperties.put("testPrefix.dead-letter.producer.topic", "testTopic");

        DeadLetterProducerConfiguration config = new DeadLetterProducerConfiguration();

        // When
        config.loadConfigProperties(configurationPrefix, retryablConsumerConfigProperties);

        // Then
        assertEquals("value1", config.getProperties().get("key1"));
        assertEquals("testTopic", config.getTopic());
    }

    @Test
    void testLoadConfigProperties_withoutPrefix() {
        // Given
        String configurationPrefix = "";
        Properties retryablConsumerConfigProperties = new Properties();
        retryablConsumerConfigProperties.put("dead-letter.producer.properties.key1", "value1");
        retryablConsumerConfigProperties.put("dead-letter.producer.topic", "testTopic");

        DeadLetterProducerConfiguration config = new DeadLetterProducerConfiguration();

        // When
        config.loadConfigProperties(configurationPrefix, retryablConsumerConfigProperties);

        // Then
        assertEquals("value1", config.getProperties().get("key1"));
        assertEquals("testTopic", config.getTopic());
    }

    @Test
    void testLoadConfigProperties_withNullPrefix() {
        // Given
        String configurationPrefix = null;
        Properties retryablConsumerConfigProperties = new Properties();
        retryablConsumerConfigProperties.put("dead-letter.producer.properties.key1", "value1");
        retryablConsumerConfigProperties.put("dead-letter.producer.topic", "testTopic");

        DeadLetterProducerConfiguration config = new DeadLetterProducerConfiguration();

        // When
        config.loadConfigProperties(configurationPrefix, retryablConsumerConfigProperties);

        // Then
        assertEquals("value1", config.getProperties().get("key1"));
        assertEquals("testTopic", config.getTopic());
    }
}
