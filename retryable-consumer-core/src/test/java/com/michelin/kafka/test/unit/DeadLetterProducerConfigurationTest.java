package com.michelin.kafka.test.unit;

import com.michelin.kafka.configuration.DeadLetterProducerConfiguration;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.Properties;

public class DeadLetterProducerConfigurationTest {

    @Test
    public void testBuilder() {
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
    public void testSetProperties() {
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
    public void testSetTopic() {
        // Given
        String expectedTopic = "testTopic";

        // When
        DeadLetterProducerConfiguration config = new DeadLetterProducerConfiguration();
        config.setTopic(expectedTopic);

        // Then
        assertEquals(expectedTopic, config.getTopic());
    }

    @Test
    public void testLoadConfigProperties_withPrefix() {
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
    public void testLoadConfigProperties_withoutPrefix() {
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
    public void testLoadConfigProperties_withNullPrefix() {
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