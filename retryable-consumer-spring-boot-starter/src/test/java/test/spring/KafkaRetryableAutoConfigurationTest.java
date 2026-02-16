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
package test.spring;

import static org.junit.jupiter.api.Assertions.*;

import com.michelin.kafka.autoconfigure.KafkaRetryableAutoConfiguration;
import com.michelin.kafka.configuration.DeadLetterProducerConfiguration;
import com.michelin.kafka.configuration.KafkaRetryableConfiguration;
import com.michelin.kafka.configuration.RetryableConsumerConfiguration;
import com.michelin.kafka.error.DeadLetterProducer;
import java.util.Properties;
import org.junit.jupiter.api.Test;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;

class KafkaRetryableAutoConfigurationTest {

    private KafkaRetryableConfiguration minimalConfiguration() {
        RetryableConsumerConfiguration consumer = new RetryableConsumerConfiguration();
        Properties p = new Properties();
        // Use a resolvable localhost address to avoid DNS validation failures in Kafka client during tests
        p.put("bootstrap.servers", "127.0.0.1:9092");
        // Required deserializers for KafkaConsumer construction
        p.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        p.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        consumer.setProperties(p);
        consumer.setTopics(java.util.List.of("TOPIC"));

        DeadLetterProducerConfiguration dl = new DeadLetterProducerConfiguration();
        Properties dlProps = new Properties();
        // Provide minimal producer properties so KafkaProducer constructor succeeds when DLQ bean created
        dlProps.put("bootstrap.servers", "127.0.0.1:9092");
        dlProps.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        dlProps.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        dl.setProperties(dlProps);
        dl.setTopic(null);

        KafkaRetryableConfiguration config = new KafkaRetryableConfiguration();
        config.setConsumer(consumer);
        config.setDeadLetter(dl);
        return config;
    }

    @Test
    void whenEnabledBeansAreCreated() {
        try (AnnotationConfigApplicationContext ctx = new AnnotationConfigApplicationContext()) {
            ctx.getBeanFactory().registerSingleton("kafkaRetryableConfiguration", minimalConfiguration());
            ctx.register(KafkaRetryableAutoConfiguration.class);
            ctx.refresh();

            assertTrue(ctx.containsBean("retryableConsumer"));
            // DLQ bean should not exist because DLQ topic not set and bean is conditional
            assertFalse(ctx.containsBean("deadLetterProducer"));
        }
    }

    @Test
    void whenDlqTopicProvidedDeadLetterProducerIsCreated() {
        // Build a configuration with DLQ topic and properties and call auto-config directly
        KafkaRetryableConfiguration cfg = minimalConfiguration();
        cfg.getDeadLetter().setTopic("DL_TOPIC");

        KafkaRetryableAutoConfiguration auto = new KafkaRetryableAutoConfiguration();
        DeadLetterProducer dlq = auto.deadLetterProducer(cfg);

        assertNotNull(dlq);
    }

    @Test
    void whenMissingRequiredPropertiesRetryableConsumerCreationFails() {
        // Create a config missing bootstrap.servers
        RetryableConsumerConfiguration consumer = new RetryableConsumerConfiguration();
        consumer.setTopics(java.util.List.of("TOPIC"));
        consumer.setProperties(new Properties());

        KafkaRetryableConfiguration cfg = new KafkaRetryableConfiguration();
        cfg.setConsumer(consumer);
        cfg.setDeadLetter(new DeadLetterProducerConfiguration());

        // Call auto-configuration method directly to validate behavior (avoids starting Spring context and Kafka
        // clients)
        KafkaRetryableAutoConfiguration autoConfig = new KafkaRetryableAutoConfiguration();
        IllegalStateException ex =
                assertThrows(IllegalStateException.class, () -> autoConfig.retryableConsumer(cfg, null));
        assertTrue(ex.getMessage().contains("bootstrap.servers"));
    }
}
