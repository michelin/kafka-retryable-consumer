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
package test.properties;

import static org.junit.jupiter.api.Assertions.*;

import com.michelin.kafka.configuration.KafkaRetryableConfiguration;
import com.michelin.kafka.mapper.KafkaRetryableConfigurationMapper;
import com.michelin.kafka.properties.ConsumerSpringProperties;
import com.michelin.kafka.properties.DeadLetterProducerSpringProperties;
import com.michelin.kafka.properties.KafkaRetryableSpringProperties;
import org.junit.jupiter.api.Test;

class KafkaRetryablePropertiesFullTest {

    @Test
    void shouldBindAllConsumerAndDlqProperties() {
        KafkaRetryableSpringProperties props = new KafkaRetryableSpringProperties();
        ConsumerSpringProperties consumer = new ConsumerSpringProperties();
        consumer.getTopics().add("TOPIC");
        consumer.setRetryMax(10L);
        consumer.setPollBackoffMs(2345L);
        props.setConsumer(consumer);

        DeadLetterProducerSpringProperties dl = new DeadLetterProducerSpringProperties();
        dl.setTopic("DL_TOPIC");
        props.setDeadLetter(dl);

        KafkaRetryableConfiguration config = KafkaRetryableConfigurationMapper.map(props);

        assertNotNull(config);
        assertNotNull(config.getConsumer());
        assertEquals(1, config.getConsumer().getTopics().size());
        assertTrue(config.getConsumer().getTopics().contains("TOPIC"));
        assertEquals(10L, config.getConsumer().getRetryMax());
        assertEquals(2345L, config.getConsumer().getPollBackoffMs());
        assertNotNull(config.getDeadLetter());
        assertEquals("DL_TOPIC", config.getDeadLetter().getTopic());
    }
}
