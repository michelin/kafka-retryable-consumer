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

import static org.junit.jupiter.api.Assertions.*;

import com.michelin.kafka.properties.ConsumerSpringProperties;
import org.junit.jupiter.api.Test;

class ConsumerSpringPropertiesTest {

    @Test
    void defaultsShouldBeSet() {
        ConsumerSpringProperties p = new ConsumerSpringProperties();
        assertEquals(1000L, p.getPollBackoffMs());
        assertEquals(0L, p.getRetryMax());
        assertNotNull(p.getTopics());
        assertEquals(0, p.getTopics().size());
    }

    @Test
    void shouldSetAndGetValues() {
        ConsumerSpringProperties p = new ConsumerSpringProperties();
        p.setPollBackoffMs(2000L);
        p.setRetryMax(5L);
        p.getTopics().add("TOPIC");

        assertEquals(2000L, p.getPollBackoffMs());
        assertEquals(5L, p.getRetryMax());
        assertEquals(1, p.getTopics().size());
        assertTrue(p.getTopics().contains("TOPIC"));
    }
}
