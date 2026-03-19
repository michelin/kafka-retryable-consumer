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

import static org.mockito.Mockito.*;

import com.michelin.kafka.RetryableConsumerRebalanceListener;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

class RetryableConsumerRebalanceListenerTest {

    @Mock
    private Consumer<String, String> consumer;

    private Map<TopicPartition, OffsetAndMetadata> offsets;
    private RetryableConsumerRebalanceListener listener;

    AutoCloseable mockCloseable;

    @BeforeEach
    void setUp() {
        mockCloseable = MockitoAnnotations.openMocks(this);
        offsets = new HashMap<>();
        listener = new RetryableConsumerRebalanceListener(consumer, offsets);
    }

    @AfterEach
    void tearDown() throws Exception {
        if (mockCloseable != null) {
            mockCloseable.close();
        }
    }

    @Test
    void onPartitionsRevoked_shouldRemoveOffsets() {
        TopicPartition partition = new TopicPartition("topic", 1);
        offsets.put(partition, new OffsetAndMetadata(1L));

        listener.onPartitionsRevoked(Collections.singletonList(partition));

        verify(consumer, never()).committed(anySet());
        Assertions.assertTrue(offsets.isEmpty());
    }

    @Test
    void onPartitionsAssigned_shouldAddOffsets() {
        TopicPartition partition = new TopicPartition("topic", 1);
        OffsetAndMetadata offset = new OffsetAndMetadata(1L);
        when(consumer.committed(anySet())).thenReturn(Collections.singletonMap(partition, offset));

        listener.onPartitionsAssigned(Collections.singletonList(partition));

        Assertions.assertTrue(offsets.containsKey(partition));
        Assertions.assertEquals(offsets.get(partition), offset);
    }

    @Test
    void onPartitionsAssigned_shouldNotAddNullOffsets() {
        TopicPartition partition = new TopicPartition("topic", 1);
        when(consumer.committed(anySet())).thenReturn(Collections.singletonMap(partition, null));

        listener.onPartitionsAssigned(Collections.singletonList(partition));

        Assertions.assertTrue(!offsets.containsKey(partition));
    }
}
