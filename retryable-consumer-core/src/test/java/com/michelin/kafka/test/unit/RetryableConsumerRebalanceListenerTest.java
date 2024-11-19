package com.michelin.kafka.test.unit;

import com.michelin.kafka.RetryableConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.util.*;

import static org.mockito.Mockito.*;

class RetryableConsumerRebalanceListenerTest {

    @Mock
    private Consumer<String, String> consumer;

    private Map<TopicPartition, OffsetAndMetadata> offsets;
    private RetryableConsumerRebalanceListener listener;

    @BeforeEach
    void setUp() {
        MockitoAnnotations.openMocks(this);
        offsets = new HashMap<>();
        listener = new RetryableConsumerRebalanceListener(consumer, offsets);
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
