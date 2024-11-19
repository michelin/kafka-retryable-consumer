package com.michelin.kafka;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;

import java.util.Collection;
import java.util.HashSet;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Retryable consumer rebalance listener used for managing offsets and partitions when kafka makes rebalancings
 */
@Slf4j
public class RetryableConsumerRebalanceListener implements ConsumerRebalanceListener {
    private final Consumer<?, ?> consumer;
    private final Map<TopicPartition, OffsetAndMetadata> offsets;

    /**
     * Constructor
     *
     * @param consumer The consumer
     * @param offsets  The current offset
     */
    public RetryableConsumerRebalanceListener(Consumer<?, ?> consumer, Map<TopicPartition, OffsetAndMetadata> offsets) {
        this.consumer = consumer;
        this.offsets = offsets;
    }

    /**
     * Override of onPartitionsRevoked method
     *
     * @param partitions The list of partitions that were assigned to the consumer and now need to be revoked (may not
     *                   include all currently assigned partitions, i.e. there may still be some partitions left)
     */
    @Override
    public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
        log.info("Partition revoked : {}", partitions);

        for (TopicPartition topicPartition : partitions) {
            offsets.remove(topicPartition);
        }
    }

    /**
     * Override of onPartitionsAssigned method
     *
     * @param partitions The list of partitions that are now assigned to the consumer (previously owned partitions will
     *                   NOT be included, i.e. this list will only include newly added partitions)
     */
    @Override
    public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
        log.info("Partition assigned : {}", partitions);

        Map<TopicPartition, OffsetAndMetadata> offsetsTopicPartitions = consumer.committed(new HashSet<>(partitions));
        // Position can be unknown when you first initialize the group, when messages will be consumed, the map will be filled.
        offsets.putAll(offsetsTopicPartitions.entrySet()
                .stream()
                .filter(entry -> entry.getValue() != null)
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue)));
    }
}
