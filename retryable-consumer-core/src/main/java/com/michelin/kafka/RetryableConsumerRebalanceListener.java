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
package com.michelin.kafka;

import java.util.Collection;
import java.util.HashSet;
import java.util.Map;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;

/** Retryable consumer rebalance listener used for managing offsets and partitions when kafka makes rebalancings */
@Slf4j
public class RetryableConsumerRebalanceListener implements ConsumerRebalanceListener {
    private final Consumer<?, ?> consumer;
    private final Map<TopicPartition, OffsetAndMetadata> offsets;

    /**
     * Constructor
     *
     * @param consumer The consumer
     * @param offsets The current offset
     */
    public RetryableConsumerRebalanceListener(Consumer<?, ?> consumer, Map<TopicPartition, OffsetAndMetadata> offsets) {
        this.consumer = consumer;
        this.offsets = offsets;
    }

    /**
     * Override of onPartitionsRevoked method
     *
     * @param partitions The list of partitions that were assigned to the consumer and now need to be revoked (may not
     *     include all currently assigned partitions, i.e. there may still be some partitions left)
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
     *     NOT be included, i.e. this list will only include newly added partitions)
     */
    @Override
    public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
        log.info("Partition assigned : {}", partitions);

        Map<TopicPartition, OffsetAndMetadata> offsetsTopicPartitions = consumer.committed(new HashSet<>(partitions));
        // Position can be unknown when you first initialize the group, when messages will be consumed, the map will be
        // filled.
        offsets.putAll(offsetsTopicPartitions.entrySet().stream()
                .filter(entry -> entry.getValue() != null)
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue)));
    }
}
