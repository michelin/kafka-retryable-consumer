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
package com.michelin.kafka.test.integration;

import static org.awaitility.Awaitility.await;

import com.michelin.kafka.avro.GenericErrorModel;
import com.michelin.kafka.configuration.KafkaRetryableConfiguration;
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroDeserializerConfig;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.integration.utils.EmbeddedKafkaCluster;
import org.junit.jupiter.api.extension.BeforeAllCallback;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.ExtensionContext;

/**
 * Base class for Kafka integration tests.
 *
 * <p>It owns a single in-memory Kafka cluster shared by every integration test of the JVM, and exposes deterministic
 * helpers (topic creation with leader election, bulk production, dead letter topic inspection) so that test methods
 * never rely on arbitrary sleeps or on a shared mutable configuration.
 */
@Slf4j
@ExtendWith(AbstractKafkaIntegrationTest.EmbeddedKafkaClusterExtension.class)
public abstract class AbstractKafkaIntegrationTest {

    /** Scope of the mocked Confluent schema registry shared by the dead letter producer and the test consumers. */
    protected static final String SCHEMA_REGISTRY_URL = "mock://kafka-retryable-consumer-it";

    /**
     * Upper bound used by the helpers waiting for a cluster side effect. Generous on purpose: it is never reached on a
     * healthy run because every wait exits as soon as its condition is met.
     */
    protected static final Duration CLUSTER_OPERATION_TIMEOUT = Duration.ofSeconds(60);

    /** Key under which the cluster is memoized in the JUnit root store, hence once for the whole JVM. */
    private static final String CLUSTER_STORE_KEY = "embedded-kafka-cluster";

    private static volatile EmbeddedKafkaCluster kafkaCluster;

    private static EmbeddedKafkaCluster cluster() {
        EmbeddedKafkaCluster current = kafkaCluster;
        if (current == null) {
            throw new IllegalStateException("Embedded kafka cluster has not been started");
        }
        return current;
    }

    /**
     * Starts the shared cluster on the first integration test class and closes it at the end of the whole test plan.
     *
     * <p>The cluster is deliberately not started in a {@code @BeforeAll} (it would be subject to the JUnit timeouts of
     * the test class and would restart a broker for every class) nor stopped from a JVM shutdown hook: shutdown hooks
     * run after Surefire has been told the tests are over, inside the {@code forkedProcessExitTimeoutInSeconds} budget
     * (30s by default). Stopping a broker can exceed it on a slow CI runner, in which case Surefire kills the fork and
     * fails the build even though every test passed. Closing the resource from the JUnit lifecycle keeps the shutdown
     * inside the part of the run Surefire actually waits for.
     */
    static class EmbeddedKafkaClusterExtension implements BeforeAllCallback {

        @Override
        public void beforeAll(ExtensionContext context) {
            context.getRoot()
                    .getStore(ExtensionContext.Namespace.GLOBAL)
                    .getOrComputeIfAbsent(CLUSTER_STORE_KEY, key -> new ClusterResource(), ClusterResource.class);
        }
    }

    /** Holder closed by JUnit once every test class has run. */
    static class ClusterResource implements ExtensionContext.Store.CloseableResource {

        ClusterResource() {
            Properties brokerConfig = new Properties();
            // Internal topics must be creatable on a single broker
            brokerConfig.put("offsets.topic.replication.factor", "1");
            brokerConfig.put("transaction.state.log.replication.factor", "1");
            brokerConfig.put("transaction.state.log.min.isr", "1");
            // The default 50 partitions of __consumer_offsets are pure overhead for a single broker test cluster
            brokerConfig.put("offsets.topic.num.partitions", "1");
            // Do not delay the first rebalance of a brand new consumer group
            brokerConfig.put("group.initial.rebalance.delay.ms", "0");
            // Every topic used by the tests is created explicitly
            brokerConfig.put("auto.create.topics.enable", "false");

            EmbeddedKafkaCluster started = new EmbeddedKafkaCluster(1, brokerConfig);
            started.start();
            started.verifyClusterReadiness();
            kafkaCluster = started;
            log.info("Embedded kafka cluster started on {}", started.bootstrapServers());
        }

        @Override
        public void close() {
            EmbeddedKafkaCluster current = kafkaCluster;
            if (current == null) {
                return;
            }
            kafkaCluster = null;
            try {
                current.stop();
                log.info("Embedded kafka cluster stopped");
            } catch (Exception e) {
                // Never fail a green build on a teardown glitch: the JVM is about to exit anyway
                log.warn("Ignoring error raised while stopping the embedded kafka cluster", e);
            }
        }
    }

    /**
     * Build a topic name that is unique for the whole JVM.
     *
     * @param prefix human readable prefix, typically the test method name
     * @return a unique topic name
     */
    protected static String uniqueName(String prefix) {
        return prefix + "-" + UUID.randomUUID();
    }

    /**
     * Address of the embedded broker, known only once it is started.
     *
     * <p>Needed by the tests that do not build their configuration with {@link #newConfiguration(String, String)}, for
     * instance those loading the configuration file of an example and overriding only the broker coordinates.
     *
     * @return the bootstrap servers of the embedded cluster
     */
    protected static String bootstrapServers() {
        return cluster().bootstrapServers();
    }

    /**
     * Create a topic and wait until every partition has an elected leader. Producing before the leader election is
     * complete is the main source of unexplained delays in Kafka integration tests.
     *
     * @param topic the topic to create
     * @param partitions the number of partitions
     */
    protected static void createTopic(String topic, int partitions) {
        try (Admin admin = cluster().createAdminClient()) {
            admin.createTopics(Collections.singletonList(new NewTopic(topic, partitions, (short) 1)))
                    .all()
                    .get(CLUSTER_OPERATION_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);

            await("leader election of topic " + topic)
                    .atMost(CLUSTER_OPERATION_TIMEOUT)
                    .pollInterval(Duration.ofMillis(100))
                    .until(() -> hasLeaderOnEveryPartition(admin, topic));
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("Interrupted while creating topic " + topic, e);
        } catch (ExecutionException | TimeoutException e) {
            throw new IllegalStateException("Unable to create topic " + topic, e);
        }
    }

    private static boolean hasLeaderOnEveryPartition(Admin admin, String topic) throws Exception {
        TopicDescription description;
        try {
            description = admin.describeTopics(Collections.singletonList(topic))
                    .allTopicNames()
                    .get(CLUSTER_OPERATION_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS)
                    .get(topic);
        } catch (ExecutionException e) {
            if (e.getCause() instanceof UnknownTopicOrPartitionException) {
                // The creation is acknowledged by the controller before the metadata is applied by the broker
                return false;
            }
            throw e;
        }
        return description != null
                && description.partitions().stream()
                        .allMatch(partition -> partition.leader() != null
                                && !partition.leader().isEmpty());
    }

    /**
     * Produce {@code count} string records with keys {@code k0..k(count-1)}. Records are sent in a single batch and
     * flushed once, instead of waiting for the acknowledgment of every single record.
     *
     * @param topic the destination topic
     * @param count the number of records to produce
     */
    protected static void produceStringRecords(String topic, int count) {
        Properties producerConfig = new Properties();
        producerConfig.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, cluster().bootstrapServers());
        producerConfig.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        producerConfig.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        producerConfig.put(ProducerConfig.ACKS_CONFIG, "all");

        try (KafkaProducer<String, String> producer = new KafkaProducer<>(producerConfig)) {
            for (int i = 0; i < count; i++) {
                producer.send(new ProducerRecord<>(topic, "k" + i, "value" + i));
            }
            producer.flush();
        }
        log.info("{} records produced into topic {}", count, topic);
    }

    /**
     * Build a self-contained configuration for the class under test. Each test owns its own instance, its own consumer
     * group and its own topics: no state can leak from one test to another.
     *
     * @param dataTopic the topic consumed by the retryable consumer
     * @param deadLetterTopic the dead letter topic
     * @return a ready to use configuration
     */
    protected static KafkaRetryableConfiguration newConfiguration(String dataTopic, String deadLetterTopic) {
        KafkaRetryableConfiguration configuration = new KafkaRetryableConfiguration();

        Properties consumerProperties = configuration.getConsumer().getProperties();
        consumerProperties.put(
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, cluster().bootstrapServers());
        consumerProperties.put(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, SCHEMA_REGISTRY_URL);
        consumerProperties.put(ConsumerConfig.GROUP_ID_CONFIG, uniqueName("group-" + dataTopic));
        consumerProperties.put(ConsumerConfig.CLIENT_ID_CONFIG, uniqueName("client-" + dataTopic));
        consumerProperties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        consumerProperties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        consumerProperties.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 10);
        consumerProperties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        consumerProperties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        // Detect a dead member quickly and refresh metadata often: both shorten the recovery time on a slow CI agent
        consumerProperties.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, 10000);
        consumerProperties.put(ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG, 1000);
        consumerProperties.put(ConsumerConfig.METADATA_MAX_AGE_CONFIG, 2000);
        consumerProperties.put(ConsumerConfig.FETCH_MAX_WAIT_MS_CONFIG, 100);
        configuration.getConsumer().setPollBackoffMs(500L);
        configuration.getConsumer().setTopics(Collections.singletonList(dataTopic));

        Properties deadLetterProperties = configuration.getDeadLetter().getProperties();
        deadLetterProperties.put(
                ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, cluster().bootstrapServers());
        deadLetterProperties.put(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, SCHEMA_REGISTRY_URL);
        deadLetterProperties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        deadLetterProperties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class.getName());
        deadLetterProperties.put(ProducerConfig.ACKS_CONFIG, "all");
        configuration.getDeadLetter().setTopic(deadLetterTopic);

        return configuration;
    }

    /**
     * Wait until the dead letter topic holds at least {@code expectedRecordCount} records and return them.
     *
     * @param deadLetterTopic the dead letter topic to read
     * @param expectedRecordCount the minimum number of records to wait for
     * @return the records found in the dead letter topic
     */
    protected static List<ConsumerRecord<String, GenericErrorModel>> awaitDeadLetterRecords(
            String deadLetterTopic, int expectedRecordCount) {
        List<ConsumerRecord<String, GenericErrorModel>> records = new ArrayList<>();

        try (KafkaConsumer<String, GenericErrorModel> consumer = new KafkaConsumer<>(deadLetterConsumerConfig())) {
            List<TopicPartition> partitions = consumer.partitionsFor(deadLetterTopic).stream()
                    .map(partitionInfo -> new TopicPartition(partitionInfo.topic(), partitionInfo.partition()))
                    .collect(Collectors.toList());
            consumer.assign(partitions);
            consumer.seekToBeginning(partitions);

            await("%d record(s) in dead letter topic %s".formatted(expectedRecordCount, deadLetterTopic))
                    .atMost(CLUSTER_OPERATION_TIMEOUT)
                    .pollInterval(Duration.ofMillis(50))
                    .until(() -> {
                        consumer.poll(Duration.ofMillis(200)).forEach(records::add);
                        return records.size() >= expectedRecordCount;
                    });
        }
        return records;
    }

    /**
     * Assert that the dead letter topic stays empty. The check is based on the topic end offsets, which is immediate,
     * and is repeated during a short grace period to catch a late asynchronous dead letter production.
     *
     * @param deadLetterTopic the dead letter topic that must remain empty
     */
    protected static void assertDeadLetterTopicIsEmpty(String deadLetterTopic) {
        await("dead letter topic %s to stay empty".formatted(deadLetterTopic))
                .during(Duration.ofSeconds(1))
                .atMost(Duration.ofSeconds(5))
                .pollInterval(Duration.ofMillis(100))
                .until(() -> countRecords(deadLetterTopic) == 0L);
    }

    /**
     * Count the records currently stored in a topic, without joining any consumer group.
     *
     * @param topic the topic to inspect
     * @return the number of records held by the topic
     */
    protected static long countRecords(String topic) {
        try (Admin admin = cluster().createAdminClient()) {
            TopicDescription description = admin.describeTopics(Collections.singletonList(topic))
                    .allTopicNames()
                    .get(CLUSTER_OPERATION_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS)
                    .get(topic);

            List<TopicPartition> partitions = description.partitions().stream()
                    .map(partition -> new TopicPartition(topic, partition.partition()))
                    .toList();

            Map<TopicPartition, Long> earliest = listOffsets(admin, partitions, OffsetSpec.earliest());
            Map<TopicPartition, Long> latest = listOffsets(admin, partitions, OffsetSpec.latest());

            return partitions.stream()
                    .mapToLong(partition -> latest.get(partition) - earliest.get(partition))
                    .sum();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("Interrupted while counting records of topic " + topic, e);
        } catch (ExecutionException | TimeoutException e) {
            throw new IllegalStateException("Unable to count records of topic " + topic, e);
        }
    }

    private static Map<TopicPartition, Long> listOffsets(
            Admin admin, List<TopicPartition> partitions, OffsetSpec offsetSpec)
            throws InterruptedException, ExecutionException, TimeoutException {
        return admin
                .listOffsets(partitions.stream().collect(Collectors.toMap(partition -> partition, p -> offsetSpec)))
                .all()
                .get(CLUSTER_OPERATION_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS)
                .entrySet()
                .stream()
                .collect(Collectors.toMap(
                        Map.Entry::getKey, entry -> entry.getValue().offset()));
    }

    private static Properties deadLetterConsumerConfig() {
        Properties config = new Properties();
        config.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, cluster().bootstrapServers());
        config.put(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, SCHEMA_REGISTRY_URL);
        config.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        config.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaAvroDeserializer.class.getName());
        config.put(KafkaAvroDeserializerConfig.SPECIFIC_AVRO_READER_CONFIG, true);
        config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        config.put(ConsumerConfig.GROUP_ID_CONFIG, uniqueName("dead-letter-checker"));
        return config;
    }
}
