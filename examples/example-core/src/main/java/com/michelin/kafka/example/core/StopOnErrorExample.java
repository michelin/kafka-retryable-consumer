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
package com.michelin.kafka.example.core;

import com.michelin.kafka.RetryableConsumer;
import com.michelin.kafka.configuration.KafkaConfigurationException;
import com.michelin.kafka.configuration.KafkaRetryableConfiguration;
import java.io.Closeable;
import java.util.concurrent.Future;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;

/**
 * Stop the consumer instead of skipping a record that could not be processed.
 *
 * <p>By default an unrecoverable record is sent to the dead letter topic and the consumer carries on with the next one.
 * With {@code stop-on-error: true} the record is still published to the dead letter topic, but the consumer then stops
 * rather than committing the offset and moving forward.
 *
 * <p>Use it when silently skipping a record is not acceptable: an operator must look at the problem before the stream
 * resumes. The offset of the failed record stays uncommitted, so restarting the application replays it.
 */
@Slf4j
public class StopOnErrorExample implements Closeable {

    /** Configuration of this example, loaded from the classpath. */
    public static final String CONFIG_FILE = "stop-on-error-example.yml";

    private final RetryableConsumer<String, String> consumer;

    /** Build the example from its own configuration file. */
    public StopOnErrorExample() throws KafkaConfigurationException {
        this(KafkaRetryableConfiguration.load(CONFIG_FILE));
    }

    /** Build the example from an already loaded configuration, see {@link SimpleConsumerExample}. */
    public StopOnErrorExample(KafkaRetryableConfiguration configuration) {
        this.consumer = new RetryableConsumer<>(configuration);
    }

    public Future<Void> start() {
        return consumer.listenAsync(this::process);
    }

    private void process(ConsumerRecord<String, String> record) {
        log.info("Processing record key={}", record.key());
        throw new IllegalStateException("Unrecoverable error on record " + record.key());
    }

    /** Whether the consumer has been stopped by the error handling. */
    public boolean isStopped() {
        return consumer.isStopped();
    }

    @Override
    public void close() {
        consumer.close();
    }

    public static void main(String[] args) throws Exception {
        try (StopOnErrorExample example = new StopOnErrorExample()) {
            example.start().get();
            log.info("Consumer stopped after an unrecoverable error");
        }
    }
}
