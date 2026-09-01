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

import com.michelin.kafka.configuration.KafkaConfigurationException;
import java.util.concurrent.ExecutionException;
import lombok.extern.slf4j.Slf4j;

/**
 * Boots an example, waits for it, and turns the outcome into a process exit code.
 *
 * <p>The nine {@code main} methods of this module all need exactly the same thing: build the example, wait until the
 * consumer stops, and report a failure properly instead of letting a raw stack trace escape. That handling lives here,
 * written once and covered by tests, rather than copied nine times where no test would ever reach it.
 *
 * <p>A real application would simply inline this logic in its own {@code main}; it is factored out here only because
 * this module holds nine variations of the same entry point.
 */
@Slf4j
public final class ExampleRunner {

    private ExampleRunner() {
        // Utility class
    }

    /**
     * Builds an example.
     *
     * <p>Building is part of what {@link #run} supervises, so that an invalid configuration file is reported like any
     * other failure rather than crashing the process.
     */
    @FunctionalInterface
    public interface ExampleFactory {

        /**
         * Build the example.
         *
         * @return the example, ready to be started
         * @throws KafkaConfigurationException if the configuration file is missing or invalid
         */
        Example create() throws KafkaConfigurationException;
    }

    /**
     * Build the example, run it until the consumer stops, and report how it ended.
     *
     * @param factory builds the example, typically a constructor reference
     * @return {@code 0} when the consumer stopped normally, {@code 1} on any failure
     */
    public static int run(ExampleFactory factory) {
        try (Example example = factory.create()) {
            example.start().get();
            log.info("Consumer stopped");
            return 0;
        } catch (InterruptedException e) {
            // Never swallow an interruption: restore the flag so the callers up the stack still see it
            Thread.currentThread().interrupt();
            log.info("Interrupted, shutting down");
            return 1;
        } catch (ExecutionException e) {
            // The consumer runs on another thread, so the real failure is the cause, not the wrapper
            log.error("Consumer stopped on an unrecoverable error", e.getCause());
            return 1;
        } catch (KafkaConfigurationException e) {
            log.error("Could not read the configuration of this example", e);
            return 1;
        }
    }
}
