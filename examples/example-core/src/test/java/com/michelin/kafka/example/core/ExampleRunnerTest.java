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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.michelin.kafka.configuration.KafkaConfigurationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * Unit tests of the shared entry point of the examples.
 *
 * <p>The nine {@code main} methods delegate to {@link ExampleRunner#run}, so this is where their shutdown and error
 * reporting is actually verified. No broker is involved: the example is a stub, which is the whole point of having
 * extracted this logic out of the {@code main} methods.
 */
class ExampleRunnerTest {

    /** An example that is never really started, so that the runner can be exercised without Kafka. */
    private static class StubExample implements Example {

        private final Future<Void> outcome;
        private boolean closed;

        StubExample(Future<Void> outcome) {
            this.outcome = outcome;
        }

        @Override
        public Future<Void> start() {
            return outcome;
        }

        @Override
        public void close() {
            closed = true;
        }
    }

    @AfterEach
    void clearInterruptFlag() {
        // Make sure an interruption raised by one test cannot leak into the next one
        Thread.interrupted();
    }

    @Test
    @DisplayName("Reports success and closes the example when the consumer stops normally")
    void shouldReturnZeroWhenTheConsumerStopsNormally() {
        StubExample example = new StubExample(CompletableFuture.completedFuture(null));

        assertEquals(0, ExampleRunner.run(() -> example));
        assertTrue(example.closed, "the example must be closed on the way out");
    }

    @Test
    @DisplayName("Reports a failure and still closes the example when the consumer fails")
    void shouldReturnOneWhenTheConsumerFails() {
        StubExample example = new StubExample(CompletableFuture.failedFuture(new IllegalStateException("boom")));

        assertEquals(1, ExampleRunner.run(() -> example));
        assertTrue(example.closed, "the example must be closed even when it failed");
    }

    @Test
    @DisplayName("Restores the interrupt flag rather than swallowing the interruption")
    void shouldRestoreTheInterruptFlagWhenInterrupted() {
        // A future that never completes: get() returns only because the thread is already interrupted
        StubExample example = new StubExample(new CompletableFuture<>());
        Thread.currentThread().interrupt();

        assertEquals(1, ExampleRunner.run(() -> example));
        assertTrue(Thread.interrupted(), "the interrupt flag must be restored for the callers up the stack");
        assertTrue(example.closed, "the example must be closed after an interruption");
    }

    @Test
    @DisplayName("Reports an invalid configuration instead of letting it crash the process")
    void shouldReturnOneWhenTheConfigurationCannotBeRead() {
        assertEquals(1, ExampleRunner.run(() -> {
            throw new KafkaConfigurationException("no such file");
        }));
    }

    @Test
    @DisplayName("Does not start the example when it could not be built")
    void shouldNotStartTheExampleWhenItCannotBeBuilt() {
        StubExample example = new StubExample(CompletableFuture.completedFuture(null));

        assertEquals(1, ExampleRunner.run(() -> {
            throw new KafkaConfigurationException("no such file");
        }));
        assertFalse(example.closed, "an example that was never built cannot have been closed");
    }
}
