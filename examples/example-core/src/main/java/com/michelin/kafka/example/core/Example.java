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

import java.io.Closeable;
import java.util.concurrent.Future;

/**
 * What all the examples of this module have in common: they start a consumer in the background, and they are closeable.
 *
 * <p>This exists only so that {@link ExampleRunner} can boot any of them, which lets the nine {@code main} methods
 * share a single, tested implementation of the shutdown and error reporting.
 */
public interface Example extends Closeable {

    /**
     * Start consuming, without blocking the calling thread.
     *
     * @return a future that completes once the consumer has stopped
     */
    Future<Void> start();

    /**
     * Close the underlying consumer.
     *
     * <p>Narrowed from {@link Closeable#close()} to declare no checked exception, since closing a consumer cannot fail.
     */
    @Override
    void close();
}
