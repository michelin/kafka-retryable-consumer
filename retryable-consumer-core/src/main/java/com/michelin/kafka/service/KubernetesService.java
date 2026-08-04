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
package com.michelin.kafka.service;

import com.michelin.kafka.AbstractRetryableConsumer;
import com.michelin.kafka.ConsumerState;
import java.net.HttpURLConnection;
import lombok.extern.slf4j.Slf4j;

/**
 * Kubernetes health check service for the retryable consumer. Evaluates the consumer lifecycle state and returns
 * appropriate HTTP status codes for readiness and liveness probes.
 */
@Slf4j
public class KubernetesService {

    public static final String READINESS_PATH_PROPERTY_NAME = "kubernetes.readiness.path";
    public static final String LIVENESS_PATH_PROPERTY_NAME = "kubernetes.liveness.path";
    public static final String DEFAULT_READINESS_PATH = "ready";
    public static final String DEFAULT_LIVENESS_PATH = "liveness";

    private final AbstractRetryableConsumer<?, ?, ?> consumer;

    /**
     * Constructor.
     *
     * @param consumer The retryable consumer instance
     */
    public KubernetesService(AbstractRetryableConsumer<?, ?, ?> consumer) {
        this.consumer = consumer;
    }

    /**
     * Kubernetes readiness probe. Returns:
     *
     * <ul>
     *   <li>200 OK if consumer is RUNNING
     *   <li>204 No Content if consumer is STARTING
     *   <li>503 Service Unavailable if consumer is in ERROR or STOPPED
     * </ul>
     *
     * @return An HTTP response code
     */
    public int getReadiness() {
        if (consumer == null) {
            return HttpURLConnection.HTTP_BAD_REQUEST;
        }

        ConsumerState state = consumer.getConsumerState();
        log.debug("Consumer \"{}\" state: {}", consumer.getName(), state);

        return switch (state) {
            case RUNNING -> HttpURLConnection.HTTP_OK;
            case STARTING -> HttpURLConnection.HTTP_NO_CONTENT;
            case ERROR, STOPPED -> HttpURLConnection.HTTP_UNAVAILABLE;
        };
    }

    /**
     * Kubernetes liveness probe. Returns:
     *
     * <ul>
     *   <li>200 OK if consumer is RUNNING or STARTING
     *   <li>503 Service Unavailable if consumer is in ERROR or STOPPED
     * </ul>
     *
     * @return An HTTP response code
     */
    public int getLiveness() {
        if (consumer == null) {
            return HttpURLConnection.HTTP_NO_CONTENT;
        }

        ConsumerState state = consumer.getConsumerState();

        return switch (state) {
            case RUNNING, STARTING -> HttpURLConnection.HTTP_OK;
            case ERROR, STOPPED -> HttpURLConnection.HTTP_UNAVAILABLE;
        };
    }
}
