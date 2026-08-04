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
package com.michelin.kafka.test.unit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.michelin.kafka.AbstractRetryableConsumer;
import com.michelin.kafka.ConsumerState;
import com.michelin.kafka.service.KubernetesService;
import java.net.HttpURLConnection;
import org.junit.jupiter.api.Test;

class KubernetesServiceTest {

    @Test
    void shouldReturnOkWhenConsumerIsRunning() {
        AbstractRetryableConsumer<?, ?, ?> consumer = mock(AbstractRetryableConsumer.class);
        when(consumer.getConsumerState()).thenReturn(ConsumerState.RUNNING);
        when(consumer.getName()).thenReturn("test-consumer");

        KubernetesService service = new KubernetesService(consumer);

        assertEquals(HttpURLConnection.HTTP_OK, service.getReadiness());
        assertEquals(HttpURLConnection.HTTP_OK, service.getLiveness());
    }

    @Test
    void shouldReturnNoContentWhenConsumerIsStarting() {
        AbstractRetryableConsumer<?, ?, ?> consumer = mock(AbstractRetryableConsumer.class);
        when(consumer.getConsumerState()).thenReturn(ConsumerState.STARTING);
        when(consumer.getName()).thenReturn("test-consumer");

        KubernetesService service = new KubernetesService(consumer);

        assertEquals(HttpURLConnection.HTTP_NO_CONTENT, service.getReadiness());
        assertEquals(HttpURLConnection.HTTP_OK, service.getLiveness());
    }

    @Test
    void shouldReturnUnavailableWhenConsumerIsInError() {
        AbstractRetryableConsumer<?, ?, ?> consumer = mock(AbstractRetryableConsumer.class);
        when(consumer.getConsumerState()).thenReturn(ConsumerState.ERROR);
        when(consumer.getName()).thenReturn("test-consumer");

        KubernetesService service = new KubernetesService(consumer);

        assertEquals(HttpURLConnection.HTTP_UNAVAILABLE, service.getReadiness());
        assertEquals(HttpURLConnection.HTTP_UNAVAILABLE, service.getLiveness());
    }

    @Test
    void shouldReturnUnavailableWhenConsumerIsStopped() {
        AbstractRetryableConsumer<?, ?, ?> consumer = mock(AbstractRetryableConsumer.class);
        when(consumer.getConsumerState()).thenReturn(ConsumerState.STOPPED);
        when(consumer.getName()).thenReturn("test-consumer");

        KubernetesService service = new KubernetesService(consumer);

        assertEquals(HttpURLConnection.HTTP_UNAVAILABLE, service.getReadiness());
        assertEquals(HttpURLConnection.HTTP_UNAVAILABLE, service.getLiveness());
    }

    @Test
    void shouldReturnBadRequestForReadinessWhenConsumerIsNull() {
        KubernetesService service = new KubernetesService(null);

        assertEquals(HttpURLConnection.HTTP_BAD_REQUEST, service.getReadiness());
    }

    @Test
    void shouldReturnNoContentForLivenessWhenConsumerIsNull() {
        KubernetesService service = new KubernetesService(null);

        assertEquals(HttpURLConnection.HTTP_NO_CONTENT, service.getLiveness());
    }
}
