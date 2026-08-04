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
package test.unit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.michelin.kafka.controller.KubernetesController;
import com.michelin.kafka.service.KubernetesService;
import java.net.HttpURLConnection;
import org.junit.jupiter.api.Test;
import org.springframework.http.ResponseEntity;

class KubernetesControllerTest {

    @Test
    void shouldReturnReadinessStatus() {
        KubernetesService kubernetesService = mock(KubernetesService.class);
        when(kubernetesService.getReadiness()).thenReturn(HttpURLConnection.HTTP_OK);

        KubernetesController controller = new KubernetesController(kubernetesService);
        ResponseEntity<Void> response = controller.readiness();

        assertEquals(HttpURLConnection.HTTP_OK, response.getStatusCode().value());
    }

    @Test
    void shouldReturnReadinessUnavailable() {
        KubernetesService kubernetesService = mock(KubernetesService.class);
        when(kubernetesService.getReadiness()).thenReturn(HttpURLConnection.HTTP_UNAVAILABLE);

        KubernetesController controller = new KubernetesController(kubernetesService);
        ResponseEntity<Void> response = controller.readiness();

        assertEquals(
                HttpURLConnection.HTTP_UNAVAILABLE, response.getStatusCode().value());
    }

    @Test
    void shouldReturnLivenessStatus() {
        KubernetesService kubernetesService = mock(KubernetesService.class);
        when(kubernetesService.getLiveness()).thenReturn(HttpURLConnection.HTTP_OK);

        KubernetesController controller = new KubernetesController(kubernetesService);
        ResponseEntity<Void> response = controller.liveness();

        assertEquals(HttpURLConnection.HTTP_OK, response.getStatusCode().value());
    }

    @Test
    void shouldReturnLivenessUnavailable() {
        KubernetesService kubernetesService = mock(KubernetesService.class);
        when(kubernetesService.getLiveness()).thenReturn(HttpURLConnection.HTTP_UNAVAILABLE);

        KubernetesController controller = new KubernetesController(kubernetesService);
        ResponseEntity<Void> response = controller.liveness();

        assertEquals(
                HttpURLConnection.HTTP_UNAVAILABLE, response.getStatusCode().value());
    }
}
