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
package com.michelin.kafka.controller;

import com.michelin.kafka.service.KubernetesService;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

/** Kafka retryable consumer controller for Kubernetes health checks. Exposes readiness and liveness probe endpoints. */
@RestController
@ConditionalOnBean(KubernetesService.class)
public class KubernetesController {

    private final KubernetesService kubernetesService;

    /**
     * Constructor.
     *
     * @param kubernetesService The kubernetes service
     */
    public KubernetesController(KubernetesService kubernetesService) {
        this.kubernetesService = kubernetesService;
    }

    /**
     * Readiness Kubernetes probe endpoint.
     *
     * @return An HTTP response based on the consumer state
     */
    @GetMapping("/${kubernetes.readiness.path:ready}")
    public ResponseEntity<Void> readiness() {
        int readinessStatus = kubernetesService.getReadiness();
        return ResponseEntity.status(readinessStatus).build();
    }

    /**
     * Liveness Kubernetes probe endpoint.
     *
     * @return An HTTP response based on the consumer state
     */
    @GetMapping("/${kubernetes.liveness.path:liveness}")
    public ResponseEntity<Void> liveness() {
        int livenessStatus = kubernetesService.getLiveness();
        return ResponseEntity.status(livenessStatus).build();
    }
}
