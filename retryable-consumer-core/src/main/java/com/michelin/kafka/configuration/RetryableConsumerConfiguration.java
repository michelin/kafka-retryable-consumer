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
package com.michelin.kafka.configuration;

import static com.michelin.kafka.configuration.KafkaRetryableConfiguration.PROPERTY_SEPARATOR;

import java.util.*;
import lombok.*;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;

@Builder
@NoArgsConstructor
@AllArgsConstructor
@Slf4j
@Getter
@Setter
public class RetryableConsumerConfiguration {

    /**
     * Contains the Kafka properties defined in the application.properties file. The properties are mapped according to
     * the prefix defined in the ConfigurationProperties annotation and the name of this variable.
     */
    @Builder.Default
    private Properties properties = new Properties();

    /** Topic */
    @Builder.Default
    private Collection<String> topics = Collections.emptyList();

    /** Not retryable exceptions */
    @Builder.Default
    private Collection<String> notRetryableExceptions = Collections.emptyList();

    /** Circuit break max retry */
    @Builder.Default
    private Long retryMax = 0L;

    /** Circuit breaker backoff between each retry */
    @Builder.Default
    private Long retryBackoffMs = 0L;

    private static final Long DEFAULT_POLL_BACKOFF_MS = 1000L;

    /**
     * Timeout duration in ms for polling. This is the time until the consumer will wait if there is no record before
     * stopping to poll Default value is 1000 milliseconds
     */
    @Builder.Default
    private Long pollBackoffMs = DEFAULT_POLL_BACKOFF_MS;

    public void loadConfigMap(Map<String, Object> retryablConsumerConfigMap) {
        retryablConsumerConfigMap.forEach((k, v) -> {
            if (k.startsWith("properties")) {
                this.getProperties().put(StringUtils.substringAfter(k, PROPERTY_SEPARATOR), v);
            }
            if (k.startsWith("topics")) {
                if (v instanceof String s) {
                    this.setTopics(Arrays.asList((s).replace(" ", "").split(",")));
                } else if (v instanceof ArrayList l) {
                    this.setTopics((ArrayList<String>) l);
                } else {
                    throw new IllegalArgumentException(
                            "Parameter 'not-retryable-exceptions' must be a list of exception classes");
                }
            }
            if (k.startsWith("poll.backoff.ms")) {
                String pollBackOff = String.valueOf(v);
                this.setPollBackoffMs(Long.valueOf(pollBackOff));
            }
            if (k.startsWith("retry.max")) {
                String retryMaxStr = String.valueOf(v);
                this.setRetryMax(Long.valueOf(retryMaxStr));
            }
            if (k.startsWith("retry.backoff.ms")) {
                String retryBackoffMsStr = String.valueOf(v);
                this.setRetryBackoffMs(Long.valueOf(retryBackoffMsStr));
            }
            if (k.startsWith("not-retryable-exceptions")) {
                if (v instanceof String s) {
                    this.setNotRetryableExceptions(
                            Arrays.asList((s).replace(" ", "").split(",")));
                } else if (v instanceof ArrayList) {
                    this.setNotRetryableExceptions((ArrayList<String>) v);
                } else {
                    throw new IllegalArgumentException(
                            "Parameter 'not-retryable-exceptions' must be a list of exception classes");
                }
            }
        });
    }

    public void loadConfigProperties(String configurationPrefix, Properties retryablConsumerConfigProperties) {
        final HashMap<String, Object> consumerConfigMap = new HashMap<>();

        String prefix;
        if (configurationPrefix != null && !configurationPrefix.isEmpty()) {
            prefix = configurationPrefix + ".";
        } else {
            prefix = "";
        }

        retryablConsumerConfigProperties.forEach((k, v) -> {
            if (StringUtils.startsWith((String) k, prefix + "consumer.")) {
                consumerConfigMap.put(StringUtils.substringAfter((String) k, prefix + "consumer."), v);
            }
        });

        if (consumerConfigMap.isEmpty()) {
            throw new IllegalArgumentException("No '" + prefix + "consumer' configuration found in configuration file");
        }

        this.loadConfigMap(consumerConfigMap);
    }
}
