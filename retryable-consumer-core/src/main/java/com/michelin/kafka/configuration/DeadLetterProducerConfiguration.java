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

@Setter
@Getter
@Builder
@NoArgsConstructor
@AllArgsConstructor
@Slf4j
public class DeadLetterProducerConfiguration {

    /**
     * Contains the Kafka properties defined in the application.properties file. The properties are mapped according to
     * the prefix defined in the ConfigurationProperties annotation and the name of this variable.
     */
    @Builder.Default
    private Properties properties = new Properties();

    /** Dead Letter Topic */
    private String topic;

    public void loadConfigMap(Map<String, String> deadLetterProducerConfigMap) {
        deadLetterProducerConfigMap.forEach((k, v) -> {
            if (k.startsWith("properties")) {
                this.getProperties().put(StringUtils.substringAfter(k, PROPERTY_SEPARATOR), v);
            }
            if (k.startsWith("topic")) {
                this.setTopic(v);
            }
        });
    }

    public void loadConfigProperties(String configurationPrefix, Properties retryablConsumerConfigProperties) {

        String prefix;
        if (configurationPrefix != null && !configurationPrefix.isEmpty()) {
            prefix = configurationPrefix + ".";
        } else {
            prefix = "";
        }

        final HashMap<String, String> dlProducerConfigMap = new HashMap<>();
        retryablConsumerConfigProperties.forEach((k, v) -> {
            if (StringUtils.startsWith((String) k, prefix + "dead-letter.producer.")) {
                dlProducerConfigMap.put(
                        StringUtils.substringAfter((String) k, prefix + "dead-letter.producer."), String.valueOf(v));
            }
        });

        if (dlProducerConfigMap.isEmpty()) {
            throw new IllegalArgumentException(
                    "No '" + prefix + "dead-letter.producer' configuration found in configuration file");
        }
        this.loadConfigMap(dlProducerConfigMap);
    }
}
