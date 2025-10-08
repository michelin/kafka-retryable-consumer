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

import lombok.*;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import static com.michelin.kafka.configuration.KafkaRetryableConfiguration.PROPERTY_SEPARATOR;

@Setter
@Getter
@Builder
@NoArgsConstructor
@AllArgsConstructor
@Slf4j
public class KafkaProducerConfiguration {

    /**
     * Contains the Kafka properties defined in the application.properties file. The properties are mapped according to
     * the prefix defined in the ConfigurationProperties annotation and the name of this variable.
     */
    @Builder.Default
    private Properties properties = new Properties();

    public void loadConfigMap(Map<String, String> kafkaProducerConfigMap) {
        kafkaProducerConfigMap.forEach((k, v) -> {
            if (k.startsWith("properties")) {
                this.getProperties().put(StringUtils.substringAfter(k, PROPERTY_SEPARATOR), v);
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

        final HashMap<String, String> kafkaProducerConfigMap = new HashMap<>();
        retryablConsumerConfigProperties.forEach((k, v) -> {
            if (StringUtils.startsWith((String) k, prefix + "producer.")) {
                kafkaProducerConfigMap.put(
                        StringUtils.substringAfter((String) k, prefix + "producer."), String.valueOf(v));
            }
        });

        if (kafkaProducerConfigMap.isEmpty()) {
            throw new IllegalArgumentException("No '" + prefix + "producer' configuration found in configuration file");
        }
        this.loadConfigMap(kafkaProducerConfigMap);
    }
}
