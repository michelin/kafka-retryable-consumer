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

import java.io.IOException;
import java.io.InputStream;
import java.util.LinkedHashMap;
import java.util.Properties;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.yaml.snakeyaml.Yaml;

@Builder
@AllArgsConstructor
@Getter
@Setter
@Slf4j
public class KafkaRetryableConfiguration {

    /** Default property file name */
    private static final String DEFAULT_CONFIG_FILE_NAME = "application";

    /** Property separator */
    public static final String PROPERTY_SEPARATOR = ".";

    private static final String CONFIGURATION_PREFIX = "kafka.retryable";

    protected String name;
    protected RetryableConsumerConfiguration consumer;
    protected DeadLetterProducerConfiguration deadLetter;
    protected KafkaProducerConfiguration producer;

    private static final ClassLoader classLoader = KafkaRetryableConfiguration.class.getClassLoader();

    public KafkaRetryableConfiguration() {
        this.consumer = new RetryableConsumerConfiguration();
        this.deadLetter = new DeadLetterProducerConfiguration();
        this.producer = new KafkaProducerConfiguration();
    }

    public static KafkaRetryableConfiguration load(String filePath) throws KafkaConfigurationException {
        String configFileExtension = StringUtils.substringAfterLast(filePath, ".");
        InputStream is = classLoader.getResourceAsStream(filePath);
        if (is == null) {
            throw new KafkaConfigurationException("Cannot find configuration file '" + filePath + "'");
        }
        switch (configFileExtension) {
            case "yml", "yaml" -> {
                return loadYaml(is);
            }
            case "properties" -> {
                return loadProperties(is);
            }
            default ->
                throw new KafkaConfigurationException(
                        "Unknown configuration file type '" + filePath + "'. Properties or a Yaml file expected");
        }
    }

    private static boolean isYmlConfigPresent() {
        return classLoader.getResource(DEFAULT_CONFIG_FILE_NAME + ".yml") != null;
    }

    private static boolean isYamlConfigPresent() {
        return classLoader.getResource(DEFAULT_CONFIG_FILE_NAME + ".yaml") != null;
    }

    private static boolean isPropertiesConfigPresent() {
        return classLoader.getResource(DEFAULT_CONFIG_FILE_NAME + ".properties") != null;
    }

    public static KafkaRetryableConfiguration load() throws KafkaConfigurationException {
        if (isYamlConfigPresent()) {
            return load(DEFAULT_CONFIG_FILE_NAME + ".yaml");
        }
        if (isYmlConfigPresent()) {
            return load(DEFAULT_CONFIG_FILE_NAME + ".yml");
        }
        if (isPropertiesConfigPresent()) {
            return load(DEFAULT_CONFIG_FILE_NAME + ".properties");
        }
        throw new KafkaConfigurationException("Cannot find any configuration file in classpath, please provide '"
                + DEFAULT_CONFIG_FILE_NAME + ".yaml or .yml or properties");
    }

    /**
     * Load configuration from properties file
     *
     * @param is InputStream of the properties file
     * @return Built object of RetryableConsumerConfiguration
     */
    private static KafkaRetryableConfiguration loadProperties(InputStream is) throws KafkaConfigurationException {
        Properties properties = new Properties();
        try {
            properties.load(is);
            return buildConfigFromFileProperties(properties);
        } catch (IOException e) {
            throw new KafkaConfigurationException(e);
        }
    }

    /**
     * Load configuration from yaml file
     *
     * @param is InputStream of the yaml file
     * @return Built object of RetryableConsumerConfiguration
     */
    private static KafkaRetryableConfiguration loadYaml(InputStream is) {
        Yaml yaml = new Yaml();
        LinkedHashMap<String, Object> configMap = yaml.load(is);
        Properties properties = parseMapToProperties(configMap);
        return buildConfigFromFileProperties(properties);
    }

    /**
     * Conversion from Properties object config to KafkaRetryableConfiguration
     *
     * @param properties the properties object to convert
     * @return Built object of KafkaRetryableConfiguration
     */
    public static KafkaRetryableConfiguration buildConfigFromFileProperties(Properties properties) {
        KafkaRetryableConfiguration kafkaRetryableConfiguration = new KafkaRetryableConfiguration();
        kafkaRetryableConfiguration.getConsumer().loadConfigProperties(CONFIGURATION_PREFIX, properties);
        kafkaRetryableConfiguration.getDeadLetter().loadConfigProperties(CONFIGURATION_PREFIX, properties);
        kafkaRetryableConfiguration.getProducer().loadConfigProperties(CONFIGURATION_PREFIX, properties);
        return kafkaRetryableConfiguration;
    }

    /**
     * Parse a map into Properties
     *
     * @param map The map
     * @return The properties
     */
    private static Properties parseMapToProperties(LinkedHashMap<String, Object> map) {
        return parseMapToProperties("", map, null);
    }

    /**
     * Parse a recursive map from Yaml.load() into properties
     *
     * @param key The current key of the map to process
     * @param mapOrValue The underlying map
     * @param props The returned properties that contains parsed data
     * @return The properties
     */
    private static Properties parseMapToProperties(String key, Object mapOrValue, Properties props) {
        if (props == null) {
            props = new Properties();
        }
        String sep = PROPERTY_SEPARATOR;
        if (StringUtils.isBlank(key)) {
            sep = "";
        }
        if (mapOrValue instanceof LinkedHashMap) {
            for (Object k : ((LinkedHashMap<?, ?>) mapOrValue).keySet()) {
                parseMapToProperties(key + sep + k, ((LinkedHashMap<?, ?>) mapOrValue).get(k), props);
            }
        } else { // it is a value object
            props.put(key, mapOrValue);
        }
        return props;
    }
}
