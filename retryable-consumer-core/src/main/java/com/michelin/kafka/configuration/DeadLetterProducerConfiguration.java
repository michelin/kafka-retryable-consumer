package com.michelin.kafka.configuration;

import lombok.*;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;

import java.util.*;

import static com.michelin.kafka.configuration.KafkaRetryableConfiguration.PROPERTY_SEPARATOR;

@Setter
@Getter
@Builder
@NoArgsConstructor
@AllArgsConstructor
@Slf4j
public class DeadLetterProducerConfiguration {

    /**
     * Contains the Kafka properties defined in the application.properties file.
     * The properties are mapped according to the prefix defined in the ConfigurationProperties annotation
     * and the name of this variable.
     */
    @Builder.Default
    private Properties properties = new Properties();

    /**
     * Dead Letter Topic
     */
    private String topic;

    public void loadConfigMap(Map<String, String> deadLetterProducerConfigMap) {
        deadLetterProducerConfigMap.forEach((k,v) -> {
            if(k.startsWith("properties")) {
                this.getProperties().put(StringUtils.substringAfter(k, PROPERTY_SEPARATOR), v);
            }
            if(k.startsWith("topic")) {
                this.setTopic(v);
            }
        });
    }

    public void loadConfigProperties(String configurationPrefix, Properties retryablConsumerConfigProperties) {

        String prefix;
        if(configurationPrefix != null && !configurationPrefix.isEmpty()) {
            prefix = configurationPrefix + ".";
        } else {
            prefix = "";
        }

        final HashMap<String, String> dlProducerConfigMap = new HashMap<>();
        retryablConsumerConfigProperties.forEach((k, v) -> {
            if(StringUtils.startsWith((String)k, prefix + "dead-letter.producer.")) {
                dlProducerConfigMap.put(
                        StringUtils.substringAfter((String)k, prefix + "dead-letter.producer."),
                        String.valueOf(v)
                );
            }
        });

        if(dlProducerConfigMap.isEmpty()) {
            throw new IllegalArgumentException("No '" + prefix + "dead-letter.producer' configuration found in configuration file");
        }
        this.loadConfigMap(dlProducerConfigMap);
    }


}
