<div align="center">

<img src=".readme/logo-color.svg" alt="KafkaRertryableConsumer"/>

# Kafka Retryable Consumer

[![GitHub Build](https://img.shields.io/github/actions/workflow/status/michelin/kafka-retryable-consumer/build.yml?branch=main&logo=github&style=for-the-badge)](https://img.shields.io/github/actions/workflow/status/michelin/kafka-retryable-consumer/build.yml)
[![GitHub release](https://img.shields.io/github/v/release/michelin/kafka-retryable-consumer?logo=github&style=for-the-badge)](https://github.com/michelin/kafka-retryable-consumer/releases)
[![GitHub Stars](https://img.shields.io/github/stars/michelin/kafka-retryable-consumer?logo=github&style=for-the-badge)](https://github.com/michelin/kafka-retryable-consumer)
[![SonarCloud Coverage](https://img.shields.io/sonar/coverage/michelin_kafka-retryable-consumer?logo=sonarcloud&server=https%3A%2F%2Fsonarcloud.io&style=for-the-badge)](https://sonarcloud.io/component_measures?id=michelin_kafka-retryable-consumer&metric=coverage&view=list)
[![SonarCloud Tests](https://img.shields.io/sonar/tests/michelin_kafka-retryable-consumer/main?server=https%3A%2F%2Fsonarcloud.io&style=for-the-badge&logo=sonarcloud)](https://sonarcloud.io/component_measures?metric=tests&view=list&id=michelin_kafka-retryable-consumer)
[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg?logo=apache&style=for-the-badge)](https://opensource.org/licenses/Apache-2.0)

Simple Kafka consumer library with retry capabilities.

Retryable Consumer Libraries, allow you to easily build a Kafka API consumer with retry capabilities.
This lib is proposed to ease the usage of kafka for simple data consumer with external system call.

</div>

## Table of Contents
* [Features](#features)
    * [Current version](#current-version)
    * [To be implemented](#to-be-implemented)
* [Getting Started](#getting-started)
  * [Vanilla Java](#vanilla-java)
  * [Springboot](#springboot)
* [Custom Error Processing](#custom-error-processing)
* [Code References](#code-references)

## Features
### Current version
- Error management with Dead Letter Queue
- Automatic record commit management in case of error or partition rebalance
- Retry on error with Exception exclusion management
- Custom Error processing

### To be implemented
- Implement core lib Log&Fail feature
- Health checks in core library for non springboot projects
- Springboot starter with core lib health checks integration 


## Getting started
Kafka consumer libs repository hosts various libraries that might be usefull for your Kafka project.
- retryable-consumer-core : Core library for easy retryable consumers implementation

### Vanilla Java

Insert retryable-consumer-core dependency in your POM :
```xml
<dependency>
    <groupId>com.michelin.kafka</groupId>
    <artifactId>retryable-consumer-core</artifactId>
    <version>${retryable-consumer-core.version}</version>
</dependency>
```

Then in your code, on a retryable consumer just call ```java listen(topics)``` method :
```java
try(RetryableConsumer<String, String> retryableConsumer = new RetryableConsumer<>()) {
    retryableConsumer.listen(
        Collections.singleton("MY_TOPIC"), //Topic name
        businessProcessor::processRecord //Function process called by RetryableConsumer for each record
    );
}
```
`businessProcessor` must be your own processing classe with a method (`processRecord` in this example) accepting one
`ConsumerRecord<K, V>` as input.

:warning: ```java listen(topics)``` method is blocker and will start an endless loop to consumer you topic record.
If you need a non-blocking method, please use ```java listenAsync(topics)``` that returns a future.

In order to work properly, the retryable consumer will try to load kafka configuration from `application.yml`
or `application.yaml` or `application.properties`.

Example of application.yml :

```yaml
kafka:
  retryable:
    name: test-retry-consumer # name of the retryable consumer
    consumer: # All retryable consumer properties
      not-retryable-exceptions: # List of exceptions that will be ignored by the retry mechanism
        - java.lang.NullPointerException
        - java.lang.IllegalArgumentException
      stop-on-error: false # If true, the consumer will completely stop on not retryable error. Default value = false
      retry: # Retry configuration
        max: 10 # Maximum number of retry. 0 means infinite retry. Default value = 0
      poll:
        backoff:
          ms: 2345 # The time, in milliseconds, spent waiting in poll if data is not available in the buffer.
      properties: # All Kafka server configuration, please add your custom kafka consumer config here
        application:
          id: retryable-consumer-test
        bootstrap:
          servers: fake.server:9092
        specific:
          avro:
            reader: true
      topics: TOPIC # List of topics to listen to. Not mandatory
    dead-letter:
      producer:
        properties: # All Kafka server configuration for the dlq producer, please add your custom kafka producer config here
          application:
            id: dl-producer-test
          bootstrap:
            servers: fake.server:9092
        topic: DL_TOPIC
    
```

## Springboot

A Springboot starter should be build soon. Meanwhile, raw usage of Retryable Consumer CORE lib is possible in springboot.

### Step 1: Plug your Springboot configuration file on "KafkaRetryableConfiguration" class

Simply create a configuration that configuration class
```java
package com.michelin.oscp.huw.configuration;

import com.michelin.kafka.configuration.KafkaRetryableConfiguration;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;

@Configuration
@ConfigurationProperties(prefix = "kafka.retryable")
public class RetryableConsumerConfiguration extends KafkaRetryableConfiguration {}
```

In this example, all retryable consumer configuration should be placed under "kafka.retryable" prefix in config file.

Example of application.yml
```yaml 
kafka:
  retryable:
    name: myConsuimer # name of the retryable consumer (not mandatory)
    consumer: # All retryable consumer properties
      not-retryable-exceptions:
        - java.lang.NullPointerException
        - com.michelin.my.own.project.MyCustomException
      retry-max: 3 # Retry configuration
      poll:
        backoff:
          ms: 500 # The time, in milliseconds, spent waiting in poll if data is not available in the buffer.
      properties: # All Kafka server configuration, please add your custom kafka consumer config here
        auto.offset.reset: earliest
        bootstrap.servers: mybootstrap-server:9092 # This is kafka dev cluster
        schema.registry.url: https://my-schema-registry/dev/ # This is kafka dev sr
        basic.auth.credentials.source: USER_INFO
        basic.auth.user.info: user:user-secret
        key.deserializer: org.apache.kafka.common.serialization.StringDeserializer
        value.deserializer: io.confluent.kafka.serializers.KafkaAvroDeserializer
        specific.avro.reader: true #org.apache.avro.generic.GenericData$Record cannot be cast to HUWOutOrder # to solve this error. I added this config.
        max.poll.interval.ms: 30000 #5 min
        max.poll.records: 1
        enable.auto.commit: false  # is this for both consumer and producer ?
        fetch.min.bytes: 1
        heartbeat.interval.ms: 3000
        isolation.level: read_committed
        group.id: prefix.myProject
        sasl.jaas.config: org.apache.kafka.common.security.plain.PlainLoginModule required username="kafkaUser" password="kafkaSecret";
        sasl.mechanism: PLAIN
        security.protocol: SASL_SSL
        session.timeout.ms: 45000
        avro.remove.java.properties: true
      topics: prefix.myTopic
    dead-letter:
      properties: # All Kafka server configuration for the dlq producer, please add your custom kafka producer config
        bootstrap.servers: mybootstrap-server:9092 # This is kafka dev cluster
        schema.registry.url: https://my-schema-registry/dev/ # This is kafka dev sr
        basic.auth.credentials.source: USER_INFO
        basic.auth.user.info: user:user-secret
        acks: all
        avro.remove.java.properties: true
        enable.idempotence: true
        sasl.jaas.config: org.apache.kafka.common.security.plain.PlainLoginModule required username="kafkaUser" password="kafkaSecret";
        security.protocol: SASL_SSL
        key.serializer: org.apache.kafka.common.serialization.StringSerializer
        value.serializer: io.confluent.kafka.serializers.KafkaAvroSerializer
      topic: prefix.myTopicDeadLetterQ
```

### Step 2: Build a Kafka consumer service

Build a springboot service with a method that bootstrap the consumer.
Then call this method whenever you are ready to start your consumer.

We advise to put @Async annotation on this method to avoid blocking your springboot app while running
the synchronous KafkaRetryableConsumer method ```listen(topics)```. (or use the ```listenAsync(topics)``` method)

```java 
    ...
    @Autowired
    private RetryableConsumerConfiguration retryableConsumerConfiguration;
    ...
    @Async
    public void run(){
        log.info("Starting Out Order Event Listener");

        try(RetryableConsumer<String, MyAvroObject> retryableConsumer = new RetryableConsumer<>(retryableConsumerConfiguration)) {
            retryableConsumer.listen(
                    Collections.singleton("MY_INPUT_TOPIC"),
                    myBusinessProcessService::processRecord
            );
        }
    }
```

# Custom error processing
If you need to build custom error processing (ex: log additional metrics, send alert, use a specific Dead Letter Queue format ...etc.),
you can implement `ErrorProcessor` interface :

```java
    @Slf4j
    public class CustomErrorProcessor implements ErrorProcessor<ConsumerRecord<String, MyAvroObject>> {
        @Override
        public void processError(Throwable throwable, ConsumerRecord<String, MyAvroObject> record, Exception exception, int retryAttemptCount) {
            // Your custom error processing here
            log.error("...");
        }
    }
```

Then inject this custom error processor in your RetryableConsumer constructor :
```java
    try(RetryableConsumer<String, MyAvroObject> retryableConsumer = new RetryableConsumer<>(
            retryableConsumerConfiguration,
            new CustomErrorProcessor()
    )) {
        retryableConsumer.listen(
                Collections.singleton("MY_INPUT_TOPIC"),
                myBusinessProcessService::processRecord
        );
    }
```

# Code References
This library heavily relies on an original version from @jeanlouisboudart : https://github.com/jeanlouisboudart/retriable-consumer



