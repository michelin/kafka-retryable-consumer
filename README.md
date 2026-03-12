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
* [Configuration](#configuration)
* [Customisation](#customization)
    * [Dead Letter Queue (DLQ)](#dead-letter-queue-dlq)
    * [Custom Error Processing](#custom-error-processing)
* [Code References](#code-references)

## Features
### Current version
- Error management with Dead Letter Queue
- Automatic record commit management in case of error or partition rebalance
- Retry on error with Exception exclusion management
- Custom Error processing
- Batch Record Processing

### To be implemented
- Health checks in core library for non springboot projects

## Getting started
Kafka consumer libs repository hosts various libraries that might be usefull for your Kafka project.
- retryable-consumer-core : Core library for easy retryable consumers implementation
- retryable-consumer-springboot-starter : Spring Boot autoconfiguration for the Retryable Consumer core library
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
    retryableConsumer.listen(        Collections.singleton("MY_TOPIC"), //Topic name        businessProcessor::processRecord //Function process called by RetryableConsumer for each record    );}
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
  retryable:    name: test-retry-consumer # name of the retryable consumer    consumer: # All retryable consumer properties      not-retryable-exceptions: # List of exceptions that will be ignored by the retry mechanism        - java.lang.NullPointerException        - java.lang.IllegalArgumentException      stop-on-error: false # If true, the consumer will completely stop on not retryable error. Default value = false      retry: # Retry configuration        max: 10 # Maximum number of retry. 0 means infinite retry. Default value = 0      poll:        backoff:          ms: 2345 # The time, in milliseconds, spent waiting in poll if data is not available in the buffer.      properties: # All Kafka server configuration, please add your custom kafka consumer config here        application:          id: retryable-consumer-test        bootstrap:          servers: fake.server:9092        specific:          avro:            reader: true      topics: TOPIC # List of topics to listen to. Not mandatory    dead-letter:      producer:        properties: # All Kafka server configuration for the dlq producer, please add your custom kafka producer config here          application:            id: dl-producer-test          bootstrap:            servers: fake.server:9092        topic: DL_TOPIC      
```  

## Springboot

Add the starter as a dependency to your Spring Boot application:

```xml
<dependency>
  <groupId>com.michelin.kafka</groupId>
  <artifactId>retryable-consumer-spring-boot-starter</artifactId>
  <version><!-- project version -->${retryable-consumer-core.version}</version>
</dependency>
```

Then configure properties in `application.yml` or `application.properties`.

Example `application.yml` minimal:

```yaml
kafka:
  retryable:
    enabled: true
    consumer:
      topics:
        - my-topic
      properties:
        bootstrap.servers: 127.0.0.1:9092
        key.deserializer: org.apache.kafka.common.serialization.StringDeserializer
        value.deserializer: org.apache.kafka.common.serialization.StringDeserializer
```

This will create a `RetryableConsumer` bean with default behavior. To inject it into your code:

```java
@Autowired
private RetryableConsumer<String, String> retryableConsumer;
```

Then use `retryableConsumer.listen(...)` or `listenAsync(...)` with a `RecordProcessor`.
### Advanced usage

- If you want full control over the configuration object, define a `@Bean` of type `KafkaRetryableConfiguration` in your application context — the starter will not override it (it uses `@ConditionalOnMissingBean`).
- You can also provide `KafkaRetryableSpringProperties` (for binding) instead of `KafkaRetryableConfiguration` and the mapper will convert it.

Example customizing error handling:

```java
@Bean
public RetryableConsumer<String, MyValue> customRetryableConsumer(KafkaRetryableConfiguration cfg, DeadLetterProducer dlq) {
  // build a RetryableConsumer with custom ErrorProcessor or inject the DLQ
  return new RetryableConsumer<>(cfg, dlq);
}
```

### Testing notes

- Unit tests in this module avoid creating real Kafka clients during Spring context refresh. The starter defers Kafka client construction (lazy) and provides defaults for deserializers/serializers in tests to avoid Kafka client ConfigExceptions.
- For true integration tests against Kafka use `spring-kafka-test`'s `EmbeddedKafkaBroker` (create a separate integration test profile).
- If tests are flaky due to async background processing, prefer synchronization helpers such as `CountDownLatch` or inject a test Executor.

### Troubleshooting

- Error: "Missing required property: kafka.retryable.consumer.topics must be configured and non-empty"
    - Ensure `kafka.retryable.consumer.topics` is set in application properties or provide a `KafkaRetryableConfiguration` bean.

- Error: "Missing required property: kafka.retryable.consumer.properties.bootstrap.servers must be configured"
    - Add `bootstrap.servers` under `kafka.retryable.consumer.properties`.

- If tests fail with Kafka client DNS/connection errors in CI, ensure tests either mock Kafka clients or run integration tests against Embedded Kafka. Unit tests should remain network-free.


## Configuration

| Configuration key                               | Value Type         | Default Value | Description                                                  | Note                                                               |
| ----------------------------------------------- | ------------------ | ------------- | ------------------------------------------------------------ | ------------------------------------------------------------------ |
| kafka.retryable.enabled                         | boolean            | true          | Toggle the auto-configuration on/off                         | Only available with retryable-consumer-spring-boot-starter library |
| kafka.retryable.consumer.topics                 | Collection<String> | none          | Input consumer topic list                                    | Mandatory                                                          |
| kafka.retryable.consumer.properties             | Properties         | none          | All standard Kafka Consumer configuration properties         | bootstrap.servers, key.deserializer, value.deserializer ...etc     |
| kafka.retryable.consumer.pollBackoffMs          | Long               | 1000          | Timeout duration in ms for polling                           | Optional                                                           |
| kafka.retryable.consumer.retryBackoffMs         | Long               | 0             | Circuit breaker backoff between each retry                   | Optional                                                           |
| kafka.retryable.consumer.retryMax               | Long               | 0             | Circuit breaker max number of retry                          | Optional                                                           |
| kafka.retryable.consumer.notRetryableExceptions | Collection<String> | empty list    | List of not retryable exception when retry option is enabled | Optional                                                           |
| kafka.retryable.consumer.stopOnError            | boolean            | false         | Fully stop consumer on error                                 | Optional                                                           |
| kafka.retryable.dead-letter.producer.topic      | String             | none          | Dead Letter Queue topic                                      |                                                                    |
| kafka.retryable.dead-letter.producer.properties | Properties         | none          | All standard Kafka Producer configuration properties         | bootstrap.servers, key.serializer, value.serializer ...etc         |



## Customization
### Dead Letter Queue (DLQ)

The DLQ is optional and will only be created if `kafka.retryable.dead-letter.producer.topic` is configured. If configured, the DLQ producer requires producer properties including `bootstrap.servers` and serializers.

Example DLQ config:

```yaml
kafka:
  retryable:
    dead-letter:
      producer:
        topic: my-dlq
        properties:
          bootstrap.servers: 127.0.0.1:9092
          key.serializer: org.apache.kafka.common.serialization.StringSerializer
          value.serializer: org.apache.kafka.common.serialization.StringSerializer
```

If you don't set serializers for the DLQ producer, the starter will provide sensible defaults (StringSerializer) for tests and simple setups.

### Custom error processing
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
This library relies on an original version from @jeanlouisboudart : https://github.com/jeanlouisboudart/retriable-consumer
