<div align="center">

<img src=".readme/logo-color.svg" alt="KafkaRetryableConsumer"/>

# Kafka Retryable Consumer

[![GitHub Build](https://img.shields.io/github/actions/workflow/status/michelin/kafka-retryable-consumer/build.yml?branch=main&logo=github&style=for-the-badge)](https://img.shields.io/github/actions/workflow/status/michelin/kafka-retryable-consumer/build.yml)
[![GitHub release](https://img.shields.io/github/v/release/michelin/kafka-retryable-consumer?logo=github&style=for-the-badge)](https://github.com/michelin/kafka-retryable-consumer/releases)
[![GitHub Stars](https://img.shields.io/github/stars/michelin/kafka-retryable-consumer?logo=github&style=for-the-badge)](https://github.com/michelin/kafka-retryable-consumer)
[![SonarCloud Coverage](https://img.shields.io/sonar/coverage/michelin_kafka-retryable-consumer?logo=sonarcloud&server=https%3A%2F%2Fsonarcloud.io&style=for-the-badge)](https://sonarcloud.io/component_measures?id=michelin_kafka-retryable-consumer&metric=coverage&view=list)
[![SonarCloud Tests](https://img.shields.io/sonar/tests/michelin_kafka-retryable-consumer/main?server=https%3A%2F%2Fsonarcloud.io&style=for-the-badge&logo=sonarcloud)](https://sonarcloud.io/component_measures?metric=tests&view=list&id=michelin_kafka-retryable-consumer)
[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg?logo=apache&style=for-the-badge)](https://opensource.org/licenses/Apache-2.0)

A simple, robust Kafka consumer library with built-in retry, dead letter queue, and Kubernetes health check support.

</div>

## Table of Contents

- [Features](#features)
- [Modules](#modules)
- [Getting Started](#getting-started)
  - [Vanilla Java](#vanilla-java)
  - [Spring Boot](#spring-boot)
- [Configuration](#configuration)
  - [Consumer Properties](#consumer-properties)
  - [Dead Letter Producer Properties](#dead-letter-producer-properties)
  - [Health Check Properties](#health-check-properties)
- [Health Checks](#health-checks)
- [Customization](#customization)
  - [Dead Letter Queue (DLQ)](#dead-letter-queue-dlq)
  - [Custom Error Processing](#custom-error-processing)
- [Contribution & Development](#contribution--development)
- [License](#license)
- [Credits](#credits)

## Features

- **Retry on error** with configurable max retries, backoff, and exception exclusion
- **Dead Letter Queue (DLQ)** for unrecoverable records
- **Automatic commit management** on error or partition rebalance
- **Batch record processing**
- **Custom error processing** via the `ErrorProcessor` interface
- **Kubernetes health checks** — readiness and liveness endpoints (core & Spring Boot)
- **Blocking and non-blocking** consumption (`listen()` / `listenAsync()`)

## Modules

| Module | Description |
|--------|-------------|
| [retryable-consumer-core](retryable-consumer-core/) | Core Java library — no framework dependency |
| [retryable-consumer-spring-boot-starter](retryable-consumer-spring-boot-starter/) | Spring Boot autoconfiguration & health endpoints |

## Getting Started

### Vanilla Java

Add the core dependency:

```xml
<dependency>
    <groupId>com.michelin.kafka</groupId>
    <artifactId>retryable-consumer-core</artifactId>
    <version>${retryable-consumer-core.version}</version>
</dependency>
```

Create a consumer and start listening:

```java
try (RetryableConsumer<String, String> retryableConsumer = new RetryableConsumer<>()) {
    retryableConsumer.listen(
        Collections.singleton("MY_TOPIC"),
        businessProcessor::processRecord
    );
}
```

> **Note:** `businessProcessor` must have a method accepting a `ConsumerRecord<K, V>` as input.

> **Warning:** `listen()` is a blocking call that starts an endless polling loop.
> For non-blocking consumption, use `listenAsync()` which returns a `Future`.

The consumer loads its configuration from `application.yml`, `application.yaml`, or `application.properties`:

```yaml
kafka:
  retryable:
    name: my-retryable-consumer
    consumer:
      topics: MY_TOPIC
      stop-on-error: false
      not-retryable-exceptions:
        - java.lang.NullPointerException
        - java.lang.IllegalArgumentException
      retry:
        max: 10
      poll:
        backoff:
          ms: 2000
      properties:
        application.id: retryable-consumer-app
        bootstrap.servers: localhost:9092
        specific.avro.reader: true
    dead-letter:
      producer:
        topic: DL_TOPIC
        properties:
          application.id: dl-producer-app
          bootstrap.servers: localhost:9092
```

### Spring Boot

Add the starter dependency:

```xml
<dependency>
    <groupId>com.michelin.kafka</groupId>
    <artifactId>retryable-consumer-spring-boot-starter</artifactId>
    <version>${retryable-consumer-core.version}</version>
</dependency>
```

Configure `application.yml`:

```yaml
kafka:
  retryable:
    enabled: true
    consumer:
      topics:
        - my-topic
      properties:
        bootstrap.servers: localhost:9092
        key.deserializer: org.apache.kafka.common.serialization.StringDeserializer
        value.deserializer: org.apache.kafka.common.serialization.StringDeserializer
```

Inject and use:

```java
@Autowired
private RetryableConsumer<String, String> retryableConsumer;
```

For advanced usage (custom beans, error handling, testing), see the [Spring Boot Starter README](retryable-consumer-spring-boot-starter/README.md).

## Configuration

### Consumer Properties

Configuration prefix: `kafka.retryable`

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| `enabled` | boolean | `true` | Toggle auto-configuration on/off (Spring Boot only) |
| `consumer.topics` | Collection\<String\> | — | Input topic list (**required**) |
| `consumer.properties` | Properties | — | Standard Kafka consumer properties (`bootstrap.servers`, etc.) |
| `consumer.poll-backoff-ms` | Long | `1000` | Poll timeout in milliseconds |
| `consumer.retry-backoff-ms` | Long | `0` | Backoff between retries in milliseconds |
| `consumer.retry-max` | Long | `0` | Max retry count (`0` = infinite) |
| `consumer.not-retryable-exceptions` | Collection\<String\> | `[]` | Exceptions that skip retry |
| `consumer.stop-on-error` | boolean | `false` | Stop consumer entirely on non-retryable error |

### Dead Letter Producer Properties

Configuration prefix: `kafka.retryable.dead-letter.producer`

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| `topic` | String | — | DLQ topic name |
| `properties` | Properties | — | Standard Kafka producer properties (`bootstrap.servers`, serializers, etc.) |

### Health Check Properties

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| `kubernetes.readiness.path` | String | `ready` | Readiness endpoint path |
| `kubernetes.liveness.path` | String | `liveness` | Liveness endpoint path |

## Health Checks

The library tracks consumer lifecycle state (`STARTING` -> `RUNNING` -> `ERROR` / `STOPPED`) and exposes Kubernetes-compatible health endpoints.

**Spring Boot** — If `spring-boot-starter-web` is on the classpath, endpoints are auto-configured:
- `GET /{kubernetes.readiness.path}` (default: `/ready`)
- `GET /{kubernetes.liveness.path}` (default: `/liveness`)

**Core (Vanilla Java)** — Use `KubernetesService` directly to query health status and integrate with your own HTTP server.

| Consumer State | Readiness | Liveness |
|----------------|-----------|----------|
| `RUNNING`    | 200 OK    | 200 OK   |
| `STARTING`   | 204 No Content | 200 OK |
| `ERROR`      | 503 Unavailable | 503 Unavailable |
| `STOPPED`    | 503 Unavailable | 503 Unavailable |
| `null` (no consumer) | 400 Bad Request | 204 No Content |

## Customization

### Dead Letter Queue (DLQ)

The DLQ is optional. It is activated when `kafka.retryable.dead-letter.producer.topic` is configured along with producer properties.

```yaml
kafka:
  retryable:
    dead-letter:
      producer:
        topic: my-dlq
        properties:
          bootstrap.servers: localhost:9092
          key.serializer: org.apache.kafka.common.serialization.StringSerializer
          value.serializer: org.apache.kafka.common.serialization.StringSerializer
```

### Custom Error Processing

Implement the `ErrorProcessor` interface to add custom logic (metrics, alerts, custom DLQ format, etc.):

```java
@Slf4j
public class CustomErrorProcessor implements ErrorProcessor<ConsumerRecord<String, MyAvroObject>> {
    @Override
    public void processError(Throwable throwable, ConsumerRecord<String, MyAvroObject> record,
                             Exception exception, int retryAttemptCount) {
        log.error("Custom error handling for record {}", record.key());
    }
}
```

Then inject it into your consumer:

```java
try (RetryableConsumer<String, MyAvroObject> retryableConsumer = new RetryableConsumer<>(
        retryableConsumerConfiguration,
        new CustomErrorProcessor()
)) {
    retryableConsumer.listen(
        Collections.singleton("MY_INPUT_TOPIC"),
        myBusinessProcessService::processRecord
    );
}
```

## Contribution & Development

We welcome contributions! See [CONTRIBUTING.md](CONTRIBUTING.md) for guidelines.

```bash
# Run all tests
mvn clean test

# Format code (required before submitting a PR)
mvn spotless:apply
```

## License

This project is licensed under the [Apache License 2.0](LICENSE).

## Credits

This library is inspired by the original work from [@jeanlouisboudart](https://github.com/jeanlouisboudart): [retriable-consumer](https://github.com/jeanlouisboudart/retriable-consumer).