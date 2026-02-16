# Retryable Consumer Spring Boot Starter

This module provides a Spring Boot autoconfiguration for the Retryable Consumer core library (retryable-consumer-core).
It wires a `RetryableConsumer` and an optional Dead Letter Producer (DLQ) based on application properties.

This README explains how to add and use the starter, important configuration properties, examples, and testing notes.

## Quick summary

- Artifact: `com.michelin.kafka:retryable-consumer-spring-boot-starter`
- Provides an auto-configured `RetryableConsumer` bean and an optional `DeadLetterProducer` bean.
- The starter expects a `KafkaRetryableConfiguration` (mapped from Spring properties) or will map `KafkaRetryableSpringProperties` provided via configuration.
- Fail-fast validations are performed for required properties (consumer topics and `bootstrap.servers`).

## Minimal usage example

Add the starter as a dependency to your Spring Boot application:

```xml
<dependency>
  <groupId>com.michelin.kafka</groupId>
  <artifactId>retryable-consumer-spring-boot-starter</artifactId>
  <version><!-- project version --></version>
</dependency>
```

Then configure properties in `application.yml` or `application.properties`.

Example `application.yml` minimal:

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

This will create a `RetryableConsumer` bean with default behavior. To inject it into your code:

```java
@Autowired
private RetryableConsumer<String, String> retryableConsumer;
```

Then use `retryableConsumer.listen(...)` or `listenAsync(...)` with a `RecordProcessor`.

## Dead Letter Queue (DLQ)

The DLQ is optional and will only be created if `kafka.retryable.dead-letter.producer.topic` is configured.
If configured, the DLQ producer requires producer properties including `bootstrap.servers` and serializers.

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

## Configuration properties (summary)

- `kafka.retryable.enabled` (boolean)
  - Default: true
  - Toggle the auto-configuration on/off.

Consumer block (`kafka.retryable.consumer` or via `KafkaRetryableConfiguration`):
- `topics` (list) — REQUIRED for production use (auto-config validates presence and non-empty list).
- `properties.bootstrap.servers` — REQUIRED (fail-fast). Kafka bootstrap servers.
- `properties.key.deserializer` / `properties.value.deserializer` — deserializers; defaults to `StringDeserializer` when not provided (the starter will supply defaults to avoid Kafka client ConfigException in tests).
- `pollBackoffMs` — polling timeout in ms (maps to core config)
- `retryMax` — max retry count (0 = unlimited)
- `stopOnError` — whether to stop the consumer on non-retryable error

Dead-letter producer block (`kafka.retryable.dead-letter.producer`):
- `topic` — when present, DLQ bean will be created.
- `properties.bootstrap.servers` — REQUIRED for DLQ.
- `properties.key.serializer` / `properties.value.serializer` — serializers; defaults to `StringSerializer` if not provided.

Note: The starter performs fail-fast validations for missing topics and `bootstrap.servers` to avoid silent misconfiguration.

## Advanced usage

- If you want full control over the configuration object, define a `@Bean` of type `KafkaRetryableConfiguration` in your application context — the starter will not override it (it uses `@ConditionalOnMissingBean`).
- You can also provide `KafkaRetryableSpringProperties` (for binding) instead of `KafkaRetryableConfiguration` and the mapper will convert it.

Example customizing error handling:

```java
@Bean
public RetryableConsumer<String, MyValue> customRetryableConsumer(KafkaRetryableConfiguration cfg, DeadLetterProducer dlq) {
  // build a RetryableConsumer with custom ErrorProcessor or inject the DLQ
  return new RetryableConsumer<>(cfg, dlq);
}
```

## Testing notes

- Unit tests in this module avoid creating real Kafka clients during Spring context refresh. The starter defers Kafka client construction (lazy) and provides defaults for deserializers/serializers in tests to avoid Kafka client ConfigExceptions.
- For true integration tests against Kafka use `spring-kafka-test`'s `EmbeddedKafkaBroker` (create a separate integration test profile).
- If tests are flaky due to async background processing, prefer synchronization helpers such as `CountDownLatch` or inject a test Executor.

## Troubleshooting

- Error: "Missing required property: kafka.retryable.consumer.topics must be configured and non-empty"
  - Ensure `kafka.retryable.consumer.topics` is set in application properties or provide a `KafkaRetryableConfiguration` bean.

- Error: "Missing required property: kafka.retryable.consumer.properties.bootstrap.servers must be configured"
  - Add `bootstrap.servers` under `kafka.retryable.consumer.properties`.

- If tests fail with Kafka client DNS/connection errors in CI, ensure tests either mock Kafka clients or run integration tests against Embedded Kafka. Unit tests should remain network-free.

## Contribution & Development

- Run unit tests for the starter module:

```bash
mvn -am -pl retryable-consumer-spring-boot-starter test
```

- To run core module tests:

```bash
mvn -pl retryable-consumer-core -Dtest=com.michelin.kafka.test.unit.RetryableConsumerTest test
```

Please follow the contributor guidelines in the top-level `CONTRIBUTING.md`.

## License

See the top-level LICENSE file.
