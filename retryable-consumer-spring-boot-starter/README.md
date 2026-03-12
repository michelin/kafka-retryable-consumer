# Retryable Consumer Spring Boot Starter

This module provides a Spring Boot autoconfiguration for the Retryable Consumer core library (retryable-consumer-core).

See top project README for an overview of the library and its features: [Retryable Consumer README](../README.md).



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
