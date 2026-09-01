# Examples

Runnable examples of the Kafka Retryable Consumer, one capability per class.

They are part of the Maven reactor, so `mvn verify` at the root runs them against an embedded Kafka broker.
Their real purpose is twofold: document the library with code that is guaranteed to compile and work, and validate a
release end to end before publishing it.

| Module               | What it shows                                     |
|----------------------|---------------------------------------------------|
| `example-core`       | The library used from plain Java                  |
| `example-spring-boot`| The library used through the Spring Boot starter  |

## Running an example by hand

Every example has a `main` method. They expect a Kafka broker on `localhost:9092` and, for the examples publishing to a
dead letter topic, a schema registry on `localhost:8081` (the dead letter payload is an Avro record).

```shell
mvn -pl examples/example-core -am install -DskipTests
mvn -pl examples/example-core exec:java -Dexec.mainClass=com.michelin.kafka.example.core.SimpleConsumerExample
```

The topics are not created automatically: create them first, or enable topic auto creation on the broker.

Every example owns its configuration file, so that reading one class tells the whole story without hunting for a
property set somewhere else. Nothing is configured in Java: the file is the single source of truth, and the integration
tests load that very file, overriding only the broker address and the topic names.

## Running them against an embedded broker

No broker and no Docker needed, the integration tests start one in memory:

```shell
mvn -pl examples/example-core,examples/example-spring-boot test
```

## How the examples are tested

Examples are documentation, so a broken example is a documentation bug. Each one is therefore covered twice:

| Test | What it proves |
|---|---|
| One integration test per example | The example behaves as its Javadoc claims, against a real broker |
| `ExampleConfigurationTest` (`example-core`) | Every shipped configuration file is found, parses, and really declares the capability its example demonstrates |
| `ExampleRunnerTest` (`example-core`) | The shared entry point reports failures properly and always releases the consumer |

Two rules keep those tests honest:

- The integration tests load the **example's own configuration file** and override only what cannot be known in
  advance: the broker address, the schema registry and the topic names. Nothing about the demonstrated behaviour is set
  from the test, so a mistake in a configuration file fails the build instead of being papered over.
- The Spring examples are booted through their own `run` method, the one `main` delegates to, so the test cannot drift
  from the entry point a user actually runs.

The `main` methods of `example-core` hold no logic of their own: they delegate to `ExampleRunner.run`, which builds the
example, waits for it, and turns the outcome into an exit code. That is what makes the error handling testable without
a broker, and it is why it is written once instead of nine times. A real application would simply inline it in its own
`main`.

## example-core

Each class loads its own file through `KafkaRetryableConfiguration.load("<file>")`.

| Class                             | Configuration file                     | Capability                                                          |
|-----------------------------------|----------------------------------------|---------------------------------------------------------------------|
| `SimpleConsumerExample`           | `simple-consumer-example.yml`          | Consume and process records one by one                              |
| `BatchConsumerExample`            | `batch-consumer-example.yml`           | Process the whole result of a poll in a single call                 |
| `InfiniteRetryExample`            | `infinite-retry-example.yml`           | Retry forever until the processing succeeds (`retry.max: 0`)        |
| `LimitedRetryToDeadLetterExample` | `limited-retry-example.yml`            | Retry a bounded number of times, then route to the dead letter topic|
| `NonRetryableExceptionExample`    | `non-retryable-exception-example.yml`  | Declare exceptions that must skip the retries entirely              |
| `StopOnErrorExample`              | `stop-on-error-example.yml`            | Stop the consumer instead of skipping an unrecoverable record       |
| `CustomErrorProcessorExample`     | `custom-error-processor-example.yml`   | Replace the dead letter topic by your own error handling            |
| `DeserializationErrorExample`     | `deserialization-error-example.yml`    | Survive a poison pill the consumer cannot deserialize               |
| `DefaultConfigurationFileExample` | `application.yml`                      | Let the library find its configuration on its own                   |

## example-spring-boot

Each class selects its own file with `spring.config.name`, so booting one example never reads the configuration of
another.

| Class                               | Configuration file           | Capability                                                     |
|-------------------------------------|------------------------------|----------------------------------------------------------------|
| `SimpleConsumerRunner`              | `simple-consumer.yml`        | Inject the auto-configured consumer and start listening        |
| `DeadLetterConfiguration`           | `dead-letter.yml`            | Route unrecoverable records to a dead letter topic             |
| `CustomErrorProcessorConfiguration` | `custom-error-processor.yml` | Plug your own `ErrorProcessor` into the starter                |
| `StopOnErrorRunner`                 | `stop-on-error.yml`          | Stop the consumer on an unrecoverable record, by configuration |

There is deliberately no `application.yml` here: it would be merged into every example and blur which property belongs
to which capability.

The retry policy itself needs no Java code with the starter: it is entirely driven by the `kafka.retryable.*`
properties of the configuration file.

> The starter binds flat, kebab-case keys (`retry-max`, `retry-backoff-ms`), where the core configuration file uses a
> nested form (`retry.max`, `retry.backoff.ms`). Mixing them up binds nothing and silently falls back to the defaults.

Each example is a `@Configuration` with `@EnableAutoConfiguration` and no component scan, so booting one never drags the
others in.
