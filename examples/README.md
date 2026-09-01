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

## Running them against an embedded broker

No broker and no Docker needed, the integration tests start one in memory:

```shell
mvn -pl examples/example-core,examples/example-spring-boot test
```

## example-core

| Class                              | Capability                                                             |
|------------------------------------|------------------------------------------------------------------------|
| `SimpleConsumerExample`            | Consume and process records one by one                                 |
| `BatchConsumerExample`             | Process the whole result of a poll in a single call                    |
| `InfiniteRetryExample`             | Retry forever until the processing finally succeeds (`retry-max = 0`)  |
| `LimitedRetryToDeadLetterExample`  | Retry a bounded number of times, then route to the dead letter topic   |
| `NonRetryableExceptionExample`     | Declare exceptions that must skip the retries entirely                 |
| `StopOnErrorExample`               | Stop the consumer instead of skipping an unrecoverable record          |
| `CustomErrorProcessorExample`      | Replace the dead letter topic by your own error handling               |
| `DeserializationErrorExample`      | Survive a poison pill the consumer cannot deserialize                  |
| `YamlConfigurationExample`         | Configure everything from `application.yml` instead of Java            |
| `ExampleConfiguration`             | Plumbing: the configuration used by the `main` methods                 |

## example-spring-boot

| Class                                | Capability                                                    |
|--------------------------------------|---------------------------------------------------------------|
| `SimpleConsumerRunner`               | Inject the auto-configured consumer and start listening        |
| `DeadLetterConfiguration`            | Route unrecoverable records to a dead letter topic             |
| `CustomErrorProcessorConfiguration`  | Plug your own `ErrorProcessor` into the starter                |
| `StopOnErrorRunner`                  | Stop the consumer on an unrecoverable record, by configuration |

The retry policy itself needs no Java code with the starter: it is entirely driven by the `kafka.retryable.*`
properties of `application.yml`.

> The starter binds flat, kebab-case keys (`retry-max`, `retry-backoff-ms`), where the core configuration file uses a
> nested form (`retry.max`, `retry.backoff.ms`). Mixing them up binds nothing and silently falls back to the defaults.

Each example is a `@Configuration` with `@EnableAutoConfiguration` and no component scan, so booting one never drags the
others in.
