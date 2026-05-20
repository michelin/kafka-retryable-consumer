# Retryable Consumer Spring Boot Starter

Spring Boot autoconfiguration for [Kafka Retryable Consumer](../README.md).

## Features
- Auto-configures `RetryableConsumer` as a Spring bean
- Supports all core features (retry, DLQ, custom error processing)
- **Kubernetes-style health check endpoints** (`/ready`, `/liveness`)
- Minimal configuration required

## Getting Started
Add to your Spring Boot app:
```xml
<dependency>
  <groupId>com.michelin.kafka</groupId>
  <artifactId>retryable-consumer-spring-boot-starter</artifactId>
  <version>${retryable-consumer-core.version}</version>
</dependency>
```

Minimal `application.yml`:
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

## Health Check Endpoints

If `spring-boot-starter-web` is present, the following endpoints are exposed:
- `GET /ready` (readiness)
- `GET /liveness` (liveness)

You can override the paths:
```yaml
kubernetes:
  readiness:
    path: custom-ready
  liveness:
    path: custom-liveness
```

Status codes:

| State    | Readiness | Liveness |
|----------|-----------|----------|
| RUNNING  | 200       | 200      |
| STARTING | 204       | 200      |
| ERROR    | 503       | 503      |
| STOPPED  | 503       | 503      |
| null     | 400       | 204      |

## Advanced Usage
- Provide your own `KafkaRetryableConfiguration` bean for full control
- Customize error handling by implementing `ErrorProcessor`
- See [main README](../README.md) for core usage and customization

## Contributing
See [CONTRIBUTING.md](../CONTRIBUTING.md).

## License
[Apache 2.0](../LICENSE)
