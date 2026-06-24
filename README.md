# timewheel-service

A distributed delay-message service built on Redis, Kafka, and a timing wheel.

`timewheel-service` accepts delayed messages over HTTP or Kafka, stores them in
Redis-backed timing-wheel slots, and publishes the original JSON payload to a
dynamic Kafka target topic when the delay expires.

## Why

Kafka does not provide native per-message delayed delivery. Common workarounds
usually involve polling databases, retry topics, or scheduler services with poor
ownership semantics.

This project provides a small service-first implementation:

- HTTP and Kafka input for delayed messages.
- Redis-backed timing-wheel placement.
- Distributed tick ownership with stale-node takeover.
- Dynamic Kafka output topics.
- Dead-letter handling for validation, scheduling, and publishing failures.
- Spring Boot Actuator health for tick progress and producer readiness.

## Features

- **Service-first API**: submit one message or a batch through HTTP.
- **Kafka input**: consume delayed-message requests from a configured topic.
- **Redis timing wheel**: store entries by `cycle:tick` slot with entry-slot
  metadata.
- **Distributed worker**: only the current owner advances the wheel; another
  node can take over when the owner is stale.
- **Immediate publishing**: `delayMillis <= 0` publishes directly.
- **Bounded range validation**: delays outside the configured wheel range are
  rejected or sent to DLQ.
- **Dynamic output topic**: each message carries its own `targetTopic`.
- **Payload-first output**: expired output publishes only the original JSON
  business payload.
- **DLQ support**: failed validation, scheduling, and publishing paths are
  wrapped with metadata.
- **Publish retry**: expired-output failures are retried before DLQ.
- **Health checks**: reports stale tick progress, Redis read failures, and
  Kafka producer metric failures.

## Architecture

```text
HTTP / Kafka input
       |
       v
DelayScheduler
       |
       v
Redis slots + entry-slot metadata
       |
       v
RedisTimewheelWorker
       |
       v
ExpiredMessagePublisher
       |
       v
Kafka target topic
```

Modules:

| Module | Responsibility |
| --- | --- |
| `timewheel-engine` | Core message contracts, scheduling interfaces, and wheel settings. |
| `timewheel-redis` | Redisson-backed scheduling state, slot placement, tick ownership, and expiration draining. |
| `timewheel-kafka` | Kafka output publisher, Kafka input listener, DLQ publisher, and publish retry wrapper. |
| `timewheel-server` | Spring Boot application, HTTP API, configuration, worker lifecycle, and health indicator. |

## Requirements

- Java 17+
- Maven 3.9+
- Redis 7+
- Kafka 3+
- Docker Compose, optional for local dependencies

## Quick Start

Start Redis and Kafka:

```bash
docker compose up -d
```

Run tests:

```bash
./mvnw test
```

Run the service:

```bash
./mvnw -pl timewheel-server -am spring-boot:run
```

The service starts on `http://localhost:8080` by default.

Health endpoint:

```bash
curl http://localhost:8080/actuator/health
```

## Submit Messages

### Single Message

```bash
curl -X POST http://localhost:8080/api/delay-messages \
  -H "Content-Type: application/json" \
  -d '{
    "id": "trace-10001",
    "targetTopic": "orders.timeout",
    "key": "order-10001",
    "payload": {
      "orderId": "10001",
      "reason": "payment-timeout"
    },
    "headers": {
      "source": "demo"
    },
    "delayMillis": 60000
  }'
```

Response:

```json
{
  "result": "SCHEDULED"
}
```

### Batch Submit

```bash
curl -X POST http://localhost:8080/api/delay-messages:batch \
  -H "Content-Type: application/json" \
  -d '{
    "messages": [
      {
        "id": "trace-10001",
        "targetTopic": "orders.timeout",
        "key": "order-10001",
        "payload": {
          "orderId": "10001"
        },
        "delayMillis": 60000
      },
      {
        "id": "trace-10002",
        "targetTopic": "orders.timeout",
        "key": "order-10002",
        "payload": {
          "orderId": "10002"
        },
        "delayMillis": 120000
      }
    ]
  }'
```

## Message Contract

```json
{
  "id": "trace-id",
  "targetTopic": "orders.timeout",
  "key": "order-10001",
  "payload": {
    "orderId": "10001"
  },
  "headers": {
    "tenant": "north"
  },
  "delayMillis": 60000
}
```

Fields:

| Field | Required | Description |
| --- | --- | --- |
| `id` | No | Trace-only identifier. It is not used for deduplication. |
| `targetTopic` | Yes | Kafka topic used when the delay expires. |
| `key` | No | Kafka record key for expired output. Blank values publish as `null`. |
| `payload` | Yes | JSON object published to the target topic after expiration. |
| `headers` | No | String-to-string Kafka headers. |
| `delayMillis` | Yes | Delay duration in milliseconds. `<= 0` publishes immediately. |

Expired output publishes only `payload`, not the full wrapper message.

## Kafka Input

The service can also consume delayed-message requests from Kafka.

Default input topic:

```text
timewheel.delay.input
```

Produce a request:

```bash
kafka-console-producer.sh \
  --bootstrap-server localhost:9092 \
  --topic timewheel.delay.input
```

Message:

```json
{"id":"trace-1","targetTopic":"orders.timeout","key":"order-1","payload":{"orderId":"1"},"headers":{"source":"kafka"},"delayMillis":60000}
```

Scheduling failures from Kafka input are sent to the DLQ.

## Dead Letter Queue

Default DLQ topic:

```text
timewheel.delay.dlq
```

DLQ messages include:

| Field | Description |
| --- | --- |
| `errorCode` | Stable error code, for example `DELAY_OUT_OF_RANGE` or `PUBLISH_FAILED`. |
| `errorMessage` | Human-readable failure message. |
| `failedStage` | `VALIDATION`, `SCHEDULING`, or `PUBLISHING`. |
| `failedAt` | Failure timestamp. |
| `source` | Failure source, such as `KAFKA_INPUT` or `EXPIRATION_OUTPUT`. |
| `originalMessage` | Original delayed-message wrapper when available. |

## Configuration

Default configuration lives in
`timewheel-server/src/main/resources/application.yml`.

```yaml
timewheel:
  tick-duration: 10s
  ticks-per-wheel: 100
  max-cycle: 2
  node-id:
  redis:
    key-prefix: timewheel-service
  kafka:
    input-topic: timewheel.delay.input
    consumer-group: timewheel-service
    dead-letter-topic: timewheel.delay.dlq
    publish-max-attempts: 3
  health:
    max-tick-idle: 60s
```

Important settings:

| Property | Default | Description |
| --- | --- | --- |
| `timewheel.tick-duration` | `10s` | Duration of one timing-wheel tick. |
| `timewheel.ticks-per-wheel` | `100` | Number of ticks in one wheel cycle. |
| `timewheel.max-cycle` | `2` | Maximum cycle range. |
| `timewheel.node-id` | local host address | Optional stable node id used for tick ownership. |
| `timewheel.redis.key-prefix` | `timewheel-service` | Prefix for Redis keys. |
| `timewheel.kafka.input-topic` | `timewheel.delay.input` | Kafka delayed-message input topic. |
| `timewheel.kafka.consumer-group` | `timewheel-service` | Kafka input consumer group. |
| `timewheel.kafka.dead-letter-topic` | `timewheel.delay.dlq` | DLQ topic. |
| `timewheel.kafka.publish-max-attempts` | `3` | Attempts for expired-output publishing. |
| `timewheel.health.max-tick-idle` | `60s` | Maximum allowed tick idle time before health is `DOWN`. |

Maximum schedulable delay is:

```text
tick-duration * ticks-per-wheel * max-cycle
```

With the defaults, the maximum delay is `2000s`.

## Runtime Semantics

- Redis and Kafka are required.
- `delayMillis` is the only scheduling field.
- `delayMillis <= 0` publishes immediately.
- Over-range delays are rejected by HTTP and sent to DLQ from Kafka input.
- The worker removes expired entries from Redis before publishing.
- Entry-slot metadata is removed only if it still points at the expired slot.
- Expired output publishes only the business payload JSON.
- `id` is trace-only and is not a deduplication key.
- Cancellation is not supported in the first version.

## Development

Use the Maven wrapper:

```bash
./mvnw test
```

Run a single module and its dependencies:

```bash
./mvnw -pl timewheel-server -am test
```

## Project Status

Implemented:

- Engine contracts and validation.
- HTTP submit and batch submit.
- Kafka input listener.
- Kafka expired-output publisher.
- Kafka DLQ publisher.
- Redis slot placement.
- Distributed tick ownership and stale-owner takeover.
- Expiration slot draining and indicator cleanup.
- Background worker lifecycle.
- Publish retry and `PUBLISHING` DLQ on retry exhaustion.
- Actuator health indicator.

Planned:

- Broker-backed Kafka integration tests.
- Malformed Kafka JSON error-handler coverage.
- More operational examples for production deployment.

## Contributing

Issues and pull requests are welcome.

Before opening a pull request:

1. Run `mvn test`.
2. Add or update tests for behavior changes.
3. Keep module boundaries intact: engine code should not depend on Redis,
   Kafka, or Spring.
4. Update documentation when behavior or configuration changes.

## License

Licensed under the Apache License, Version 2.0. See [LICENSE](LICENSE).
