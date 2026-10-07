# Distributed Kafka Prototype

A Java implementation of core ideas behind a Kafka-style distributed log: brokers, topics, partitions, append-only message storage, custom producer/consumer protocol handling, controller metadata, broker heartbeats, replication flow, and Raft-inspired leader election.

## Problem Statement

Distributed messaging systems need to accept writes, store ordered records durably, let consumers read from offsets, and keep cluster metadata healthy while brokers join, fail, or replicate data. This project explores those mechanics from first principles instead of using Kafka as a dependency.

## What I Built

This repository is Sriram Madduri's implementation of a Kafka-like prototype with:

- Topic and partition management inside a broker.
- Append-only per-partition message storage with offset-based fetches.
- Netty-based TCP server path and custom binary request/response builders.
- Producer and consumer clients for produce/fetch workflows.
- Controller metadata for broker registration, partition assignment, and broker liveness.
- Heartbeat-based failure detection and partition reassignment hooks.
- Replication manager and follower synchronization flow for leader/follower replicas.
- Raft-inspired node state, term tracking, request-vote handling, and election timeout demo.
- REST API and small static web UI for local broker interaction.

The implementation is intentionally educational and prototype-oriented. The Raft and replication demos simulate parts of the distributed network path, while broker storage, local produce/fetch, controller metadata, and heartbeat behavior are executable.

## Tech Stack

- Java 11
- Gradle 8.5 wrapper
- Netty for broker networking
- Java HTTP server for the REST API and static UI
- JUnit 5 and Mockito for tests
- SLF4J and Logback for structured logs
- Snappy and Gzip compression modules
- Redis for broker liveness keys with a TTL, and a cache of partition assignments

## Architecture

```text
src/main/java/com/kafkads
├── api/           REST API server and static web UI serving
├── broker/        Broker, topic, partition, heartbeat, and log storage
├── compression/   Compression codec abstraction and implementations
├── config/        Broker configuration loading and environment overrides
├── consensus/     Raft-style node state, election, and log replication concepts
├── consumer/      Consumer client and offset management
├── controller/    Cluster metadata, partition assignment, broker tracking
├── producer/      Producer client and producer configuration
├── protocol/      Binary request parsing and response construction
├── replication/   Leader/follower replication protocol and synchronization
├── security/      SSL and ACL support scaffolding
├── transaction/   Transaction coordinator and manager scaffolding
└── util/          Error handling and concurrency helpers
```

At a high level:

```text
Producer/Consumer
      |
      v
Netty Broker TCP API ---- append/fetch ---- LogSegment storage
      |
      v
Controller metadata ---- heartbeats ---- Redis TTL liveness + assignment cache
      |
      v
Replication manager ---- follower sync ---- high-water mark tracking
      |
      v
Raft-inspired consensus state and leader election demo
```

## Quick Local Run

Prerequisites:

- Java 11 or newer
- No system Gradle installation required; the Gradle wrapper is included

Build and test:

```bash
./gradlew test --no-daemon
```

Run the broker, REST API, and web UI:

```bash
./gradlew run --no-daemon
```

Then open:

```text
http://localhost:8080
```

Optional configuration is loaded from `src/main/resources/application.properties` and can be overridden with environment variables:

```bash
BROKER_PORT=9092 BROKER_DATA_DIR=./data/broker ./gradlew run --no-daemon
```

Broker heartbeats refresh a Redis key at `localhost:6379` (`redis-server`) with a 10-second TTL, and partition assignments are cached beside that. Partition logs and consumer offsets stay on disk. Set `REDIS_ENABLED=false` to keep liveness in memory only.

## Full Experiment Workflow

Run the end-to-end feature demo:

```bash
./gradlew runDemo --no-daemon
```

The demo exercises:

- Raft-inspired leader election state transitions and term increments across three nodes.
- Broker heartbeat registration, periodic liveness tracking, timeout detection, and dead-broker marking.
- Controller-driven topic creation with two partitions and replication factor three.
- Partition leader/replica assignment across three brokers.
- Simulated synchronous replication and follower acknowledgment flow.

The demo writes logs to `logs/`:

```text
logs/demo-node-*.log
logs/demo-broker-*.log
logs/demo-controller.log
```

Useful follow-up commands:

```bash
./gradlew clean test --no-daemon
./gradlew runDemo --no-daemon
```

## Results

Latest local verification:

- `./gradlew test --no-daemon`: passed.
- `./gradlew runDemo --no-daemon`: passed.
- Heartbeat demo: broker status changed to `DEAD` after heartbeat shutdown and timeout detection.
- Replication demo: created topic `replicated-topic` with 2 partitions and replication factor 3.
- Partition assignment demo:
  - Partition 0 leader: broker 1; replicas: `[1, 2, 3]`
  - Partition 1 leader: broker 2; replicas: `[2, 3, 1]`
- Replication demo: simulated message replicated successfully and high-water mark reached offset `0`.

Reproducible demo path:

```bash
./gradlew clean test runDemo --no-daemon
```

The leader election demo currently shows election timeouts, candidate transitions, voting state, and term increments. It does not implement full inter-node RequestVote RPC networking yet, so the demo is best interpreted as a consensus-state prototype rather than a production Raft implementation.

## Reference Material

Additional implementation notes are kept as reference documentation:

- `DEMO.md`
- `FRONTEND.md`
- `HEARTBEAT.md`
- `LOGS.md`
- `README-LOGS.md`
- `LOG-FILES-SUMMARY.md`

These files document specific subsystems and demo usage; the README is the primary ownership and project overview.

## License

MIT

