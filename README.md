# Distributed Kafka Prototype

Built a distributed message broker from scratch in Java with partitioned offset-based logs, producer/consumer APIs over a custom Netty binary protocol, replication, failure detection, partition assignment, and Raft-inspired leader election.

A short visual brief is at [smadduri9.github.io/distributed-kafka-prototype](https://smadduri9.github.io/distributed-kafka-prototype/).

## What is real

- **Partitioned log.** Each topic partition is an append-only file. Produce returns an offset. Fetch reads from an offset. Records are written through a file channel, so they are still there after a restart.
- **Binary protocol.** Producers and consumers use a small Netty framing: request type, version, body length, body. The broker listens on port 9092.
- **Failure detection.** A heartbeat refreshes a Redis key with a 10-second TTL. When that key expires, the controller marks the broker dead. With Redis off, the same timeout is tracked in memory.
- **Partition assignment.** The controller chooses a leader and replicas for each partition and writes that map to Redis. The controller's in-memory map is the source of truth.
- **Local UI.** `./gradlew run` starts the broker and an HTTP UI on port 8080 for creating topics and producing or fetching records.

Consumer offsets stay in a local file. Redis holds the shared, expiring cluster view: liveness keys and the current assignment map.

## What is a prototype

The election demo advances Raft-style node state, terms, and votes inside one process. The replication demo records follower acknowledgements and a high-water mark inside one process. SSL, ACLs, and transactions are scaffolding.

This is a from-scratch study of the mechanics.

## How to run it

Java 11 or newer. The Gradle wrapper is in the repo.

```bash
./gradlew test --no-daemon
./gradlew run --no-daemon
```

Open [http://localhost:8080](http://localhost:8080).

Optional Redis on `localhost:6379` (`redis-server`) stores liveness keys and the assignment cache. `REDIS_ENABLED=false` keeps that state in memory.

```bash
./gradlew runDemo --no-daemon
```

runs the election, heartbeat, assignment, and replication demonstrations. Logs land in `logs/`.

## What to inspect

- [Log segment](src/main/java/com/kafkads/broker/storage/LogSegment.java) — append and fetch by offset
- [Request parser](src/main/java/com/kafkads/protocol/RequestParser.java) — binary framing
- [Redis cluster cache](src/main/java/com/kafkads/controller/RedisClusterCache.java) — TTL heartbeats and the assignment hash
- [Controller](src/main/java/com/kafkads/controller/Controller.java) — registration, assignment, and dead-broker detection

```text
Producer / Consumer
        |
        v
Netty broker (port 9092) ---- append / fetch ---- partition log on disk
        |
        v
Controller ---- heartbeat ---- Redis TTL key + assignment cache
        |
        +---- replication demo (in process)
        +---- Raft-inspired election demo (in process)
```

## Design notes

Stack: Java 11, Gradle 8.5 wrapper, Netty, a small Java HTTP server for the UI, JUnit 5, SLF4J and Logback, Snappy and Gzip, Redis (Jedis) for liveness and assignment cache.

```text
src/main/java/com/kafkads
├── api/           REST API and static UI
├── broker/        Broker, topics, partitions, heartbeats, log storage
├── compression/   Codec interface, Gzip, Snappy
├── config/        Properties and environment overrides
├── consensus/     Raft-style state, election, log replication demo
├── consumer/      Consumer client and file-backed offsets
├── controller/    Metadata, assignment, Redis cluster cache
├── producer/      Producer client
├── protocol/      Binary request parsing and responses
├── replication/   Leader/follower sync and high-water mark
├── security/      SSL and ACL scaffolding
├── transaction/   Transaction scaffolding
└── util/          Errors and concurrency helpers
```

The feature demo covers:

- Election state transitions and term increments across three nodes.
- Heartbeat registration, timeout detection, and dead-broker marking.
- Topic creation with two partitions and replication factor three.
- Leader and replica assignment across three brokers.
- A simulated replicate-and-acknowledge path.

A previous local run of `./gradlew test` and `./gradlew runDemo` completed, marked a broker `DEAD` after the heartbeat stopped, assigned partition 0 to broker 1 with replicas `[1, 2, 3]` and partition 1 to broker 2 with replicas `[2, 3, 1]`, and advanced the demo high-water mark to offset `0`.

Subsystem notes:

- [DEMO.md](DEMO.md)
- [FRONTEND.md](FRONTEND.md)
- [HEARTBEAT.md](HEARTBEAT.md)
- [LOGS.md](LOGS.md)
- [README-LOGS.md](README-LOGS.md)
- [LOG-FILES-SUMMARY.md](LOG-FILES-SUMMARY.md)

## License

MIT
