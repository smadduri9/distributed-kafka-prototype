# Distributed message broker

Built a distributed message broker from scratch in Java with partitioned offset-based logs, producer/consumer APIs over a custom Netty binary protocol, replication, failure detection, partition assignment, and Raft-inspired leader election.

Project brief: [smadduri9.github.io/distributed-kafka-prototype](https://smadduri9.github.io/distributed-kafka-prototype/).

## What it does

- **Partitioned log.** Each topic partition is an append-only file. Produce returns an offset. Fetch reads from an offset. Records are written through a file channel.
- **Binary protocol.** Producers and consumers use Netty framing: request type, version, body length, body. The broker listens on port 9092.
- **Failure detection.** A heartbeat refreshes a Redis key with a 10-second TTL. When that key expires, the controller marks the broker dead.
- **Partition assignment.** The controller chooses a leader and replicas for each partition and writes that map to Redis.
- **Replication.** Leaders replicate records to followers and track a high-water mark.
- **Leader election.** Raft-inspired node state, terms, votes, and election timeouts.
- **Local UI.** `./gradlew run` starts the broker and an HTTP UI on port 8080.

```text
Producer / Consumer
        |
        v
Netty broker (port 9092) ---- append / fetch ---- partition log on disk
        |
        v
Controller ---- heartbeat ---- Redis TTL key + assignment cache
        |
        v
Replication and Raft-inspired leader election
```

## How to run it

Java 11 or newer. The Gradle wrapper is in the repo.

```bash
./gradlew test --no-daemon
./gradlew run --no-daemon
```

Open [http://localhost:8080](http://localhost:8080).

Redis on `localhost:6379` stores broker liveness and the assignment cache.

```bash
./gradlew runDemo --no-daemon
```

## Where to look

- [Log segment](src/main/java/com/kafkads/broker/storage/LogSegment.java) — append and fetch by offset
- [Request parser](src/main/java/com/kafkads/protocol/RequestParser.java) — binary framing
- [Redis cluster cache](src/main/java/com/kafkads/controller/RedisClusterCache.java) — TTL heartbeats and the assignment hash
- [Controller](src/main/java/com/kafkads/controller/Controller.java) — registration, assignment, and failure detection
- [Replication](src/main/java/com/kafkads/replication/ReplicationManager.java) — follower sync and the high-water mark
- [Leader election](src/main/java/com/kafkads/consensus/LeaderElection.java) — terms, votes, and leader transition

## License

MIT
