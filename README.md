# KafkaDS: Distributed Message Broker

[![CI](https://github.com/smadduri9/distributed-kafka-prototype/actions/workflows/ci.yml/badge.svg)](https://github.com/smadduri9/distributed-kafka-prototype/actions/workflows/ci.yml)

A distributed message broker in Java, modeled on Apache Kafka. It implements three core pieces end to end, each with tests that exercise the failure cases:

1. An append-only, segmented commit log with sparse offset indexing and crash recovery.
2. Leader-follower replication with a high-watermark and `acks=1` / `acks=all` semantics.
3. Raft leader election and log replication over the network, with persistence and automatic failover.

## What it does

- **Binary protocol.** Netty, length-prefixed frames. Requests: `PING`, `PRODUCE`, `FETCH`, `REPLICATE`, and the Raft RPCs.
- **Commit log.** CRC-checked records in rolling segments with a sparse offset index. On restart, the log rebuilds its end offset and index from the last indexed position.
- **Replication.** The leader advances the high-watermark only when followers have the records. Consumers never read past it. `acks=all` waits for every replica; if a follower is down, the producer gets `NOT_ENOUGH_REPLICAS`.
- **Raft.** Terms, randomized election timeouts, vote persistence, log-consistency checks, conflicting-tail truncation, and commit only by current-term majority. Leadership runs over the real Netty transport.
- **Peer discovery.** Static `id@host:port`, DNS hostname (A/AAAA), or DNS SRV (for example, a Kubernetes headless service). A peer is re-resolved after a failed connection.
- **Metrics and benchmark.** Produce/fetch/replication counters and a produce benchmark.

Not in scope: consumer groups, transactions and idempotent producers, compression and compaction, ISR tuning, security (SASL, ACLs, quotas), and Connect/Streams. The partition count and replica set are fixed. See [Known simplifications](docs/DESIGN.md#known-simplifications).

## Benchmark

Single broker, 128-byte records, `acks=1` with an fsync per produce, loopback, JDK 17 on an Apple M4 Pro laptop (`java -cp target/kafkads.jar com.kafkads.bench.Benchmark`). Median of 3 runs, with the range in brackets:

| Metric | Result |
| --- | --- |
| Throughput (batch=500) | ~86,000 records/sec, 10.5 MB/sec [82,087–97,070] |
| Produce latency, single record | p50 ~4.0 ms [4.04–4.06], p99 ~5.4 ms [5.24–5.90] |

Single-record latency is dominated by the per-produce `fsync`; throughput improves with batching.

## Tests

Each piece has a test that demonstrates it, including failure cases:

| Test | What it shows |
| --- | --- |
| `MillionRecordRestartTest` | Writes 1,000,000 records, restarts, and reads them all back |
| `LogTest` | Index lookups, segment rolling, CRC corruption detection |
| `ReplicationTest` | `acks=all` reaches every follower; when a follower dies, the high-watermark stops and consumers never see uncommitted records |
| `RaftConsensusTest` | Election, replication and commit, re-election after leader failure, stale candidates rejected, conflicting tails truncated |
| `RaftFailoverNetworkTest` | Over real Netty: a leader is elected, killed, and replaced by a new leader with a higher term (election timeout 150–300 ms) |
| `RaftDnsReResolutionTest` | A peer moves to a new address; the transport re-resolves it and the cluster reconverges |

CI runs the full suite on pushes to `main` and on pull requests.

## Build and run

Java 17 and Maven.

```bash
mvn clean test      # unit + integration tests
mvn clean package   # runnable jar at target/kafkads.jar
```

Start a 3-node cluster (static leadership):

```bash
java -cp target/kafkads.jar com.kafkads.broker.Main config/broker-1.properties
java -cp target/kafkads.jar com.kafkads.broker.Main config/broker-2.properties
java -cp target/kafkads.jar com.kafkads.broker.Main config/broker-3.properties
```

For Raft-elected leadership, set `raft.enabled=true` in each properties file, start all three, then stop the leader and watch a new one take over.

## Where to look

| Package | Contents |
| --- | --- |
| `com.kafkads.storage` | `Log`, `Segment`, `OffsetIndex`, record format |
| `com.kafkads.replication` | Leader-side replication and high-watermark advancement |
| `com.kafkads.raft` | Raft node, network transport, persistence, scheduler |
| `com.kafkads.network` / `protocol` | Netty server/client, framing, wire messages |
| `com.kafkads.discovery` / `config` | Peer resolution and broker configuration |
| `com.kafkads.broker` | Node lifecycle, request handling, partitions |
| `com.kafkads.bench` | Benchmark harness |

[docs/DESIGN.md](docs/DESIGN.md) covers the architecture, wire protocol, on-disk format, replication semantics, Raft, and the concurrency model.
