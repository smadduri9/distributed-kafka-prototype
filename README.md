# KafkaDS: Distributed Message Broker

[![CI](https://github.com/smadduri9/distributed-kafka-prototype/actions/workflows/ci.yml/badge.svg)](https://github.com/smadduri9/distributed-kafka-prototype/actions/workflows/ci.yml)

KafkaDS is a distributed message broker in Java, inspired by Apache Kafka. It is a
focused, learning-grade implementation that demonstrates three core distributed-systems
concepts end to end rather than reproducing all of Kafka:

1. An append-only, segmented commit log with sparse offset indexing and crash recovery.
2. Leader-follower replication with a high-watermark and `acks=1` / `acks=all` semantics.
3. Raft leader election and log replication (terms, votes, election timeouts, commit index).

## Status: implemented vs planned

Implemented (with tests):

- [x] Netty length-prefixed binary protocol (`PING`, `PRODUCE`, `FETCH`, `REPLICATE`, Raft RPCs)
- [x] Segmented append-only log: CRC-checked records, sparse offset index, segment rolling
- [x] Crash recovery: rebuilds end offset and index by scanning from the last indexed position
- [x] Produce / fetch over the wire on a single broker
- [x] Leader-follower replication with high-watermark
- [x] `acks=1` (leader) and `acks=all` (committed to all replicas) durability
- [x] Raft: leader election, log replication, commit index, conflict truncation, persistence
- [x] Raft leadership over the real network with automatic failover
- [x] Peer discovery: static `id@host:port`, DNS hostname (A/AAAA), or DNS SRV (e.g. a Kubernetes headless service), re-resolved on failure
- [x] Metrics counters and a produce benchmark

Planned / out of scope (intentionally):

- [ ] Consumer groups and rebalancing
- [ ] Exactly-once / transactions, idempotent producer
- [ ] Compression, log compaction, tiered storage
- [ ] ISR shrink/expand tuning, SASL/ACLs/quotas
- [ ] Kafka Connect / Streams / Schema Registry

## Benchmark

Single broker, 128-byte records, `acks=1` with an fsync per produce, loopback, JDK 17 on an
Apple M4 Pro laptop (`java -cp target/kafkads.jar com.kafkads.bench.Benchmark`). Median of 3 runs,
with the range in brackets:

| Metric | Result |
| --- | --- |
| Throughput (batch=500) | ~86,000 records/sec, 10.5 MB/sec [82,087–97,070] |
| Produce latency, single record | p50 ~4.0 ms [4.04–4.06], p99 ~5.4 ms [5.24–5.90] |
| Leader failover | new leader elected within the election timeout (150-300 ms), verified by test |

Single-record latency is dominated by the per-produce `fsync`; throughput improves with batching.

## Tech stack

- Java 17, Maven
- Netty 4 (binary transport)
- JUnit 5 (unit + integration tests)

## Project structure

- `com.kafkads.network` Netty server/client and length-prefixed framing
- `com.kafkads.protocol` wire messages and codec
- `com.kafkads.storage` `Log`, `Segment`, `OffsetIndex`, record format
- `com.kafkads.replication` leader-side replication and high-watermark advancement
- `com.kafkads.raft` Raft consensus (node, transport, persistence, scheduler)
- `com.kafkads.discovery` static and DNS-based peer resolution
- `com.kafkads.config` broker and peer configuration
- `com.kafkads.broker` node lifecycle, request handling, partitions
- `com.kafkads.bench` benchmark harness

See [docs/DESIGN.md](docs/DESIGN.md) for the architecture, wire protocol, on-disk format,
replication/high-watermark semantics, the Raft summary, and the concurrency model.

## Build and test

```bash
mvn clean test      # run all unit + integration tests
mvn clean package   # build the runnable jar at target/kafkads.jar
```

## Run a 3-node cluster (static leadership)

```bash
java -cp target/kafkads.jar com.kafkads.broker.Main config/broker-1.properties
java -cp target/kafkads.jar com.kafkads.broker.Main config/broker-2.properties
java -cp target/kafkads.jar com.kafkads.broker.Main config/broker-3.properties
```

To run with Raft-elected leadership and observe failover, set `raft.enabled=true` in each
broker's properties file, start all three, then kill the current leader and watch a new
leader take over (this is exercised by `RaftFailoverNetworkTest`).

## Tests as proof

Each concept is backed by a test that demonstrates it, including failure cases:

- `MillionRecordRestartTest` writes 1,000,000 records, restarts, and reads them all back.
- `LogTest` covers index lookups, segment rolling, and CRC corruption detection.
- `ReplicationTest` proves the high-watermark does not advance past un-replicated records
  when a follower dies, and that consumers never read uncommitted records.
- `RaftConsensusTest` covers election, replication/commit, re-election after leader failure,
  rejection of stale candidates, and conflicting-tail truncation.
- `RaftFailoverNetworkTest` elects and re-elects a leader over the real Netty transport.
- `RaftDnsReResolutionTest` re-resolves a peer that moves to a new address and checks the cluster reconverges.
