# KafkaDS: Distributed Message Broker

[![CI](https://github.com/smadduri9/distributed-kafka-prototype/actions/workflows/ci.yml/badge.svg)](https://github.com/smadduri9/distributed-kafka-prototype/actions/workflows/ci.yml)

KafkaDS is a Kafka-style message broker prototype written in Java 17 on Netty. It is a learning and portfolio project. It is not Kafka-compatible, and it is not production software.

## What it does

- **Partitioned logs and assignment.** A cluster has a fixed set of brokers and a fixed number of partitions (`partition.count`). Each partition is assigned a replica list of `replication.factor` brokers (default 3, capped at the broker count). The first broker in the list is the preferred leader. Every broker computes the assignment locally from its own config, so all brokers must share the same settings.
- **Replication and election.** Each partition is one Raft group. The partition's segmented log is the Raft log. The replica list does not change on failover; only the leader within it does.
- **Acknowledgment modes.** Producers choose one of three modes, described in the table below.
- **Producer and consumer clients.** `Producer` finds the partition leader from metadata and follows `NOT_LEADER` hints. `Consumer` fetches by partition and offset. Consumer positions are kept in memory.
- **Binary protocol.** Length-prefixed frames over Netty. Frames are limited to 16 MB. Every length and count is checked against the bytes left in the frame before anything is allocated. The server accepts at most 1024 unanswered requests per connection and 10,000 queued requests per partition; beyond that it answers `BUSY`. Fetch responses are capped at 4 MB.
- **Storage and recovery.** Records are CRC-checked and stored in segments that roll at `segment.bytes` (default 16 MB). The offset index is in memory and rebuilt on open. Open validates every record, so start-up time grows with log size. An incomplete record at the end of the last segment is truncated. Other damage makes the open fail rather than dropping data.
- **Peer discovery.** Peers can be listed statically as `id@host:port`, or resolved through DNS A/AAAA records or DNS SRV records (for example, a Kubernetes headless service).

### Acknowledgment modes

| Mode | What OK means | What OK does not mean |
| --- | --- | --- |
| `leader` | The leader has appended and fsynced the records locally. | The records are not committed. Consumers cannot see them until they are committed, and they are discarded if the leader is replaced before a majority has them. |
| `quorum` (producer default) | The records are committed: durable on a majority of the partition's replicas in the leader's term. Consumers can see them. | That every replica has the records. |
| `all` | The records are committed and durable on every configured replica. | A rollback on failure. If a replica is down, the producer gets `NOT_ENOUGH_REPLICAS` after `produce.timeout.ms`, but the records may still have been committed by the majority and become visible. |

A batch that is acknowledged or rejected was appended whole or not at all, with consecutive offsets. If a leader dies while writing a batch, a prefix of it can survive and later commit; the producer got no answer, so that batch is an uncertain write.

## Guarantees and limits

### Guaranteed and tested

- Records acknowledged with `quorum` are still readable after the leader of their partition is killed with SIGKILL, and a new leader takes over (`MultiProcessFailoverTest`).
- Writes continue to commit with one broker down, and a restarted broker converges to the leader's log with no record stored twice (`MultiProcessFailoverTest`, `ClusterIntegrationTest`).
- A minority of brokers cannot commit writes (`ClusterIntegrationTest`, `RaftConsensusTest`).
- A restart does not expose records that were never committed (`ReportedDefectsTest`, `RaftConsensusTest`).
- Malformed or oversized frames get an `INVALID_REQUEST` error and the connection is closed (`ProtocolRobustnessTest`).
- With concurrent producers and consumers, every acknowledged record is stored once and each response reaches the request that caused it (`ClusterIntegrationTest`, `ClientCorrelationTest`).
- A log with 1,000,000 records can be closed, reopened and read back in full (`MillionRecordRestartTest`).

Delivery is never exactly-once. The producer retries only when the broker proves that nothing was appended. An ambiguous outcome throws `UncertainWriteException` and is not retried automatically. The result is at-least-once if the caller resends after an uncertain write, and at-most-once if it does not. There are no producer IDs or sequence numbers.

### Not tested or not implemented

- **Power loss or kernel crash.** Untested. SIGKILL does not drop the page cache, so the correctness of fsync is not proven.
- **I/O errors during append.** The code stops the affected partition replica (fail-stop), but no test exercises this path.
- **Platforms.** The new code has not yet been run on Linux or on JDK 17, which is what CI targets.
- **Scale and duration.** Untested beyond 4 brokers, and no long runs or very large logs have been tested.
- **Network partitions between live processes.** Not tested. The minority tests stop brokers instead.
- **Bit rot in the final record of the active segment.** It cannot be told apart from a torn write, so it is truncated.
- **Raft features.** No membership change, no snapshots or log compaction (the log grows without limit and start-up scans all of it), no pre-vote (a rejoining broker with a higher term can force an election), no leadership transfer, and no linearizable follower reads (fetch is leader-only). Partition assignment is fixed at start-up, and leadership does not move back to the preferred leader automatically.
- **Client and operations features.** No consumer groups, no stored consumer offsets, no idempotent producer, no transactions, no TLS, no authentication, no idle-connection timeouts, and no flow control based on channel writability. Metrics are four in-process counters with no exporter.

## Build and test

Requires Java 17 and Maven.

```bash
mvn clean test      # runs the 84-test suite
mvn clean package   # builds the runnable jar at target/kafkads.jar
```

The suite includes `MultiProcessFailoverTest`, which starts three broker processes and kills one with SIGKILL.

## Run a cluster

The three files in `config/` describe a 3-broker cluster on 127.0.0.1 with ports 9091 to 9093 and 3 partitions. Each broker keeps its data under `data/broker-N` relative to the working directory. Start each broker in its own terminal:

```bash
java -jar target/kafkads.jar config/broker-1.properties
```

```bash
java -jar target/kafkads.jar config/broker-2.properties
```

```bash
java -jar target/kafkads.jar config/broker-3.properties
```

Once the brokers are running, the CLI can inspect and use the cluster. The bootstrap list is `127.0.0.1:9091,127.0.0.1:9092,127.0.0.1:9093`.

Show the cluster metadata: brokers, and for each partition its replicas, preferred leader, current leader, term and offsets.

```bash
java -cp target/kafkads.jar com.kafkads.cli.Cli metadata 127.0.0.1:9091,127.0.0.1:9092,127.0.0.1:9093
```

Produce records to partition 0 with `quorum` acknowledgment. The accepted modes are `leader`, `quorum` and `all`.

```bash
java -cp target/kafkads.jar com.kafkads.cli.Cli produce 127.0.0.1:9091,127.0.0.1:9092,127.0.0.1:9093 0 quorum hello world
```

Read partition 0 from offset 0. Each record is printed as its offset, a tab, and its value.

```bash
java -cp target/kafkads.jar com.kafkads.cli.Cli consume 127.0.0.1:9091,127.0.0.1:9092,127.0.0.1:9093 0 0
```

`status <host:port>` prints one broker's own view of the cluster.

## Failover demonstration

`scripts/failover-demo.sh` runs a complete failover check with three broker processes on ports 9091 to 9093. It builds `target/kafkads.jar` with Maven if the jar is missing. Run it from the repository root with no other cluster on those ports.

```bash
scripts/failover-demo.sh
```

The script:

1. Writes three broker property files into a temporary directory, with 3 partitions, replication factor 3 and shorter election timeouts, and starts the three brokers.
2. Waits for a leader for each partition and prints the metadata.
3. Produces 20 records to each partition with `quorum` acknowledgment, and reads each partition back to check the count and that there are no duplicates.
4. Sends SIGKILL to the broker that leads partition 0.
5. Waits for new leaders for all partitions and reads back the 20 records in each partition.
6. Produces 10 more records to each partition while the killed broker is down, and checks for 30 records in each partition.
7. Produces to partition 0 with `acks=all` while a replica is down, and checks that the produce fails.
8. Restarts the killed broker on its old data directory, and waits until it answers and its log end offset and high-watermark match the leader's for every partition.
9. Checks the record counts again. Partition 0 now has 31 records, because the majority committed the record refused in step 7. Partitions 1 and 2 have 30.
10. Produces one `acks=all` record with every replica up, and checks that partition 0 has 32 records.
11. Prints `PASS`. On exit it stops any remaining broker processes and deletes the temporary directory.

One recorded run of the script took about 77 seconds.

## Tests

| Test | Kind of evidence | What it shows |
| --- | --- | --- |
| `RaftConsensusTest` (14) | In-memory: fake synchronous transport and manual clock | Elections, replication and commit, a minority leader cannot commit, restart forgets the commit index until a leader re-establishes it, conflicting tails are truncated |
| `LogTest` (8) | Unit | Segment rolling, offset index lookups, CRC corruption detection |
| `LogRecoveryTest` (9) | Unit, with storage damage simulated by editing files | Incomplete trailing record is truncated; damage in an older segment fails the open |
| `FileRaftMetaStoreTest` (4) | Unit | Term and vote file is checksummed and replaced atomically; a corrupt file is rejected |
| `PartitionAssignmentTest` (4) | Unit | Round-robin and explicit partition assignment |
| `DnsPeerResolverTest` (9) | Unit | DNS-based peer resolution |
| `StaticPeerResolverTest` (3) | Unit | Static `id@host:port` peer lists |
| `PingRoundTripTest` (1) | Real sockets, one broker | A ping round trip over Netty |
| `ProduceFetchTest` (1) | Real sockets, one broker | Produce and fetch over a socket |
| `ProtocolRobustnessTest` (7) | Real sockets, one broker, hand-built frames from a raw socket | Malformed or oversized frames get `INVALID_REQUEST` and the connection closes |
| `ClientCorrelationTest` (6) | Real sockets, scripted server | Responses are matched to requests by correlation ID; pending requests are bounded and fail when the connection is lost |
| `ClusterIntegrationTest` (10) | Several brokers in one JVM over loopback, graceful `close()` | Partitions with independent offsets, acknowledgment modes at single and multi-record boundaries, a minority cannot commit, concurrent clients, leader shutdown |
| `ReportedDefectsTest` (3) | Several brokers in one JVM over loopback, graceful `close()` | Regression tests for three reported defects, including that a restart does not expose uncommitted records |
| `RaftFailoverNetworkTest` (1) | Several brokers in one JVM over loopback, graceful `close()` | A leader is elected over Netty, and a new leader with a higher term takes over after the old one stops |
| `RaftDnsReResolutionTest` (2) | Several brokers in one JVM over loopback, graceful `close()` | A peer moves to a new address; the transport re-resolves it and the cluster reconverges |
| `MultiProcessFailoverTest` (1) | Separate OS processes, SIGKILL | Records committed before the leader is killed remain readable from the new leader; the restarted broker converges with no duplicates |
| `MillionRecordRestartTest` (1) | Unit, single log | Clean close and reopen of 1,000,000 records, all read back |

## Where to look

| Package | Contents |
| --- | --- |
| `com.kafkads.broker` | `Main` and `Broker` (node lifecycle), `PartitionReplica` (one thread per partition), `RequestHandler`, `PartitionAssignment` |
| `com.kafkads.client` | `Producer`, `Consumer`, `ClusterClient` (leader lookup and metadata) |
| `com.kafkads.cli` | `Cli`, the command-line client |
| `com.kafkads.network` | Netty `Server` and `Client`, framing, message encoder and decoder |
| `com.kafkads.protocol` | Wire messages, API keys, error codes, `Acks` |
| `com.kafkads.raft` | `RaftNode`, `NetworkRaftTransport`, `SegmentRaftLog`, `FileRaftMetaStore` |
| `com.kafkads.storage` | `Log`, `Segment`, record format |
| `com.kafkads.discovery` | Static and DNS peer resolution |
| `com.kafkads.config` | `BrokerConfig` and `PeerConfig` |
| `com.kafkads.metrics` | `Metrics`, four in-process counters |
| `com.kafkads.bench` | `Benchmark`, a single-broker produce benchmark. Measured results and their limits are in `docs/HANDOFF.md`. |

Design notes are in [docs/DESIGN.md](docs/DESIGN.md), and the handoff notes are in [docs/HANDOFF.md](docs/HANDOFF.md).
