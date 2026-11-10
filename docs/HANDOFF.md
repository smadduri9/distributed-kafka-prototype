# KafkaDS handoff notes

This document is for the project owner preparing for a 20-minute hiring-manager interview. It describes what the code does, where the evidence is, and where the limits are. Items marked "(you to confirm)" need a fact only the owner can supply.

## 1. Résumé claim and evidence

Résumé bullet, quoted exactly:

> Built a distributed message broker from scratch in Java with partitioned offset-based logs, producer/consumer APIs over a custom binary protocol on TCP using Netty NIO, replication, failure detection, partition assignment, and Raft-inspired leader election for fault tolerance.

| Clause | Implementation (class names) | Executable evidence (test class and method) | Scope and caveat |
|---|---|---|---|
| distributed message broker | `broker.Broker`, `broker.Main`, `broker.PartitionReplica`, `network.Server`, `broker.RequestHandler`, `raft.NetworkRaftTransport` | `cluster.ClusterIntegrationTest` (several broker objects in one JVM over loopback); `cluster.MultiProcessFailoverTest.acknowledgedRecordsSurviveSigkillOfTheLeaderProcess` (separate OS processes) | Static membership. Tests use one, three and four brokers. More than four brokers is not tested. |
| partitioned offset-based logs | `storage.Log`, `storage.Segment`, `storage.Record`, `broker.PartitionReplica`, `broker.PartitionAssignment` | `cluster.ClusterIntegrationTest.partitionsHaveIndependentOffsetsStorageAndReplicaSets`; `storage.LogTest` (8 tests) | Offsets are per partition, strictly increasing, not dense. Each election writes a no-op entry that occupies an offset, and the fetch path skips it. |
| producer/consumer APIs | `client.Producer` (`send`, `partitionFor`), `client.Consumer` (`fetch`, `seek`, `poll`, `readAll`, `endOffset`), `client.ClusterClient`, `cli.Cli` | `cluster.ClusterIntegrationTest` (end-to-end); `cluster.ClusterIntegrationTest.consumerTracksPositionsAndSupportsExplicitOffsets` | Consumer positions are in memory only. No consumer groups and no stored offsets. |
| custom binary protocol on TCP using Netty NIO | `protocol.Message`, `protocol.MessageRegistry`, `protocol.messages.*`, `network.Framing`, `network.MessageDecoder`, `network.MessageEncoder`, `network.Server`, `network.Client` | `network.ProtocolRobustnessTest` (7 tests); `network.ClientCorrelationTest` (6 tests); `network.PingRoundTripTest` | Transport is Netty 4.1. Frames are length-prefixed, maximum 16 MB. No TLS, no authentication, no idle-connection timeout. |
| replication | `raft.SegmentRaftLog`, `raft.RaftNode`, `raft.NetworkRaftTransport`, `broker.PartitionReplica`; the replica list comes from `broker.PartitionAssignment` | `cluster.ClusterIntegrationTest.committedRecordsSurviveLeaderShutdownAndTheOldLeaderConverges`; `raft.RaftConsensusTest.replicatesAndCommitsEntries` | Each partition is its own Raft group. Its replica list is fixed. A record is visible once a majority of replicas has committed it. No membership change. |
| failure detection | `raft.RaftNode` (`tick`, `majorityHeardWithin`, election deadline, step-down) | `raft.RaftConsensusTest.leaderWithoutAMajorityStepsDownAfterAnElectionTimeout`; `raft.RaftConsensusTest.reElectsLeaderAfterLeaderFails`; `raft.RaftFailoverNetworkTest` (over sockets); `cluster.ClusterIntegrationTest.minorityCannotProduceCommittedWrites` (step-down) | This is not a separate subsystem. It is Raft heartbeats plus an election timeout on followers, and the leader steps down when it has not heard from a majority for `raft.election.timeout.max.ms`. No pre-vote. |
| partition assignment | `broker.PartitionAssignment` (`roundRobin`, `parse`, `replicas`, `preferredLeader`, `isReplica`), used by `broker.Broker` | `broker.PartitionAssignmentTest` (4 tests); `cluster.ClusterIntegrationTest.partitionsHaveIndependentOffsetsStorageAndReplicaSets` (4 brokers, 3 replicas: storage exists only on assigned brokers, others answer `NOT_REPLICA`); `preferredLeadersWinTheInitialElection`; `explicitAssignmentIsHonoured` | Each broker computes the assignment from its own config, so configs must agree. There is no runtime reassignment. The preferred leader gets an earlier first election, as a start-up bias only. There is no move back to the preferred leader after failover. |
| Raft-inspired leader election | `raft.RaftNode` (`startElection`, `handleRequestVote`, `candidateLogUpToDate`, `becomeLeader`), `raft.FileRaftMetaStore` | `raft.RaftConsensusTest.electsASingleLeader`; `raft.RaftConsensusTest.reElectsLeaderAfterLeaderFails`; `raft.RaftConsensusTest.staleCandidateWithShorterLogCannotWin` | Implements terms, randomized election timeouts, a durable vote before replying, and the up-to-date-log check. Not implemented: pre-vote, leadership transfer, membership change, snapshots. "Inspired" is the right word: the code follows Raft's rules but is not a complete Raft implementation. |
| "for fault tolerance" | Replication and election classes above | `cluster.MultiProcessFailoverTest.acknowledgedRecordsSurviveSigkillOfTheLeaderProcess`; `cluster.ClusterIntegrationTest.minorityCannotProduceCommittedWrites` | Exact scope: acknowledged (QUORUM) records survive the failure of a minority of a partition's replicas. This was shown with SIGKILL of a broker process. Power loss and kernel crash are untested. A write requested with ALL can be answered NOT_ENOUGH_REPLICAS and still become visible, so a failed ALL is not a rollback. |
| "from scratch" | Netty 4.1 and dnsjava are dependencies. The protocol, storage, Raft, assignment and client code are in `src/main/java/com/kafkads`. | Not applicable | Authorship and what "from scratch" means in your résumé: (you to confirm). Be ready to say which libraries you used and which parts you wrote, if you wrote them yourself. |

## 2. Required scenarios and where they are tested

| Scenario | Test | Kind of evidence |
|---|---|---|
| 1. Three or more partitions with independent offsets | `cluster.ClusterIntegrationTest.partitionsHaveIndependentOffsetsStorageAndReplicaSets` | In-JVM, several broker objects over loopback |
| 2. Producer and consumer APIs end to end | `cluster.ClusterIntegrationTest` (all); `consumerTracksPositionsAndSupportsExplicitOffsets` | In-JVM, several broker objects over loopback |
| 3. Three brokers over real sockets | `cluster.ClusterIntegrationTest`; `cluster.MultiProcessFailoverTest` | Loopback sockets; the second test uses separate OS processes |
| 4. Committed writes, then abrupt leader death | `cluster.MultiProcessFailoverTest` (SIGKILL) | Separate OS processes, SIGKILL |
| 5. Read all acknowledged records from the new leader | `cluster.MultiProcessFailoverTest` step 3; `cluster.ClusterIntegrationTest.committedRecordsSurviveLeaderShutdownAndTheOldLeaderConverges` | SIGKILL (first); graceful shutdown (second) |
| 6. Continue committed writes with one broker down | `cluster.MultiProcessFailoverTest` step 4 | Separate OS processes, SIGKILL |
| 7. Restart the old broker, converge, no duplicate replay | `cluster.MultiProcessFailoverTest` steps 5 to 7 (compares files on disk); `cluster.ClusterIntegrationTest.wholeClusterRestartKeepsCommittedRecordsWithoutDuplicates` | Files on disk after SIGKILL; whole-cluster restart in one JVM |
| 8. A minority cannot commit | `cluster.ClusterIntegrationTest.minorityCannotProduceCommittedWrites`; `raft.RaftConsensusTest.minorityLeaderCannotCommit` | Integration test (brokers stopped to form the minority); in-memory deterministic test |
| 9. Restart cannot expose uncommitted data | `regression.ReportedDefectsTest.restartDoesNotExposeUncommittedRecords`; `raft.RaftConsensusTest.restartForgetsCommitIndexUntilALeaderReEstablishesIt` | Regression test; in-memory deterministic test |
| 10. Single and multi-record acknowledgement boundaries | `cluster.ClusterIntegrationTest.acknowledgementModesAtSingleAndMultiRecordBoundaries`; `regression.ReportedDefectsTest.acksAllSingleRecordIsNotAcknowledgedWithoutFollowers` | Integration test; regression test |
| 11. Concurrent clients, correlated responses | `cluster.ClusterIntegrationTest.concurrentProducersAndConsumersSeeEveryAcknowledgedRecordExactlyOnce`; `network.ClientCorrelationTest` | Integration test; client correlation over sockets |
| 12. Malformed input fails in a bounded way | `network.ProtocolRobustnessTest` | Real sockets, one real broker, hand-built frames from a raw socket |
| 13. Incomplete trailing record | `storage.LogRecoveryTest.incompleteTrailingPayloadIsTruncatedAndTheLogStaysUsable` (also `incompleteTrailingHeaderIsTruncated`) | Unit test; damage simulated by editing files |
| 14. Existing tests kept and green, except those intentionally replaced | See the list of intentionally replaced or changed tests below | Test suite result |

Intentionally replaced or changed tests:

- `ReplicationTest` was removed. It asserted the old "all replicas" high-watermark. Visibility is now majority commit. Its acks=ALL assertions are in `ClusterIntegrationTest`.
- `FileRaftPersistenceTest` was removed with its class. It is replaced by `FileRaftMetaStoreTest` and the log tests.
- `ProduceFetchTest`: the first data offset is now 1 and the high-watermark is 4, because offset 0 is the no-op entry.
- `LogTest` corruption test: damage is now detected at open as well as on read.
- `RaftConsensusTest`: indexes shifted by one no-op per election, and more cases were added.
- `RaftDnsReResolutionTest`: the fake server now speaks correlated frames.

## 3. Test results

Environment for everything below: macOS (Darwin 27.2.0, arm64, Apple M4 Pro), OpenJDK 25.0.2, Maven 3.9.16, offline. The project compiles with `--release 17`, but nothing has been run on JDK 17 or on Linux; CI (JDK 17, Ubuntu) will be the first such run.

History of full-suite runs (`mvn -o -B test`, 84 tests), in order:

1. The first full run after the last tests were added failed one test, `RaftConsensusTest.restartPreservesTermAndVote`, on three consecutive runs. Cause: the test depended on the order in which a candidate contacts its peers, which was not fixed. Fix: `RaftNode` now contacts peers in id order.
2. Three consecutive full runs then passed. A fourth failed `RaftDnsReResolutionTest.clusterReconvergesAfterFollowerMovesToNewPort`. Cause: the Raft transport reconnected after an idle close without invalidating the cached peer address. Fix: `NetworkRaftTransport` now invalidates it.
3. After that fix, the five timing-sensitive classes (`RaftDnsReResolutionTest`, `RaftFailoverNetworkTest`, `ClusterIntegrationTest`, `ReportedDefectsTest`, `MultiProcessFailoverTest`) passed 12 of 12 runs.
4. Final verification on the committed code: four sequential full runs, each 84 tests, 0 failures, 0 errors, about 33 seconds. (Three further full runs that were accidentally started concurrently also passed; they are not counted.)

Other observations:

- In those four runs `MultiProcessFailoverTest` reported 138 to 151 writes acknowledged around the SIGKILL of the leader and 1 uncertain write each time. Every acknowledged record was read back once.
- `scripts/failover-demo.sh` printed PASS on its one run, in 77 seconds.
- Test kinds: in-memory with a fake synchronous transport and manual clock (`RaftConsensusTest`); unit tests (`LogTest`, `LogRecoveryTest`, `FileRaftMetaStoreTest`, `PartitionAssignmentTest`, `DnsPeerResolverTest`, `StaticPeerResolverTest`, `MillionRecordRestartTest`); real sockets with one broker or a scripted server (`PingRoundTripTest`, `ProduceFetchTest`, `ProtocolRobustnessTest`, `ClientCorrelationTest`); several broker objects in one JVM over loopback with graceful close (`ClusterIntegrationTest`, `ReportedDefectsTest`, `RaftFailoverNetworkTest`, `RaftDnsReResolutionTest`); separate OS processes with SIGKILL (`MultiProcessFailoverTest`, `scripts/failover-demo.sh`).

Benchmark (`java -cp target/kafkads.jar com.kafkads.bench.Benchmark 200000 128 500`), four runs on the machine above. Single broker with a single-replica partition, broker and client in one JVM over loopback, `acks=QUORUM`, one fsync per request:

| Run | Throughput, batches of 500 x 128-byte records | Single-record produce latency p50 / p99 / max |
|---|---|---|
| 1 | 91,080 records/s (11.1 MB/s) | 4.040 / 5.185 / 11.074 ms |
| 2 | 88,968 records/s (10.9 MB/s) | 4.042 / 5.310 / 18.498 ms |
| 3 | 97,179 records/s (11.9 MB/s) | 4.078 / 5.650 / 12.641 ms |
| 4 | 94,888 records/s (11.6 MB/s) | 4.050 / 5.280 / 9.961 ms |

These numbers measure one fsync per request on one laptop SSD with no replication. They say nothing about a replicated cluster, about other hardware, or about how the system compares with anything else. No replicated benchmark has been run.

## 4. Remaining defects and untested scenarios

Not tested:

- Power loss or kernel crash. SIGKILL does not drop page-cache data, so the correctness of fsync is unproven.
- An I/O error during append. The fail-stop path exists, but no test exercises it.
- Linux and JDK 17.
- Long runs and large logs.
- More than four brokers.
- Network partitions between live processes. The minority tests stop brokers instead.
- Bit rot in the last record of the active segment cannot be told apart from a torn write, so it is truncated.

Not implemented:

- Pre-vote. A rejoining broker with a higher term can force an election.
- Snapshots and log compaction. The log grows without bound, and start-up scans all of it.
- Membership change. Membership is static.
- Leadership transfer. There is no automatic move back to the preferred leader.
- Linearizable reads from followers. Fetch is leader-only.
- Consumer groups and stored consumer offsets.
- Idempotent producer. There are no producer ids or sequence numbers.
- TLS and authentication.
- Idle-connection timeouts. Flow control through channel writability is also absent.
- Metrics: there are four in-process counters and no exporter.
- A benchmark of a replicated cluster. Only the single-broker numbers in section 3 exist.

## 5. Interview walkthrough (about 2 minutes)

Speaking notes. Adjust the wording to your own role in the project (you to confirm).

1. This is a Kafka-style message broker prototype in Java 17, built on Netty 4.1 with dnsjava for peer discovery. It is not Kafka-compatible and it is not production-grade. Records are written to partitions, and each partition has its own strictly increasing offset sequence. The offsets are not dense, because each election writes a no-op entry that takes an offset.

2. Clients talk to brokers over TCP using a custom binary protocol. Each frame is a 4-byte length, a 1-byte API key, a 4-byte correlation id, and a payload, with a 16 MB maximum. Netty reassembles partial reads. The decoder reads exactly one message per frame and checks every length against the bytes left in the frame before allocating memory. A malformed frame gets an error response and the connection is closed.

3. Storage is a segmented log per partition. Each record carries its offset, its Raft term, a type (data or no-op), its length and a CRC32 over all of those and the payload. Segments roll at 16 MB. The sparse offset index is kept in memory and rebuilt on open, which means open validates every record. An incomplete tail in the last segment is truncated. Damage anywhere else makes the broker refuse to start, rather than drop data.

4. The key design decision is that each partition is one Raft group, and the partition's log is the Raft log. Raft index i is offset i minus 1. There is no separate state machine, so there is nothing to replay. The commit index is the high-watermark that consumers can see. Each partition's replica list from the assignment is its Raft peer set, so the commit quorum is that list. Each partition runs its log and Raft work on one dedicated thread.

5. What a producer's OK means depends on the acks mode. LEADER means the leader has appended and fsynced the record, but it is not committed and can be discarded. QUORUM, the default, means a majority of the partition's replicas in the leader's term has it, and consumers can see it. ALL means it is committed and stored on every replica, and if a replica is down the producer gets NOT_ENOUGH_REPLICAS after the timeout, although the records may still become visible. The producer retries only failures that prove nothing was appended. Any ambiguous outcome throws an UncertainWriteException, and the producer never retries it automatically, because a retry could write the record twice.

6. The limits I would state up front: the tests are in-process or use separate processes killed with SIGKILL, not real power loss. The code has not run on JDK 17 or Linux yet. There is no exactly-once delivery, no consumer groups, no snapshots, no pre-vote, and no membership change. The only benchmark is single-broker.

## 6. Design decisions to be ready to defend

1. **One Raft group per partition, not one group for the whole broker.**
   - Alternative: a single Raft group that orders all records for all partitions.
   - Reason: with a single group every partition would have the same replicas and the same leader, so a partition assignment could not affect anything. With a group per partition the assignment decides where data is stored, which brokers form the quorum and which may lead. Partitions also do not wait on each other.
   - Cost: each partition runs its own election and heartbeat traffic, and each partition needs its own thread and its own log.

2. **The partition log is the Raft log, with no separate state machine.**
   - Alternative: a Raft log that is separate from the storage log, with the storage log built by applying committed entries.
   - Reason: there is one copy of each record, the commit index is simply the exclusive high-watermark, and there is nothing to replay or to keep in sync.
   - Cost: a Raft log entry must also serve as a storage record, so offsets follow Raft indexes. Log compaction would need a snapshot design that does not exist yet.

3. **No-op entry on election, and non-dense offsets.**
   - Alternative: a new leader commits nothing until a client writes, and offsets are dense.
   - Reason: a new leader must commit an entry of its own term before it can serve reads or claim earlier entries as committed. The no-op provides that entry, and the fetch path returns `nextOffset` and filters no-ops out.
   - Cost: offsets have gaps, so the offset arithmetic and the tests (for example `ProduceFetchTest`, where the first data offset is 1) must account for them. Consumers must follow `nextOffset`, not assume `offset + 1`.

4. **Volatile commit index, not a persisted high-watermark.**
   - Alternative: persist the commit index (or high-watermark) on disk so that a restarted broker knows what was committed.
   - Reason: a record present in a local log is not proof that it was committed. After restart the commit index starts at 0, and it advances only when a leader re-establishes it, so local presence never makes an uncommitted record visible.
   - Cost: after a restart a broker cannot serve consumers until a leader re-establishes the commit index. The consumer's view may lag briefly.

5. **One dedicated thread per partition, instead of locks around shared state.**
   - Alternative: a shared thread pool with locks around each partition's log and Raft state.
   - Reason: all log and Raft work for a partition runs on one thread, so there is no lock ordering to get wrong. Netty event loops only enqueue work and write responses. Other threads read a volatile snapshot (`View`).
   - Cost: a partition is limited to one thread, and a busy partition cannot use more than one core for its own work. The thread count grows with the number of partitions hosted on a broker.

6. **In-memory sparse index with a full validation scan on open, instead of an index file.**
   - Alternative: write an index file and trust it on start-up.
   - Reason: there is no index file that can disagree with the data. The open scan checks length, offset continuity and CRC for every record, so a damaged index cannot cause wrong reads.
   - Cost: start-up time grows with the size of the log. With no snapshots, the log is never compacted, so this cost keeps growing. The `MillionRecordRestartTest` covers reopening 1,000,000 records, but no larger log is tested.

7. **Fail-stop on mid-log corruption, instead of truncating to the last good record.**
   - Alternative: on any bad record, truncate the log at that point and continue.
   - Reason: damage in an older segment, a missing middle segment, or a bad record followed by a valid record would mean silently dropping data that may have been acknowledged. Refusing to start keeps that data visible to the operator.
   - Cost: a broker with mid-log damage needs manual repair before it can start. The only automatic repair is truncating an incomplete or invalid tail in the last segment. Bit rot in the last record is indistinguishable from a torn write, so it is also truncated.

8. **No automatic retry of uncertain writes.**
   - Alternative: retry any write that timed out, so that the producer does not have to decide.
   - Reason: an uncertain write may already be in the leader's log, and a retry may write it twice. The project has no producer ids or sequence numbers, so it cannot deduplicate, and it reports the uncertainty to the caller instead.
   - Cost: the caller must decide whether to resend. The result is at-least-once if the caller resends and at-most-once if not. Exactly-once is not provided.

## 7. Likely questions

1. **Why Raft for each partition instead of a Kafka-style in-sync replica set?**
   Each partition is a Raft group whose replica list is fixed by `broker.PartitionAssignment`. A record is committed when a majority of the replicas in the leader's term holds it, and a minority cannot commit. The implementation follows Raft's terms, votes and log matching rules in `raft.RaftNode`. Read `raft/RaftNode.java`, especially `maybeAdvanceCommitIndex` and `handleAppendEntries`.

2. **What happens to a write that was acknowledged when the leader crashes?**
   With QUORUM the record is committed, so a new leader that wins the election has it. The SIGKILL test in `cluster.MultiProcessFailoverTest` kills the leader process and reads back every acknowledged record exactly once from the new leader. Power loss is not covered, because SIGKILL does not drop page-cache data. Read `MultiProcessFailoverTest.java`.

3. **Is this exactly-once?**
   No. There are no producer ids or sequence numbers, so the broker cannot deduplicate. If a caller resends after an uncertain write, the record may be written twice. Read `client/Producer.java` and `protocol/ErrorCode.java`.

4. **What does the producer do on a timeout?**
   It retries only outcomes that prove nothing was appended, such as NOT_LEADER. Any ambiguous outcome throws `UncertainWriteException` and is not retried. Read `client/Producer.java` and `client/UncertainWriteException.java`.

5. **Why are offsets not consecutive?**
   Each election appends a no-op entry, and that entry takes an offset. Consumers skip no-ops and follow `nextOffset` in the fetch response. Read `raft/RaftNode.java` (no-op on election) and `storage/Log.java`.

6. **Why does the commit index start at zero after a restart?**
   A record in a local log is not proof it was committed. The leader must re-establish the commit index, and only then can the broker serve reads. This prevents an uncommitted record from becoming visible after a restart. Read `ReportedDefectsTest.restartDoesNotExposeUncommittedRecords` and `RaftConsensusTest.restartForgetsCommitIndexUntilALeaderReEstablishesIt`.

7. **How do you handle a corrupted log?**
   An incomplete or invalid tail in the last segment is truncated. Damage in any other place makes the open fail, so data is not silently dropped. Read `storage/Log.java`, `storage/Segment.java` and `storage/LogRecoveryTest.java`, especially `damageInAnOlderSegmentFailsTheOpen`.

8. **How does a broker know which partitions it hosts?**
   Every broker computes `PartitionAssignment` from its own config. The default is round-robin over sorted broker ids with `replication.factor`. The configs must match on every broker, and there is no runtime reassignment. Read `broker/PartitionAssignment.java` and `broker/Broker.java`.

9. **How do you handle a network partition?**
   A minority leader cannot commit, and a leader steps down when it has not heard from a majority within the election timeout. The tests stop brokers to form a minority, rather than partitioning live processes, so a true network partition between live processes is not tested. Read `raft/RaftNode.java` (`majorityHeardWithin`) and `raft/RaftConsensusTest.java`.

10. **Is this production-ready, and how does it perform?**
    No. It is a prototype with static membership, no snapshots, no pre-vote, no TLS and no authentication. The only measurements are single-broker (section 3): about 90,000 records per second in batches of 500 and about 4 ms per single-record produce, both dominated by one fsync per request. Start-up validates every record of the log, so it slows down as the log grows. Read the "Guarantees and limits" section of `README.md`, and `storage/Log.java` for the open path.

## 8. Learning guide

Read the files in this order. Each row lists the invariant to be able to state without notes.

| Order | File (under `src/main/java/com/kafkads`) | What to understand | The invariant to be able to state |
|---|---|---|---|
| 1 | `protocol/Message.java` | Field encoding and the bounds checks in `readBytes` and `readCount` | Every length or count is checked against the bytes remaining in the frame before any allocation. |
| 2 | `network/Framing.java` and `network/MessageDecoder.java` | Length-prefixed frames, reassembly, one message per frame | A frame is at most 16 MB. Exactly one message is decoded per frame, and trailing bytes are an error. |
| 3 | `network/Client.java` | Correlation ids, per-request timeouts, the 1024 pending limit | Each response is matched to its request by correlation id. When the connection is lost, all pending requests fail. |
| 4 | `storage/Segment.java` | Record layout, CRC coverage, the file magic, scanning on open | The CRC covers offset, term, type, length and payload. A record is written in one segment. |
| 5 | `storage/Log.java` | Segment rolling, the in-memory index, truncation, recovery | A batch is written to exactly one segment, and a segment is fsynced before rolling. Open fails on damage that is not an incomplete tail in the last segment. |
| 6 | `raft/RaftNode.java` (elections) | `startElection`, `handleRequestVote`, `candidateLogUpToDate`, `becomeLeader`, the election deadline in `tick` | A vote is made durable before the reply is sent. A candidate must have a log at least as up to date as the voter's. |
| 7 | `raft/RaftNode.java` (replication and commit) | `propose`, `sendAppendEntries`, `handleAppendEntries`, `maybeAdvanceCommitIndex`, `stepDown` | The leader commits an entry only when a majority holds it and the entry is from the leader's current term. A follower advances its commit index only to what the request verified. |
| 8 | `raft/FileRaftMetaStore.java` | The 16-byte checksummed file, atomic replacement by temp file, fsync and rename | The term and vote are never partly written, and a corrupt file stops start-up. |
| 9 | `broker/PartitionAssignment.java` | Round-robin default, explicit spec, `preferredLeader`, `isReplica` | Every broker computes the same assignment from the same config, and the assignment does not change at runtime. |
| 10 | `broker/PartitionReplica.java` | The log plus Raft node plus pending produces, the single thread, the `View` snapshot, `MAX_QUEUED_REQUESTS` | All log and Raft work for a partition runs on one thread, and other threads only read the volatile `View`. |
| 11 | `broker/Broker.java` | Creating storage and Raft members only for assigned partitions, `NOT_REPLICA` and `UNKNOWN_PARTITION` answers | A broker hosts a partition only if it is in that partition's replica list. |
| 12 | `client/ClusterClient.java`, `client/Producer.java`, `client/Consumer.java` | Metadata lookup, NOT_LEADER hints, the retry rule in `Producer`, the consumer's in-memory positions | The producer retries only outcomes that prove nothing was appended, and an uncertain outcome is never retried. Consumer positions are not stored anywhere. |

Tests to read first:

1. `cluster.MultiProcessFailoverTest.acknowledgedRecordsSurviveSigkillOfTheLeaderProcess`: the strongest evidence for the fault-tolerance claim.
2. `regression.ReportedDefectsTest.restartDoesNotExposeUncommittedRecords`: the restart rule for the commit index.
3. `raft.RaftConsensusTest.minorityLeaderCannotCommit`: the commit quorum rule in the in-memory setting.
4. `cluster.ClusterIntegrationTest.concurrentProducersAndConsumersSeeEveryAcknowledgedRecordExactlyOnce`: end-to-end behavior with concurrent clients.
5. `storage.LogRecoveryTest.damageInAnOlderSegmentFailsTheOpen`: the fail-stop rule for corruption.

## 9. Things only you can answer

- How the project was built, and what tools or references were used (you to confirm).
- How long it took to build (you to confirm).
- Which parts you can explain without notes. Practise walking through the replication path in `raft/RaftNode.java` and the storage layout in `storage/Segment.java` (you to confirm).
- Your Spring Boot, Linux and scripting experience from other work (you to confirm).
