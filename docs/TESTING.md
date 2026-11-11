# Testing and measurements

## What each scenario is tested by

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
| 9. Restart cannot expose uncommitted data | `regression.CommitVisibilityRegressionTest.restartDoesNotExposeUncommittedRecords`; `raft.RaftConsensusTest.restartForgetsCommitIndexUntilALeaderReEstablishesIt` | Regression test; in-memory deterministic test |
| 10. Single and multi-record acknowledgement boundaries | `cluster.ClusterIntegrationTest.acknowledgementModesAtSingleAndMultiRecordBoundaries`; `regression.CommitVisibilityRegressionTest.acksAllSingleRecordIsNotAcknowledgedWithoutFollowers` | Integration test; regression test |
| 11. Concurrent clients, correlated responses | `cluster.ClusterIntegrationTest.concurrentProducersAndConsumersSeeEveryAcknowledgedRecordExactlyOnce`; `network.ClientCorrelationTest` | Integration test; client correlation over sockets |
| 12. Malformed input fails in a bounded way | `network.ProtocolRobustnessTest` | Real sockets, one real broker, hand-built frames from a raw socket |
| 13. Incomplete trailing record | `storage.LogRecoveryTest.incompleteTrailingPayloadIsTruncatedAndTheLogStaysUsable` (also `incompleteTrailingHeaderIsTruncated`) | Unit test; damage simulated by editing files |

Kinds of evidence, from weakest to strongest:

- **In-memory:** `RaftConsensusTest` drives Raft nodes through a fake synchronous transport and a manual clock.
- **Unit:** storage, recovery from damaged files, term/vote file, partition assignment, peer resolution.
- **Real sockets, one broker or a scripted server:** protocol robustness and client request bookkeeping.
- **Several brokers in one JVM over loopback:** brokers are stopped with a graceful `close()`.
- **Separate OS processes:** `MultiProcessFailoverTest` and `scripts/failover-demo.sh` kill a broker with SIGKILL.

## Results

`mvn test` runs 84 tests. CI runs them on JDK 17 on Ubuntu. Locally they were run on macOS (arm64, Apple M4 Pro) with OpenJDK 25.

In `MultiProcessFailoverTest`, a writer keeps producing while the partition leader's process is killed. In local runs it had 138 to 151 writes acknowledged and 1 write with an uncertain outcome around the kill; every acknowledged record was read back once.

## Benchmark

`java -cp target/kafkads.jar com.kafkads.bench.Benchmark 200000 128 500`, four runs on the machine above. Single broker with a single-replica partition, broker and client in one JVM over loopback, `acks=QUORUM`, one fsync per request:

| Run | Throughput, batches of 500 x 128-byte records | Single-record produce latency p50 / p99 / max |
|---|---|---|
| 1 | 91,080 records/s (11.1 MB/s) | 4.040 / 5.185 / 11.074 ms |
| 2 | 88,968 records/s (10.9 MB/s) | 4.042 / 5.310 / 18.498 ms |
| 3 | 97,179 records/s (11.9 MB/s) | 4.078 / 5.650 / 12.641 ms |
| 4 | 94,888 records/s (11.6 MB/s) | 4.050 / 5.280 / 9.961 ms |

These numbers measure one fsync per request on one laptop SSD with no replication. They say nothing about a replicated cluster or other hardware. No replicated benchmark has been run.

## Not tested

- Power loss or kernel crash. SIGKILL does not drop page-cache data, so fsync-based durability is unverified under power loss.
- An I/O error during append. The fail-stop path exists, but no test exercises it.
- Network partitions between live processes. The minority tests stop brokers instead.
- Long runs, large logs, and more than four brokers.
