package com.kafkads.cluster;

import com.kafkads.broker.Broker;
import com.kafkads.broker.PartitionAssignment;
import com.kafkads.client.Consumer;
import com.kafkads.client.ConsumerRecord;
import com.kafkads.client.FetchResult;
import com.kafkads.client.KafkaDsException;
import com.kafkads.client.Producer;
import com.kafkads.client.RecordMetadata;
import com.kafkads.client.RequestRejectedException;
import com.kafkads.network.Client;
import com.kafkads.protocol.Acks;
import com.kafkads.protocol.ErrorCode;
import com.kafkads.protocol.messages.FetchRequest;
import com.kafkads.protocol.messages.FetchResponse;
import com.kafkads.protocol.messages.MetadataResponse;
import com.kafkads.protocol.messages.ProduceRequest;
import com.kafkads.protocol.messages.ProduceResponse;
import com.kafkads.testutil.TestBrokers;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * End-to-end behaviour of several brokers in one JVM, connected by real loopback
 * sockets and driven through the public {@link Producer} and {@link Consumer}.
 * Brokers are stopped with a graceful {@code close()}; process death is exercised
 * separately by {@code MultiProcessFailoverTest}.
 */
class ClusterIntegrationTest {

    @TempDir
    Path tmp;

    private static List<String> strings(List<ConsumerRecord> records) {
        List<String> out = new ArrayList<>();
        for (ConsumerRecord r : records) {
            out.add(new String(r.value(), StandardCharsets.UTF_8));
        }
        return out;
    }

    private static void assertStrictlyIncreasingOffsets(List<ConsumerRecord> records) {
        for (int i = 1; i < records.size(); i++) {
            assertTrue(records.get(i).offset() > records.get(i - 1).offset(), "offsets must increase");
        }
    }

    // ---- partitions and assignment ----

    @Test
    void partitionsHaveIndependentOffsetsStorageAndReplicaSets() throws Exception {
        // 4 brokers, 4 partitions, 3 replicas each: every partition lives on a different
        // subset of brokers, so the assignment is visibly doing something.
        try (TestBrokers cluster = new TestBrokers(tmp, 20100, 4, 4, 3).startAll()) {
            cluster.awaitAllLeaders(10_000);
            PartitionAssignment assignment = cluster.broker(1).assignment();
            assertEquals(List.of(1, 2, 3), assignment.replicas(0));
            assertEquals(List.of(2, 3, 4), assignment.replicas(1));
            assertEquals(List.of(3, 4, 1), assignment.replicas(2));
            assertEquals(List.of(4, 1, 2), assignment.replicas(3));

            try (Producer producer = new Producer(cluster.bootstrap());
                 Consumer consumer = new Consumer(cluster.bootstrap())) {
                assertEquals(4, producer.partitionCount());
                int[] counts = {7, 3, 5, 1};
                for (int p = 0; p < 4; p++) {
                    for (int i = 0; i < counts[p]; i++) {
                        producer.send(p, ("p" + p + "-" + i).getBytes(StandardCharsets.UTF_8));
                    }
                }
                for (int p = 0; p < 4; p++) {
                    List<ConsumerRecord> records = consumer.readAll(p, 0);
                    assertEquals(TestBrokers.strings("p" + p + "-", 0, counts[p]), strings(records),
                            "partition " + p + " holds exactly its own records, in order");
                    assertStrictlyIncreasingOffsets(records);
                    // Offsets are per partition: each starts again just after its own first no-op.
                    assertEquals(1, records.get(0).offset());
                    assertEquals(counts[p], records.get(records.size() - 1).offset());
                }

                // The assignment decides where data physically is.
                for (int id = 1; id <= 4; id++) {
                    for (int p = 0; p < 4; p++) {
                        boolean assigned = assignment.isReplica(p, id);
                        assertEquals(assigned, Files.isDirectory(cluster.partitionDir(id, p)),
                                "broker " + id + " storage for partition " + p);
                        assertEquals(assigned, cluster.broker(id).partition(p) != null);
                    }
                }
                // ...which brokers may lead a partition...
                for (int p = 0; p < 4; p++) {
                    assertTrue(assignment.replicas(p).contains(cluster.leaderOf(p)));
                }
                // ...and which brokers will accept requests for it.
                try (Client raw = new Client()) {
                    raw.connect("127.0.0.1", cluster.port(4)); // broker 4 is not a replica of partition 0
                    ProduceResponse pr = (ProduceResponse) raw.sendAndWait(
                            new ProduceRequest(0, Acks.QUORUM, TestBrokers.values("x", 0, 1)), 5, TimeUnit.SECONDS);
                    assertEquals(ErrorCode.NOT_REPLICA, pr.errorCode());
                    FetchResponse fr = (FetchResponse) raw.sendAndWait(
                            new FetchRequest(0, 0, 1024), 5, TimeUnit.SECONDS);
                    assertEquals(ErrorCode.NOT_REPLICA, fr.errorCode());
                    ProduceResponse unknown = (ProduceResponse) raw.sendAndWait(
                            new ProduceRequest(99, Acks.QUORUM, TestBrokers.values("x", 0, 1)), 5, TimeUnit.SECONDS);
                    assertEquals(ErrorCode.UNKNOWN_PARTITION, unknown.errorCode());
                }
                assertThrows(RequestRejectedException.class,
                        () -> producer.send(99, "nope".getBytes(StandardCharsets.UTF_8)));

                // Metadata exposes the same assignment to clients.
                MetadataResponse metadata = consumer.metadata();
                assertEquals(4, metadata.brokers().size());
                for (MetadataResponse.PartitionInfo info : metadata.partitions()) {
                    assertEquals(assignment.replicas(info.partition()), info.replicas());
                    assertEquals(assignment.preferredLeader(info.partition()), info.preferredLeader());
                }
            }
        }
    }

    @Test
    void preferredLeadersWinTheInitialElection() throws Exception {
        try (TestBrokers cluster = new TestBrokers(tmp, 20120, 3, 3, 3).startAll()) {
            cluster.awaitAllLeaders(10_000);
            PartitionAssignment assignment = cluster.broker(1).assignment();
            Set<Integer> leaders = new HashSet<>();
            for (int p = 0; p < 3; p++) {
                assertEquals(assignment.preferredLeader(p), cluster.leaderOf(p),
                        "partition " + p + " should initially be led by its preferred replica");
                leaders.add(cluster.leaderOf(p));
            }
            assertEquals(Set.of(1, 2, 3), leaders, "leadership is spread across the brokers");
        }
    }

    @Test
    void explicitAssignmentIsHonoured() throws Exception {
        try (TestBrokers cluster = new TestBrokers(tmp, 20130, 3, 2, 3)
                .with("partition.assignment", "0:3,1;1:2").startAll()) {
            cluster.awaitAllLeaders(10_000);
            assertEquals(3, cluster.leaderOf(0));
            assertEquals(2, cluster.leaderOf(1));
            assertNull(cluster.broker(2).partition(0));
            assertNull(cluster.broker(1).partition(1));
            assertNotNull(cluster.broker(1).partition(0));
            try (Producer producer = new Producer(cluster.bootstrap());
                 Consumer consumer = new Consumer(cluster.bootstrap())) {
                producer.send(1, "single-replica".getBytes(StandardCharsets.UTF_8));
                producer.send(0, List.of("two-replicas".getBytes(StandardCharsets.UTF_8)), Acks.ALL);
                assertEquals(List.of("single-replica"), strings(consumer.readAll(1, 0)));
                assertEquals(List.of("two-replicas"), strings(consumer.readAll(0, 0)));
            }
        }
    }

    // ---- producer / consumer API ----

    @Test
    void consumerTracksPositionsAndSupportsExplicitOffsets() throws Exception {
        try (TestBrokers cluster = new TestBrokers(tmp, 20140, 3, 3, 3).startAll()) {
            cluster.awaitAllLeaders(10_000);
            try (Producer producer = new Producer(cluster.bootstrap());
                 Consumer consumer = new Consumer(cluster.bootstrap())) {
                RecordMetadata first = producer.send(1, TestBrokers.values("a", 0, 5), Acks.QUORUM);
                assertEquals(5, first.count());
                assertEquals(first.baseOffset() + 4, first.lastOffset());

                assertEquals(0, consumer.position(1));
                assertEquals(TestBrokers.strings("a", 0, 5), strings(consumer.poll(1)));
                assertEquals(first.lastOffset() + 1, consumer.position(1));
                assertEquals(List.of(), consumer.poll(1), "nothing new yet");
                assertEquals(first.lastOffset() + 1, consumer.endOffset(1));

                RecordMetadata second = producer.send(1, TestBrokers.values("b", 0, 2), Acks.QUORUM);
                assertEquals(first.lastOffset() + 1, second.baseOffset(), "batches are contiguous");
                assertEquals(TestBrokers.strings("b", 0, 2), strings(consumer.poll(1)));

                // Explicit offset: re-read from the third record of the first batch.
                consumer.seek(1, first.baseOffset() + 2);
                assertEquals(List.of("a2", "a3", "a4", "b0", "b1"), strings(consumer.poll(1)));

                // A byte budget limits a fetch ("a0" and "a1" are two bytes each)...
                FetchResult limited = consumer.fetch(1, first.baseOffset(), 4);
                assertEquals(List.of("a0", "a1"), strings(limited.records()));
                assertEquals(first.baseOffset() + 2, limited.nextOffset());
                // ...but a fetch always makes progress, even with a budget below one record.
                FetchResult tiny = consumer.fetch(1, first.baseOffset(), 1);
                assertEquals(List.of("a0"), strings(tiny.records()));
                assertEquals(first.baseOffset() + 1, tiny.nextOffset());

                // Other partitions are untouched.
                assertEquals(List.of(), consumer.poll(0));
                assertEquals(List.of(), consumer.poll(2));

                // Key routing is deterministic and stays within the partition range.
                int partition = producer.partitionFor("user-42".getBytes(StandardCharsets.UTF_8));
                assertTrue(partition >= 0 && partition < 3);
                assertEquals(partition, producer.partitionFor("user-42".getBytes(StandardCharsets.UTF_8)));
            }
        }
    }

    // ---- acknowledgements ----

    @Test
    void acknowledgementModesAtSingleAndMultiRecordBoundaries() throws Exception {
        try (TestBrokers cluster = new TestBrokers(tmp, 20150, 3, 1, 3).startAll()) {
            int leader = cluster.awaitLeader(0, 10_000);
            Broker leaderBroker = cluster.broker(leader);
            try (Client raw = new Client()) {
                raw.connect("127.0.0.1", cluster.port(leader));

                // All replicas up: every mode succeeds for one record and for several.
                for (Acks acks : Acks.values()) {
                    for (int n : new int[]{1, 4}) {
                        ProduceResponse r = (ProduceResponse) raw.sendAndWait(
                                new ProduceRequest(0, acks, TestBrokers.values(acks + "-" + n + "-", 0, n)),
                                10, TimeUnit.SECONDS);
                        assertEquals(ErrorCode.OK, r.errorCode(), acks + " x" + n);
                        long end = r.baseOffset() + n; // exclusive end of this batch
                        if (acks != Acks.LEADER) {
                            assertTrue(leaderBroker.partition(0).raftNode().commitIndex() >= end,
                                    acks + " x" + n + ": OK means the last record of the batch is committed");
                        }
                        if (acks == Acks.ALL) {
                            assertTrue(leaderBroker.partition(0).raftNode().minMatchIndex() >= end,
                                    "ALL x" + n + ": OK means every replica has the last record");
                        }
                    }
                }
                assertTrue(TestBrokers.await(
                        () -> leaderBroker.partition(0).highWatermark() == leaderBroker.partition(0).logEndOffset(), 5000));
                long committedBefore = leaderBroker.partition(0).highWatermark();

                // One follower down: a majority still commits, but ALL cannot be satisfied.
                int follower = leader == 1 ? 2 : 1;
                cluster.stop(follower);
                for (int n : new int[]{1, 4}) {
                    ProduceResponse quorum = (ProduceResponse) raw.sendAndWait(
                            new ProduceRequest(0, Acks.QUORUM, TestBrokers.values("q", 0, n)), 10, TimeUnit.SECONDS);
                    assertEquals(ErrorCode.OK, quorum.errorCode(), "QUORUM x" + n + " with one replica down");

                    ProduceResponse all = (ProduceResponse) raw.sendAndWait(
                            new ProduceRequest(0, Acks.ALL, TestBrokers.values("all", 0, n)), 10, TimeUnit.SECONDS);
                    assertEquals(ErrorCode.NOT_ENOUGH_REPLICAS, all.errorCode(), "ALL x" + n + " with one replica down");
                    assertTrue(all.baseOffset() >= 0, "the response says where the records were appended");
                }
                // NOT_ENOUGH_REPLICAS is an "uncertain" outcome, not a rollback: the majority
                // still committed those records, and consumers can read them.
                assertTrue(TestBrokers.await(
                        () -> leaderBroker.partition(0).highWatermark() == committedBefore + 10, 5000));
                assertFalse(ErrorCode.produceWasRejected(ErrorCode.NOT_ENOUGH_REPLICAS));
            }
        }
    }

    @Test
    void acksLeaderIsAcknowledgedBeforeCommitAndIsNotACommit() throws Exception {
        try (TestBrokers cluster = new TestBrokers(tmp, 20160, 1, 1, 1).startAll()) {
            cluster.awaitLeader(0, 10_000);
            try (Producer producer = new Producer(cluster.bootstrap());
                 Consumer consumer = new Consumer(cluster.bootstrap())) {
                // On a single-replica partition the leader alone is a majority, so even a
                // LEADER-acknowledged record commits at once. The contract difference is
                // exercised with a lost majority in minorityCannotProduceCommittedWrites.
                RecordMetadata m = producer.send(0, TestBrokers.values("solo", 0, 1), Acks.LEADER);
                assertEquals(List.of("solo0"), strings(consumer.readAll(0, 0)));
                assertEquals(1, m.baseOffset());
            }
        }
    }

    // ---- failover, minority, restart ----

    @Test
    void committedRecordsSurviveLeaderShutdownAndTheOldLeaderConverges() throws Exception {
        List<String> expected = new ArrayList<>();
        int oldLeader;
        try (TestBrokers cluster = new TestBrokers(tmp, 20170, 3, 3, 3).startAll()) {
            cluster.awaitAllLeaders(10_000);
            try (Producer producer = new Producer(cluster.bootstrap());
                 Consumer consumer = new Consumer(cluster.bootstrap())) {
                producer.send(0, TestBrokers.values("before-", 0, 50), Acks.QUORUM);
                expected.addAll(TestBrokers.strings("before-", 0, 50));

                oldLeader = cluster.leaderOf(0);
                int oldTerm = cluster.broker(oldLeader).partition(0).raftNode().currentTerm();
                cluster.stop(oldLeader); // graceful shutdown of the leader

                // The same producer and consumer keep working: they find the new leader.
                producer.send(0, TestBrokers.values("during-", 0, 30), Acks.QUORUM);
                expected.addAll(TestBrokers.strings("during-", 0, 30));
                int newLeader = cluster.leaderOf(0);
                assertNotEquals(oldLeader, newLeader);
                assertTrue(cluster.broker(newLeader).partition(0).raftNode().currentTerm() > oldTerm);
                assertEquals(expected, strings(consumer.readAll(0, 0)),
                        "every acknowledged record is readable from the new leader");

                // The old leader comes back and catches up as a follower.
                Broker restarted = cluster.start(oldLeader);
                long end = cluster.broker(newLeader).partition(0).logEndOffset();
                assertTrue(TestBrokers.await(() -> restarted.partition(0).logEndOffset() == end
                        && restarted.partition(0).highWatermark() == end, 10_000),
                        "the restarted broker converges to the leader's log");
                producer.send(0, TestBrokers.values("after-", 0, 20), Acks.ALL);
                expected.addAll(TestBrokers.strings("after-", 0, 20));
                assertEquals(expected, strings(consumer.readAll(0, 0)));
            }
        }
        // With everything stopped, each broker's files hold the same records exactly once.
        TestBrokers stopped = new TestBrokers(tmp, 20170, 3, 3, 3);
        for (int id = 1; id <= 3; id++) {
            assertEquals(expected, TestBrokers.dataOnDisk(stopped.logDir(id, 0)),
                    "broker " + id + " on disk: no loss, no duplicate, same order");
        }
    }

    @Test
    void minorityCannotProduceCommittedWrites() throws Exception {
        try (TestBrokers cluster = new TestBrokers(tmp, 20180, 3, 1, 3).startAll()) {
            int leader = cluster.awaitLeader(0, 10_000);
            try (Producer producer = new Producer(cluster.bootstrap())) {
                producer.send(0, TestBrokers.values("committed-", 0, 10), Acks.QUORUM);
            }
            Broker survivor = cluster.broker(leader);
            assertTrue(TestBrokers.await(() -> survivor.partition(0).highWatermark() == 11, 5000));

            // Two of three replicas go away. What remains is a minority.
            for (int id = 1; id <= 3; id++) {
                if (id != leader) {
                    cluster.stop(id);
                }
            }
            try (Client raw = new Client()) {
                raw.connect("127.0.0.1", cluster.port(leader));
                for (Acks acks : new Acks[]{Acks.QUORUM, Acks.ALL}) {
                    ProduceResponse r = (ProduceResponse) raw.sendAndWait(
                            new ProduceRequest(0, acks, TestBrokers.values("minority-", 0, 1)), 10, TimeUnit.SECONDS);
                    assertNotEquals(ErrorCode.OK, r.errorCode(), acks + " must not succeed without a majority");
                }
            }
            // The isolated broker gives up leadership instead of pretending to lead.
            assertTrue(TestBrokers.await(() -> !survivor.isLeader(0), 5000),
                    "a leader without a majority steps down");
            assertEquals(11, survivor.partition(0).raftNode().commitIndex(), "nothing new was committed");

            try (Producer producer = new Producer(
                    ClusterAddresses.of(cluster, leader), 500, 1500)) {
                assertThrows(KafkaDsException.class,
                        () -> producer.send(0, TestBrokers.values("minority-", 0, 1), Acks.QUORUM));
                // Even the weakest mode is refused once the broker knows it is not the leader.
                assertThrows(RequestRejectedException.class,
                        () -> producer.send(0, TestBrokers.values("minority-", 0, 1), Acks.LEADER));
            }
            try (Consumer consumer = new Consumer(ClusterAddresses.of(cluster, leader), 500, 1500)) {
                assertThrows(KafkaDsException.class, () -> consumer.fetch(0, 0, 1024),
                        "and it does not serve reads either");
            }
        }
    }

    @Test
    void wholeClusterRestartKeepsCommittedRecordsWithoutDuplicates() throws Exception {
        List<String> expected = TestBrokers.strings("r", 0, 40);
        try (TestBrokers cluster = new TestBrokers(tmp, 20190, 3, 2, 3).startAll()) {
            cluster.awaitAllLeaders(10_000);
            try (Producer producer = new Producer(cluster.bootstrap())) {
                producer.send(0, TestBrokers.values("r", 0, 25), Acks.QUORUM);
                producer.send(0, TestBrokers.values("r", 25, 15), Acks.QUORUM);
                producer.send(1, TestBrokers.values("other", 0, 3), Acks.QUORUM);
            }
        }
        for (int round = 0; round < 2; round++) {
            try (TestBrokers cluster = new TestBrokers(tmp, 20190, 3, 2, 3).startAll()) {
                // Immediately after start-up no replica knows what is committed.
                cluster.awaitAllLeaders(10_000);
                try (Consumer consumer = new Consumer(cluster.bootstrap())) {
                    List<ConsumerRecord> records = consumer.readAll(0, 0);
                    assertEquals(expected, strings(records), "restart " + (round + 1) + ": replay must not duplicate");
                    assertStrictlyIncreasingOffsets(records);
                    assertEquals(TestBrokers.strings("other", 0, 3), strings(consumer.readAll(1, 0)));
                }
            }
        }
    }

    // ---- concurrency ----

    @Test
    void concurrentProducersAndConsumersSeeEveryAcknowledgedRecordExactlyOnce() throws Exception {
        int producers = 8;
        int perProducer = 60;
        int partitions = 3;
        try (TestBrokers cluster = new TestBrokers(tmp, 20200, 3, partitions, 3).startAll()) {
            cluster.awaitAllLeaders(10_000);
            Map<String, RecordMetadata> acknowledged = new ConcurrentHashMap<>();
            List<Throwable> failures = new CopyOnWriteArrayList<>();
            ExecutorService pool = Executors.newFixedThreadPool(producers + 2);
            List<Future<?>> futures = new ArrayList<>();
            // One Producer shared by half of the threads, separate ones for the rest.
            try (Producer shared = new Producer(cluster.bootstrap())) {
                for (int t = 0; t < producers; t++) {
                    final int thread = t;
                    futures.add(pool.submit(() -> {
                        Producer own = thread % 2 == 0 ? shared : new Producer(cluster.bootstrap());
                        try {
                            for (int i = 0; i < perProducer; i++) {
                                String value = "t" + thread + "-" + i;
                                int partition = (thread + i) % partitions;
                                Acks acks = i % 3 == 0 ? Acks.ALL : Acks.QUORUM;
                                acknowledged.put(value,
                                        own.send(partition, List.of(value.getBytes(StandardCharsets.UTF_8)), acks));
                            }
                        } catch (Throwable e) {
                            failures.add(e);
                        } finally {
                            if (own != shared) {
                                own.close();
                            }
                        }
                    }));
                }
                // Readers run while the writers are active and must only ever see a prefix.
                for (int r = 0; r < 2; r++) {
                    futures.add(pool.submit(() -> {
                        try (Consumer consumer = new Consumer(cluster.bootstrap())) {
                            for (int i = 0; i < 40; i++) {
                                for (int p = 0; p < partitions; p++) {
                                    assertStrictlyIncreasingOffsets(consumer.readAll(p, 0));
                                }
                            }
                        } catch (Throwable e) {
                            failures.add(e);
                        }
                    }));
                }
                for (Future<?> f : futures) {
                    f.get(120, TimeUnit.SECONDS);
                }
            } finally {
                pool.shutdownNow();
            }
            assertEquals(List.of(), failures);
            assertEquals(producers * perProducer, acknowledged.size());

            try (Consumer consumer = new Consumer(cluster.bootstrap())) {
                Map<String, Long> seen = new ConcurrentHashMap<>();
                for (int p = 0; p < partitions; p++) {
                    List<ConsumerRecord> records = consumer.readAll(p, 0);
                    assertStrictlyIncreasingOffsets(records);
                    Map<Integer, Integer> lastPerThread = new ConcurrentHashMap<>();
                    for (ConsumerRecord r : records) {
                        String value = new String(r.value(), StandardCharsets.UTF_8);
                        assertNull(seen.put(value, r.offset()), "duplicate record " + value);
                        RecordMetadata ack = acknowledged.get(value);
                        assertNotNull(ack, "unacknowledged record " + value);
                        assertEquals(p, ack.partition(), "response matched to the wrong request: partition");
                        assertEquals(r.offset(), ack.baseOffset(), "response matched to the wrong request: offset");
                        // Each thread sends sequentially, so its records keep their order.
                        int thread = Integer.parseInt(value.substring(1, value.indexOf('-')));
                        int seq = Integer.parseInt(value.substring(value.indexOf('-') + 1));
                        Integer previous = lastPerThread.put(thread, seq);
                        assertTrue(previous == null || previous < seq, "per-producer order in partition " + p);
                    }
                }
                assertEquals(acknowledged.keySet(), seen.keySet(), "every acknowledged record is readable");
            }
        }
    }
}
