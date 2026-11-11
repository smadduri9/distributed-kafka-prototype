package com.kafkads.regression;

import com.kafkads.broker.Broker;
import com.kafkads.broker.PartitionReplica;
import com.kafkads.client.Consumer;
import com.kafkads.client.KafkaDsException;
import com.kafkads.network.Client;
import com.kafkads.protocol.Acks;
import com.kafkads.protocol.ErrorCode;
import com.kafkads.protocol.messages.FetchRequest;
import com.kafkads.protocol.messages.FetchResponse;
import com.kafkads.protocol.messages.ProduceRequest;
import com.kafkads.protocol.messages.ProduceResponse;
import com.kafkads.raft.FileRaftMetaStore;
import com.kafkads.storage.Log;
import com.kafkads.storage.Record;
import com.kafkads.testutil.TestBrokers;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.net.InetSocketAddress;
import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Regression tests for three properties of the acknowledgment and visibility contract:
 * acks=ALL covers the last record of a batch, a restart never makes uncommitted records
 * visible, and acknowledged writes are replicated and readable.
 *
 * <p>Three brokers in one JVM over loopback sockets.
 */
class CommitVisibilityRegressionTest {

    @TempDir
    Path tmp;

    private static ProduceResponse produce(Client client, Acks acks, List<byte[]> values) throws Exception {
        return (ProduceResponse) client.sendAndWait(new ProduceRequest(0, acks, values), 10, TimeUnit.SECONDS);
    }

    /** acks=ALL must wait for every record of the batch, including the last (and only) one. */
    @Test
    void acksAllSingleRecordIsNotAcknowledgedWithoutFollowers() throws Exception {
        try (TestBrokers cluster = new TestBrokers(tmp, 19601, 3, 1, 3).startAll()) {
            int leader = cluster.awaitLeader(0, 5000);
            int follower = leader == 1 ? 2 : 1;
            cluster.stop(follower);

            try (Client client = new Client()) {
                client.connect("127.0.0.1", cluster.port(leader));
                ProduceResponse all = produce(client, Acks.ALL, TestBrokers.values("m", 0, 1));
                assertNotEquals(ErrorCode.OK, all.errorCode(),
                        "acks=all acknowledged a record that one replica does not have");
                assertEquals(ErrorCode.NOT_ENOUGH_REPLICAS, all.errorCode());
            }

            // And with no follower at all, not even a quorum acknowledgement may succeed.
            int other = 6 - leader - follower;
            cluster.stop(other);
            try (Client client = new Client()) {
                client.connect("127.0.0.1", cluster.port(leader));
                ProduceResponse quorum = produce(client, Acks.QUORUM, TestBrokers.values("n", 0, 1));
                assertNotEquals(ErrorCode.OK, quorum.errorCode(),
                        "a record that exists on no follower was acknowledged as committed");
            }
        }
    }

    /**
     * Records that were never committed must stay invisible after a restart.
     *
     * <p>The on-disk state is built directly: a replica whose log holds five records of
     * term 1 that no other replica has. That is exactly what a leader that appended
     * without reaching a majority leaves behind.
     */
    @Test
    void restartDoesNotExposeUncommittedRecords() throws Exception {
        TestBrokers cluster = new TestBrokers(tmp, 19611, 3, 1, 3);
        new FileRaftMetaStore(cluster.partitionDir(1, 0).resolve("raft")).save(1, 1);
        try (Log log = new Log(cluster.logDir(1, 0), 1 << 20, 4096)) {
            log.append(1, Record.DATA, TestBrokers.values("uncommitted-", 0, 5));
            log.flush();
        }

        try (cluster) {
            Broker alone = cluster.start(1); // brokers 2 and 3 are down
            Thread.sleep(1500);              // several election timeouts
            PartitionReplica.View view = alone.partition(0).view();
            assertEquals(5, view.logEndOffset(), "the records are on local disk");
            assertEquals(0, view.highWatermark(), "but local presence does not make them committed");

            try (Client client = new Client()) {
                client.connect("127.0.0.1", cluster.port(1));
                FetchResponse f = (FetchResponse) client.sendAndWait(
                        new FetchRequest(0, 0, 1 << 20), 10, TimeUnit.SECONDS);
                assertEquals(0, f.records().size(), "uncommitted records became visible after a restart");
                assertNotEquals(ErrorCode.OK, f.errorCode(), "a replica without a quorum must not serve reads");
            }
            try (Consumer consumer = new Consumer(
                    List.of(new InetSocketAddress("127.0.0.1", cluster.port(1))), 500, 1000)) {
                assertThrows(KafkaDsException.class, () -> consumer.fetch(0, 0, 1 << 20));
            }

            // Once a majority exists, Raft may legitimately commit those entries: the replica
            // holding them has the most complete log, wins the election and replicates them.
            cluster.start(2);
            assertEquals(1, cluster.awaitLeader(0, 5000), "only the replica with the longer log can win");
            try (Consumer consumer = new Consumer(cluster.bootstrap())) {
                assertEquals(5, consumer.readAll(0, 0).size());
            }
        }
    }

    /** An acknowledged write is replicated to every replica and readable from the leader. */
    @Test
    void raftModeReplicatesClientWrites() throws Exception {
        try (TestBrokers cluster = new TestBrokers(tmp, 19621, 3, 1, 3).startAll()) {
            int leader = cluster.awaitLeader(0, 5000);
            try (Client client = new Client()) {
                client.connect("127.0.0.1", cluster.port(leader));
                ProduceResponse r = produce(client, Acks.QUORUM, TestBrokers.values("m", 0, 10));
                assertEquals(ErrorCode.OK, r.errorCode());

                FetchResponse f = (FetchResponse) client.sendAndWait(
                        new FetchRequest(0, 0, 1 << 20), 10, TimeUnit.SECONDS);
                assertEquals(10, f.records().size(), "acknowledged records are not readable");
                long end = r.baseOffset() + 10;
                assertTrue(TestBrokers.await(() -> cluster.runningIds().stream()
                                .allMatch(id -> cluster.broker(id).partition(0).logEndOffset() >= end
                                        && cluster.broker(id).partition(0).highWatermark() >= end), 5000),
                        "records were not replicated to the other brokers");
            }
        }
    }
}
