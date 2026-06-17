package com.kafkads.replication;

import com.kafkads.broker.Broker;
import com.kafkads.config.BrokerConfig;
import com.kafkads.network.Client;
import com.kafkads.protocol.Acks;
import com.kafkads.protocol.messages.FetchRequest;
import com.kafkads.protocol.messages.FetchResponse;
import com.kafkads.protocol.messages.ProduceRequest;
import com.kafkads.protocol.messages.ProduceResponse;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Phase 2 gate: a 3-broker in-process cluster. Verifies acks=all replication and the
 * core safety property: when a follower dies, the high-watermark must not advance past
 * un-replicated records, and consumers must never read them.
 */
class ReplicationTest {

    @TempDir
    Path tempDir;

    private final List<Broker> brokers = new ArrayList<>();

    @AfterEach
    void tearDown() {
        for (Broker b : brokers) {
            b.close();
        }
        brokers.clear();
    }

    private BrokerConfig cfg(int id, int port, String peers) {
        Properties p = new Properties();
        p.setProperty("node.id", Integer.toString(id));
        p.setProperty("port", Integer.toString(port));
        p.setProperty("peers", peers);
        p.setProperty("log.dir", tempDir.resolve("broker-" + id).toString());
        p.setProperty("partition.count", "1");
        return BrokerConfig.fromProperties(p);
    }

    private List<Broker> startCluster(int basePort) throws InterruptedException {
        int p1 = basePort, p2 = basePort + 1, p3 = basePort + 2;
        BrokerConfig c1 = cfg(1, p1, "2@127.0.0.1:" + p2 + ",3@127.0.0.1:" + p3);
        BrokerConfig c2 = cfg(2, p2, "1@127.0.0.1:" + p1 + ",3@127.0.0.1:" + p3);
        BrokerConfig c3 = cfg(3, p3, "1@127.0.0.1:" + p1 + ",2@127.0.0.1:" + p2);
        Broker b1 = new Broker(c1);
        Broker b2 = new Broker(c2);
        Broker b3 = new Broker(c3);
        brokers.add(b1);
        brokers.add(b2);
        brokers.add(b3);
        b1.start();
        b2.start();
        b3.start();
        b1.startReplication();
        b2.startReplication();
        b3.startReplication();
        return List.of(b1, b2, b3);
    }

    private static boolean awaitTrue(BooleanSupplier cond, long timeoutMs) throws InterruptedException {
        long deadline = System.currentTimeMillis() + timeoutMs;
        while (System.currentTimeMillis() < deadline) {
            if (cond.getAsBoolean()) {
                return true;
            }
            Thread.sleep(10);
        }
        return cond.getAsBoolean();
    }

    private static List<byte[]> values(int from, int count) {
        List<byte[]> v = new ArrayList<>(count);
        for (int i = from; i < from + count; i++) {
            v.add(("m" + i).getBytes(StandardCharsets.UTF_8));
        }
        return v;
    }

    @Test
    void acksAllReplicatesToAllFollowers() throws Exception {
        List<Broker> cluster = startCluster(19210);
        Broker leader = cluster.get(0);
        assertTrue(leader.isLeader());

        try (Client client = new Client()) {
            client.connect("127.0.0.1", leader.config().port());
            ProduceResponse pr = (ProduceResponse) client.sendAndWait(
                    new ProduceRequest(0, Acks.ALL, values(0, 100)), 10, TimeUnit.SECONDS);
            assertEquals(ProduceResponse.OK, pr.errorCode());
            assertEquals(0, pr.baseOffset());

            // All replicas converge to 100 records and the high-watermark reaches 100.
            assertTrue(awaitTrue(() -> leader.partition(0).highWatermark() == 100, 5000),
                    "leader high-watermark should reach 100");
            assertEquals(100, cluster.get(1).partition(0).logEndOffset());
            assertEquals(100, cluster.get(2).partition(0).logEndOffset());

            FetchResponse fr = (FetchResponse) client.sendAndWait(
                    new FetchRequest(0, 0, 1 << 20), 10, TimeUnit.SECONDS);
            assertEquals(100, fr.highWatermark());
            assertEquals(100, fr.records().size());
        }
    }

    @Test
    void highWatermarkDoesNotAdvancePastUnreplicatedRecordsWhenFollowerDies() throws Exception {
        List<Broker> cluster = startCluster(19220);
        Broker leader = cluster.get(0);
        Broker follower2 = cluster.get(1);
        Broker follower3 = cluster.get(2);

        try (Client client = new Client()) {
            client.connect("127.0.0.1", leader.config().port());

            // Commit 50 records with all three replicas alive.
            ProduceResponse pr1 = (ProduceResponse) client.sendAndWait(
                    new ProduceRequest(0, Acks.ALL, values(0, 50)), 10, TimeUnit.SECONDS);
            assertEquals(ProduceResponse.OK, pr1.errorCode());
            assertTrue(awaitTrue(() -> leader.partition(0).highWatermark() == 50, 5000),
                    "high-watermark should reach 50 with all replicas alive");

            // Kill one follower.
            follower3.close();
            brokers.remove(follower3);

            // Attempt to produce 10 more with acks=all: cannot commit (a replica is down).
            ProduceResponse pr2 = (ProduceResponse) client.sendAndWait(
                    new ProduceRequest(0, Acks.ALL, values(50, 10)), 10, TimeUnit.SECONDS);
            assertEquals(ProduceResponse.NOT_ENOUGH_REPLICAS, pr2.errorCode(),
                    "acks=all must not succeed while a replica is down");

            // Safety: high-watermark stays at 50, even though the live follower advanced.
            assertEquals(50, leader.partition(0).highWatermark(),
                    "high-watermark must not advance past un-replicated records");
            assertTrue(follower2.partition(0).logEndOffset() >= 50);

            // Consumers must never see the un-committed records.
            FetchResponse fr = (FetchResponse) client.sendAndWait(
                    new FetchRequest(0, 0, 1 << 20), 10, TimeUnit.SECONDS);
            assertEquals(50, fr.highWatermark());
            assertEquals(50, fr.records().size());
        }
    }
}
