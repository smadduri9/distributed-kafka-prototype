package com.kafkads.regression;

import com.kafkads.broker.Broker;
import com.kafkads.config.BrokerConfig;
import com.kafkads.network.Client;
import com.kafkads.protocol.Acks;
import com.kafkads.protocol.messages.FetchRequest;
import com.kafkads.protocol.messages.FetchResponse;
import com.kafkads.protocol.messages.ProduceRequest;
import com.kafkads.protocol.messages.ProduceResponse;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;

/**
 * Assertion-based reproductions of three defects reported in review. Each test states
 * the behaviour the system is supposed to have; all three fail against the
 * static-replication / Raft-election split design.
 */
class ReportedDefectsTest {

    @TempDir
    Path tmp;

    private BrokerConfig cfg(int id, int port, String peers, boolean raft) {
        Properties p = new Properties();
        p.setProperty("node.id", Integer.toString(id));
        p.setProperty("port", Integer.toString(port));
        p.setProperty("peers", peers);
        p.setProperty("log.dir", tmp.resolve("broker-" + id).toString());
        if (raft) {
            p.setProperty("raft.enabled", "true");
            p.setProperty("raft.election.timeout.min.ms", "150");
            p.setProperty("raft.election.timeout.max.ms", "300");
            p.setProperty("raft.heartbeat.interval.ms", "50");
        }
        return BrokerConfig.fromProperties(p);
    }

    private static List<byte[]> values(int n) {
        List<byte[]> v = new ArrayList<>();
        for (int i = 0; i < n; i++) {
            v.add(("m" + i).getBytes(StandardCharsets.UTF_8));
        }
        return v;
    }

    /** acks=all must not succeed for a single record that no follower has. */
    @Test
    void acksAllSingleRecordIsNotAcknowledgedWithoutFollowers() throws Exception {
        int a = 19601;
        Broker leader = new Broker(cfg(1, a, "2@127.0.0.1:19602,3@127.0.0.1:19603", false));
        leader.start();
        leader.startReplication();
        try (Client client = new Client()) {
            client.connect("127.0.0.1", a);
            ProduceResponse r = (ProduceResponse) client.sendAndWait(
                    new ProduceRequest(0, Acks.ALL, values(1)), 10, TimeUnit.SECONDS);
            assertNotEquals(ProduceResponse.OK, r.errorCode(),
                    "acks=all acknowledged a record that exists on no follower");
        } finally {
            leader.close();
        }
    }

    /** Records that were never committed must stay invisible after the leader restarts. */
    @Test
    void restartDoesNotExposeUncommittedRecords() throws Exception {
        int a = 19611;
        String peers = "2@127.0.0.1:19612,3@127.0.0.1:19613";
        Broker leader = new Broker(cfg(1, a, peers, false));
        leader.start();
        leader.startReplication();
        try (Client client = new Client()) {
            client.connect("127.0.0.1", a);
            ProduceResponse r = (ProduceResponse) client.sendAndWait(
                    new ProduceRequest(0, Acks.ALL, values(5)), 10, TimeUnit.SECONDS);
            assertEquals(ProduceResponse.NOT_ENOUGH_REPLICAS, r.errorCode());
        } finally {
            leader.close();
        }
        Thread.sleep(300);
        leader = new Broker(cfg(1, a, peers, false));
        leader.start();
        leader.startReplication();
        try (Client client = new Client()) {
            client.connect("127.0.0.1", a);
            FetchResponse f = (FetchResponse) client.sendAndWait(
                    new FetchRequest(0, 0, 1 << 20), 10, TimeUnit.SECONDS);
            assertEquals(0, f.records().size(),
                    "uncommitted records became visible after a restart");
        } finally {
            leader.close();
        }
    }

    /** With Raft leadership, an acknowledged write must reach the other brokers and be readable. */
    @Test
    void raftModeReplicatesClientWrites() throws Exception {
        int a = 19621, b = 19622, c = 19623;
        List<Broker> brokers = List.of(
                new Broker(cfg(1, a, "2@127.0.0.1:" + b + ",3@127.0.0.1:" + c, true)),
                new Broker(cfg(2, b, "1@127.0.0.1:" + a + ",3@127.0.0.1:" + c, true)),
                new Broker(cfg(3, c, "1@127.0.0.1:" + a + ",2@127.0.0.1:" + b, true)));
        try {
            for (Broker x : brokers) {
                x.start();
            }
            for (Broker x : brokers) {
                x.startReplication();
                x.startRaft();
            }
            Broker leader = null;
            for (int i = 0; i < 250 && leader == null; i++) {
                leader = brokers.stream().filter(Broker::isLeader).findFirst().orElse(null);
                Thread.sleep(20);
            }
            try (Client client = new Client()) {
                client.connect("127.0.0.1", leader.config().port());
                ProduceResponse r = (ProduceResponse) client.sendAndWait(
                        new ProduceRequest(0, Acks.LEADER, values(10)), 10, TimeUnit.SECONDS);
                assertEquals(ProduceResponse.OK, r.errorCode());
                Thread.sleep(1000);
                FetchResponse f = (FetchResponse) client.sendAndWait(
                        new FetchRequest(0, 0, 1 << 20), 10, TimeUnit.SECONDS);
                assertEquals(10, f.records().size(), "acknowledged records are not readable in Raft mode");
            }
            long replicasWithData = brokers.stream()
                    .filter(x -> x.partition(0).logEndOffset() >= 10).count();
            assertEquals(3, replicasWithData, "records were not replicated to the other brokers");
        } finally {
            for (Broker x : brokers) {
                x.close();
            }
        }
    }
}
