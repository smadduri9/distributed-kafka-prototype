package com.kafkads.raft;

import com.kafkads.broker.Broker;
import com.kafkads.config.BrokerConfig;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Properties;
import java.util.function.BooleanSupplier;

import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Phase 3 gate (networked): a 3-broker cluster runs Raft over the real Netty transport,
 * elects a leader, and re-elects a new leader within the election timeout after the
 * current leader is killed.
 */
class RaftFailoverNetworkTest {

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
        p.setProperty("raft.enabled", "true");
        p.setProperty("raft.election.timeout.min.ms", "150");
        p.setProperty("raft.election.timeout.max.ms", "300");
        p.setProperty("raft.heartbeat.interval.ms", "50");
        return BrokerConfig.fromProperties(p);
    }

    private static boolean awaitTrue(BooleanSupplier cond, long timeoutMs) throws InterruptedException {
        long deadline = System.currentTimeMillis() + timeoutMs;
        while (System.currentTimeMillis() < deadline) {
            if (cond.getAsBoolean()) {
                return true;
            }
            Thread.sleep(20);
        }
        return cond.getAsBoolean();
    }

    private Optional<Broker> currentLeader() {
        return brokers.stream().filter(Broker::isLeader).findFirst();
    }

    @Test
    void electsAndReElectsLeaderOverNetwork() throws Exception {
        int base = 19301;
        int p1 = base, p2 = base + 1, p3 = base + 2;
        Broker b1 = new Broker(cfg(1, p1, "2@127.0.0.1:" + p2 + ",3@127.0.0.1:" + p3));
        Broker b2 = new Broker(cfg(2, p2, "1@127.0.0.1:" + p1 + ",3@127.0.0.1:" + p3));
        Broker b3 = new Broker(cfg(3, p3, "1@127.0.0.1:" + p1 + ",2@127.0.0.1:" + p2));
        brokers.add(b1);
        brokers.add(b2);
        brokers.add(b3);
        for (Broker b : brokers) {
            b.start();
        }
        for (Broker b : brokers) {
            b.startRaft();
        }

        assertTrue(awaitTrue(() -> currentLeader().isPresent(), 5000), "a leader should be elected");
        Broker firstLeader = currentLeader().orElseThrow();
        int firstLeaderId = firstLeader.config().nodeId();
        int firstTerm = firstLeader.raftNode().currentTerm();

        // Kill the leader.
        firstLeader.close();
        brokers.remove(firstLeader);

        // A new leader must emerge from the surviving quorum, in a higher term.
        assertTrue(awaitTrue(() -> currentLeader().isPresent(), 5000),
                "a new leader should be elected after failover");
        Broker newLeader = currentLeader().orElseThrow();
        assertNotEquals(firstLeaderId, newLeader.config().nodeId(),
                "the new leader must be a different node");
        assertTrue(newLeader.raftNode().currentTerm() > firstTerm,
                "the new leader's term must be higher");
    }
}
