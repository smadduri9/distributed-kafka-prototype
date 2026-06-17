package com.kafkads.raft;

import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Phase 3 gate: deterministic Raft consensus tests covering leader election, log
 * replication and commit, leader-failure re-election, and the core safety properties
 * (committed entries are never lost; stale candidates cannot win; conflicting tails
 * are overwritten).
 */
class RaftConsensusTest {

    @Test
    void electsASingleLeader() {
        TestCluster c = new TestCluster(1, 2, 3);
        c.node(1).startElection();

        assertEquals(1, c.leaders().size(), "exactly one leader");
        assertTrue(c.node(1).isLeader());
        assertEquals(RaftRole.FOLLOWER, c.node(2).role());
        assertEquals(RaftRole.FOLLOWER, c.node(3).role());
        assertEquals(c.node(1).currentTerm(), c.node(2).currentTerm());
        assertEquals(c.node(1).currentTerm(), c.node(3).currentTerm());
    }

    @Test
    void replicatesAndCommitsEntries() {
        TestCluster c = new TestCluster(1, 2, 3);
        c.node(1).startElection();

        c.node(1).propose(TestCluster.cmd("a"));
        c.node(1).propose(TestCluster.cmd("b"));
        // a second round of heartbeats propagates the advanced commit index to followers
        c.node(1).broadcastHeartbeat();

        assertEquals(2, c.node(1).commitIndex());
        assertEquals(List.of("a", "b"), c.appliedStrings(1));
        assertEquals(List.of("a", "b"), c.appliedStrings(2));
        assertEquals(List.of("a", "b"), c.appliedStrings(3));
    }

    @Test
    void reElectsLeaderAfterLeaderFails() {
        TestCluster c = new TestCluster(1, 2, 3);
        c.node(1).startElection();
        c.node(1).propose(TestCluster.cmd("x"));
        c.node(1).broadcastHeartbeat();

        // leader 1 crashes
        c.setDown(1, true);

        // a follower times out and starts an election
        c.node(2).startElection();
        assertTrue(c.node(2).isLeader(), "node 2 should win with node 3's vote");
        assertTrue(c.node(2).currentTerm() > 1);

        // new leader can still make progress with the remaining quorum
        c.node(2).propose(TestCluster.cmd("y"));
        c.node(2).broadcastHeartbeat();
        assertEquals(List.of("x", "y"), c.appliedStrings(2));
        assertEquals(List.of("x", "y"), c.appliedStrings(3));
    }

    @Test
    void staleCandidateWithShorterLogCannotWin() {
        TestCluster c = new TestCluster(1, 2, 3);
        c.node(1).startElection();
        c.node(1).propose(TestCluster.cmd("a"));
        c.node(1).broadcastHeartbeat();

        // isolate node 3, then commit more entries via the {1,2} quorum
        c.setDown(3, true);
        c.node(1).propose(TestCluster.cmd("b"));
        c.node(1).propose(TestCluster.cmd("c"));
        assertEquals(3, c.node(1).commitIndex());

        // node 3 (log = [a]) reconnects and tries to become leader
        c.setDown(3, false);
        c.node(3).startElection();

        assertFalse(c.node(3).isLeader(), "a candidate with a shorter log must be denied");
        // committed entries on the up-to-date replicas are preserved
        assertEquals(3, c.node(1).lastLogIndex());
        assertEquals(3, c.node(2).lastLogIndex());
    }

    @Test
    void overwritesConflictingFollowerTail() {
        TestCluster c = new TestCluster(1, 2, 3);
        c.node(1).startElection();
        c.node(1).propose(TestCluster.cmd("e1"));
        c.node(1).broadcastHeartbeat(); // commit e1 everywhere

        // Partition the old leader; it appends uncommitted entries it can't replicate.
        c.setDown(1, true);
        c.node(1).propose(TestCluster.cmd("e2-stale"));
        c.node(1).propose(TestCluster.cmd("e3-stale"));
        assertEquals(3, c.node(1).lastLogIndex());
        assertEquals(1, c.node(1).commitIndex(), "stale entries must not be committed");

        // A new leader is elected in a higher term and commits its own entry.
        c.node(2).startElection();
        assertTrue(c.node(2).isLeader());
        c.node(2).propose(TestCluster.cmd("f2"));
        c.node(2).broadcastHeartbeat();

        // Heal the partition; the old leader must drop its conflicting tail and converge.
        c.setDown(1, false);
        for (int i = 0; i < 6; i++) {
            c.node(2).broadcastHeartbeat();
        }

        assertEquals(c.node(2).lastLogIndex(), c.node(1).lastLogIndex(),
                "old leader converges to the new leader's log length");
        assertEquals(List.of("e1", "f2"), c.appliedStrings(2));
        // node 1's stale, uncommitted entries were overwritten (never applied)
        assertFalse(c.appliedStrings(1).contains("e2-stale"));
        assertFalse(c.appliedStrings(1).contains("e3-stale"));
    }
}
