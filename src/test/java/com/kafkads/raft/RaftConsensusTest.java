package com.kafkads.raft;

import com.kafkads.raft.messages.AppendEntriesRequest;
import com.kafkads.raft.messages.AppendEntriesResponse;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Deterministic, in-memory Raft tests (synchronous fake transport, manual clock). They
 * cover leader election, log replication and commit, re-election, and the safety
 * properties: committed entries are never lost, stale candidates cannot win, conflicting
 * tails are overwritten, and nothing is committed without a majority.
 *
 * <p>Index arithmetic: every election appends one no-op entry, so the first client
 * command of the first leader has index 2.
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
    void newLeaderCommitsANoOpAndBecomesReady() {
        TestCluster c = new TestCluster(1, 2, 3);
        assertFalse(c.node(1).isLeaderReady());
        c.node(1).startElection();

        assertEquals(1, c.node(1).lastLogIndex(), "the no-op entry");
        assertEquals(1, c.node(1).commitIndex());
        assertTrue(c.node(1).isLeaderReady());
        assertEquals(List.of(), c.appliedStrings(1), "the no-op is not client data");
    }

    @Test
    void replicatesAndCommitsEntries() {
        TestCluster c = new TestCluster(1, 2, 3);
        c.node(1).startElection();

        c.node(1).propose(TestCluster.cmd("a"));
        c.node(1).propose(TestCluster.cmd("b"));

        assertEquals(3, c.node(1).commitIndex(), "no-op + a + b");
        assertEquals(3, c.node(1).minMatchIndex(), "every replica has all three entries");
        assertEquals(List.of("a", "b"), c.appliedStrings(1));
        assertEquals(List.of("a", "b"), c.appliedStrings(2));
        assertEquals(List.of("a", "b"), c.appliedStrings(3));
    }

    @Test
    void proposeAppendsABatchAtomicallyWithConsecutiveIndexes() {
        TestCluster c = new TestCluster(1, 2, 3);
        c.node(1).startElection();

        long first = c.node(1).propose(List.of(TestCluster.cmd("a"), TestCluster.cmd("b"), TestCluster.cmd("c")));

        assertEquals(2, first);
        assertEquals(4, c.node(1).commitIndex());
        assertEquals(List.of("a", "b", "c"), c.appliedStrings(3));
        assertEquals(-1, c.node(2).propose(TestCluster.cmd("x")), "a follower refuses proposals");
    }

    @Test
    void reElectsLeaderAfterLeaderFails() {
        TestCluster c = new TestCluster(1, 2, 3);
        c.node(1).startElection();
        c.node(1).propose(TestCluster.cmd("x"));

        // leader 1 crashes
        c.setDown(1, true);

        // a follower times out and starts an election
        c.node(2).startElection();
        assertTrue(c.node(2).isLeader(), "node 2 should win with node 3's vote");
        assertTrue(c.node(2).currentTerm() > 1);

        // new leader can still make progress with the remaining quorum
        c.node(2).propose(TestCluster.cmd("y"));
        assertEquals(List.of("x", "y"), c.appliedStrings(2));
        assertEquals(List.of("x", "y"), c.appliedStrings(3));
    }

    @Test
    void staleCandidateWithShorterLogCannotWin() {
        TestCluster c = new TestCluster(1, 2, 3);
        c.node(1).startElection();
        c.node(1).propose(TestCluster.cmd("a"));

        // isolate node 3, then commit more entries via the {1,2} quorum
        c.setDown(3, true);
        c.node(1).propose(TestCluster.cmd("b"));
        c.node(1).propose(TestCluster.cmd("c"));
        assertEquals(4, c.node(1).commitIndex());

        // node 3 (log = [no-op, a]) reconnects and tries to become leader
        c.setDown(3, false);
        c.node(3).startElection();

        assertFalse(c.node(3).isLeader(), "a candidate with a shorter log must be denied");
        // committed entries on the up-to-date replicas are preserved
        assertEquals(4, c.node(1).lastLogIndex());
        assertEquals(4, c.node(2).lastLogIndex());
    }

    @Test
    void overwritesConflictingFollowerTail() {
        TestCluster c = new TestCluster(1, 2, 3);
        c.node(1).startElection();
        c.node(1).propose(TestCluster.cmd("e1")); // committed everywhere

        // Partition the old leader; it appends uncommitted entries it can't replicate.
        c.setDown(1, true);
        c.node(1).propose(TestCluster.cmd("e2-stale"));
        c.node(1).propose(TestCluster.cmd("e3-stale"));
        assertEquals(4, c.node(1).lastLogIndex());
        assertEquals(2, c.node(1).commitIndex(), "stale entries must not be committed");

        // A new leader is elected in a higher term and commits its own entry.
        c.node(2).startElection();
        assertTrue(c.node(2).isLeader());
        c.node(2).propose(TestCluster.cmd("f2"));

        // Heal the partition; the old leader must drop its conflicting tail and converge.
        c.setDown(1, false);
        c.node(2).broadcastHeartbeat();

        assertEquals(c.node(2).lastLogIndex(), c.node(1).lastLogIndex(),
                "old leader converges to the new leader's log length");
        assertEquals(List.of("e1", "f2"), c.appliedStrings(2));
        assertEquals(List.of("e1", "f2"), c.logStrings(1), "the stale tail is gone from the old leader's log");
        assertEquals(List.of("e1", "f2"), c.appliedStrings(1));
        assertFalse(c.node(1).isLeader());
    }

    @Test
    void minorityLeaderCannotCommit() {
        TestCluster c = new TestCluster(1, 2, 3);
        c.node(1).startElection();
        c.node(1).propose(TestCluster.cmd("committed"));
        long commitBefore = c.node(1).commitIndex();

        c.setDown(2, true);
        c.setDown(3, true);
        long index = c.node(1).propose(TestCluster.cmd("lonely"));

        assertTrue(index > commitBefore, "the entry is appended locally");
        assertEquals(commitBefore, c.node(1).commitIndex(), "but one of three replicas is not a majority");
        assertEquals(List.of("committed"), c.appliedStrings(1));
    }

    @Test
    void leaderWithoutAMajorityStepsDownAfterAnElectionTimeout() {
        TestCluster c = new TestCluster(1, 2, 3);
        c.node(1).startElection();
        assertTrue(c.node(1).isLeader());

        c.setDown(2, true);
        c.setDown(3, true);
        c.now += 100;
        c.node(1).tick(c.now);
        assertTrue(c.node(1).isLeader(), "still within the election timeout");

        c.now += 400; // more than the 300 ms maximum election timeout with no acknowledgements
        c.node(1).tick(c.now);
        assertFalse(c.node(1).isLeader(), "a leader that cannot reach a majority must step down");
        assertEquals(-1, c.node(1).leaderId());
        assertEquals(-1, c.node(1).propose(TestCluster.cmd("rejected")));
    }

    @Test
    void leaderWithOneReachableFollowerStaysLeaderAndCommits() {
        TestCluster c = new TestCluster(1, 2, 3);
        c.node(1).startElection();
        c.setDown(3, true);

        for (int i = 0; i < 20; i++) {
            c.now += 50;
            c.node(1).tick(c.now); // heartbeats keep node 2's acknowledgement fresh
        }
        assertTrue(c.node(1).isLeader());
        c.node(1).propose(TestCluster.cmd("two-of-three"));
        assertEquals(List.of("two-of-three"), c.appliedStrings(1));
        assertEquals(List.of("two-of-three"), c.appliedStrings(2));
        assertTrue(c.node(1).minMatchIndex() < c.node(1).commitIndex(),
                "committed by the majority although one replica is behind");
    }

    @Test
    void restartForgetsCommitIndexUntilALeaderReEstablishesIt() {
        TestCluster c = new TestCluster(1, 2, 3);
        c.node(1).startElection();
        c.node(1).propose(TestCluster.cmd("a"));
        assertEquals(List.of("a"), c.appliedStrings(2));

        // Node 2 restarts alone: its log still holds the entries, but nothing tells it they
        // are committed, so it must not treat them as such.
        c.setDown(1, true);
        c.setDown(3, true);
        c.restart(2);
        assertEquals(2, c.node(2).lastLogIndex(), "entries survive the restart");
        assertEquals(0, c.node(2).commitIndex(), "commit index does not");
        assertEquals(List.of(), c.appliedStrings(2));
        c.node(2).startElection();
        assertFalse(c.node(2).isLeader(), "no majority, no leadership");
        assertEquals(0, c.node(2).commitIndex());

        // With a majority back, a leader is elected and re-commits the existing entries.
        c.setDown(3, false);
        c.restart(3);
        c.node(2).startElection();
        assertTrue(c.node(2).isLeader());
        assertEquals(List.of("a"), c.appliedStrings(2), "no entry was lost and none was duplicated");
        assertEquals(List.of("a"), c.logStrings(2));
    }

    @Test
    void restartPreservesTermAndVote() {
        TestCluster c = new TestCluster(1, 2, 3);
        c.node(1).startElection();
        int term = c.node(2).currentTerm();
        // Peers are asked in id order, so node 2's vote is the one that made node 1 leader.
        assertEquals(1, c.node(2).votedFor());

        c.restart(2);
        assertEquals(term, c.node(2).currentTerm());
        assertEquals(1, c.node(2).votedFor(), "a restarted node must not be able to vote twice in a term");
    }

    @Test
    void followerDoesNotCommitAStaleSuffixTheLeaderHasNotVerified() {
        TestCluster c = new TestCluster(1, 2, 3);
        c.node(1).startElection();
        c.node(1).propose(TestCluster.cmd("a")); // index 2, committed

        // Node 1 is cut off and appends two entries nobody else has.
        c.setDown(1, true);
        c.node(1).propose(TestCluster.cmd("stale-1"));
        c.node(1).propose(TestCluster.cmd("stale-2"));

        // Node 2 becomes leader in term 2 and commits past the length of node 1's log.
        c.node(2).startElection();
        c.node(2).propose(TestCluster.cmd("b"));
        c.node(2).propose(TestCluster.cmd("c"));
        long leaderCommit = c.node(2).commitIndex();
        assertTrue(leaderCommit >= 4);

        // A heartbeat that only vouches for the prefix up to index 2 must not let node 1
        // mark its own stale entries at 3 and 4 as committed.
        AppendEntriesResponse resp = c.node(1).handleAppendEntries(new AppendEntriesRequest(
                0, c.node(2).currentTerm(), 2, 2, 1, List.of(), leaderCommit));
        assertTrue(resp.success());
        assertEquals(2, c.node(1).commitIndex());
        assertEquals(List.of("a"), c.appliedStrings(1));
    }

    @Test
    void startupRejectsALogWithoutItsTermRecord() {
        InMemoryRaftLog log = new InMemoryRaftLog();
        log.append(List.of(new LogEntry(1, 3, TestCluster.cmd("orphan"))));
        assertThrows(IllegalStateException.class, () -> RaftNode.builder(1, java.util.Set.of(2, 3))
                .log(log).metaStore(new InMemoryRaftMetaStore()).build());
    }
}
