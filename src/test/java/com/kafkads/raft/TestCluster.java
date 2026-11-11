package com.kafkads.raft;

import com.kafkads.raft.messages.AppendEntriesRequest;
import com.kafkads.raft.messages.AppendEntriesResponse;
import com.kafkads.raft.messages.RequestVoteRequest;
import com.kafkads.raft.messages.RequestVoteResponse;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;

/**
 * A deterministic in-memory Raft cluster for testing. RPCs are delivered synchronously
 * by invoking the target node's handler, so a single call to {@code startElection} or
 * {@code propose} resolves an entire round of messaging before returning. Reachability
 * is controlled via {@link #setDown} to simulate crashes and partitions, and time is a
 * manually advanced clock.
 *
 * <p>Logs and term/vote records are held outside the nodes, so {@link #restart} can
 * rebuild a node from its "disk" the way a real restart does.
 */
final class TestCluster {

    final Map<Integer, RaftNode> nodes = new LinkedHashMap<>();
    final Map<Integer, InMemoryRaftLog> logs = new HashMap<>();
    final Map<Integer, InMemoryRaftMetaStore> metas = new HashMap<>();
    final Set<Integer> down = new HashSet<>();
    final int[] ids;
    long now = 1_000_000;

    TestCluster(int... ids) {
        this.ids = ids;
        for (int id : ids) {
            logs.put(id, new InMemoryRaftLog());
            metas.put(id, new InMemoryRaftMetaStore());
        }
        for (int id : ids) {
            nodes.put(id, build(id));
        }
    }

    private RaftNode build(int id) {
        Set<Integer> peers = new HashSet<>();
        for (int other : ids) {
            if (other != id) {
                peers.add(other);
            }
        }
        final int self = id;
        RaftTransport transport = new RaftTransport() {
            @Override
            public CompletableFuture<RequestVoteResponse> requestVote(int peer, RequestVoteRequest req) {
                if (unreachable(self, peer)) {
                    return CompletableFuture.failedFuture(new RuntimeException("unreachable"));
                }
                return CompletableFuture.completedFuture(nodes.get(peer).handleRequestVote(req));
            }

            @Override
            public CompletableFuture<AppendEntriesResponse> appendEntries(int peer, AppendEntriesRequest req) {
                if (unreachable(self, peer)) {
                    return CompletableFuture.failedFuture(new RuntimeException("unreachable"));
                }
                return CompletableFuture.completedFuture(nodes.get(peer).handleAppendEntries(req));
            }
        };
        return RaftNode.builder(id, peers)
                .transport(transport)
                .log(logs.get(id))
                .metaStore(metas.get(id))
                .clock(() -> now)
                .timing(150, 300, 50)
                .build();
    }

    /** Discards a node's volatile state and rebuilds it from its log and term/vote record. */
    void restart(int id) {
        nodes.put(id, build(id));
    }

    private boolean unreachable(int from, int to) {
        return down.contains(from) || down.contains(to);
    }

    void setDown(int id, boolean isDown) {
        if (isDown) {
            down.add(id);
        } else {
            down.remove(id);
        }
    }

    RaftNode node(int id) {
        return nodes.get(id);
    }

    List<RaftNode> leaders() {
        List<RaftNode> result = new ArrayList<>();
        for (RaftNode n : nodes.values()) {
            if (n.isLeader()) {
                result.add(n);
            }
        }
        return result;
    }

    /** Client commands this node knows to be committed, in order (leader no-ops excluded). */
    List<String> appliedStrings(int id) {
        List<String> out = new ArrayList<>();
        long commit = nodes.get(id).commitIndex();
        for (LogEntry e : logs.get(id).entries(1, Integer.MAX_VALUE)) {
            if (e.index() <= commit && !e.isNoop()) {
                out.add(new String(e.command(), StandardCharsets.UTF_8));
            }
        }
        return out;
    }

    /** Every client command in this node's log, committed or not. */
    List<String> logStrings(int id) {
        List<String> out = new ArrayList<>();
        for (LogEntry e : logs.get(id).entries(1, Integer.MAX_VALUE)) {
            if (!e.isNoop()) {
                out.add(new String(e.command(), StandardCharsets.UTF_8));
            }
        }
        return out;
    }

    static byte[] cmd(String s) {
        return s.getBytes(StandardCharsets.UTF_8);
    }
}
