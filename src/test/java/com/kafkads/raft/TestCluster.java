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
 * is controlled via {@link #setDown} to simulate crashes and partitions.
 */
final class TestCluster {

    final Map<Integer, RaftNode> nodes = new LinkedHashMap<>();
    final Map<Integer, List<byte[]>> applied = new HashMap<>();
    final Set<Integer> down = new HashSet<>();

    TestCluster(int... ids) {
        for (int id : ids) {
            applied.put(id, new ArrayList<>());
        }
        for (int id : ids) {
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
            RaftNode node = new RaftNode(id, peers, transport, new InMemoryRaftPersistence(),
                    cmd -> applied.get(self).add(cmd), 150, 300, 50);
            nodes.put(id, node);
        }
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

    List<String> appliedStrings(int id) {
        List<String> out = new ArrayList<>();
        for (byte[] b : applied.get(id)) {
            out.add(new String(b, StandardCharsets.UTF_8));
        }
        return out;
    }

    static byte[] cmd(String s) {
        return s.getBytes(StandardCharsets.UTF_8);
    }
}
