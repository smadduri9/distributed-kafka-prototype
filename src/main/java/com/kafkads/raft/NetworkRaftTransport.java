package com.kafkads.raft;

import com.kafkads.config.PeerConfig;
import com.kafkads.network.Client;
import com.kafkads.raft.messages.AppendEntriesRequest;
import com.kafkads.raft.messages.AppendEntriesResponse;
import com.kafkads.raft.messages.RequestVoteRequest;
import com.kafkads.raft.messages.RequestVoteResponse;

import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Raft transport over the KafkaDS Netty protocol. Maintains one lazily-established
 * client connection per peer and reconnects after failures. RPC responses arrive on
 * Netty threads and complete the returned futures.
 */
public final class NetworkRaftTransport implements RaftTransport, AutoCloseable {

    private final Map<Integer, PeerConfig> peers = new ConcurrentHashMap<>();
    private final Map<Integer, Client> clients = new ConcurrentHashMap<>();

    public NetworkRaftTransport(Iterable<PeerConfig> peerConfigs) {
        for (PeerConfig p : peerConfigs) {
            peers.put(p.nodeId(), p);
        }
    }

    private synchronized Client clientFor(int peerId) throws InterruptedException {
        Client existing = clients.get(peerId);
        if (existing != null) {
            return existing;
        }
        PeerConfig cfg = peers.get(peerId);
        Client client = new Client();
        client.connect(cfg.host(), cfg.port());
        clients.put(peerId, client);
        return client;
    }

    private synchronized void drop(int peerId) {
        Client client = clients.remove(peerId);
        if (client != null) {
            client.close();
        }
    }

    @Override
    public CompletableFuture<RequestVoteResponse> requestVote(int peerId, RequestVoteRequest request) {
        try {
            return clientFor(peerId).send(request)
                    .thenApply(m -> (RequestVoteResponse) m)
                    .exceptionallyCompose(ex -> {
                        drop(peerId);
                        return CompletableFuture.failedFuture(ex);
                    });
        } catch (Exception e) {
            drop(peerId);
            return CompletableFuture.failedFuture(e);
        }
    }

    @Override
    public CompletableFuture<AppendEntriesResponse> appendEntries(int peerId, AppendEntriesRequest request) {
        try {
            return clientFor(peerId).send(request)
                    .thenApply(m -> (AppendEntriesResponse) m)
                    .exceptionallyCompose(ex -> {
                        drop(peerId);
                        return CompletableFuture.failedFuture(ex);
                    });
        } catch (Exception e) {
            drop(peerId);
            return CompletableFuture.failedFuture(e);
        }
    }

    @Override
    public synchronized void close() {
        for (Client c : clients.values()) {
            c.close();
        }
        clients.clear();
    }
}
