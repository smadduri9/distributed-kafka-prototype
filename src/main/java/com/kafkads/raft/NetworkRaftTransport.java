package com.kafkads.raft;

import com.kafkads.config.PeerConfig;
import com.kafkads.discovery.PeerResolver;
import com.kafkads.discovery.ResolvedPeer;
import com.kafkads.discovery.StaticPeerResolver;
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
 * client connection per peer and reconnects after failures. Peer addresses come from
 * a pluggable {@link PeerResolver} (static config or DNS discovery); on connection
 * failure the resolver entry is invalidated so the next attempt re-resolves, picking
 * up peers that moved to a new address (e.g. a rescheduled pod). RPC responses arrive
 * on Netty threads and complete the returned futures.
 */
public final class NetworkRaftTransport implements RaftTransport, AutoCloseable {

    private final PeerResolver resolver;
    private final Map<Integer, Client> clients = new ConcurrentHashMap<>();

    public NetworkRaftTransport(PeerResolver resolver) {
        this.resolver = resolver;
    }

    /** Convenience constructor for the static {@code id@host:port} peer list. */
    public NetworkRaftTransport(Iterable<PeerConfig> peerConfigs) {
        this(new StaticPeerResolver(peerConfigs));
    }

    private synchronized Client clientFor(int peerId) throws Exception {
        Client existing = clients.get(peerId);
        if (existing != null) {
            return existing;
        }
        ResolvedPeer resolved = resolver.resolve(peerId);
        Client client = new Client();
        try {
            client.connect(resolved.addresses());
        } catch (Exception e) {
            client.close();
            resolver.invalidate(peerId);
            throw e;
        }
        clients.put(peerId, client);
        return client;
    }

    private synchronized void drop(int peerId) {
        Client client = clients.remove(peerId);
        if (client != null) {
            client.close();
        }
        // Force a fresh DNS resolution on the next connect: the peer may have
        // moved to a new address (pod restart / reschedule).
        resolver.invalidate(peerId);
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
