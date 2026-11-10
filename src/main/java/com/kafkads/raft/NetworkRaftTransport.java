package com.kafkads.raft;

import com.kafkads.config.PeerConfig;
import com.kafkads.discovery.PeerResolver;
import com.kafkads.discovery.ResolvedPeer;
import com.kafkads.discovery.StaticPeerResolver;
import com.kafkads.network.Client;
import com.kafkads.protocol.Message;
import com.kafkads.raft.messages.AppendEntriesRequest;
import com.kafkads.raft.messages.AppendEntriesResponse;
import com.kafkads.raft.messages.RequestVoteRequest;
import com.kafkads.raft.messages.RequestVoteResponse;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;

import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * Raft transport over the KafkaDS Netty protocol, shared by every Raft group on a
 * broker: one connection per peer broker carries the RPCs of all partitions, told apart
 * by the group id in each request and matched to responses by correlation id.
 *
 * <p>Nothing here blocks the caller. Address resolution (which may hit DNS) runs on a
 * dedicated thread, connecting is asynchronous, and every RPC has a timeout. Any failed
 * RPC drops the connection and invalidates the resolver entry, so the next attempt
 * re-resolves and reaches a peer that moved to a new address. A short back-off after a
 * failed connect keeps heartbeats from hammering an unreachable peer.
 */
public final class NetworkRaftTransport implements RaftTransport, AutoCloseable {

    private static final long RECONNECT_BACKOFF_MS = 100;
    private static final int CONNECT_TIMEOUT_MS = 1_000;

    private record Connection(CompletableFuture<Client> client, long notBeforeMs) {
    }

    private final PeerResolver resolver;
    private final EventLoopGroup group;
    private final boolean ownsGroup;
    private final long rpcTimeoutMs;
    private final Map<Integer, Connection> connections = new ConcurrentHashMap<>();
    private final ExecutorService connector = Executors.newSingleThreadExecutor(r -> {
        Thread t = new Thread(r, "raft-connector");
        t.setDaemon(true);
        return t;
    });
    private volatile boolean closed;

    public NetworkRaftTransport(PeerResolver resolver, EventLoopGroup group, long rpcTimeoutMs) {
        this.resolver = resolver;
        this.group = group;
        this.ownsGroup = false;
        this.rpcTimeoutMs = rpcTimeoutMs;
    }

    public NetworkRaftTransport(PeerResolver resolver) {
        this.resolver = resolver;
        this.group = new NioEventLoopGroup(1);
        this.ownsGroup = true;
        this.rpcTimeoutMs = 1_000;
    }

    /** Convenience constructor for the static {@code id@host:port} peer list. */
    public NetworkRaftTransport(Iterable<PeerConfig> peerConfigs) {
        this(new StaticPeerResolver(peerConfigs));
    }

    private CompletableFuture<Client> clientFor(int peerId) {
        if (closed) {
            return CompletableFuture.failedFuture(new IllegalStateException("transport closed"));
        }
        Connection conn = connections.compute(peerId, (id, existing) -> {
            long now = System.currentTimeMillis();
            if (existing != null) {
                CompletableFuture<Client> f = existing.client();
                if (!f.isDone()) {
                    return existing; // connect in progress
                }
                if (!f.isCompletedExceptionally() && f.join().isActive()) {
                    return existing; // healthy
                }
                if (now < existing.notBeforeMs()) {
                    return existing; // recently failed; still backing off
                }
                if (!f.isCompletedExceptionally()) {
                    // The peer closed an idle connection. It may have moved, so the cached
                    // address must not be trusted for the reconnect.
                    f.join().close();
                    resolver.invalidate(id);
                }
            }
            return new Connection(connect(id), now + RECONNECT_BACKOFF_MS);
        });
        return conn.client();
    }

    private CompletableFuture<Client> connect(int peerId) {
        return CompletableFuture
                .supplyAsync(() -> {
                    try {
                        return resolver.resolve(peerId);
                    } catch (Exception e) {
                        throw new CompletionException(e);
                    }
                }, connector)
                .thenCompose((ResolvedPeer resolved) -> {
                    Client client = new Client(group);
                    return client.connectAsync(resolved.addresses(), CONNECT_TIMEOUT_MS)
                            .handle((v, err) -> {
                                if (err != null) {
                                    client.close();
                                    throw new CompletionException(err);
                                }
                                return client;
                            });
                })
                .whenComplete((c, err) -> {
                    if (err != null) {
                        resolver.invalidate(peerId);
                    }
                });
    }

    /** Drops a peer's connection so the next RPC reconnects through a fresh resolution. */
    private void drop(int peerId, CompletableFuture<Client> used) {
        connections.computeIfPresent(peerId, (id, existing) -> {
            if (existing.client() != used) {
                return existing; // already replaced
            }
            if (used.isDone() && !used.isCompletedExceptionally()) {
                used.join().close();
            }
            return null;
        });
        resolver.invalidate(peerId);
    }

    private <T extends Message> CompletableFuture<T> rpc(int peerId, Message request, Class<T> type) {
        CompletableFuture<Client> clientFuture = clientFor(peerId);
        CompletableFuture<T> result = clientFuture
                .thenCompose(client -> client.send(request, rpcTimeoutMs))
                .thenApply(response -> {
                    if (!type.isInstance(response)) {
                        throw new IllegalStateException("Unexpected reply to " + request.apiKey()
                                + ": " + response);
                    }
                    return type.cast(response);
                });
        result.whenComplete((r, err) -> {
            if (err != null && clientFuture.isDone() && !clientFuture.isCompletedExceptionally()) {
                drop(peerId, clientFuture);
            }
        });
        return result;
    }

    @Override
    public CompletableFuture<RequestVoteResponse> requestVote(int peerId, RequestVoteRequest request) {
        return rpc(peerId, request, RequestVoteResponse.class);
    }

    @Override
    public CompletableFuture<AppendEntriesResponse> appendEntries(int peerId, AppendEntriesRequest request) {
        return rpc(peerId, request, AppendEntriesResponse.class);
    }

    @Override
    public void close() {
        closed = true;
        connector.shutdownNow();
        for (Connection c : connections.values()) {
            CompletableFuture<Client> f = c.client();
            if (f.isDone() && !f.isCompletedExceptionally()) {
                f.join().close();
            } else {
                f.whenComplete((client, err) -> {
                    if (client != null) {
                        client.close();
                    }
                });
            }
        }
        connections.clear();
        if (ownsGroup) {
            group.shutdownGracefully(0, 100, TimeUnit.MILLISECONDS);
        }
    }
}
