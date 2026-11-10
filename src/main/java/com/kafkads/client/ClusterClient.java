package com.kafkads.client;

import com.kafkads.network.Client;
import com.kafkads.network.RequestFailedException;
import com.kafkads.protocol.ErrorCode;
import com.kafkads.protocol.Message;
import com.kafkads.protocol.messages.ErrorResponse;
import com.kafkads.protocol.messages.FetchResponse;
import com.kafkads.protocol.messages.MetadataRequest;
import com.kafkads.protocol.messages.MetadataResponse;
import com.kafkads.protocol.messages.ProduceResponse;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

/**
 * Shared routing logic for {@link Producer} and {@link Consumer}: connection pooling,
 * cluster metadata, and finding the current leader of a partition.
 *
 * <p>Routing: the first target for a partition is the last leader this client saw, else
 * the leader reported by metadata, else the partition's preferred leader. A broker that
 * is not the leader answers {@code NOT_LEADER} with a hint, which the client follows;
 * without a hint, or when a broker is unreachable, the client moves to the next replica.
 * Every call is bounded by an overall deadline.
 */
final class ClusterClient implements AutoCloseable {

    private static final long RETRY_BACKOFF_MS = 50;

    private final List<InetSocketAddress> bootstrap;
    private final long requestTimeoutMs;
    private final EventLoopGroup group = new NioEventLoopGroup(1);
    private final Map<String, Client> connections = new ConcurrentHashMap<>();
    private final Map<Integer, InetSocketAddress> brokers = new ConcurrentHashMap<>();
    private final Map<Integer, Integer> leaderHints = new ConcurrentHashMap<>();
    private volatile MetadataResponse metadata;

    ClusterClient(List<InetSocketAddress> bootstrap, long requestTimeoutMs) {
        if (bootstrap.isEmpty()) {
            throw new IllegalArgumentException("At least one bootstrap address is required");
        }
        this.bootstrap = List.copyOf(bootstrap);
        this.requestTimeoutMs = requestTimeoutMs;
    }

    static List<InetSocketAddress> parseBootstrap(String spec) {
        List<InetSocketAddress> out = new ArrayList<>();
        for (String part : spec.split(",")) {
            String s = part.trim();
            if (s.isEmpty()) {
                continue;
            }
            int colon = s.lastIndexOf(':');
            if (colon < 0) {
                throw new IllegalArgumentException("Expected host:port, got: " + s);
            }
            out.add(InetSocketAddress.createUnresolved(s.substring(0, colon),
                    Integer.parseInt(s.substring(colon + 1))));
        }
        return out;
    }

    /** One request to one broker, bounded by {@code timeoutMs}. */
    Message request(InetSocketAddress address, Message request, long timeoutMs) {
        String key = address.getHostString() + ":" + address.getPort();
        Client client = connections.compute(key, (k, existing) -> {
            if (existing != null && existing.isActive()) {
                return existing;
            }
            if (existing != null) {
                existing.close();
            }
            Client c = new Client(group);
            try {
                c.connectAsync(List.of(address), (int) Math.max(1, Math.min(timeoutMs, 2_000)))
                        .get(timeoutMs + 100, TimeUnit.MILLISECONDS);
            } catch (Exception e) {
                c.close();
                if (e instanceof InterruptedException) {
                    Thread.currentThread().interrupt();
                }
                throw new RequestFailedException(RequestFailedException.Kind.NOT_SENT,
                        "cannot connect to " + k, e);
            }
            return c;
        });
        try {
            return client.send(request, Math.max(1, timeoutMs)).get();
        } catch (ExecutionException e) {
            if (e.getCause() instanceof RequestFailedException rfe) {
                throw rfe;
            }
            throw new KafkaDsException("Request to " + key + " failed", e.getCause());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RequestFailedException(RequestFailedException.Kind.TIMEOUT, "interrupted", e);
        }
    }

    /** Fetches metadata from the first broker that answers. */
    MetadataResponse refreshMetadata(long deadlineNanos) {
        Set<InetSocketAddress> candidates = new LinkedHashSet<>(brokers.values());
        candidates.addAll(bootstrap);
        RuntimeException last = null;
        for (InetSocketAddress address : candidates) {
            long remaining = remainingMs(deadlineNanos);
            if (remaining <= 0) {
                break;
            }
            try {
                Message resp = request(address, new MetadataRequest(), Math.min(requestTimeoutMs, remaining));
                if (resp instanceof MetadataResponse m) {
                    for (MetadataResponse.BrokerInfo b : m.brokers()) {
                        brokers.put(b.id(), InetSocketAddress.createUnresolved(b.host(), b.port()));
                    }
                    metadata = m;
                    return m;
                }
            } catch (RuntimeException e) {
                last = e;
            }
        }
        throw new KafkaDsException("No broker answered a metadata request", last);
    }

    MetadataResponse metadata(long deadlineNanos) {
        MetadataResponse m = metadata;
        return m != null ? m : refreshMetadata(deadlineNanos);
    }

    /** Asks one specific broker for its own view (used by tools and tests). */
    MetadataResponse metadataFrom(InetSocketAddress address) {
        return (MetadataResponse) request(address, new MetadataRequest(), requestTimeoutMs);
    }

    private MetadataResponse.PartitionInfo partitionInfo(int partition, long deadlineNanos) {
        for (MetadataResponse.PartitionInfo p : metadata(deadlineNanos).partitions()) {
            if (p.partition() == partition) {
                return p;
            }
        }
        throw new RequestRejectedException(ErrorCode.UNKNOWN_PARTITION, "partition " + partition);
    }

    static long remainingMs(long deadlineNanos) {
        return TimeUnit.NANOSECONDS.toMillis(deadlineNanos - System.nanoTime());
    }

    static short codeOf(Message response) {
        if (response instanceof ProduceResponse p) {
            return p.errorCode();
        }
        if (response instanceof FetchResponse f) {
            return f.errorCode();
        }
        if (response instanceof ErrorResponse e) {
            return e.errorCode();
        }
        return ErrorCode.ERROR;
    }

    private static int leaderOf(Message response) {
        if (response instanceof ProduceResponse p) {
            return p.leaderId();
        }
        if (response instanceof FetchResponse f) {
            return f.leaderId();
        }
        return -1;
    }

    /**
     * Sends a partition request to the partition's leader, following redirects until a
     * final response arrives or the deadline passes.
     *
     * @param idempotent true if repeating the request is harmless (reads). When false
     *                   (writes), the request is only ever repeated after an outcome that
     *                   proves the previous attempt was not performed; any ambiguous
     *                   failure is thrown to the caller as a {@link RequestFailedException}.
     * @return the first response whose code is not a routing or retry code
     */
    Message sendToLeader(int partition, Message request, long deadlineNanos, boolean idempotent) {
        MetadataResponse.PartitionInfo info = partitionInfo(partition, deadlineNanos);
        List<Integer> replicas = info.replicas();
        Integer hint = leaderHints.get(partition);
        int target = hint != null ? hint : info.leader() >= 0 ? info.leader() : info.preferredLeader();
        String lastProblem = "no attempt made";
        while (true) {
            long remaining = remainingMs(deadlineNanos);
            if (remaining <= 0) {
                throw new RequestRejectedException(ErrorCode.NOT_LEADER, "no leader for partition "
                        + partition + " accepted the request before the deadline (" + lastProblem + ")");
            }
            InetSocketAddress address = brokers.get(target);
            Message response = null;
            if (address == null) {
                lastProblem = "no address for broker " + target;
            } else {
                try {
                    response = request(address, request, Math.min(requestTimeoutMs, remaining));
                } catch (RequestFailedException e) {
                    if (e.requestMayHaveBeenProcessed() && !idempotent) {
                        throw e;
                    }
                    lastProblem = "broker " + target + ": " + e.getMessage();
                }
            }
            if (response == null) {
                leaderHints.remove(partition);
                target = nextReplica(replicas, target);
                pause(deadlineNanos);
                continue;
            }
            short code = codeOf(response);
            switch (code) {
                case ErrorCode.NOT_LEADER, ErrorCode.NOT_REPLICA -> {
                    int leader = leaderOf(response);
                    lastProblem = "broker " + target + " answered " + ErrorCode.name(code);
                    if (leader >= 0 && leader != target) {
                        leaderHints.put(partition, leader);
                        target = leader;
                    } else {
                        leaderHints.remove(partition);
                        target = nextReplica(replicas, target);
                        pause(deadlineNanos);
                    }
                }
                case ErrorCode.LEADER_NOT_READY, ErrorCode.BUSY -> {
                    lastProblem = "broker " + target + " answered " + ErrorCode.name(code);
                    pause(deadlineNanos);
                }
                default -> {
                    if (code == ErrorCode.ERROR && idempotent) {
                        lastProblem = "broker " + target + " answered ERROR";
                        target = nextReplica(replicas, target);
                        pause(deadlineNanos);
                    } else {
                        if (code == ErrorCode.OK) {
                            leaderHints.put(partition, target);
                        }
                        return response;
                    }
                }
            }
        }
    }

    private static int nextReplica(List<Integer> replicas, int current) {
        int i = replicas.indexOf(current);
        return replicas.get((i + 1) % replicas.size());
    }

    private static void pause(long deadlineNanos) {
        long sleep = Math.min(RETRY_BACKOFF_MS, remainingMs(deadlineNanos));
        if (sleep > 0) {
            try {
                Thread.sleep(sleep);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new KafkaDsException("interrupted", e);
            }
        }
    }

    @Override
    public void close() {
        for (Client c : connections.values()) {
            c.close();
        }
        connections.clear();
        group.shutdownGracefully(0, 100, TimeUnit.MILLISECONDS);
    }
}
