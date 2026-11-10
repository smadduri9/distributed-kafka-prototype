package com.kafkads.broker;

import com.kafkads.config.BrokerConfig;
import com.kafkads.config.PeerConfig;
import com.kafkads.discovery.DnsLookup;
import com.kafkads.discovery.DnsPeerResolver;
import com.kafkads.discovery.PeerResolver;
import com.kafkads.discovery.ResolvedPeer;
import com.kafkads.discovery.StaticPeerResolver;
import com.kafkads.metrics.Metrics;
import com.kafkads.network.Server;
import com.kafkads.protocol.ErrorCode;
import com.kafkads.protocol.Message;
import com.kafkads.protocol.messages.FetchRequest;
import com.kafkads.protocol.messages.FetchResponse;
import com.kafkads.protocol.messages.MetadataRequest;
import com.kafkads.protocol.messages.MetadataResponse;
import com.kafkads.protocol.messages.PingRequest;
import com.kafkads.protocol.messages.PingResponse;
import com.kafkads.protocol.messages.ProduceRequest;
import com.kafkads.protocol.messages.ProduceResponse;
import com.kafkads.raft.NetworkRaftTransport;
import com.kafkads.raft.RaftNode;
import com.kafkads.raft.messages.AppendEntriesRequest;
import com.kafkads.raft.messages.RequestVoteRequest;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * A KafkaDS broker node.
 *
 * <p>On start-up the broker computes the cluster's {@link PartitionAssignment} and
 * creates a {@link PartitionReplica} (log + Raft group member) for each partition
 * assigned to it. All partitions share one server socket and one outgoing connection per
 * peer broker; Raft RPCs carry the partition id.
 *
 * <p>Client writes go through the partition's Raft log, and consumers only see offsets
 * below the Raft commit index, so the data that is replicated and the data that
 * leadership election protects are the same data.
 */
public class Broker implements AutoCloseable {

    private final BrokerConfig config;
    private final Server server;
    private final PeerResolver peerResolver;
    private final PartitionAssignment assignment;
    private final Map<Integer, PartitionReplica> replicas = new TreeMap<>();
    private final Metrics metrics = new Metrics();
    private final EventLoopGroup clientGroup = new NioEventLoopGroup(2);
    private final NetworkRaftTransport raftTransport;
    private final ExecutorService adminExecutor = Executors.newSingleThreadExecutor(r -> {
        Thread t = new Thread(r, "broker-admin");
        t.setDaemon(true);
        return t;
    });

    public Broker(BrokerConfig config) {
        this(config, null);
    }

    /** Test seam: a non-null {@code peerResolverOverride} replaces the config-derived resolver. */
    public Broker(BrokerConfig config, PeerResolver peerResolverOverride) {
        this.config = config;
        this.server = new Server(config.port(), () -> new RequestHandler(this));
        this.peerResolver = peerResolverOverride != null ? peerResolverOverride : createResolver(config);

        Set<Integer> brokerIds = new TreeSet<>(peerResolver.peerIds());
        brokerIds.add(config.nodeId());
        this.assignment = config.partitionAssignment().isEmpty()
                ? PartitionAssignment.roundRobin(brokerIds, config.partitionCount(), config.replicationFactor())
                : PartitionAssignment.parse(config.partitionAssignment(), brokerIds, config.partitionCount());
        this.raftTransport = new NetworkRaftTransport(peerResolver, clientGroup, config.electionTimeoutMaxMs());

        try {
            for (int partition : assignment.partitionsOf(config.nodeId())) {
                Path dir = Path.of(config.logDir()).resolve("partition-" + partition);
                replicas.put(partition, new PartitionReplica(partition, config.nodeId(),
                        assignment.replicas(partition), dir, config, raftTransport, metrics));
            }
        } catch (IOException | RuntimeException e) {
            close();
            throw new RuntimeException("Failed to initialize broker " + config.nodeId(), e);
        }
    }

    /** Builds the peer resolver selected by {@code raft.discovery.mode}. */
    private static PeerResolver createResolver(BrokerConfig config) {
        DnsPeerResolver.IpPreference ipPreference =
                "ipv6".equalsIgnoreCase(config.discoveryIpPreference())
                        ? DnsPeerResolver.IpPreference.IPV6
                        : DnsPeerResolver.IpPreference.IPV4;
        return switch (config.discoveryMode()) {
            case "dns-srv" -> DnsPeerResolver.forSrv(
                    DnsLookup.system(), config.discoverySrvName(), config.discoveryHostnamePrefix(),
                    config.nodeId(), config.discoveryTtlMinMs(), config.discoveryTtlMaxMs(),
                    ipPreference);
            case "dns-hostname" -> DnsPeerResolver.forHostnames(
                    DnsLookup.system(), config.peers(),
                    config.discoveryTtlMinMs(), config.discoveryTtlMaxMs(), ipPreference);
            default -> new StaticPeerResolver(config.peers());
        };
    }

    /** Binds the server socket. Call {@link #startRaft()} once the cluster's servers are up. */
    public void start() throws InterruptedException {
        server.start();
    }

    /** Starts election timers and heartbeats for every hosted partition. */
    public void startRaft() {
        for (PartitionReplica r : replicas.values()) {
            r.start();
        }
    }

    public PartitionAssignment assignment() {
        return assignment;
    }

    /** This broker's replica of {@code id}, or null if the partition is not assigned to it. */
    public PartitionReplica partition(int id) {
        return replicas.get(id);
    }

    public boolean isLeader(int partition) {
        PartitionReplica r = replicas.get(partition);
        return r != null && r.isLeader();
    }

    /** Whether this broker currently leads partition 0 (convenience for single-partition use). */
    public boolean isLeader() {
        return isLeader(0);
    }

    /** This broker's view of the leader of {@code partition}, or -1. */
    public int leaderId(int partition) {
        PartitionReplica r = replicas.get(partition);
        return r == null ? -1 : r.leaderId();
    }

    public int leaderId() {
        return leaderId(0);
    }

    /** The Raft node of partition 0 (convenience for single-partition use). */
    public RaftNode raftNode() {
        PartitionReplica r = replicas.get(0);
        return r == null ? null : r.raftNode();
    }

    public Metrics metrics() {
        return metrics;
    }

    public BrokerConfig config() {
        return config;
    }

    /**
     * Dispatches a request and returns a future for its response. Never blocks: partition
     * work is queued to the partition's own thread.
     */
    public CompletableFuture<? extends Message> handle(Message request) {
        if (request instanceof PingRequest ping) {
            return CompletableFuture.completedFuture(new PingResponse(ping.nonce()));
        }
        if (request instanceof ProduceRequest produce) {
            PartitionReplica r = replicas.get(produce.partition());
            if (r == null) {
                return CompletableFuture.completedFuture(
                        new ProduceResponse(missingPartitionCode(produce.partition()), -1L, -1));
            }
            return r.produce(produce);
        }
        if (request instanceof FetchRequest fetch) {
            PartitionReplica r = replicas.get(fetch.partition());
            if (r == null) {
                return CompletableFuture.completedFuture(new FetchResponse(
                        missingPartitionCode(fetch.partition()), 0L, fetch.fromOffset(), -1, List.of()));
            }
            return r.fetch(fetch);
        }
        if (request instanceof MetadataRequest) {
            return CompletableFuture.supplyAsync(this::metadata, adminExecutor);
        }
        if (request instanceof RequestVoteRequest vote) {
            PartitionReplica r = replicas.get(vote.groupId());
            return r != null ? r.handleRequestVote(vote) : notHosted(vote.groupId());
        }
        if (request instanceof AppendEntriesRequest append) {
            PartitionReplica r = replicas.get(append.groupId());
            return r != null ? r.handleAppendEntries(append) : notHosted(append.groupId());
        }
        return CompletableFuture.failedFuture(
                new IllegalArgumentException("Unsupported request: " + request.apiKey()));
    }

    private short missingPartitionCode(int partition) {
        return assignment.hasPartition(partition) ? ErrorCode.NOT_REPLICA : ErrorCode.UNKNOWN_PARTITION;
    }

    private static <T extends Message> CompletableFuture<T> notHosted(int groupId) {
        return CompletableFuture.failedFuture(
                new IllegalArgumentException("Partition " + groupId + " is not hosted by this broker"));
    }

    private MetadataResponse metadata() {
        List<MetadataResponse.BrokerInfo> brokers = new ArrayList<>();
        brokers.add(new MetadataResponse.BrokerInfo(config.nodeId(), config.host(), config.port()));
        Map<Integer, PeerConfig> configured = new TreeMap<>();
        for (PeerConfig p : config.peers()) {
            configured.put(p.nodeId(), p);
        }
        for (int id : new TreeSet<>(peerResolver.peerIds())) {
            PeerConfig p = configured.get(id);
            if (p != null) {
                brokers.add(new MetadataResponse.BrokerInfo(id, p.host(), p.port()));
                continue;
            }
            try {
                ResolvedPeer resolved = peerResolver.resolve(id);
                InetSocketAddress a = resolved.addresses().get(0);
                brokers.add(new MetadataResponse.BrokerInfo(id, a.getHostString(), a.getPort()));
            } catch (Exception e) {
                // Address currently unknown; the client can still reach it via another broker's view.
            }
        }
        List<MetadataResponse.PartitionInfo> partitions = new ArrayList<>();
        for (int p = 0; p < assignment.partitionCount(); p++) {
            PartitionReplica r = replicas.get(p);
            PartitionReplica.View v = r == null ? null : r.view();
            partitions.add(new MetadataResponse.PartitionInfo(p, assignment.replicas(p),
                    assignment.preferredLeader(p),
                    v == null ? -1 : v.leaderId(),
                    v == null ? -1 : v.term(),
                    v == null ? -1L : v.logEndOffset(),
                    v == null ? -1L : v.highWatermark()));
        }
        return new MetadataResponse(config.nodeId(), brokers, partitions);
    }

    @Override
    public void close() {
        server.close();
        for (PartitionReplica r : replicas.values()) {
            try {
                r.close();
            } catch (IOException ignored) {
                // best-effort on shutdown
            }
        }
        if (raftTransport != null) {
            raftTransport.close();
        }
        peerResolver.close();
        adminExecutor.shutdownNow();
        clientGroup.shutdownGracefully(0, 200, TimeUnit.MILLISECONDS);
    }
}
