package com.kafkads.broker;

import com.kafkads.config.BrokerConfig;
import com.kafkads.config.PeerConfig;
import com.kafkads.metrics.Metrics;
import com.kafkads.network.Server;
import com.kafkads.protocol.Acks;
import com.kafkads.protocol.Message;
import com.kafkads.protocol.messages.FetchRequest;
import com.kafkads.protocol.messages.FetchResponse;
import com.kafkads.protocol.messages.PingRequest;
import com.kafkads.protocol.messages.PingResponse;
import com.kafkads.protocol.messages.ProduceRequest;
import com.kafkads.protocol.messages.ProduceResponse;
import com.kafkads.protocol.messages.ReplicateAck;
import com.kafkads.protocol.messages.ReplicateRequest;
import com.kafkads.raft.FileRaftPersistence;
import com.kafkads.raft.NetworkRaftTransport;
import com.kafkads.raft.RaftNode;
import com.kafkads.raft.RaftScheduler;
import com.kafkads.raft.messages.AppendEntriesRequest;
import com.kafkads.raft.messages.RequestVoteRequest;
import com.kafkads.replication.ReplicationManager;
import com.kafkads.storage.Record;

import java.io.IOException;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * A KafkaDS broker node. Owns the network server, the partition logs, and (when it
 * is the partition leader) the replication manager.
 *
 * Phase 2: leadership is static -- the lowest node id in the cluster is the leader.
 * Phase 3 replaces this with Raft-elected leadership.
 */
public class Broker implements AutoCloseable {

    private static final long PRODUCE_ACK_TIMEOUT_MS = 3000;
    private static final int REPLICATION_POLL_MS = 5;

    private final BrokerConfig config;
    private final Server server;
    private final Map<Integer, Partition> partitions = new HashMap<>();
    private final Set<Integer> followerIds;
    private final Metrics metrics = new Metrics();

    private volatile int leaderId;
    private ReplicationManager replication;

    private RaftNode raftNode;
    private RaftScheduler raftScheduler;
    private NetworkRaftTransport raftTransport;
    private FileRaftPersistence raftPersistence;

    public Broker(BrokerConfig config) {
        this.config = config;
        this.server = new Server(config.port(), () -> new RequestHandler(this));
        this.leaderId = computeLeaderId(config);
        this.followerIds = config.peers().stream()
                .map(PeerConfig::nodeId)
                .collect(Collectors.toSet());
        try {
            for (int id = 0; id < config.partitionCount(); id++) {
                Path dir = Path.of(config.logDir()).resolve("partition-" + id);
                Partition partition = new Partition(id, dir, config.segmentBytes(), config.indexIntervalBytes());
                partition.setFollowers(followerIds);
                partitions.put(id, partition);
            }
            if (config.raftEnabled()) {
                buildRaft();
            }
        } catch (IOException e) {
            throw new RuntimeException("Failed to initialize broker", e);
        }
    }

    private void buildRaft() throws IOException {
        this.raftTransport = new NetworkRaftTransport(config.peers());
        this.raftPersistence = new FileRaftPersistence(Path.of(config.logDir()).resolve("raft"));
        this.raftNode = new RaftNode(
                config.nodeId(), followerIds, raftTransport, raftPersistence,
                this::applyRaftCommand,
                config.electionTimeoutMinMs(), config.electionTimeoutMaxMs(), config.heartbeatIntervalMs());
        this.raftScheduler = new RaftScheduler(raftNode, Math.max(5, config.heartbeatIntervalMs() / 3));
    }

    /** Applies a committed Raft command by appending it to partition 0 and committing it. */
    private void applyRaftCommand(byte[] command) {
        Partition partition = partitions.get(0);
        if (partition == null) {
            return;
        }
        try {
            long offset = partition.logEndOffset();
            partition.appendAt(offset, command);
            partition.setFollowerHighWatermark(partition.logEndOffset());
        } catch (IOException e) {
            throw new RuntimeException("Failed to apply Raft command", e);
        }
    }

    private static int computeLeaderId(BrokerConfig config) {
        int min = config.nodeId();
        for (PeerConfig peer : config.peers()) {
            min = Math.min(min, peer.nodeId());
        }
        return min;
    }

    public void start() throws InterruptedException {
        server.start();
    }

    /** Starts leader-side replication (static-leadership mode). No-op when Raft is enabled. */
    public void startReplication() {
        if (config.raftEnabled()) {
            return;
        }
        if (isLeader() && !config.peers().isEmpty()) {
            replication = new ReplicationManager(
                    config.nodeId(), config.peers(), partitions.values(), REPLICATION_POLL_MS);
            replication.start();
        }
    }

    /** Starts Raft (election timers + heartbeats). Only effective when raft.enabled=true. */
    public void startRaft() {
        if (raftScheduler != null) {
            raftScheduler.start();
        }
    }

    public boolean isLeader() {
        if (config.raftEnabled()) {
            return raftNode.isLeader();
        }
        return config.nodeId() == leaderId;
    }

    public int leaderId() {
        if (config.raftEnabled()) {
            return raftNode.leaderId();
        }
        return leaderId;
    }

    public RaftNode raftNode() {
        return raftNode;
    }

    public Metrics metrics() {
        return metrics;
    }

    public BrokerConfig config() {
        return config;
    }

    public Partition partition(int id) {
        return partitions.get(id);
    }

    /** Dispatches a request and returns a future for its response. */
    public CompletableFuture<Message> handle(Message request) {
        if (request instanceof PingRequest ping) {
            return CompletableFuture.completedFuture(new PingResponse(ping.nonce()));
        }
        if (request instanceof ProduceRequest produce) {
            return handleProduce(produce);
        }
        if (request instanceof FetchRequest fetch) {
            return CompletableFuture.completedFuture(handleFetch(fetch));
        }
        if (request instanceof ReplicateRequest replicate) {
            return CompletableFuture.completedFuture(handleReplicate(replicate));
        }
        if (request instanceof RequestVoteRequest vote && raftNode != null) {
            return CompletableFuture.completedFuture(raftNode.handleRequestVote(vote));
        }
        if (request instanceof AppendEntriesRequest append && raftNode != null) {
            return CompletableFuture.completedFuture(raftNode.handleAppendEntries(append));
        }
        return CompletableFuture.failedFuture(
                new IllegalArgumentException("Unsupported request: " + request.getClass().getName()));
    }

    private CompletableFuture<Message> handleProduce(ProduceRequest req) {
        if (!isLeader()) {
            return CompletableFuture.completedFuture(new ProduceResponse(ProduceResponse.NOT_LEADER, -1L));
        }
        Partition partition = partitions.get(req.partition());
        if (partition == null) {
            return CompletableFuture.completedFuture(new ProduceResponse(ProduceResponse.ERROR, -1L));
        }
        try {
            long base = partition.append(req.values());
            partition.flush();
            long bytes = req.values().stream().mapToLong(v -> v.length).sum();
            metrics.recordProduced(req.values().size(), bytes);

            if (req.acks() == Acks.LEADER || req.values().isEmpty()) {
                return CompletableFuture.completedFuture(new ProduceResponse(ProduceResponse.OK, base));
            }
            long lastOffset = base + req.values().size() - 1;
            return partition.awaitCommit(lastOffset)
                    .orTimeout(PRODUCE_ACK_TIMEOUT_MS, TimeUnit.MILLISECONDS)
                    .handle((v, ex) -> ex == null
                            ? new ProduceResponse(ProduceResponse.OK, base)
                            : new ProduceResponse(ProduceResponse.NOT_ENOUGH_REPLICAS, base));
        } catch (IOException e) {
            return CompletableFuture.completedFuture(new ProduceResponse(ProduceResponse.ERROR, -1L));
        }
    }

    private FetchResponse handleFetch(FetchRequest req) {
        Partition partition = partitions.get(req.partition());
        if (partition == null) {
            return new FetchResponse(FetchResponse.ERROR, 0L, List.of());
        }
        try {
            List<Record> records = partition.read(req.fromOffset(), req.maxBytes());
            metrics.recordFetched(records.size());
            return new FetchResponse(FetchResponse.OK, partition.highWatermark(), records);
        } catch (IOException e) {
            return new FetchResponse(FetchResponse.ERROR, partition.highWatermark(), List.of());
        }
    }

    private ReplicateAck handleReplicate(ReplicateRequest req) {
        Partition partition = partitions.get(req.partition());
        if (partition == null) {
            return new ReplicateAck(req.partition(), config.nodeId(), ReplicateAck.ERROR, 0L);
        }
        try {
            for (Record r : req.records()) {
                long leo = partition.logEndOffset();
                if (r.offset() == leo) {
                    partition.appendAt(r.offset(), r.value());
                } else if (r.offset() > leo) {
                    break; // gap; wait for the missing records first
                }
            }
            metrics.recordReplicated(req.records().size());
            partition.setFollowerHighWatermark(req.leaderHighWatermark());
            partition.flush();
            return new ReplicateAck(req.partition(), config.nodeId(),
                    ReplicateAck.OK, partition.logEndOffset());
        } catch (IOException e) {
            return new ReplicateAck(req.partition(), config.nodeId(),
                    ReplicateAck.ERROR, partition.logEndOffset());
        }
    }

    @Override
    public void close() {
        if (replication != null) {
            replication.close();
        }
        if (raftScheduler != null) {
            raftScheduler.close();
        }
        if (raftTransport != null) {
            raftTransport.close();
        }
        server.close();
        if (raftPersistence != null) {
            try {
                raftPersistence.close();
            } catch (IOException ignored) {
                // best-effort on shutdown
            }
        }
        for (Partition p : partitions.values()) {
            try {
                p.close();
            } catch (IOException ignored) {
                // best-effort on shutdown
            }
        }
    }
}
