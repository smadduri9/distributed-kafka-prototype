package com.kafkads.broker;

import com.kafkads.config.BrokerConfig;
import com.kafkads.metrics.Metrics;
import com.kafkads.protocol.Acks;
import com.kafkads.protocol.ErrorCode;
import com.kafkads.protocol.Message;
import com.kafkads.protocol.messages.FetchRequest;
import com.kafkads.protocol.messages.FetchResponse;
import com.kafkads.protocol.messages.ProduceRequest;
import com.kafkads.protocol.messages.ProduceResponse;
import com.kafkads.raft.FileRaftMetaStore;
import com.kafkads.raft.RaftNode;
import com.kafkads.raft.RaftRole;
import com.kafkads.raft.RaftTransport;
import com.kafkads.raft.SegmentRaftLog;
import com.kafkads.raft.messages.AppendEntriesRequest;
import com.kafkads.raft.messages.AppendEntriesResponse;
import com.kafkads.raft.messages.RequestVoteRequest;
import com.kafkads.raft.messages.RequestVoteResponse;
import com.kafkads.storage.Log;
import com.kafkads.storage.Record;
import com.kafkads.storage.Segment;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;

/**
 * This broker's replica of one partition: the segmented log, the Raft node that
 * replicates it, and the produce requests waiting for their records to commit.
 *
 * <p><b>Threading.</b> Each replica owns one thread. Every operation on the log and the
 * Raft node runs on it: client requests, Raft RPCs, RPC responses, timer ticks and
 * produce timeouts. Netty event-loop threads only enqueue work and later write the
 * response, so no disk I/O or lock wait ever happens on an event loop. State that other
 * threads need (leader, term, offsets) is published through a volatile snapshot.
 *
 * <p><b>Bounds.</b> At most {@link #MAX_QUEUED_REQUESTS} client requests may be queued or
 * awaiting commit per partition; beyond that requests are refused with {@code BUSY}.
 * Every waiting produce has a timeout that removes it.
 *
 * <p><b>Failure.</b> A storage error stops this replica (it stops voting, leading and
 * serving) rather than continuing with a log in an unknown state.
 */
public final class PartitionReplica implements AutoCloseable {

    public static final int MAX_QUEUED_REQUESTS = 10_000;
    public static final int MAX_FETCH_BYTES = 4 * 1024 * 1024;

    /** Immutable snapshot of replica state, safe to read from any thread without blocking. */
    public record View(RaftRole role, int leaderId, int term, long highWatermark, long logEndOffset,
                       boolean failed) {
    }

    private static final class PendingProduce {
        final long lastIndex;
        final int term;
        final Acks acks;
        final long baseOffset;
        final CompletableFuture<Message> future;
        ScheduledFuture<?> timer;

        PendingProduce(long lastIndex, int term, Acks acks, long baseOffset, CompletableFuture<Message> future) {
            this.lastIndex = lastIndex;
            this.term = term;
            this.acks = acks;
            this.baseOffset = baseOffset;
            this.future = future;
        }
    }

    private final int partitionId;
    private final int brokerId;
    private final int produceTimeoutMs;
    private final long tickIntervalMs;
    private final Metrics metrics;
    private final Log log;
    private final RaftNode node;
    private final ScheduledThreadPoolExecutor executor;
    private final List<PendingProduce> pending = new ArrayList<>();
    private final AtomicInteger queued = new AtomicInteger();

    private volatile View view;
    private volatile boolean failed;

    public PartitionReplica(int partitionId, int brokerId, List<Integer> replicas, Path dir,
                            BrokerConfig config, RaftTransport transport, Metrics metrics) throws IOException {
        this.partitionId = partitionId;
        this.brokerId = brokerId;
        this.produceTimeoutMs = config.produceTimeoutMs();
        this.tickIntervalMs = Math.max(5, config.heartbeatIntervalMs() / 3);
        this.metrics = metrics;
        this.executor = new ScheduledThreadPoolExecutor(1, r -> {
            Thread t = new Thread(r, "broker-" + brokerId + "-partition-" + partitionId);
            t.setDaemon(true);
            return t;
        });
        this.executor.setRemoveOnCancelPolicy(true);
        this.log = new Log(dir.resolve("log"), config.segmentBytes(), config.indexIntervalBytes());

        Set<Integer> peers = new HashSet<>(replicas);
        peers.remove(brokerId);
        int rank = replicas.indexOf(brokerId);
        try {
            this.node = RaftNode.builder(brokerId, peers)
                    .groupId(partitionId)
                    .transport(transport)
                    .log(new SegmentRaftLog(log))
                    .metaStore(new FileRaftMetaStore(dir.resolve("raft")))
                    .listener(this::onRaftStateChanged)
                    .callbackExecutor(this::executeInternal)
                    .timing(config.electionTimeoutMinMs(), config.electionTimeoutMaxMs(),
                            config.heartbeatIntervalMs())
                    // The preferred leader (rank 0) times out first on startup.
                    .electionDelayMs(rank * config.electionTimeoutMaxMs())
                    .build();
        } catch (RuntimeException e) {
            log.close();
            executor.shutdownNow();
            throw e;
        }
        refreshView();
    }

    public int partitionId() {
        return partitionId;
    }

    public RaftNode raftNode() {
        return node;
    }

    public View view() {
        return view;
    }

    public boolean isLeader() {
        return view.role() == RaftRole.LEADER;
    }

    /** This replica's view of the partition leader, or -1 if unknown. */
    public int leaderId() {
        return view.leaderId();
    }

    /** Exclusive upper bound of committed offsets, as known to this replica. */
    public long highWatermark() {
        return view.highWatermark();
    }

    public long logEndOffset() {
        return view.logEndOffset();
    }

    /** Starts election timers and heartbeats. */
    public void start() {
        executeInternal(() -> node.initTiming(System.currentTimeMillis()));
        executor.scheduleWithFixedDelay(
                () -> guarded(() -> node.tick(System.currentTimeMillis())),
                tickIntervalMs, tickIntervalMs, TimeUnit.MILLISECONDS);
    }

    // ---- client requests ----

    public CompletableFuture<Message> produce(ProduceRequest req) {
        return submitClient(result -> doProduce(req, result),
                code -> new ProduceResponse(code, -1L, view.leaderId()));
    }

    private void doProduce(ProduceRequest req, CompletableFuture<Message> result) {
        List<byte[]> values = req.values();
        if (values.isEmpty()) {
            result.complete(new ProduceResponse(ErrorCode.INVALID_REQUEST, -1L, node.leaderId()));
            return;
        }
        long bytes = 0;
        for (byte[] v : values) {
            if (v.length > Segment.MAX_RECORD_BYTES) {
                result.complete(new ProduceResponse(ErrorCode.INVALID_REQUEST, -1L, node.leaderId()));
                return;
            }
            bytes += v.length;
        }
        // propose() appends the whole batch durably or throws; it never appends part of it.
        long firstIndex = node.propose(values);
        if (firstIndex < 0) {
            result.complete(new ProduceResponse(ErrorCode.NOT_LEADER, -1L, node.leaderId()));
            return;
        }
        metrics.recordProduced(values.size(), bytes);
        long baseOffset = firstIndex - 1;
        long lastIndex = firstIndex + values.size() - 1;
        if (req.acks() == Acks.LEADER) {
            // Appended and fsynced here only. Deliberately not waiting for a commit.
            result.complete(new ProduceResponse(ErrorCode.OK, baseOffset, brokerId));
            return;
        }
        PendingProduce p = new PendingProduce(lastIndex, node.currentTerm(), req.acks(), baseOffset, result);
        pending.add(p);
        p.timer = executor.schedule(() -> guarded(() -> timeOut(p)), produceTimeoutMs, TimeUnit.MILLISECONDS);
        onRaftStateChanged(); // a single-replica partition has already committed
    }

    private void timeOut(PendingProduce p) {
        if (pending.remove(p)) {
            short code = p.acks == Acks.ALL ? ErrorCode.NOT_ENOUGH_REPLICAS : ErrorCode.COMMIT_TIMEOUT;
            p.future.complete(new ProduceResponse(code, p.baseOffset, node.leaderId()));
        }
    }

    /** Re-evaluates waiting produces. Runs on the partition thread whenever Raft state changes. */
    private void onRaftStateChanged() {
        if (pending.isEmpty()) {
            return;
        }
        boolean leader = node.isLeader();
        int term = node.currentTerm();
        long commit = node.commitIndex();
        long minMatch = leader ? node.minMatchIndex() : 0;
        for (Iterator<PendingProduce> it = pending.iterator(); it.hasNext(); ) {
            PendingProduce p = it.next();
            short code;
            if (!leader || term != p.term) {
                // The entry may still be committed by a later leader; this node can no longer tell.
                code = ErrorCode.LEADERSHIP_LOST;
            } else if (commit >= p.lastIndex && (p.acks == Acks.QUORUM || minMatch >= p.lastIndex)) {
                code = ErrorCode.OK;
            } else {
                continue;
            }
            it.remove();
            if (p.timer != null) {
                p.timer.cancel(false);
            }
            p.future.complete(new ProduceResponse(code, p.baseOffset, node.leaderId()));
        }
    }

    public CompletableFuture<Message> fetch(FetchRequest req) {
        return submitClient(result -> result.complete(doFetch(req)),
                code -> new FetchResponse(code, view.highWatermark(), req.fromOffset(), view.leaderId(), List.of()));
    }

    private FetchResponse doFetch(FetchRequest req) throws IOException {
        long from = req.fromOffset();
        long hw = node.commitIndex();
        int leader = node.leaderId();
        if (from < 0) {
            return new FetchResponse(ErrorCode.INVALID_REQUEST, hw, from, leader, List.of());
        }
        if (!node.isLeader()) {
            return new FetchResponse(ErrorCode.NOT_LEADER, hw, from, leader, List.of());
        }
        if (!node.isLeaderReady()) {
            return new FetchResponse(ErrorCode.LEADER_NOT_READY, hw, from, leader, List.of());
        }
        int maxBytes = Math.min(Math.max(req.maxBytes(), 1), MAX_FETCH_BYTES);
        List<Record> scanned = log.read(from, maxBytes, hw);
        List<Record> data = new ArrayList<>(scanned.size());
        for (Record r : scanned) {
            if (r.isData()) {
                data.add(r);
            }
        }
        long next = scanned.isEmpty() ? from : scanned.get(scanned.size() - 1).offset() + 1;
        metrics.recordFetched(data.size());
        return new FetchResponse(ErrorCode.OK, hw, next, leader, data);
    }

    // ---- Raft RPCs ----

    public CompletableFuture<RequestVoteResponse> handleRequestVote(RequestVoteRequest req) {
        return submitInternal(() -> node.handleRequestVote(req));
    }

    public CompletableFuture<AppendEntriesResponse> handleAppendEntries(AppendEntriesRequest req) {
        return submitInternal(() -> {
            AppendEntriesResponse resp = node.handleAppendEntries(req);
            if (resp.success() && !req.entries().isEmpty()) {
                metrics.recordReplicated(req.entries().size());
            }
            return resp;
        });
    }

    // ---- execution on the partition thread ----

    private interface IoTask {
        void run() throws IOException;
    }

    private interface ClientTask {
        void run(CompletableFuture<Message> result) throws IOException;
    }

    private CompletableFuture<Message> submitClient(ClientTask task,
                                                    java.util.function.Function<Short, Message> errorResponse) {
        if (queued.incrementAndGet() > MAX_QUEUED_REQUESTS) {
            queued.decrementAndGet();
            return CompletableFuture.completedFuture(errorResponse.apply(ErrorCode.BUSY));
        }
        CompletableFuture<Message> result = new CompletableFuture<>();
        result.whenComplete((r, e) -> queued.decrementAndGet());
        try {
            executor.execute(() -> {
                if (failed) {
                    result.complete(errorResponse.apply(ErrorCode.ERROR));
                    return;
                }
                try {
                    task.run(result);
                } catch (Throwable t) {
                    fail(t);
                    result.complete(errorResponse.apply(ErrorCode.ERROR));
                } finally {
                    refreshView();
                }
            });
        } catch (RejectedExecutionException e) {
            result.complete(errorResponse.apply(ErrorCode.ERROR));
        }
        return result;
    }

    private <T> CompletableFuture<T> submitInternal(Supplier<T> task) {
        CompletableFuture<T> result = new CompletableFuture<>();
        try {
            executor.execute(() -> {
                if (failed) {
                    result.completeExceptionally(new IllegalStateException("replica has failed"));
                    return;
                }
                try {
                    result.complete(task.get());
                } catch (Throwable t) {
                    fail(t);
                    result.completeExceptionally(t);
                } finally {
                    refreshView();
                }
            });
        } catch (RejectedExecutionException e) {
            result.completeExceptionally(e);
        }
        return result;
    }

    private void executeInternal(Runnable task) {
        try {
            executor.execute(() -> guarded(task::run));
        } catch (RejectedExecutionException e) {
            // shutting down
        }
    }

    private void guarded(IoTask task) {
        if (failed) {
            return;
        }
        try {
            task.run();
        } catch (Throwable t) {
            fail(t);
        } finally {
            refreshView();
        }
    }

    /** Fail-stop: after a storage or invariant error this replica stops participating. */
    private void fail(Throwable t) {
        if (failed) {
            return;
        }
        failed = true;
        node.stop();
        System.err.println("[broker " + brokerId + "] partition " + partitionId
                + " stopped after an unrecoverable error: " + t);
        failPending(ErrorCode.ERROR);
    }

    private void failPending(short code) {
        for (PendingProduce p : pending) {
            if (p.timer != null) {
                p.timer.cancel(false);
            }
            p.future.complete(new ProduceResponse(code, p.baseOffset, -1));
        }
        pending.clear();
    }

    private void refreshView() {
        view = new View(node.role(), node.leaderId(), node.currentTerm(), node.commitIndex(),
                log.logEndOffset(), failed);
    }

    @Override
    public void close() throws IOException {
        node.stop();
        executor.shutdown();
        try {
            if (!executor.awaitTermination(2, TimeUnit.SECONDS)) {
                executor.shutdownNow();
                executor.awaitTermination(2, TimeUnit.SECONDS);
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        failPending(ErrorCode.ERROR);
        refreshView();
        log.close();
    }
}
