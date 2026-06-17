package com.kafkads.broker;

import com.kafkads.storage.Log;
import com.kafkads.storage.Record;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;

/**
 * A partition: an append-only {@link Log} plus replication state and a high-watermark.
 *
 * The high-watermark is the exclusive upper bound of committed offsets; consumers
 * never read past it. On the leader it advances to the minimum log-end-offset across
 * the leader and all follower replicas. Producers using acks=all block (via
 * {@link #awaitCommit}) until the high-watermark covers their records.
 */
public final class Partition implements AutoCloseable {

    private final int id;
    private final Log log;
    private volatile long highWatermark;

    // Replication state (leader side).
    private Set<Integer> followerIds = Set.of();
    private final Map<Integer, Long> followerEndOffsets = new HashMap<>();
    private final List<PendingCommit> pendingCommits = new ArrayList<>();

    private record PendingCommit(long targetOffset, CompletableFuture<Void> future) {
    }

    public Partition(int id, Path logDir, int segmentBytes, int indexIntervalBytes) throws IOException {
        this.id = id;
        this.log = new Log(logDir, segmentBytes, indexIntervalBytes);
        this.highWatermark = log.logEndOffset();
    }

    public int id() {
        return id;
    }

    /** Declares the follower replica set this partition expects to replicate to. */
    public synchronized void setFollowers(Set<Integer> followers) {
        this.followerIds = Set.copyOf(followers);
        for (Integer f : followers) {
            followerEndOffsets.putIfAbsent(f, 0L);
        }
    }

    /** Appends records to the local log, returning the base offset of the batch. */
    public synchronized long append(List<byte[]> values) throws IOException {
        long base = log.logEndOffset();
        for (byte[] v : values) {
            log.append(v);
        }
        maybeAdvanceHighWatermark();
        return base;
    }

    /** Appends a single record at the log end (follower replication path). */
    public synchronized long appendAt(long offset, byte[] value) throws IOException {
        return log.appendAt(offset, value);
    }

    public long logEndOffset() {
        return log.logEndOffset();
    }

    public long highWatermark() {
        return highWatermark;
    }

    /** Follower side: set the high-watermark from the leader, bounded by local log end. */
    public synchronized void setFollowerHighWatermark(long leaderHw) {
        this.highWatermark = Math.min(leaderHw, log.logEndOffset());
    }

    public synchronized long followerEndOffset(int followerId) {
        return followerEndOffsets.getOrDefault(followerId, 0L);
    }

    /** Leader side: record a follower's log end offset and recompute the high-watermark. */
    public synchronized void updateFollowerEndOffset(int followerId, long endOffset) {
        followerEndOffsets.merge(followerId, endOffset, Math::max);
        maybeAdvanceHighWatermark();
    }

    private void maybeAdvanceHighWatermark() {
        long minLeo = log.logEndOffset();
        for (Integer f : followerIds) {
            minLeo = Math.min(minLeo, followerEndOffsets.getOrDefault(f, 0L));
        }
        if (minLeo > highWatermark) {
            highWatermark = minLeo;
        }
        completeSatisfiedCommits();
    }

    private void completeSatisfiedCommits() {
        pendingCommits.removeIf(pc -> {
            if (highWatermark >= pc.targetOffset) {
                pc.future.complete(null);
                return true;
            }
            return false;
        });
    }

    /**
     * Returns a future that completes once the high-watermark reaches
     * {@code targetOffset} (i.e. records up to and including that offset are committed).
     */
    public synchronized CompletableFuture<Void> awaitCommit(long targetOffset) {
        CompletableFuture<Void> future = new CompletableFuture<>();
        if (highWatermark >= targetOffset) {
            future.complete(null);
        } else {
            pendingCommits.add(new PendingCommit(targetOffset, future));
        }
        return future;
    }

    /** Reads committed records (strictly below the high-watermark). */
    public synchronized List<Record> read(long fromOffset, int maxBytes) throws IOException {
        return log.read(fromOffset, maxBytes, highWatermark);
    }

    /** Reads records up to the log end, including uncommitted ones (replication path). */
    public synchronized List<Record> readForReplication(long fromOffset, int maxBytes) throws IOException {
        return log.read(fromOffset, maxBytes, log.logEndOffset());
    }

    public void flush() throws IOException {
        log.flush();
    }

    @Override
    public void close() throws IOException {
        log.close();
    }
}
