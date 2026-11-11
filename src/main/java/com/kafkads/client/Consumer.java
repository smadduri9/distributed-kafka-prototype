package com.kafkads.client;

import com.kafkads.network.RequestFailedException;
import com.kafkads.protocol.ErrorCode;
import com.kafkads.protocol.Message;
import com.kafkads.protocol.messages.FetchRequest;
import com.kafkads.protocol.messages.FetchResponse;
import com.kafkads.protocol.messages.MetadataResponse;
import com.kafkads.storage.Record;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

/**
 * Reads committed records from explicit partitions and offsets. Thread-safe.
 *
 * <p>The consumer only ever sees committed records (below the partition's
 * high-watermark), and reads them from the partition leader. Reads are idempotent, so
 * the consumer retries freely across leader changes until its timeout.
 *
 * <p>Positions are kept in memory per {@code Consumer} instance. There are no consumer
 * groups and no stored offsets: a new consumer starts at offset 0 unless told otherwise
 * with {@link #seek}. Offsets within a partition increase strictly but are not dense,
 * because an offset can hold a leader's internal no-op entry; always continue from the
 * returned {@code nextOffset}.
 */
public final class Consumer implements AutoCloseable {

    public static final int DEFAULT_MAX_BYTES = 1 << 20;

    private final ClusterClient cluster;
    private final long fetchTimeoutMs;
    private final Map<Integer, Long> positions = new ConcurrentHashMap<>();

    public Consumer(String bootstrap) {
        this(ClusterClient.parseBootstrap(bootstrap), 5_000, 10_000);
    }

    /**
     * @param requestTimeoutMs bound on each individual request
     * @param fetchTimeoutMs   bound on one fetch call including redirects and retries
     */
    public Consumer(List<InetSocketAddress> bootstrap, long requestTimeoutMs, long fetchTimeoutMs) {
        this.cluster = new ClusterClient(bootstrap, requestTimeoutMs);
        this.fetchTimeoutMs = fetchTimeoutMs;
    }

    public MetadataResponse metadata() {
        return cluster.refreshMetadata(deadline());
    }

    /** One broker's own view of the cluster, for inspection. */
    public MetadataResponse metadataFrom(String hostPort) {
        return cluster.metadataFrom(ClusterClient.parseBootstrap(hostPort).get(0));
    }

    /** Sets the offset the next {@link #poll} of this partition starts from. */
    public void seek(int partition, long offset) {
        if (offset < 0) {
            throw new IllegalArgumentException("Negative offset");
        }
        positions.put(partition, offset);
    }

    public long position(int partition) {
        return positions.getOrDefault(partition, 0L);
    }

    /** Fetches from this consumer's position in the partition and advances the position. */
    public List<ConsumerRecord> poll(int partition) {
        FetchResult result = fetch(partition, position(partition), DEFAULT_MAX_BYTES);
        positions.put(partition, result.nextOffset());
        return result.records();
    }

    /** Stateless fetch of committed records starting at an explicit offset. */
    public FetchResult fetch(int partition, long offset, int maxBytes) {
        Message response;
        try {
            response = cluster.sendToLeader(partition, new FetchRequest(partition, offset, maxBytes),
                    deadline(), true);
        } catch (RequestFailedException e) {
            throw new KafkaDsException("Fetch from partition " + partition + " failed", e);
        }
        short code = ClusterClient.codeOf(response);
        if (code != ErrorCode.OK || !(response instanceof FetchResponse fetch)) {
            throw new RequestRejectedException(code, "fetch from partition " + partition);
        }
        List<ConsumerRecord> records = new ArrayList<>(fetch.records().size());
        for (Record r : fetch.records()) {
            records.add(new ConsumerRecord(partition, r.offset(), r.value()));
        }
        return new FetchResult(records, fetch.nextOffset(), fetch.highWatermark());
    }

    /** Reads every committed record from {@code fromOffset} up to the current high-watermark. */
    public List<ConsumerRecord> readAll(int partition, long fromOffset) {
        List<ConsumerRecord> all = new ArrayList<>();
        long offset = fromOffset;
        while (true) {
            FetchResult r = fetch(partition, offset, DEFAULT_MAX_BYTES);
            all.addAll(r.records());
            if (r.nextOffset() >= r.highWatermark() || r.nextOffset() == offset) {
                return all;
            }
            offset = r.nextOffset();
        }
    }

    /** The partition's current high-watermark (exclusive end of committed offsets). */
    public long endOffset(int partition) {
        return fetch(partition, Long.MAX_VALUE, 1).highWatermark();
    }

    private long deadline() {
        return System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(fetchTimeoutMs);
    }

    @Override
    public void close() {
        cluster.close();
    }
}
