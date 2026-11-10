package com.kafkads.client;

import com.kafkads.network.RequestFailedException;
import com.kafkads.protocol.Acks;
import com.kafkads.protocol.ErrorCode;
import com.kafkads.protocol.Message;
import com.kafkads.protocol.messages.MetadataResponse;
import com.kafkads.protocol.messages.ProduceRequest;
import com.kafkads.protocol.messages.ProduceResponse;

import java.net.InetSocketAddress;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * Appends records to explicit partitions of a KafkaDS cluster. Thread-safe.
 *
 * <p><b>Delivery contract.</b> {@code send} returns only when the leader answered OK for
 * the requested {@link Acks} mode. With the default, {@link Acks#QUORUM}, that means the
 * batch is committed. Otherwise it throws one of two exceptions:
 * <ul>
 *   <li>{@link RequestRejectedException}: the batch was definitely not appended. The
 *       producer has already retried internally (following leader redirects) until
 *       {@code deliveryTimeoutMs} ran out, which is safe because a rejected attempt
 *       stores nothing.</li>
 *   <li>{@link UncertainWriteException}: a request was sent and no definite answer came
 *       back (timeout, lost connection, commit timeout, leadership change). The producer
 *       does <b>not</b> retry, because that could store the batch twice. There are no
 *       idempotent producer ids, so duplicates are possible if the caller resends:
 *       delivery is at-least-once if you resend and at-most-once if you do not. It is
 *       never exactly-once.</li>
 * </ul>
 *
 * A batch is appended whole or not at all, and its records get consecutive offsets.
 */
public final class Producer implements AutoCloseable {

    private final ClusterClient cluster;
    private final long deliveryTimeoutMs;

    public Producer(String bootstrap) {
        this(ClusterClient.parseBootstrap(bootstrap), 5_000, 10_000);
    }

    /**
     * @param requestTimeoutMs  bound on each individual request
     * @param deliveryTimeoutMs bound on one {@code send} call including redirects and retries
     */
    public Producer(List<InetSocketAddress> bootstrap, long requestTimeoutMs, long deliveryTimeoutMs) {
        this.cluster = new ClusterClient(bootstrap, requestTimeoutMs);
        this.deliveryTimeoutMs = deliveryTimeoutMs;
    }

    /** Number of partitions in the cluster. */
    public int partitionCount() {
        return metadata().partitions().size();
    }

    public MetadataResponse metadata() {
        return cluster.refreshMetadata(deadline());
    }

    /** Maps a key to a partition by hash. A convenience for callers; keys are not stored. */
    public int partitionFor(byte[] key) {
        return Math.floorMod(Arrays.hashCode(key), cluster.metadata(deadline()).partitions().size());
    }

    /** Sends one record with {@link Acks#QUORUM}. */
    public RecordMetadata send(int partition, byte[] value) {
        return send(partition, List.of(value), Acks.QUORUM);
    }

    public RecordMetadata send(int partition, List<byte[]> values, Acks acks) {
        if (values.isEmpty()) {
            throw new IllegalArgumentException("Nothing to send");
        }
        Message response;
        try {
            response = cluster.sendToLeader(partition, new ProduceRequest(partition, acks, values),
                    deadline(), false);
        } catch (RequestFailedException e) {
            throw new UncertainWriteException(partition, -1L,
                    "No definite answer for the produce to partition " + partition + ": " + e.getMessage(), e);
        }
        short code = ClusterClient.codeOf(response);
        long baseOffset = response instanceof ProduceResponse p ? p.baseOffset() : -1L;
        if (code == ErrorCode.OK) {
            return new RecordMetadata(partition, baseOffset, values.size());
        }
        if (ErrorCode.produceWasRejected(code)) {
            throw new RequestRejectedException(code, "produce to partition " + partition);
        }
        throw new UncertainWriteException(partition, baseOffset,
                "Produce to partition " + partition + " answered " + ErrorCode.name(code)
                        + "; the records may or may not be committed", null);
    }

    private long deadline() {
        return System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(deliveryTimeoutMs);
    }

    @Override
    public void close() {
        cluster.close();
    }
}
