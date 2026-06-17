package com.kafkads.metrics;

import java.util.concurrent.atomic.AtomicLong;

/** Lightweight broker counters for observability. */
public final class Metrics {

    private final AtomicLong producedRecords = new AtomicLong();
    private final AtomicLong producedBytes = new AtomicLong();
    private final AtomicLong fetchedRecords = new AtomicLong();
    private final AtomicLong replicatedRecords = new AtomicLong();

    public void recordProduced(long count, long bytes) {
        producedRecords.addAndGet(count);
        producedBytes.addAndGet(bytes);
    }

    public void recordFetched(long count) {
        fetchedRecords.addAndGet(count);
    }

    public void recordReplicated(long count) {
        replicatedRecords.addAndGet(count);
    }

    public long producedRecords() {
        return producedRecords.get();
    }

    public long producedBytes() {
        return producedBytes.get();
    }

    public long fetchedRecords() {
        return fetchedRecords.get();
    }

    public long replicatedRecords() {
        return replicatedRecords.get();
    }

    @Override
    public String toString() {
        return "Metrics{producedRecords=" + producedRecords
                + ", producedBytes=" + producedBytes
                + ", fetchedRecords=" + fetchedRecords
                + ", replicatedRecords=" + replicatedRecords + '}';
    }
}
