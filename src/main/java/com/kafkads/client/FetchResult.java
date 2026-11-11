package com.kafkads.client;

import java.util.List;

/**
 * Result of one fetch. {@code nextOffset} is the offset to pass to the next fetch; it may
 * skip offsets that hold no client data. {@code highWatermark} is the exclusive upper
 * bound of committed offsets at the time of the fetch.
 */
public record FetchResult(List<ConsumerRecord> records, long nextOffset, long highWatermark) {
}
