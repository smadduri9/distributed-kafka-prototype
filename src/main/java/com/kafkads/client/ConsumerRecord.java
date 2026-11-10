package com.kafkads.client;

/** One committed record as seen by a consumer. */
public record ConsumerRecord(int partition, long offset, byte[] value) {
}
