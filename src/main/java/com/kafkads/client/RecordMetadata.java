package com.kafkads.client;

/** Where an acknowledged batch was stored: its partition and the offset of its first record. */
public record RecordMetadata(int partition, long baseOffset, int count) {

    public long lastOffset() {
        return baseOffset + count - 1;
    }
}
