package com.kafkads.storage;

/** A stored record: its assigned offset and opaque value bytes. */
public record Record(long offset, byte[] value) {
}
