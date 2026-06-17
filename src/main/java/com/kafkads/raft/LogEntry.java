package com.kafkads.raft;

/** A single Raft log entry: its 1-based index, the term it was created in, and the command bytes. */
public record LogEntry(long index, int term, byte[] command) {
}
