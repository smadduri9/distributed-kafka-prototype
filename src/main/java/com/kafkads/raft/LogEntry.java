package com.kafkads.raft;

import com.kafkads.storage.Record;

/**
 * A single Raft log entry: its 1-based index, the term it was created in, its type
 * ({@link Record#DATA} or {@link Record#NOOP}) and the command bytes.
 */
public record LogEntry(long index, int term, byte type, byte[] command) {

    public LogEntry(long index, int term, byte[] command) {
        this(index, term, Record.DATA, command);
    }

    public boolean isNoop() {
        return type == Record.NOOP;
    }
}
