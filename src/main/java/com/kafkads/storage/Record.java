package com.kafkads.storage;

/**
 * One log record. {@code term} is the Raft term in which the record was appended;
 * {@code type} distinguishes client data from the no-op a new leader appends.
 */
public record Record(long offset, int term, byte type, byte[] value) {

    public static final byte DATA = 0;
    public static final byte NOOP = 1;

    /** A data record with no term information (client-facing view). */
    public Record(long offset, byte[] value) {
        this(offset, 0, DATA, value);
    }

    public boolean isData() {
        return type == DATA;
    }
}
