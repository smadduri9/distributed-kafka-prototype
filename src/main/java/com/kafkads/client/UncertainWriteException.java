package com.kafkads.client;

/**
 * The outcome of a produce is unknown: the records reached (or may have reached) a
 * leader's log, but this client did not see them commit. They may become visible later,
 * or never.
 *
 * <p>The producer never retries in this situation, because a blind retry can store the
 * records twice and KafkaDS has no producer ids or sequence numbers to de-duplicate.
 * The caller decides: resend and accept a possible duplicate (at-least-once), give up
 * (at-most-once), or read the partition from {@link #baseOffset()} to find out.
 */
public class UncertainWriteException extends KafkaDsException {

    private final int partition;
    private final long baseOffset;

    public UncertainWriteException(int partition, long baseOffset, String message, Throwable cause) {
        super(message, cause);
        this.partition = partition;
        this.baseOffset = baseOffset;
    }

    public int partition() {
        return partition;
    }

    /** Offset the leader assigned to the first record, or -1 if no response was received. */
    public long baseOffset() {
        return baseOffset;
    }
}
