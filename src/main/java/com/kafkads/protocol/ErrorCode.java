package com.kafkads.protocol;

/**
 * Response status codes shared by all responses.
 *
 * <p>For produce requests the codes fall into two groups that clients must treat
 * differently. <b>Rejected</b> codes mean nothing was appended, so sending the request
 * again cannot create a duplicate. <b>Uncertain</b> codes mean the records are in the
 * leader's log and may or may not end up committed.
 */
public final class ErrorCode {

    public static final short OK = 0;
    /** Rejected: this broker is not the partition leader. The response carries a leader hint. */
    public static final short NOT_LEADER = 1;
    /** Internal failure on the broker (for example a storage error). Outcome uncertain for produce. */
    public static final short ERROR = 2;
    /** Uncertain: acks=ALL was not satisfied in time. The records may still be committed. */
    public static final short NOT_ENOUGH_REPLICAS = 3;
    /** Uncertain: acks=QUORUM was not satisfied in time. The records may still commit later. */
    public static final short COMMIT_TIMEOUT = 4;
    /** Uncertain: the leader lost leadership after appending but before the commit was known. */
    public static final short LEADERSHIP_LOST = 5;
    /** Rejected (fetch): a new leader has not yet committed an entry in its own term. Retry. */
    public static final short LEADER_NOT_READY = 6;
    /** Rejected: no such partition in this cluster. */
    public static final short UNKNOWN_PARTITION = 7;
    /** Rejected: the partition exists but is not assigned to this broker. */
    public static final short NOT_REPLICA = 8;
    /** Rejected: the request is malformed or exceeds a size limit. */
    public static final short INVALID_REQUEST = 9;
    /** Rejected: the broker's bounded queues are full. Retry later. */
    public static final short BUSY = 10;

    private ErrorCode() {
    }

    /** True if a produce request answered with {@code code} was definitely not appended. */
    public static boolean produceWasRejected(short code) {
        return code == NOT_LEADER || code == UNKNOWN_PARTITION || code == NOT_REPLICA
                || code == INVALID_REQUEST || code == BUSY;
    }

    public static String name(short code) {
        return switch (code) {
            case OK -> "OK";
            case NOT_LEADER -> "NOT_LEADER";
            case ERROR -> "ERROR";
            case NOT_ENOUGH_REPLICAS -> "NOT_ENOUGH_REPLICAS";
            case COMMIT_TIMEOUT -> "COMMIT_TIMEOUT";
            case LEADERSHIP_LOST -> "LEADERSHIP_LOST";
            case LEADER_NOT_READY -> "LEADER_NOT_READY";
            case UNKNOWN_PARTITION -> "UNKNOWN_PARTITION";
            case NOT_REPLICA -> "NOT_REPLICA";
            case INVALID_REQUEST -> "INVALID_REQUEST";
            case BUSY -> "BUSY";
            default -> "UNKNOWN(" + code + ")";
        };
    }
}
