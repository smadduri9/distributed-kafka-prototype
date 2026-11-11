package com.kafkads.protocol;

/**
 * What a producer asks the partition leader to wait for before answering OK.
 *
 * <p>Only {@link #QUORUM} and {@link #ALL} mean the records are committed. A record is
 * committed when a majority of the partition's replicas have it durably in a term the
 * leader owns; from then on it survives any single failure the replica set tolerates and
 * becomes visible to consumers.
 */
public enum Acks {
    /**
     * The leader has appended and fsynced the records locally. This is <b>not</b> a
     * commit: the records are not yet visible to consumers and are discarded if this
     * leader is replaced before a majority has them.
     */
    LEADER((byte) 1),
    /** The records are committed: durable on a majority of the replica set. */
    QUORUM((byte) 2),
    /**
     * The records are committed <b>and</b> durable on every configured replica. Fails
     * with {@code NOT_ENOUGH_REPLICAS} if any replica is unavailable, although the
     * records may still have been committed by the majority.
     */
    ALL((byte) -1);

    private final byte code;

    Acks(byte code) {
        this.code = code;
    }

    public byte code() {
        return code;
    }

    public static Acks fromCode(byte code) {
        for (Acks a : values()) {
            if (a.code == code) {
                return a;
            }
        }
        throw new IllegalArgumentException("Unknown acks code: " + code);
    }
}
