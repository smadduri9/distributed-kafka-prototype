package com.kafkads.protocol;

/** Producer durability mode. */
public enum Acks {
    /** Leader responds after its own local append. */
    LEADER((byte) 1),
    /** Leader responds only after all followers have replicated (high-watermark advances). */
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
