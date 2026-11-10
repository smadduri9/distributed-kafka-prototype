package com.kafkads.protocol;

/** One-byte message type carried in every frame. */
public enum ApiKey {
    PING((byte) 0),
    PRODUCE((byte) 1),
    PRODUCE_RESP((byte) 2),
    FETCH((byte) 3),
    FETCH_RESP((byte) 4),
    METADATA((byte) 5),
    METADATA_RESP((byte) 6),
    REQUEST_VOTE((byte) 7),
    REQUEST_VOTE_RESP((byte) 8),
    APPEND_ENTRIES((byte) 9),
    APPEND_ENTRIES_RESP((byte) 10),
    ERROR((byte) 11);

    private final byte code;

    ApiKey(byte code) {
        this.code = code;
    }

    public byte code() {
        return code;
    }

    public static ApiKey fromCode(byte code) {
        for (ApiKey key : values()) {
            if (key.code == code) {
                return key;
            }
        }
        throw new IllegalArgumentException("Unknown api key code: " + code);
    }
}
