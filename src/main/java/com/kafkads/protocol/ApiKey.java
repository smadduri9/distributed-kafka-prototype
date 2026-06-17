package com.kafkads.protocol;

/**
 * Wire api keys. All keys are reserved up front so the framing layer never changes
 * between build phases; later phases simply start using their reserved keys.
 */
public enum ApiKey {
    PING((byte) 0),
    PRODUCE((byte) 1),
    PRODUCE_RESP((byte) 2),
    FETCH((byte) 3),
    FETCH_RESP((byte) 4),
    REPLICATE((byte) 5),
    REPLICATE_ACK((byte) 6),
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
