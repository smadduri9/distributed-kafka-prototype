package com.kafkads.client;

/** Base class for failures reported by {@link Producer} and {@link Consumer}. */
public class KafkaDsException extends RuntimeException {

    public KafkaDsException(String message) {
        super(message);
    }

    public KafkaDsException(String message, Throwable cause) {
        super(message, cause);
    }
}
