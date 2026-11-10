package com.kafkads.network;

/**
 * A request did not produce a response. {@link #requestMayHaveBeenProcessed()} tells the
 * caller whether resending could cause the request to take effect twice.
 */
public class RequestFailedException extends RuntimeException {

    public enum Kind {
        /** The request never left this process (not connected, write failed, queue full). */
        NOT_SENT,
        /** The request was written but no response arrived in time. */
        TIMEOUT,
        /** The connection closed after the request was written. */
        CONNECTION_LOST
    }

    private final Kind kind;

    public RequestFailedException(Kind kind, String message, Throwable cause) {
        super(kind + ": " + message, cause);
        this.kind = kind;
    }

    public Kind kind() {
        return kind;
    }

    public boolean requestMayHaveBeenProcessed() {
        return kind != Kind.NOT_SENT;
    }
}
