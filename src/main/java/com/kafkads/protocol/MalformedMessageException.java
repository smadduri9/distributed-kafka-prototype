package com.kafkads.protocol;

/** A frame's contents violate the protocol: bad type, bad length, bad count, or trailing bytes. */
public class MalformedMessageException extends RuntimeException {

    private final Integer correlationId;

    public MalformedMessageException(String message) {
        this(message, null);
    }

    public MalformedMessageException(String message, Integer correlationId) {
        super(message);
        this.correlationId = correlationId;
    }

    /** The request's correlation id if it could be read before the error, else null. */
    public Integer correlationId() {
        return correlationId;
    }
}
