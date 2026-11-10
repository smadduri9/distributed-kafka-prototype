package com.kafkads.client;

import com.kafkads.protocol.ErrorCode;

/**
 * The cluster definitely did not perform the request: for a produce, nothing was
 * appended. Sending it again cannot create a duplicate.
 */
public class RequestRejectedException extends KafkaDsException {

    private final short errorCode;

    public RequestRejectedException(short errorCode, String message) {
        super(ErrorCode.name(errorCode) + ": " + message);
        this.errorCode = errorCode;
    }

    public short errorCode() {
        return errorCode;
    }
}
