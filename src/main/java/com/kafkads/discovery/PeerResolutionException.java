package com.kafkads.discovery;

/**
 * A peer could not be resolved to a network address (NXDOMAIN, SERVFAIL, timeout,
 * or unknown peer id). The transport converts this into a failed RPC future; it is
 * never allowed to crash the transport or the Raft node.
 */
public class PeerResolutionException extends Exception {

    public PeerResolutionException(String message) {
        super(message);
    }

    public PeerResolutionException(String message, Throwable cause) {
        super(message, cause);
    }
}
