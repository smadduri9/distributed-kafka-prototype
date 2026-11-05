package com.kafkads.config;

/** Address of a peer broker in the cluster. */
public record PeerConfig(int nodeId, String host, int port) {

    /** Parses a peer spec in the form {@code id@host:port}. */
    public static PeerConfig parse(String spec) {
        int at = spec.indexOf('@');
        int colon = spec.lastIndexOf(':');
        if (at < 0 || colon < 0 || colon < at) {
            throw new IllegalArgumentException("Invalid peer spec: " + spec + " (expected id@host:port)");
        }
        int id = Integer.parseInt(spec.substring(0, at).trim());
        String host = spec.substring(at + 1, colon).trim();
        int port = Integer.parseInt(spec.substring(colon + 1).trim());
        return new PeerConfig(id, host, port);
    }
}
