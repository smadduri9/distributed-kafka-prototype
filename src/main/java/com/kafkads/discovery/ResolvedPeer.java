package com.kafkads.discovery;

import java.net.InetSocketAddress;
import java.util.List;

/**
 * The result of resolving a peer id: one or more candidate addresses (multi-A/AAAA
 * answers, ordered by IP-family preference) and the wall-clock time at which the
 * entry expires and must be re-resolved. Connect attempts try addresses in order.
 */
public record ResolvedPeer(int nodeId, List<InetSocketAddress> addresses, long expiresAtMs) {

    public ResolvedPeer {
        addresses = List.copyOf(addresses);
    }
}
