package com.kafkads.discovery;

import com.kafkads.config.PeerConfig;

import java.net.InetSocketAddress;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Backward-compatible resolver over the static {@code id@host:port} peer list.
 * Addresses are handed to Netty unresolved, exactly as before DNS discovery
 * existed, so name resolution still happens implicitly at connect time.
 */
public final class StaticPeerResolver implements PeerResolver {

    private final Map<Integer, PeerConfig> peers = new HashMap<>();

    public StaticPeerResolver(Iterable<PeerConfig> peerConfigs) {
        for (PeerConfig p : peerConfigs) {
            peers.put(p.nodeId(), p);
        }
    }

    @Override
    public Set<Integer> peerIds() {
        return Set.copyOf(peers.keySet());
    }

    @Override
    public ResolvedPeer resolve(int peerId) throws PeerResolutionException {
        PeerConfig cfg = peers.get(peerId);
        if (cfg == null) {
            throw new PeerResolutionException("Unknown peer id: " + peerId);
        }
        return new ResolvedPeer(peerId,
                List.of(InetSocketAddress.createUnresolved(cfg.host(), cfg.port())),
                Long.MAX_VALUE);
    }

    @Override
    public void invalidate(int peerId) {
        // Static config never goes stale.
    }
}
