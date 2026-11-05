package com.kafkads.discovery;

import java.util.Set;

/**
 * Resolves cluster peer ids to concrete network addresses. Implementations may be
 * backed by static configuration ({@link StaticPeerResolver}) or by DNS discovery
 * ({@link DnsPeerResolver}); the Raft transport treats them interchangeably.
 */
public interface PeerResolver extends AutoCloseable {

    /** Known peer ids (static list or discovered set). */
    Set<Integer> peerIds();

    /** Resolve a peer to concrete addresses; may consult a cache. */
    ResolvedPeer resolve(int peerId) throws PeerResolutionException;

    /**
     * Invalidate any cached entry so the next {@link #resolve(int)} performs a fresh
     * lookup. Called by the transport when a connection to the peer fails, so a peer
     * that moved to a new address is picked up on reconnect.
     */
    void invalidate(int peerId);

    @Override
    default void close() {
    }
}
