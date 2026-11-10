package com.kafkads.raft;

/** Volatile {@link RaftMetaStore} for deterministic tests. */
public final class InMemoryRaftMetaStore implements RaftMetaStore {

    private Meta meta = new Meta(false, 0, null);

    @Override
    public synchronized Meta load() {
        return meta;
    }

    @Override
    public synchronized void save(int currentTerm, Integer votedFor) {
        meta = new Meta(true, currentTerm, votedFor);
    }
}
