package com.kafkads.raft;

import java.io.IOException;

/** Durable storage for the two Raft values that must survive a restart besides the log. */
public interface RaftMetaStore {

    /** {@code exists} is false when nothing has ever been saved. */
    record Meta(boolean exists, int currentTerm, Integer votedFor) {
    }

    Meta load() throws IOException;

    /** Must be durable before returning: a vote may be sent on the strength of it. */
    void save(int currentTerm, Integer votedFor) throws IOException;
}
