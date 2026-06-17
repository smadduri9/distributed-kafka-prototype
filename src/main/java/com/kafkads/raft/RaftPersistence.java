package com.kafkads.raft;

import java.io.IOException;
import java.util.List;

/**
 * Durable Raft state. Per the Raft paper, {@code currentTerm}, {@code votedFor}, and
 * the log must survive restarts. The log is append-only except for rare truncation
 * when a follower's entries conflict with the leader's.
 */
public interface RaftPersistence extends AutoCloseable {

    record State(int currentTerm, Integer votedFor, List<LogEntry> log) {
    }

    State load() throws IOException;

    void saveMeta(int currentTerm, Integer votedFor) throws IOException;

    void appendEntry(LogEntry entry) throws IOException;

    /** Rewrites the log to exactly {@code entries} (used after conflict truncation). */
    void rewriteLog(List<LogEntry> entries) throws IOException;

    @Override
    void close() throws IOException;
}
