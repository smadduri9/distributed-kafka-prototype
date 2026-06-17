package com.kafkads.raft;

import java.util.ArrayList;
import java.util.List;

/** Non-durable Raft persistence for tests and single-process experimentation. */
public final class InMemoryRaftPersistence implements RaftPersistence {

    private int currentTerm;
    private Integer votedFor;
    private final List<LogEntry> log = new ArrayList<>();

    @Override
    public State load() {
        return new State(currentTerm, votedFor, new ArrayList<>(log));
    }

    @Override
    public void saveMeta(int currentTerm, Integer votedFor) {
        this.currentTerm = currentTerm;
        this.votedFor = votedFor;
    }

    @Override
    public void appendEntry(LogEntry entry) {
        log.add(entry);
    }

    @Override
    public void rewriteLog(List<LogEntry> entries) {
        log.clear();
        log.addAll(entries);
    }

    @Override
    public void close() {
        // nothing to release
    }
}
