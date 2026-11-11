package com.kafkads.raft;

import java.util.ArrayList;
import java.util.List;

/** Volatile {@link RaftLog} for deterministic tests. */
public final class InMemoryRaftLog implements RaftLog {

    private final List<LogEntry> entries = new ArrayList<>();

    @Override
    public synchronized long lastIndex() {
        return entries.size();
    }

    @Override
    public synchronized int termAt(long index) {
        if (index <= 0 || index > entries.size()) {
            return 0;
        }
        return entries.get((int) (index - 1)).term();
    }

    @Override
    public synchronized void append(List<LogEntry> newEntries) {
        for (LogEntry e : newEntries) {
            if (e.index() != entries.size() + 1) {
                throw new IllegalArgumentException("Non-contiguous append at index " + e.index());
            }
            entries.add(e);
        }
    }

    @Override
    public synchronized void truncateFrom(long index) {
        while (entries.size() >= index && !entries.isEmpty()) {
            entries.remove(entries.size() - 1);
        }
    }

    @Override
    public synchronized List<LogEntry> entries(long fromIndex, int maxBytes) {
        List<LogEntry> out = new ArrayList<>();
        int bytes = 0;
        for (long i = Math.max(1, fromIndex); i <= entries.size(); i++) {
            LogEntry e = entries.get((int) (i - 1));
            if (!out.isEmpty() && bytes + e.command().length > maxBytes) {
                break;
            }
            out.add(e);
            bytes += e.command().length;
        }
        return out;
    }
}
