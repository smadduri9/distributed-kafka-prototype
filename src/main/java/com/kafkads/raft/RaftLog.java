package com.kafkads.raft;

import java.io.IOException;
import java.util.List;

/**
 * The replicated log as Raft sees it. Indexes are 1-based; index 0 means "before the
 * first entry". Implementations must make {@link #append} and {@link #truncateFrom}
 * durable before returning, because Raft acknowledges entries on that basis.
 */
public interface RaftLog {

    long lastIndex();

    /** Term of the entry at {@code index}, or 0 if there is none (including index 0). */
    int termAt(long index);

    /** Appends entries whose indexes continue directly from {@link #lastIndex()}. */
    void append(List<LogEntry> entries) throws IOException;

    /** Removes the entry at {@code index} and everything after it. */
    void truncateFrom(long index) throws IOException;

    /** Entries from {@code fromIndex} onward, limited to roughly {@code maxBytes} of payload. */
    List<LogEntry> entries(long fromIndex, int maxBytes) throws IOException;
}
