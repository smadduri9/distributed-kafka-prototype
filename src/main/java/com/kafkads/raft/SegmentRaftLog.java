package com.kafkads.raft;

import com.kafkads.storage.Log;
import com.kafkads.storage.Record;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Presents a partition's segmented {@link Log} as the Raft log, so there is exactly one
 * copy of the data: Raft index {@code i} is log offset {@code i - 1}, and the Raft
 * commit index is the partition's exclusive high-watermark.
 */
public final class SegmentRaftLog implements RaftLog {

    private final Log log;

    public SegmentRaftLog(Log log) {
        this.log = log;
    }

    @Override
    public long lastIndex() {
        return log.logEndOffset();
    }

    @Override
    public int termAt(long index) {
        int term = log.termAt(index - 1);
        return Math.max(term, 0);
    }

    @Override
    public void append(List<LogEntry> entries) throws IOException {
        List<Record> records = new ArrayList<>(entries.size());
        for (LogEntry e : entries) {
            records.add(new Record(e.index() - 1, e.term(), e.type(), e.command()));
        }
        log.appendRecords(records);
        log.flush();
    }

    @Override
    public void truncateFrom(long index) throws IOException {
        log.truncateTo(index - 1);
    }

    @Override
    public List<LogEntry> entries(long fromIndex, int maxBytes) throws IOException {
        List<Record> records = log.read(fromIndex - 1, maxBytes, log.logEndOffset());
        List<LogEntry> out = new ArrayList<>(records.size());
        for (Record r : records) {
            out.add(new LogEntry(r.offset() + 1, r.term(), r.type(), r.value()));
        }
        return out;
    }
}
