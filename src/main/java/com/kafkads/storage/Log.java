package com.kafkads.storage;

import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;

/**
 * An append-only log for a single partition, composed of rolling {@link Segment}s.
 * Records are addressed by a dense, monotonically increasing offset and carry the term
 * in which they were appended; terms never decrease along the log.
 *
 * <p>Durability invariants:
 * <ul>
 *   <li>A batch is written to exactly one segment, so {@link #flush()} on the active
 *       segment covers the whole batch.</li>
 *   <li>A segment is forced before the log rolls to a new one, so only the active
 *       segment can ever hold unforced data.</li>
 *   <li>On open every record of every segment is validated (length, offset continuity,
 *       CRC). An incomplete tail in the last segment is truncated; any other damage
 *       fails the open instead of silently dropping data.</li>
 * </ul>
 */
public final class Log implements AutoCloseable {

    private final Path dir;
    private final int segmentBytes;
    private final int indexIntervalBytes;
    private final List<Segment> segments = new ArrayList<>();
    /** (term, first offset with that term), ascending. Lets termAt() avoid disk reads. */
    private final List<long[]> termStarts = new ArrayList<>();
    private long truncatedBytesOnRecovery;

    public Log(Path dir, int segmentBytes, int indexIntervalBytes) throws IOException {
        this.dir = dir;
        this.segmentBytes = segmentBytes;
        this.indexIntervalBytes = indexIntervalBytes;
        Files.createDirectories(dir);
        try {
            recoverSegments();
        } catch (IOException | RuntimeException e) {
            closeQuietly();
            throw e;
        }
    }

    private void recoverSegments() throws IOException {
        List<Long> baseOffsets = new ArrayList<>();
        try (DirectoryStream<Path> stream = Files.newDirectoryStream(dir, "*.log")) {
            for (Path p : stream) {
                String name = p.getFileName().toString();
                baseOffsets.add(Long.parseLong(name.substring(0, name.length() - ".log".length())));
            }
        }
        baseOffsets.sort(Long::compareTo);
        if (baseOffsets.isEmpty()) {
            segments.add(new Segment(dir, 0L, indexIntervalBytes, true, this::observeTerm));
            syncDirectory();
            return;
        }
        if (baseOffsets.get(0) != 0L) {
            throw new CorruptRecordException("Log in " + dir + " does not start at offset 0");
        }
        for (int i = 0; i < baseOffsets.size(); i++) {
            long base = baseOffsets.get(i);
            boolean tail = i == baseOffsets.size() - 1;
            if (!segments.isEmpty() && activeSegment().nextOffset() != base) {
                throw new CorruptRecordException("Gap in log " + dir + ": segment starting at " + base
                        + " does not follow offset " + activeSegment().nextOffset());
            }
            Segment segment = new Segment(dir, base, indexIntervalBytes, tail, this::observeTerm);
            segments.add(segment);
            truncatedBytesOnRecovery += segment.truncatedBytesOnRecovery();
        }
    }

    private void observeTerm(long offset, int term) {
        if (termStarts.isEmpty() || termStarts.get(termStarts.size() - 1)[0] != term) {
            if (!termStarts.isEmpty() && term < termStarts.get(termStarts.size() - 1)[0]) {
                throw new IllegalStateException("Term decreases at offset " + offset + " in " + dir);
            }
            termStarts.add(new long[]{term, offset});
        }
    }

    private Segment activeSegment() {
        return segments.get(segments.size() - 1);
    }

    public synchronized long logEndOffset() {
        return activeSegment().nextOffset();
    }

    /** Term of the last record, or 0 for an empty log. */
    public synchronized int lastTerm() {
        return termStarts.isEmpty() ? 0 : (int) termStarts.get(termStarts.size() - 1)[0];
    }

    /** Term of the record at {@code offset}, or -1 if there is no such record. */
    public synchronized int termAt(long offset) {
        if (offset < 0 || offset >= activeSegment().nextOffset()) {
            return -1;
        }
        for (int i = termStarts.size() - 1; i >= 0; i--) {
            if (termStarts.get(i)[1] <= offset) {
                return (int) termStarts.get(i)[0];
            }
        }
        return -1;
    }

    /** Bytes of incomplete trailing data dropped when the log was opened. */
    public long truncatedBytesOnRecovery() {
        return truncatedBytesOnRecovery;
    }

    private void maybeRoll() throws IOException {
        Segment active = activeSegment();
        if (active.sizeBytes() >= segmentBytes) {
            active.flush();
            segments.add(new Segment(dir, active.nextOffset(), indexIntervalBytes, true, null));
            syncDirectory();
        }
    }

    /** Appends one data record in the log's current term, returning its offset. */
    public synchronized long append(byte[] value) throws IOException {
        return append(lastTerm(), Record.DATA, List.of(value));
    }

    /**
     * Appends a batch at the end of the log, assigning consecutive offsets, and returns
     * the first one. The batch is all-or-nothing with respect to this log.
     */
    public synchronized long append(int term, byte type, List<byte[]> values) throws IOException {
        long base = logEndOffset();
        List<Record> records = new ArrayList<>(values.size());
        long offset = base;
        for (byte[] v : values) {
            records.add(new Record(offset++, term, type, v));
        }
        appendRecords(records);
        return base;
    }

    /** Appends records that already carry their offsets; the first must equal the log end. */
    public synchronized void appendRecords(List<Record> records) throws IOException {
        if (records.isEmpty()) {
            return;
        }
        int term = lastTerm();
        for (Record r : records) {
            if (r.term() < term) {
                throw new IllegalArgumentException("Term " + r.term() + " is lower than the log's term " + term);
            }
            term = r.term();
        }
        maybeRoll();
        activeSegment().append(records);
        for (Record r : records) {
            observeTerm(r.offset(), r.term());
        }
    }

    /**
     * Reads records starting at {@code fromOffset}, up to {@code maxBytes} of payload,
     * stopping before {@code endOffsetExclusive} (typically the high-watermark).
     */
    public synchronized List<Record> read(long fromOffset, int maxBytes, long endOffsetExclusive)
            throws IOException {
        List<Record> result = new ArrayList<>();
        long next = Math.max(0, fromOffset);
        int remaining = maxBytes;
        for (Segment segment : segments) {
            if (segment.nextOffset() <= next) {
                continue;
            }
            int budget = result.isEmpty() ? Math.max(remaining, 0) : remaining;
            List<Record> batch = segment.read(next, budget, endOffsetExclusive);
            for (Record r : batch) {
                result.add(r);
                remaining -= r.value().length;
                next = r.offset() + 1;
            }
            if (remaining <= 0 || next >= endOffsetExclusive || next < segment.nextOffset()) {
                break;
            }
        }
        return result;
    }

    /**
     * Removes every record at or after {@code offset} (conflicting-suffix truncation) and
     * makes the removal durable before returning.
     */
    public synchronized void truncateTo(long offset) throws IOException {
        if (offset < 0 || offset > logEndOffset()) {
            throw new IllegalArgumentException("Cannot truncate to " + offset
                    + "; log end is " + logEndOffset());
        }
        if (offset == logEndOffset()) {
            return;
        }
        // Delete whole segments from the end first so a crash always leaves a contiguous prefix.
        while (segments.size() > 1 && activeSegment().baseOffset() >= offset) {
            segments.remove(segments.size() - 1).delete();
        }
        activeSegment().truncateTo(offset);
        syncDirectory();
        termStarts.removeIf(t -> t[1] >= offset);
    }

    /** Forces all appended data to stable storage. */
    public synchronized void flush() throws IOException {
        activeSegment().flush();
    }

    public synchronized int segmentCount() {
        return segments.size();
    }

    private void syncDirectory() {
        try (FileChannel ch = FileChannel.open(dir, StandardOpenOption.READ)) {
            ch.force(true);
        } catch (IOException | UnsupportedOperationException e) {
            // Directory fsync is not available on every platform (e.g. Windows); best effort.
        }
    }

    private void closeQuietly() {
        for (Segment s : segments) {
            try {
                s.close();
            } catch (IOException ignored) {
                // already failing
            }
        }
    }

    @Override
    public synchronized void close() throws IOException {
        IOException first = null;
        for (Segment s : segments) {
            try {
                s.close();
            } catch (IOException e) {
                if (first == null) {
                    first = e;
                }
            }
        }
        if (first != null) {
            throw first;
        }
    }
}
