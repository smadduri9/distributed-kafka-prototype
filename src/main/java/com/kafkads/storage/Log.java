package com.kafkads.storage;

import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

/**
 * An append-only log for a single partition, composed of rolling {@link Segment}s.
 * Records are addressed by a monotonically increasing offset. New segments roll
 * when the active segment reaches {@code segmentBytes}.
 */
public final class Log implements AutoCloseable {

    private final Path dir;
    private final int segmentBytes;
    private final int indexIntervalBytes;
    private final List<Segment> segments = new ArrayList<>();

    public Log(Path dir, int segmentBytes, int indexIntervalBytes) throws IOException {
        this.dir = dir;
        this.segmentBytes = segmentBytes;
        this.indexIntervalBytes = indexIntervalBytes;
        Files.createDirectories(dir);
        recoverSegments();
    }

    private void recoverSegments() throws IOException {
        List<Long> baseOffsets = new ArrayList<>();
        try (DirectoryStream<Path> stream = Files.newDirectoryStream(dir, "*.log")) {
            for (Path p : stream) {
                String name = p.getFileName().toString();
                String base = name.substring(0, name.length() - ".log".length());
                baseOffsets.add(Long.parseLong(base));
            }
        }
        baseOffsets.sort(Long::compareTo);
        for (long base : baseOffsets) {
            segments.add(new Segment(dir, base, indexIntervalBytes));
        }
        if (segments.isEmpty()) {
            segments.add(new Segment(dir, 0L, indexIntervalBytes));
        }
    }

    private Segment activeSegment() {
        return segments.get(segments.size() - 1);
    }

    public long logEndOffset() {
        return activeSegment().nextOffset();
    }

    private void maybeRoll() throws IOException {
        Segment active = activeSegment();
        if (active.sizeBytes() >= segmentBytes) {
            segments.add(new Segment(dir, active.nextOffset(), indexIntervalBytes));
        }
    }

    /** Appends a record at the end of the log, returning its offset. */
    public synchronized long append(byte[] value) throws IOException {
        maybeRoll();
        return activeSegment().append(value);
    }

    /** Appends at a specific offset (replication path); must equal logEndOffset. */
    public synchronized long appendAt(long offset, byte[] value) throws IOException {
        maybeRoll();
        return activeSegment().appendAt(offset, value);
    }

    /**
     * Reads records starting at {@code fromOffset}, up to {@code maxBytes} of payload,
     * stopping before {@code endOffsetExclusive} (typically the high-watermark).
     */
    public synchronized List<Record> read(long fromOffset, int maxBytes, long endOffsetExclusive)
            throws IOException {
        List<Record> result = new ArrayList<>();
        long next = fromOffset;
        int remaining = maxBytes;
        for (Segment segment : segments) {
            if (segment.nextOffset() <= next) {
                continue; // this segment is entirely before the requested offset
            }
            List<Record> batch = segment.read(next, remaining, endOffsetExclusive);
            for (Record r : batch) {
                result.add(r);
                remaining -= r.value().length;
                next = r.offset() + 1;
            }
            if (remaining <= 0 || next >= endOffsetExclusive) {
                break;
            }
            // Only advance to the next segment once this one is exhausted. If the
            // segment still holds records at `next`, we stopped due to maxBytes.
            if (next < segment.nextOffset()) {
                break;
            }
        }
        return result;
    }

    public synchronized void flush() throws IOException {
        activeSegment().flush();
    }

    public int segmentCount() {
        return segments.size();
    }

    @Override
    public synchronized void close() throws IOException {
        for (Segment s : segments) {
            s.close();
        }
    }
}
