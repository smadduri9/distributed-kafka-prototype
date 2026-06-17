package com.kafkads.storage;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Map;
import java.util.TreeMap;

/**
 * A sparse offset index mapping absolute offsets to byte positions in the segment
 * log file. One entry is appended roughly every {@code intervalBytes} of log data,
 * mirroring Kafka's design: lookups floor to the nearest indexed offset, then the
 * log is scanned forward from there.
 *
 * On-disk entry: [4-byte relative offset][4-byte file position].
 */
public final class OffsetIndex implements AutoCloseable {

    static final int ENTRY_BYTES = 8;

    private final long baseOffset;
    private final FileChannel channel;
    private final TreeMap<Long, Long> offsetToPosition = new TreeMap<>();

    OffsetIndex(Path path, long baseOffset) throws IOException {
        this.baseOffset = baseOffset;
        this.channel = FileChannel.open(path,
                StandardOpenOption.CREATE, StandardOpenOption.READ, StandardOpenOption.WRITE);
        loadExisting();
    }

    private void loadExisting() throws IOException {
        long size = channel.size();
        long entries = size / ENTRY_BYTES;
        ByteBuffer buf = ByteBuffer.allocate(ENTRY_BYTES);
        for (long i = 0; i < entries; i++) {
            buf.clear();
            channel.read(buf, i * ENTRY_BYTES);
            buf.flip();
            int relativeOffset = buf.getInt();
            int position = buf.getInt();
            offsetToPosition.put(baseOffset + relativeOffset, (long) position);
        }
    }

    void append(long offset, long position) throws IOException {
        ByteBuffer buf = ByteBuffer.allocate(ENTRY_BYTES);
        buf.putInt((int) (offset - baseOffset));
        buf.putInt((int) position);
        buf.flip();
        channel.write(buf, channel.size());
        offsetToPosition.put(offset, position);
    }

    /** Returns the file position to start scanning from to reach {@code targetOffset}. */
    long positionFor(long targetOffset) {
        var entry = offsetToPosition.floorEntry(targetOffset);
        return entry == null ? 0L : entry.getValue();
    }

    int size() {
        return offsetToPosition.size();
    }

    /** The most recently indexed (offset, position) entry, or null if empty. */
    Map.Entry<Long, Long> lastEntry() {
        return offsetToPosition.isEmpty() ? null : offsetToPosition.lastEntry();
    }

    @Override
    public void close() throws IOException {
        channel.force(true);
        channel.close();
    }
}
