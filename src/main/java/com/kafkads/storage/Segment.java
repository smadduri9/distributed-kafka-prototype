package com.kafkads.storage;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.zip.CRC32;

/**
 * A single append-only log segment plus its sparse {@link OffsetIndex}.
 *
 * On-disk record: [8-byte offset][4-byte payload length][4-byte CRC32][payload].
 * The CRC covers the payload bytes and is verified on read.
 */
public final class Segment implements AutoCloseable {

    static final int HEADER_BYTES = 8 + 4 + 4;

    private final long baseOffset;
    private final FileChannel logChannel;
    private final OffsetIndex index;
    private final int indexIntervalBytes;

    private long nextOffset;
    private long writePosition;
    private long bytesSinceLastIndex;

    public Segment(Path dir, long baseOffset, int indexIntervalBytes) throws IOException {
        this.baseOffset = baseOffset;
        this.indexIntervalBytes = indexIntervalBytes;
        Path logPath = dir.resolve(fileName(baseOffset, ".log"));
        Path indexPath = dir.resolve(fileName(baseOffset, ".index"));
        this.logChannel = FileChannel.open(logPath,
                StandardOpenOption.CREATE, StandardOpenOption.READ, StandardOpenOption.WRITE);
        this.index = new OffsetIndex(indexPath, baseOffset);
        this.nextOffset = baseOffset;
        recover();
    }

    private void recover() throws IOException {
        long fileSize = logChannel.size();
        Map.Entry<Long, Long> last = index.lastEntry();
        long pos = last == null ? 0L : last.getValue();
        ByteBuffer header = ByteBuffer.allocate(HEADER_BYTES);

        while (pos + HEADER_BYTES <= fileSize) {
            header.clear();
            logChannel.read(header, pos);
            header.flip();
            long offset = header.getLong();
            int len = header.getInt();
            // skip CRC during recovery scan
            if (len < 0 || pos + HEADER_BYTES + len > fileSize) {
                break; // partial/corrupt trailing write: truncate below
            }
            long recordSize = HEADER_BYTES + len;
            nextOffset = offset + 1;
            bytesSinceLastIndex += recordSize;
            if (bytesSinceLastIndex >= indexIntervalBytes && (last == null || offset > last.getKey())) {
                index.append(offset, pos);
                bytesSinceLastIndex = 0;
            }
            pos += recordSize;
        }

        if (pos < fileSize) {
            logChannel.truncate(pos);
        }
        writePosition = pos;
    }

    public long baseOffset() {
        return baseOffset;
    }

    public long nextOffset() {
        return nextOffset;
    }

    public long sizeBytes() {
        return writePosition;
    }

    /** Appends one record, assigning it the next offset, and returns that offset. */
    public long append(byte[] value) throws IOException {
        return appendAt(nextOffset, value);
    }

    /**
     * Appends a record at a specific offset (used by replication/Raft to mirror the
     * leader's offsets). The offset must equal the current end of the log.
     */
    public long appendAt(long offset, byte[] value) throws IOException {
        if (offset != nextOffset) {
            throw new IllegalArgumentException(
                    "Out-of-order append: expected " + nextOffset + " got " + offset);
        }
        CRC32 crc = new CRC32();
        crc.update(value);
        ByteBuffer buf = ByteBuffer.allocate(HEADER_BYTES + value.length);
        buf.putLong(offset);
        buf.putInt(value.length);
        buf.putInt((int) crc.getValue());
        buf.put(value);
        buf.flip();

        long position = writePosition;
        while (buf.hasRemaining()) {
            writePosition += logChannel.write(buf, writePosition);
        }

        bytesSinceLastIndex += HEADER_BYTES + value.length;
        if (bytesSinceLastIndex >= indexIntervalBytes) {
            index.append(offset, position);
            bytesSinceLastIndex = 0;
        }
        nextOffset = offset + 1;
        return offset;
    }

    /**
     * Reads records starting at {@code fromOffset}, accumulating payload bytes up to
     * (but not forcing past) {@code maxBytes}. Stops at {@code endOffsetExclusive}.
     */
    public List<Record> read(long fromOffset, int maxBytes, long endOffsetExclusive) throws IOException {
        List<Record> records = new ArrayList<>();
        if (fromOffset >= nextOffset || fromOffset >= endOffsetExclusive) {
            return records;
        }
        long pos = index.positionFor(fromOffset);
        ByteBuffer header = ByteBuffer.allocate(HEADER_BYTES);
        int accumulated = 0;

        while (pos + HEADER_BYTES <= writePosition) {
            header.clear();
            logChannel.read(header, pos);
            header.flip();
            long offset = header.getLong();
            int len = header.getInt();
            int storedCrc = header.getInt();
            long payloadPos = pos + HEADER_BYTES;

            if (offset >= endOffsetExclusive) {
                break;
            }
            if (offset < fromOffset) {
                pos = payloadPos + len;
                continue;
            }
            if (accumulated > 0 && accumulated + len > maxBytes) {
                break;
            }

            ByteBuffer payload = ByteBuffer.allocate(len);
            logChannel.read(payload, payloadPos);
            byte[] value = payload.array();

            CRC32 crc = new CRC32();
            crc.update(value);
            if ((int) crc.getValue() != storedCrc) {
                throw new CorruptRecordException("CRC mismatch at offset " + offset);
            }

            records.add(new Record(offset, value));
            accumulated += len;
            pos = payloadPos + len;
            if (accumulated >= maxBytes) {
                break;
            }
        }
        return records;
    }

    public void flush() throws IOException {
        logChannel.force(true);
    }

    int indexSize() {
        return index.size();
    }

    static String fileName(long baseOffset, String suffix) {
        return String.format("%020d%s", baseOffset, suffix);
    }

    @Override
    public void close() throws IOException {
        flush();
        index.close();
        logChannel.close();
    }
}
