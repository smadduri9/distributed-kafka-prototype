package com.kafkads.storage;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.zip.CRC32;

/**
 * A single append-only log segment file.
 *
 * <pre>
 * file   := MAGIC(8) record*
 * record := offset(8) term(4) type(1) length(4) crc32(4) payload(length)
 * </pre>
 *
 * The CRC covers offset, term, type, length and payload, so a zero-filled or
 * misdirected block cannot pass as a record. Offsets within a segment are dense and
 * start at the segment's base offset.
 *
 * <p>A sparse offset-to-position index is kept in memory only and rebuilt by the
 * validating scan on open, so there is no second file whose durability has to be kept
 * consistent with the log.
 *
 * <p>Not thread-safe; the owning {@link Log} serialises access.
 */
public final class Segment implements AutoCloseable {

    static final byte[] MAGIC = "KDSLOG02".getBytes(StandardCharsets.US_ASCII);
    static final int FILE_HEADER_BYTES = MAGIC.length;
    static final int HEADER_BYTES = 8 + 4 + 1 + 4 + 4;
    private static final int CRC_COVERED_HEADER_BYTES = 8 + 4 + 1 + 4;

    /** Upper bound for one record's payload; larger lengths on disk are treated as invalid. */
    public static final int MAX_RECORD_BYTES = 8 * 1024 * 1024;

    /** Receives every valid record header seen during the recovery scan. */
    interface ScanObserver {
        void onRecord(long offset, int term);
    }

    private final long baseOffset;
    private final Path path;
    private final FileChannel channel;
    private final int indexIntervalBytes;
    private final TreeMap<Long, Long> index = new TreeMap<>();

    private long nextOffset;
    private long writePosition;
    private long bytesSinceLastIndex;
    private boolean dirty;
    private long truncatedBytesOnRecovery;

    /**
     * Opens (or creates) a segment and validates every record in it.
     *
     * @param tail whether this is the last segment of the log. Only the tail may end in
     *             an incomplete record, which is truncated; anything invalid elsewhere is
     *             corruption and fails the open.
     */
    Segment(Path dir, long baseOffset, int indexIntervalBytes, boolean tail, ScanObserver observer)
            throws IOException {
        this.baseOffset = baseOffset;
        this.indexIntervalBytes = indexIntervalBytes;
        this.path = dir.resolve(fileName(baseOffset, ".log"));
        this.channel = FileChannel.open(path,
                StandardOpenOption.CREATE, StandardOpenOption.READ, StandardOpenOption.WRITE);
        this.nextOffset = baseOffset;
        try {
            recover(tail, observer);
        } catch (IOException | RuntimeException e) {
            channel.close();
            throw e;
        }
    }

    // ---- recovery ----

    private enum Validity { VALID, INCOMPLETE, INVALID }

    private void recover(boolean tail, ScanObserver observer) throws IOException {
        long size = channel.size();
        if (size < FILE_HEADER_BYTES) {
            if (size > 0 && !tail) {
                throw new CorruptRecordException("Segment " + path + " has a truncated file header");
            }
            // New file, or a crash between creating the file and writing its header.
            channel.truncate(0);
            writeFully(ByteBuffer.wrap(MAGIC), 0);
            channel.force(true);
            truncatedBytesOnRecovery = size;
            writePosition = FILE_HEADER_BYTES;
            return;
        }
        ByteBuffer magic = ByteBuffer.allocate(FILE_HEADER_BYTES);
        readFully(magic, 0);
        if (!Arrays.equals(magic.array(), MAGIC)) {
            throw new CorruptRecordException("Segment " + path + " is not in a recognised format");
        }

        Scanner scanner = new Scanner(size);
        long pos = FILE_HEADER_BYTES;
        long expected = baseOffset;
        while (true) {
            Validity v = scanner.validate(pos, expected);
            if (v == Validity.VALID) {
                int term = scanner.term;
                long recordSize = HEADER_BYTES + scanner.length;
                maybeIndex(expected, pos, recordSize);
                if (observer != null) {
                    observer.onRecord(expected, term);
                }
                pos += recordSize;
                expected++;
                continue;
            }
            if (pos == size) {
                break; // clean end of file
            }
            if (!tail) {
                throw new CorruptRecordException(
                        "Invalid record in non-tail segment " + path + " at position " + pos);
            }
            if (v == Validity.INVALID && scanner.lengthPlausible) {
                // A complete record with a bad checksum. If a valid successor follows it,
                // this is damage in the middle of the log, not a torn final write.
                long after = pos + HEADER_BYTES + scanner.length;
                if (new Scanner(size).validate(after, expected + 1) == Validity.VALID) {
                    throw new CorruptRecordException(
                            "Corrupt record at offset " + expected + " in " + path
                                    + " is followed by valid records; refusing to truncate");
                }
            }
            // Torn trailing write: drop it. Nothing after this point was ever acknowledged,
            // because acknowledgements are only sent after force().
            truncatedBytesOnRecovery = size - pos;
            channel.truncate(pos);
            channel.force(true);
            break;
        }
        nextOffset = expected;
        writePosition = pos;
    }

    /** Sequential, buffered record validator used by recovery. */
    private final class Scanner {
        private final long fileSize;
        private final ByteBuffer window = ByteBuffer.allocate(1 << 20);
        private long windowStart = -1;
        int term;
        int length;
        boolean lengthPlausible;

        Scanner(long fileSize) {
            this.fileSize = fileSize;
            window.limit(0);
        }

        private ByteBuffer view(long pos, int len) throws IOException {
            if (len > window.capacity()) {
                ByteBuffer big = ByteBuffer.allocate(len);
                readFully(big, pos);
                big.flip();
                return big;
            }
            if (windowStart < 0 || pos < windowStart || pos + len > windowStart + window.limit()) {
                window.clear();
                int toRead = (int) Math.min(window.capacity(), fileSize - pos);
                window.limit(toRead);
                readFully(window, pos);
                window.flip();
                windowStart = pos;
            }
            ByteBuffer dup = window.duplicate();
            dup.position((int) (pos - windowStart));
            dup.limit(dup.position() + len);
            return dup.slice();
        }

        Validity validate(long pos, long expectedOffset) throws IOException {
            lengthPlausible = false;
            if (pos + HEADER_BYTES > fileSize) {
                return Validity.INCOMPLETE;
            }
            ByteBuffer header = view(pos, HEADER_BYTES);
            long offset = header.getLong();
            term = header.getInt();
            header.get(); // type
            length = header.getInt();
            int storedCrc = header.getInt();
            if (length < 0 || length > MAX_RECORD_BYTES) {
                return Validity.INVALID;
            }
            if (pos + HEADER_BYTES + length > fileSize) {
                return Validity.INCOMPLETE;
            }
            lengthPlausible = true;
            if (offset != expectedOffset || term < 0) {
                return Validity.INVALID;
            }
            CRC32 crc = new CRC32();
            crc.update(view(pos, CRC_COVERED_HEADER_BYTES));
            crc.update(view(pos + HEADER_BYTES, length));
            return (int) crc.getValue() == storedCrc ? Validity.VALID : Validity.INVALID;
        }
    }

    // ---- accessors ----

    public long baseOffset() {
        return baseOffset;
    }

    public long nextOffset() {
        return nextOffset;
    }

    public long sizeBytes() {
        return writePosition;
    }

    /** Bytes of incomplete trailing data dropped when this segment was opened. */
    public long truncatedBytesOnRecovery() {
        return truncatedBytesOnRecovery;
    }

    int indexSize() {
        return index.size();
    }

    // ---- writes ----

    /**
     * Appends a contiguous batch in a single write. Either the whole batch becomes part
     * of the segment or, on an I/O error, the file is cut back to its previous length and
     * the in-memory state is left unchanged.
     */
    public void append(List<Record> records) throws IOException {
        if (records.isEmpty()) {
            return;
        }
        int total = 0;
        long expected = nextOffset;
        for (Record r : records) {
            if (r.offset() != expected) {
                throw new IllegalArgumentException(
                        "Out-of-order append: expected " + expected + " got " + r.offset());
            }
            if (r.value() == null || r.value().length > MAX_RECORD_BYTES) {
                throw new IllegalArgumentException("Record payload missing or too large");
            }
            total += HEADER_BYTES + r.value().length;
            expected++;
        }
        ByteBuffer buf = ByteBuffer.allocate(total);
        for (Record r : records) {
            int start = buf.position();
            buf.putLong(r.offset());
            buf.putInt(r.term());
            buf.put(r.type());
            buf.putInt(r.value().length);
            CRC32 crc = new CRC32();
            crc.update(buf.array(), start, CRC_COVERED_HEADER_BYTES);
            crc.update(r.value());
            buf.putInt((int) crc.getValue());
            buf.put(r.value());
        }
        buf.flip();

        long start = writePosition;
        dirty = true;
        try {
            writeFully(buf, start);
        } catch (IOException e) {
            try {
                channel.truncate(start);
            } catch (IOException suppressed) {
                e.addSuppressed(suppressed);
            }
            throw e;
        }
        long pos = start;
        for (Record r : records) {
            long recordSize = HEADER_BYTES + r.value().length;
            maybeIndex(r.offset(), pos, recordSize);
            pos += recordSize;
        }
        writePosition = pos;
        nextOffset = expected;
    }

    private void maybeIndex(long offset, long position, long recordSize) {
        if (index.isEmpty() || bytesSinceLastIndex >= indexIntervalBytes) {
            index.put(offset, position);
            bytesSinceLastIndex = 0;
        }
        bytesSinceLastIndex += recordSize;
    }

    /** Removes every record at or after {@code offset} and makes the removal durable. */
    public void truncateTo(long offset) throws IOException {
        if (offset < baseOffset || offset > nextOffset) {
            throw new IllegalArgumentException("Offset " + offset + " outside segment ["
                    + baseOffset + ", " + nextOffset + "]");
        }
        if (offset == nextOffset) {
            return;
        }
        long pos = positionOf(offset);
        channel.truncate(pos);
        channel.force(true);
        index.tailMap(offset, true).clear();
        nextOffset = offset;
        writePosition = pos;
        bytesSinceLastIndex = indexIntervalBytes; // index the next append
        dirty = false;
    }

    private long positionOf(long offset) throws IOException {
        Map.Entry<Long, Long> floor = index.floorEntry(offset);
        long pos = floor == null ? FILE_HEADER_BYTES : floor.getValue();
        long current = floor == null ? baseOffset : floor.getKey();
        ByteBuffer header = ByteBuffer.allocate(HEADER_BYTES);
        while (current < offset) {
            header.clear();
            readFully(header, pos);
            header.flip();
            int len = header.getInt(8 + 4 + 1);
            pos += HEADER_BYTES + len;
            current++;
        }
        return pos;
    }

    // ---- reads ----

    /**
     * Reads records starting at {@code fromOffset}, accumulating payload bytes up to
     * {@code maxBytes} (always returning at least one record if one is available), and
     * stopping before {@code endOffsetExclusive}. Every returned record is CRC-verified.
     */
    public List<Record> read(long fromOffset, int maxBytes, long endOffsetExclusive) throws IOException {
        List<Record> records = new ArrayList<>();
        long end = Math.min(endOffsetExclusive, nextOffset);
        if (fromOffset >= end || fromOffset < baseOffset) {
            return records;
        }
        long pos = positionOf(fromOffset);
        long offset = fromOffset;
        ByteBuffer header = ByteBuffer.allocate(HEADER_BYTES);
        int accumulated = 0;
        while (offset < end) {
            header.clear();
            readFully(header, pos);
            header.flip();
            long storedOffset = header.getLong();
            int term = header.getInt();
            byte type = header.get();
            int len = header.getInt();
            int storedCrc = header.getInt();
            if (storedOffset != offset || len < 0 || len > MAX_RECORD_BYTES
                    || pos + HEADER_BYTES + len > writePosition) {
                throw new CorruptRecordException("Malformed record header at offset " + offset);
            }
            if (accumulated > 0 && (long) accumulated + len > maxBytes) {
                break;
            }
            ByteBuffer payload = ByteBuffer.allocate(len);
            readFully(payload, pos + HEADER_BYTES);
            CRC32 crc = new CRC32();
            crc.update(header.array(), 0, CRC_COVERED_HEADER_BYTES);
            crc.update(payload.array());
            if ((int) crc.getValue() != storedCrc) {
                throw new CorruptRecordException("CRC mismatch at offset " + offset);
            }
            records.add(new Record(offset, term, type, payload.array()));
            accumulated += len;
            pos += HEADER_BYTES + len;
            offset++;
            if (accumulated >= maxBytes) {
                break;
            }
        }
        return records;
    }

    // ---- durability ----

    /** Forces appended data to stable storage if anything was written since the last force. */
    public void flush() throws IOException {
        if (dirty) {
            channel.force(true);
            dirty = false;
        }
    }

    private void writeFully(ByteBuffer buf, long position) throws IOException {
        long pos = position;
        while (buf.hasRemaining()) {
            pos += channel.write(buf, pos);
        }
    }

    private void readFully(ByteBuffer buf, long position) throws IOException {
        long pos = position;
        while (buf.hasRemaining()) {
            int n = channel.read(buf, pos);
            if (n < 0) {
                throw new CorruptRecordException("Unexpected end of segment " + path + " at " + pos);
            }
            pos += n;
        }
    }

    static String fileName(long baseOffset, String suffix) {
        return String.format("%020d%s", baseOffset, suffix);
    }

    /** Closes and deletes the segment file. */
    void delete() throws IOException {
        channel.close();
        Files.deleteIfExists(path);
    }

    @Override
    public void close() throws IOException {
        try {
            if (channel.isOpen()) {
                flush();
            }
        } finally {
            channel.close();
        }
    }
}
