package com.kafkads.storage;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Recovery from damaged files. These tests simulate storage faults by editing segment
 * files between a close and a reopen. They show what the recovery code does with a given
 * on-disk state; they do not show which states a real power loss can produce on a given
 * filesystem.
 */
class LogRecoveryTest {

    private static final int SEGMENT_BYTES = 1 << 20;

    @TempDir
    Path dir;

    private static byte[] bytes(String s) {
        return s.getBytes(StandardCharsets.UTF_8);
    }

    private Path segmentFile(long baseOffset) {
        return dir.resolve(Segment.fileName(baseOffset, ".log"));
    }

    private void writeRecords(int count) throws IOException {
        try (Log log = new Log(dir, SEGMENT_BYTES, 64)) {
            for (int i = 0; i < count; i++) {
                log.append(1, Record.DATA, List.of(bytes("record-" + i)));
            }
        }
    }

    private static void chop(Path file, long bytesToRemove) throws IOException {
        try (FileChannel ch = FileChannel.open(file, StandardOpenOption.WRITE)) {
            ch.truncate(ch.size() - bytesToRemove);
        }
    }

    private void assertIntactPrefix(Log log, int expectedCount) throws IOException {
        assertEquals(expectedCount, log.logEndOffset());
        List<Record> records = log.read(0, Integer.MAX_VALUE, log.logEndOffset());
        assertEquals(expectedCount, records.size());
        for (int i = 0; i < expectedCount; i++) {
            assertEquals(i, records.get(i).offset());
            assertArrayEquals(bytes("record-" + i), records.get(i).value());
        }
    }

    @Test
    void incompleteTrailingPayloadIsTruncatedAndTheLogStaysUsable() throws IOException {
        writeRecords(20);
        chop(segmentFile(0), 3); // the last record loses part of its payload

        try (Log log = new Log(dir, SEGMENT_BYTES, 64)) {
            assertIntactPrefix(log, 19);
            assertTrue(log.truncatedBytesOnRecovery() > 0);
            assertEquals(19, log.append(1, Record.DATA, List.of(bytes("after-recovery"))));
            log.flush();
        }
        try (Log log = new Log(dir, SEGMENT_BYTES, 64)) {
            assertEquals(20, log.logEndOffset());
            assertEquals(0, log.truncatedBytesOnRecovery(), "the repaired log reopens cleanly");
            assertArrayEquals(bytes("after-recovery"), log.read(19, 100, 20).get(0).value());
        }
    }

    @Test
    void incompleteTrailingHeaderIsTruncated() throws IOException {
        writeRecords(10);
        long lastRecordBytes = Segment.HEADER_BYTES + bytes("record-9").length;
        chop(segmentFile(0), lastRecordBytes - 5); // only 5 header bytes of the last record remain

        try (Log log = new Log(dir, SEGMENT_BYTES, 64)) {
            assertIntactPrefix(log, 9);
        }
    }

    @Test
    void trailingRecordWithBadChecksumIsTruncated() throws IOException {
        writeRecords(10);
        Path file = segmentFile(0);
        long size = Files.size(file);
        try (FileChannel ch = FileChannel.open(file, StandardOpenOption.WRITE)) {
            ch.write(ByteBuffer.wrap(new byte[]{0x7F}), size - 1); // damage the final payload byte
        }
        try (Log log = new Log(dir, SEGMENT_BYTES, 64)) {
            assertIntactPrefix(log, 9);
        }
    }

    @Test
    void zeroFilledTailIsNotMistakenForRecords() throws IOException {
        writeRecords(10);
        try (FileChannel ch = FileChannel.open(segmentFile(0), StandardOpenOption.WRITE)) {
            ch.write(ByteBuffer.allocate(4096), ch.size()); // a block of zeros after the last record
        }
        try (Log log = new Log(dir, SEGMENT_BYTES, 64)) {
            assertIntactPrefix(log, 10);
            assertEquals(4096, log.truncatedBytesOnRecovery());
        }
    }

    @Test
    void emptyOrHeaderlessTailSegmentIsReinitialised() throws IOException {
        writeRecords(5);
        Files.write(segmentFile(0), new byte[]{'K', 'D'}); // crash while writing the file header
        try (Log log = new Log(dir, SEGMENT_BYTES, 64)) {
            assertEquals(0, log.logEndOffset());
            assertEquals(0, log.append(1, Record.DATA, List.of(bytes("fresh"))));
        }
    }

    @Test
    void damageInAnOlderSegmentFailsTheOpen() throws IOException {
        try (Log log = new Log(dir, 128, 64)) {
            for (int i = 0; i < 60; i++) {
                log.append(1, Record.DATA, List.of(bytes("record-" + i)));
            }
            assertTrue(log.segmentCount() > 2);
        }
        chop(segmentFile(0), 3); // not the tail segment: this cannot be a torn final write

        assertThrows(CorruptRecordException.class, () -> new Log(dir, 128, 64));
    }

    @Test
    void missingMiddleSegmentFailsTheOpen() throws IOException {
        long secondBase;
        try (Log log = new Log(dir, 128, 64)) {
            for (int i = 0; i < 60; i++) {
                log.append(1, Record.DATA, List.of(bytes("record-" + i)));
            }
            assertTrue(log.segmentCount() > 2);
        }
        try (var stream = Files.list(dir)) {
            secondBase = stream.map(p -> p.getFileName().toString())
                    .filter(n -> n.endsWith(".log"))
                    .map(n -> Long.parseLong(n.substring(0, n.length() - 4)))
                    .filter(b -> b > 0).sorted().findFirst().orElseThrow();
        }
        Files.delete(segmentFile(secondBase));
        assertThrows(CorruptRecordException.class, () -> new Log(dir, 128, 64));
    }

    @Test
    void unrecognisedFileFormatFailsTheOpen() throws IOException {
        Files.createDirectories(dir);
        Files.write(segmentFile(0), bytes("this is not a KafkaDS segment file at all"));
        assertThrows(CorruptRecordException.class, () -> new Log(dir, SEGMENT_BYTES, 64));
    }

    @Test
    void recoveryRebuildsTermsAndIndexAcrossSegments() throws IOException {
        try (Log log = new Log(dir, 256, 64)) {
            for (int i = 0; i < 30; i++) {
                log.append(1, Record.DATA, List.of(bytes("t1-" + i)));
            }
            log.append(2, Record.NOOP, List.of(new byte[0]));
            for (int i = 0; i < 30; i++) {
                log.append(2, Record.DATA, List.of(bytes("t2-" + i)));
            }
        }
        try (Log log = new Log(dir, 256, 64)) {
            assertEquals(61, log.logEndOffset());
            assertEquals(1, log.termAt(29));
            assertEquals(2, log.termAt(30));
            assertEquals(2, log.lastTerm());
            List<Record> tail = log.read(45, Integer.MAX_VALUE, 61);
            assertEquals(16, tail.size());
            assertArrayEquals(bytes("t2-14"), tail.get(0).value());
            assertEquals(Record.NOOP, log.read(30, 1, 31).get(0).type());
        }
    }
}
