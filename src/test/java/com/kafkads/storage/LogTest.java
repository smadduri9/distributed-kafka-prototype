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

class LogTest {

    @TempDir
    Path dir;

    private static byte[] bytes(String s) {
        return s.getBytes(StandardCharsets.UTF_8);
    }

    @Test
    void appendAndReadRoundTrip() throws IOException {
        try (Log log = new Log(dir, 1 << 20, 4096)) {
            assertEquals(0, log.append(bytes("a")));
            assertEquals(1, log.append(bytes("b")));
            assertEquals(2, log.append(bytes("c")));

            List<Record> records = log.read(0, Integer.MAX_VALUE, log.logEndOffset());
            assertEquals(3, records.size());
            assertArrayEquals(bytes("a"), records.get(0).value());
            assertArrayEquals(bytes("c"), records.get(2).value());
            assertEquals(2, records.get(2).offset());
        }
    }

    @Test
    void readRespectsHighWatermarkBound() throws IOException {
        try (Log log = new Log(dir, 1 << 20, 4096)) {
            for (int i = 0; i < 10; i++) {
                log.append(bytes("v" + i));
            }
            // committed bound is exclusive: only offsets [0,5) are visible
            List<Record> records = log.read(0, Integer.MAX_VALUE, 5);
            assertEquals(5, records.size());
            assertEquals(4, records.get(4).offset());
        }
    }

    @Test
    void readFromMiddleOffsetUsesIndex() throws IOException {
        // small index interval forces many index entries to exercise the floor lookup
        try (Log log = new Log(dir, 1 << 20, 64)) {
            for (int i = 0; i < 1000; i++) {
                log.append(bytes("payload-" + i));
            }
            List<Record> records = log.read(750, Integer.MAX_VALUE, log.logEndOffset());
            assertEquals(250, records.size());
            assertEquals(750, records.get(0).offset());
            assertArrayEquals(bytes("payload-750"), records.get(0).value());
        }
    }

    @Test
    void rollsSegmentsWhenSizeExceeded() throws IOException {
        // tiny segment size forces frequent rolls
        try (Log log = new Log(dir, 128, 64)) {
            for (int i = 0; i < 200; i++) {
                log.append(bytes("record-" + i));
            }
            assertTrue(log.segmentCount() > 1, "expected multiple segments");
            List<Record> records = log.read(0, Integer.MAX_VALUE, log.logEndOffset());
            assertEquals(200, records.size());
            assertEquals(199, records.get(199).offset());
        }
    }

    @Test
    void detectsCrcCorruptionOnRead() throws IOException {
        try (Log log = new Log(dir, 1 << 20, 4096)) {
            log.append(bytes("hello"));
            log.append(bytes("world"));

            // Damage a payload byte while the log is open: the next read must notice.
            Path logFile = dir.resolve(Segment.fileName(0, ".log"));
            flipByte(logFile, Segment.FILE_HEADER_BYTES + Segment.HEADER_BYTES);
            assertThrows(CorruptRecordException.class,
                    () -> log.read(0, Integer.MAX_VALUE, log.logEndOffset()));
        }
    }

    @Test
    void refusesToOpenALogWithDamageBeforeValidRecords() throws IOException {
        try (Log log = new Log(dir, 1 << 20, 4096)) {
            log.append(bytes("hello"));
            log.append(bytes("world"));
        }
        // The first record is damaged but a valid record follows it, so this is not a torn
        // final write and must not be "repaired" by truncating away good data.
        flipByte(dir.resolve(Segment.fileName(0, ".log")), Segment.FILE_HEADER_BYTES + Segment.HEADER_BYTES);
        assertThrows(CorruptRecordException.class, () -> new Log(dir, 1 << 20, 4096));
    }

    @Test
    void tracksTermsAndTruncatesASuffix() throws IOException {
        try (Log log = new Log(dir, 256, 64)) {
            log.append(1, Record.DATA, List.of(bytes("a"), bytes("b")));
            log.append(1, Record.NOOP, List.of(new byte[0]));
            for (int i = 0; i < 40; i++) {
                log.append(2, Record.DATA, List.of(bytes("term-two-record-" + i)));
            }
            assertTrue(log.segmentCount() > 2);
            assertEquals(43, log.logEndOffset());
            assertEquals(1, log.termAt(0));
            assertEquals(1, log.termAt(2));
            assertEquals(2, log.termAt(3));
            assertEquals(2, log.lastTerm());
            assertEquals(-1, log.termAt(43));

            log.truncateTo(10);
            assertEquals(10, log.logEndOffset());
            assertEquals(2, log.termAt(9));
            assertEquals(10, log.read(0, Integer.MAX_VALUE, 100).size());

            log.truncateTo(2);
            assertEquals(2, log.logEndOffset());
            assertEquals(1, log.lastTerm());
            assertEquals(1, log.segmentCount());

            // The log is append-ready after truncation, and terms may only move forward.
            assertEquals(2, log.append(3, Record.DATA, List.of(bytes("c"))));
            assertThrows(IllegalArgumentException.class, () -> log.append(2, Record.DATA, List.of(bytes("d"))));
        }
        try (Log log = new Log(dir, 256, 64)) {
            assertEquals(3, log.logEndOffset(), "truncation is durable");
            List<Record> records = log.read(0, Integer.MAX_VALUE, 3);
            assertArrayEquals(bytes("c"), records.get(2).value());
            assertEquals(3, records.get(2).term());
            assertEquals(3, log.lastTerm());
        }
    }

    @Test
    void aBatchIsWrittenToOneSegmentAndRollsBeforeNotDuring() throws IOException {
        try (Log log = new Log(dir, 128, 64)) {
            List<byte[]> batch = new java.util.ArrayList<>();
            for (int i = 0; i < 50; i++) {
                batch.add(bytes("batch-record-" + i));
            }
            log.append(1, Record.DATA, batch); // far larger than one 128-byte segment
            assertEquals(1, log.segmentCount(), "the whole batch stays in the segment it started in");
            log.append(1, Record.DATA, List.of(bytes("next")));
            assertEquals(2, log.segmentCount(), "the roll happens before the next batch");
            assertEquals(51, log.read(0, Integer.MAX_VALUE, 100).size());
        }
    }

    private static void flipByte(Path file, long position) throws IOException {
        try (FileChannel ch = FileChannel.open(file, StandardOpenOption.READ, StandardOpenOption.WRITE)) {
            ByteBuffer one = ByteBuffer.allocate(1);
            ch.read(one, position);
            one.flip();
            byte original = one.get();
            ch.write(ByteBuffer.wrap(new byte[]{(byte) (original ^ 0xFF)}), position);
        }
    }
}
