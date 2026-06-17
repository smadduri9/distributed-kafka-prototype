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
    void detectsCrcCorruption() throws IOException {
        try (Log log = new Log(dir, 1 << 20, 4096)) {
            log.append(bytes("hello"));
            log.append(bytes("world"));
        }

        // Corrupt a payload byte in the segment file directly.
        Path logFile = dir.resolve(Segment.fileName(0, ".log"));
        try (FileChannel ch = FileChannel.open(logFile, StandardOpenOption.READ, StandardOpenOption.WRITE)) {
            long pos = Segment.HEADER_BYTES; // first payload byte
            ByteBuffer one = ByteBuffer.allocate(1);
            ch.read(one, pos);
            one.flip();
            byte original = one.get();
            ByteBuffer flipped = ByteBuffer.wrap(new byte[]{(byte) (original ^ 0xFF)});
            ch.write(flipped, pos);
        }

        try (Log log = new Log(dir, 1 << 20, 4096)) {
            assertThrows(CorruptRecordException.class,
                    () -> log.read(0, Integer.MAX_VALUE, log.logEndOffset()));
        }
    }
}
