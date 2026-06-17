package com.kafkads.storage;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Phase 1 durability gate: write 1,000,000 records, restart (reopen) the log,
 * and read every record back at the correct offset and value.
 */
class MillionRecordRestartTest {

    @TempDir
    Path dir;

    private static final int COUNT = 1_000_000;

    @Test
    void durableAcrossRestart() throws IOException {
        try (Log log = new Log(dir, 16 * 1024 * 1024, 4096)) {
            for (int i = 0; i < COUNT; i++) {
                long offset = log.append(payload(i));
                if (offset != i) {
                    throw new AssertionError("offset mismatch on write: " + offset + " != " + i);
                }
            }
        }

        try (Log log = new Log(dir, 16 * 1024 * 1024, 4096)) {
            assertEquals(COUNT, log.logEndOffset(), "log end offset after restart");

            long expected = 0;
            while (expected < COUNT) {
                List<Record> batch = log.read(expected, 1 << 20, COUNT);
                if (batch.isEmpty()) {
                    throw new AssertionError("empty batch at offset " + expected);
                }
                for (Record r : batch) {
                    assertEquals(expected, r.offset());
                    if (expected % 250_000 == 0) {
                        assertArrayEquals(payload((int) expected), r.value());
                    }
                    expected++;
                }
            }
            assertEquals(COUNT, expected, "total records read back");
        }
    }

    private static byte[] payload(int i) {
        return ("value-" + i).getBytes(StandardCharsets.UTF_8);
    }
}
