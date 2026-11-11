package com.kafkads.raft;

import com.kafkads.storage.CorruptRecordException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** The term/vote record survives a restart and a damaged record is detected, not guessed. */
class FileRaftMetaStoreTest {

    @TempDir
    Path dir;

    @Test
    void missingFileMeansNothingWasEverSaved() throws IOException {
        RaftMetaStore.Meta meta = new FileRaftMetaStore(dir).load();
        assertFalse(meta.exists());
        assertEquals(0, meta.currentTerm());
        assertNull(meta.votedFor());
    }

    @Test
    void savedTermAndVoteAreReadBack() throws IOException {
        new FileRaftMetaStore(dir).save(7, 3);
        RaftMetaStore.Meta meta = new FileRaftMetaStore(dir).load();
        assertTrue(meta.exists());
        assertEquals(7, meta.currentTerm());
        assertEquals(3, meta.votedFor());

        new FileRaftMetaStore(dir).save(8, null);
        meta = new FileRaftMetaStore(dir).load();
        assertEquals(8, meta.currentTerm());
        assertNull(meta.votedFor());
    }

    @Test
    void interruptedSaveLeavesThePreviousValue() throws IOException {
        new FileRaftMetaStore(dir).save(4, 2);
        // A crash after writing the temporary file but before the rename.
        Files.write(dir.resolve("raft-meta.tmp"), new byte[]{1, 2, 3});

        RaftMetaStore.Meta meta = new FileRaftMetaStore(dir).load();
        assertEquals(4, meta.currentTerm());
        assertEquals(2, meta.votedFor());
        assertFalse(Files.exists(dir.resolve("raft-meta.tmp")));
    }

    @Test
    void corruptOrTruncatedFileIsRejected() throws IOException {
        new FileRaftMetaStore(dir).save(5, 1);
        Path file = dir.resolve("raft-meta");

        byte[] bytes = Files.readAllBytes(file);
        bytes[5] ^= 0x40; // flip a bit inside the term
        Files.write(file, bytes);
        assertThrows(CorruptRecordException.class, () -> new FileRaftMetaStore(dir).load());

        Files.write(file, new byte[]{0, 0, 0});
        assertThrows(CorruptRecordException.class, () -> new FileRaftMetaStore(dir).load());
    }
}
