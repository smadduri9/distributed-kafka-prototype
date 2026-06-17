package com.kafkads.raft;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;

/** Phase 3: Raft term, vote, and log survive a restart. */
class FileRaftPersistenceTest {

    @TempDir
    Path dir;

    @Test
    void recoversTermVoteAndLog() throws IOException {
        try (FileRaftPersistence p = new FileRaftPersistence(dir)) {
            p.saveMeta(7, 3);
            p.appendEntry(new LogEntry(1, 5, "one".getBytes(StandardCharsets.UTF_8)));
            p.appendEntry(new LogEntry(2, 7, "two".getBytes(StandardCharsets.UTF_8)));
        }

        try (FileRaftPersistence p = new FileRaftPersistence(dir)) {
            RaftPersistence.State state = p.load();
            assertEquals(7, state.currentTerm());
            assertEquals(3, state.votedFor());
            assertEquals(2, state.log().size());
            assertEquals(1, state.log().get(0).index());
            assertEquals(5, state.log().get(0).term());
            assertArrayEquals("two".getBytes(StandardCharsets.UTF_8), state.log().get(1).command());
        }
    }

    @Test
    void truncationRewritesLog() throws IOException {
        try (FileRaftPersistence p = new FileRaftPersistence(dir)) {
            p.appendEntry(new LogEntry(1, 1, "a".getBytes(StandardCharsets.UTF_8)));
            p.appendEntry(new LogEntry(2, 1, "b".getBytes(StandardCharsets.UTF_8)));
            p.appendEntry(new LogEntry(3, 1, "c".getBytes(StandardCharsets.UTF_8)));
            p.rewriteLog(java.util.List.of(
                    new LogEntry(1, 1, "a".getBytes(StandardCharsets.UTF_8)),
                    new LogEntry(2, 2, "B2".getBytes(StandardCharsets.UTF_8))));
        }
        try (FileRaftPersistence p = new FileRaftPersistence(dir)) {
            RaftPersistence.State state = p.load();
            assertEquals(2, state.log().size());
            assertEquals(2, state.log().get(1).term());
            assertArrayEquals("B2".getBytes(StandardCharsets.UTF_8), state.log().get(1).command());
        }
    }
}
