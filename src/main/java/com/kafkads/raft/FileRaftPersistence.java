package com.kafkads.raft;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;

/**
 * Durable Raft state on disk.
 *
 * Meta file: [4-byte currentTerm][4-byte votedFor] (votedFor = -1 means none).
 * Log file: a sequence of [8-byte index][4-byte term][4-byte length][command bytes].
 */
public final class FileRaftPersistence implements RaftPersistence {

    private final Path metaPath;
    private final Path logPath;
    private FileChannel logChannel;

    public FileRaftPersistence(Path dir) throws IOException {
        Files.createDirectories(dir);
        this.metaPath = dir.resolve("raft-meta");
        this.logPath = dir.resolve("raft-log");
        this.logChannel = FileChannel.open(logPath,
                StandardOpenOption.CREATE, StandardOpenOption.READ, StandardOpenOption.WRITE);
    }

    @Override
    public State load() throws IOException {
        int term = 0;
        Integer votedFor = null;
        if (Files.exists(metaPath)) {
            byte[] meta = Files.readAllBytes(metaPath);
            if (meta.length >= 8) {
                ByteBuffer b = ByteBuffer.wrap(meta);
                term = b.getInt();
                int v = b.getInt();
                votedFor = v < 0 ? null : v;
            }
        }
        List<LogEntry> entries = new ArrayList<>();
        long size = logChannel.size();
        long pos = 0;
        ByteBuffer header = ByteBuffer.allocate(16);
        while (pos + 16 <= size) {
            header.clear();
            logChannel.read(header, pos);
            header.flip();
            long index = header.getLong();
            int eterm = header.getInt();
            int len = header.getInt();
            if (len < 0 || pos + 16 + len > size) {
                break; // partial trailing write
            }
            ByteBuffer cmd = ByteBuffer.allocate(len);
            logChannel.read(cmd, pos + 16);
            entries.add(new LogEntry(index, eterm, cmd.array()));
            pos += 16 + len;
        }
        return new State(term, votedFor, entries);
    }

    @Override
    public void saveMeta(int currentTerm, Integer votedFor) throws IOException {
        ByteBuffer b = ByteBuffer.allocate(8);
        b.putInt(currentTerm);
        b.putInt(votedFor == null ? -1 : votedFor);
        b.flip();
        Files.write(metaPath, b.array(),
                StandardOpenOption.CREATE, StandardOpenOption.WRITE, StandardOpenOption.TRUNCATE_EXISTING);
    }

    @Override
    public void appendEntry(LogEntry entry) throws IOException {
        writeEntry(entry, logChannel.size());
        logChannel.force(false);
    }

    private void writeEntry(LogEntry entry, long position) throws IOException {
        byte[] cmd = entry.command();
        ByteBuffer b = ByteBuffer.allocate(16 + cmd.length);
        b.putLong(entry.index());
        b.putInt(entry.term());
        b.putInt(cmd.length);
        b.put(cmd);
        b.flip();
        long pos = position;
        while (b.hasRemaining()) {
            pos += logChannel.write(b, pos);
        }
    }

    @Override
    public void rewriteLog(List<LogEntry> entries) throws IOException {
        logChannel.truncate(0);
        long pos = 0;
        for (LogEntry e : entries) {
            byte[] cmd = e.command();
            ByteBuffer b = ByteBuffer.allocate(16 + cmd.length);
            b.putLong(e.index());
            b.putInt(e.term());
            b.putInt(cmd.length);
            b.put(cmd);
            b.flip();
            while (b.hasRemaining()) {
                pos += logChannel.write(b, pos);
            }
        }
        logChannel.force(true);
    }

    @Override
    public void close() throws IOException {
        logChannel.force(true);
        logChannel.close();
    }
}
