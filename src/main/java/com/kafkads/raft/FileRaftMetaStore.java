package com.kafkads.raft;

import com.kafkads.storage.CorruptRecordException;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.util.zip.CRC32;

/**
 * Stores {@code currentTerm} and {@code votedFor} in a small checksummed file that is
 * replaced atomically: write a temporary file, force it, rename it over the old one,
 * then force the directory. A reader therefore sees either the old or the new value,
 * never a mixture, and a damaged file is detected rather than read as "term 0, no vote"
 * (which could let this node vote twice in one term).
 *
 * <pre>file := magic(4) term(4) votedFor(4, -1 = none) crc32(4)</pre>
 */
public final class FileRaftMetaStore implements RaftMetaStore {

    private static final int MAGIC = 0x4B44534D; // "KDSM"
    private static final int FILE_BYTES = 16;

    private final Path dir;
    private final Path path;
    private final Path tmpPath;

    public FileRaftMetaStore(Path dir) throws IOException {
        Files.createDirectories(dir);
        this.dir = dir;
        this.path = dir.resolve("raft-meta");
        this.tmpPath = dir.resolve("raft-meta.tmp");
    }

    @Override
    public Meta load() throws IOException {
        // A leftover temporary file is an interrupted save that never took effect.
        Files.deleteIfExists(tmpPath);
        if (!Files.exists(path)) {
            return new Meta(false, 0, null);
        }
        byte[] bytes = Files.readAllBytes(path);
        if (bytes.length != FILE_BYTES) {
            throw new CorruptRecordException("Raft meta file " + path + " has length " + bytes.length);
        }
        ByteBuffer b = ByteBuffer.wrap(bytes);
        int magic = b.getInt();
        int term = b.getInt();
        int vote = b.getInt();
        int storedCrc = b.getInt();
        CRC32 crc = new CRC32();
        crc.update(bytes, 0, FILE_BYTES - 4);
        if (magic != MAGIC || (int) crc.getValue() != storedCrc || term < 0) {
            throw new CorruptRecordException("Raft meta file " + path + " is corrupt");
        }
        return new Meta(true, term, vote < 0 ? null : vote);
    }

    @Override
    public void save(int currentTerm, Integer votedFor) throws IOException {
        ByteBuffer b = ByteBuffer.allocate(FILE_BYTES);
        b.putInt(MAGIC);
        b.putInt(currentTerm);
        b.putInt(votedFor == null ? -1 : votedFor);
        CRC32 crc = new CRC32();
        crc.update(b.array(), 0, FILE_BYTES - 4);
        b.putInt((int) crc.getValue());
        b.flip();
        try (FileChannel ch = FileChannel.open(tmpPath, StandardOpenOption.CREATE,
                StandardOpenOption.WRITE, StandardOpenOption.TRUNCATE_EXISTING)) {
            while (b.hasRemaining()) {
                ch.write(b);
            }
            ch.force(true);
        }
        Files.move(tmpPath, path, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
        try (FileChannel d = FileChannel.open(dir, StandardOpenOption.READ)) {
            d.force(true);
        } catch (IOException | UnsupportedOperationException e) {
            // Directory fsync is not available on every platform; best effort.
        }
    }
}
