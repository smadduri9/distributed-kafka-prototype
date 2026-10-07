package com.kafkads.broker.storage;

import java.io.Closeable;
import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Append-only log segment for a topic partition.
 */
public class LogSegment implements Closeable {
    private static final int RECORD_HEADER_BYTES = Long.BYTES + Integer.BYTES;

    private final Path segmentFile;
    private final List<MessageRecord> records = new ArrayList<>();
    private final FileChannel channel;
    private long nextOffset;

    public LogSegment(Path segmentFile) throws IOException {
        this.segmentFile = segmentFile;
        Files.createDirectories(segmentFile.getParent());
        this.channel = FileChannel.open(
            segmentFile,
            StandardOpenOption.CREATE,
            StandardOpenOption.READ,
            StandardOpenOption.WRITE
        );
        loadExistingRecords();
    }

    public synchronized long append(byte[] message) throws IOException {
        long offset = nextOffset++;
        byte[] messageCopy = message.clone();

        ByteBuffer buffer = ByteBuffer.allocate(RECORD_HEADER_BYTES + messageCopy.length);
        buffer.putLong(offset);
        buffer.putInt(messageCopy.length);
        buffer.put(messageCopy);
        buffer.flip();

        channel.position(channel.size());
        while (buffer.hasRemaining()) {
            channel.write(buffer);
        }
        channel.force(false);

        records.add(new MessageRecord(offset, messageCopy));
        return offset;
    }

    public synchronized List<MessageRecord> readFromOffset(long offset, int maxMessages) {
        if (maxMessages <= 0) {
            return Collections.emptyList();
        }

        List<MessageRecord> result = new ArrayList<>();
        for (MessageRecord record : records) {
            if (record.getOffset() >= offset) {
                result.add(record);
                if (result.size() >= maxMessages) {
                    break;
                }
            }
        }
        return result;
    }

    public synchronized long sizeBytes() throws IOException {
        return channel.size();
    }

    @Override
    public synchronized void close() throws IOException {
        channel.close();
    }

    private void loadExistingRecords() throws IOException {
        channel.position(0);
        ByteBuffer header = ByteBuffer.allocate(RECORD_HEADER_BYTES);

        while (true) {
            header.clear();
            int headerBytes = readFully(header);
            if (headerBytes == -1) {
                break;
            }
            if (headerBytes < RECORD_HEADER_BYTES) {
                throw new EOFException("Corrupt partial record header in " + segmentFile);
            }

            header.flip();
            long offset = header.getLong();
            int messageLength = header.getInt();
            if (messageLength < 0) {
                throw new IOException("Negative message length in " + segmentFile);
            }

            ByteBuffer payload = ByteBuffer.allocate(messageLength);
            int payloadBytes = readFully(payload);
            if (payloadBytes < messageLength) {
                throw new EOFException("Corrupt partial record payload in " + segmentFile);
            }

            records.add(new MessageRecord(offset, payload.array()));
            nextOffset = Math.max(nextOffset, offset + 1);
        }
    }

    private int readFully(ByteBuffer buffer) throws IOException {
        int totalRead = 0;
        while (buffer.hasRemaining()) {
            int bytesRead = channel.read(buffer);
            if (bytesRead == -1) {
                return totalRead == 0 ? -1 : totalRead;
            }
            totalRead += bytesRead;
        }
        return totalRead;
    }

    public static class MessageRecord {
        private final long offset;
        private final byte[] message;

        public MessageRecord(long offset, byte[] message) {
            this.offset = offset;
            this.message = message.clone();
        }

        public long getOffset() {
            return offset;
        }

        public byte[] getMessage() {
            return message.clone();
        }
    }
}
