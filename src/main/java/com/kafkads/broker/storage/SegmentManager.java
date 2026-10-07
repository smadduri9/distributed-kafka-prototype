package com.kafkads.broker.storage;

import java.io.Closeable;
import java.io.IOException;
import java.nio.file.Path;
import java.util.List;

/**
 * Owns the active log segment for one topic partition.
 */
public class SegmentManager implements Closeable {
    private static final Path DEFAULT_DATA_DIR = Path.of("./data/broker");

    private final LogSegment activeSegment;

    public SegmentManager(String topicName, int partitionId) {
        this(DEFAULT_DATA_DIR, topicName, partitionId);
    }

    public SegmentManager(Path dataDir, String topicName, int partitionId) {
        try {
            Path segmentPath = dataDir
                .resolve(topicName)
                .resolve("partition-" + partitionId)
                .resolve("segment-0.log");
            this.activeSegment = new LogSegment(segmentPath);
        } catch (IOException e) {
            throw new IllegalStateException("Unable to open segment for " + topicName + "-" + partitionId, e);
        }
    }

    public long append(byte[] message) throws IOException {
        return activeSegment.append(message);
    }

    public List<LogSegment.MessageRecord> readFromOffset(long offset, int maxMessages) {
        return activeSegment.readFromOffset(offset, maxMessages);
    }

    public long sizeBytes() throws IOException {
        return activeSegment.sizeBytes();
    }

    @Override
    public void close() throws IOException {
        activeSegment.close();
    }
}
