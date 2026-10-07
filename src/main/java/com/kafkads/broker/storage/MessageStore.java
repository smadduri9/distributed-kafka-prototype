package com.kafkads.broker.storage;

import com.kafkads.config.BrokerConfig;

import java.io.Closeable;
import java.io.IOException;
import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * Stores messages by topic partition using append-only log segments.
 */
public class MessageStore implements Closeable {
    private final Path dataDir;
    private final ConcurrentMap<String, SegmentManager> segments = new ConcurrentHashMap<>();

    public MessageStore(BrokerConfig config) {
        this.dataDir = Path.of(config.getDataDir());
    }

    public long append(String topicName, int partitionId, byte[] message) throws IOException {
        return getSegment(topicName, partitionId).append(message);
    }

    public List<LogSegment.MessageRecord> readFromOffset(
            String topicName, int partitionId, long offset, int maxMessages) {
        return getSegment(topicName, partitionId).readFromOffset(offset, maxMessages);
    }

    @Override
    public void close() throws IOException {
        IOException closeError = null;
        for (SegmentManager segment : segments.values()) {
            try {
                segment.close();
            } catch (IOException e) {
                if (closeError == null) {
                    closeError = e;
                } else {
                    closeError.addSuppressed(e);
                }
            }
        }

        if (closeError != null) {
            throw closeError;
        }
    }

    private SegmentManager getSegment(String topicName, int partitionId) {
        String key = topicName + "-" + partitionId;
        return segments.computeIfAbsent(
            key,
            ignored -> new SegmentManager(dataDir, topicName, partitionId)
        );
    }
}
