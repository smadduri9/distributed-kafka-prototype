package com.kafkads.consumer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Manages consumer offsets for tracking consumption progress.
 */
public class OffsetManager {
    private static final Logger logger = LoggerFactory.getLogger(OffsetManager.class);
    
    private final String groupId;
    private final Path offsetFilePath;
    private final ConcurrentHashMap<String, Long> offsets = new ConcurrentHashMap<>();
    
    public OffsetManager(String groupId, String dataDir) {
        this.groupId = groupId;
        this.offsetFilePath = Paths.get(dataDir, "offsets", groupId + ".offsets");
        loadOffsets();
    }
    
    /**
     * Commits an offset for a topic partition.
     */
    public void commitOffset(String topicName, int partitionId, long offset) {
        String key = topicName + "-" + partitionId;
        offsets.put(key, offset);
        saveOffsets();
        logger.debug("Offset committed: group={}, topic={}, partition={}, offset={}", 
            groupId, topicName, partitionId, offset);
    }
    
    /**
     * Gets the committed offset for a topic partition.
     */
    public Long getCommittedOffset(String topicName, int partitionId) {
        String key = topicName + "-" + partitionId;
        return offsets.get(key);
    }
    
    /**
     * Loads offsets from disk.
     */
    private void loadOffsets() {
        if (!Files.exists(offsetFilePath)) {
            logger.debug("Offset file does not exist: {}", offsetFilePath);
            return;
        }
        
        try (ObjectInputStream ois = new ObjectInputStream(
            new FileInputStream(offsetFilePath.toFile()))) {
            @SuppressWarnings("unchecked")
            Map<String, Long> loadedOffsets = (Map<String, Long>) ois.readObject();
            offsets.putAll(loadedOffsets);
            logger.info("Loaded offsets: group={}, count={}", groupId, offsets.size());
        } catch (Exception e) {
            logger.error("Error loading offsets", e);
        }
    }
    
    /**
     * Saves offsets to disk.
     */
    private void saveOffsets() {
        try {
            Files.createDirectories(offsetFilePath.getParent());
            try (ObjectOutputStream oos = new ObjectOutputStream(
                new FileOutputStream(offsetFilePath.toFile()))) {
                oos.writeObject(new HashMap<>(offsets));
            }
        } catch (Exception e) {
            logger.error("Error saving offsets", e);
        }
    }
    
    public void close() {
        saveOffsets();
    }
}

