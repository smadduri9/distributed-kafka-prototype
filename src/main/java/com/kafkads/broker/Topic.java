package com.kafkads.broker;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * Represents a topic in the broker.
 */
public class Topic {
    private final String name;
    private final int numPartitions;
    private final int replicationFactor;
    private final ConcurrentMap<Integer, Partition> partitions;
    
    public Topic(String name, int numPartitions, int replicationFactor) {
        this.name = name;
        this.numPartitions = numPartitions;
        this.replicationFactor = replicationFactor;
        this.partitions = new ConcurrentHashMap<>();
        
        // Initialize partitions
        for (int i = 0; i < numPartitions; i++) {
            partitions.put(i, new Partition(name, i));
        }
    }
    
    public String getName() {
        return name;
    }
    
    public int getNumPartitions() {
        return numPartitions;
    }
    
    public int getReplicationFactor() {
        return replicationFactor;
    }
    
    public Partition getPartition(int partitionId) {
        return partitions.get(partitionId);
    }
    
    public List<Partition> getAllPartitions() {
        return new ArrayList<>(partitions.values());
    }
    
    public boolean hasPartition(int partitionId) {
        return partitions.containsKey(partitionId);
    }
}

