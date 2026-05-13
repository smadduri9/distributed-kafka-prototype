package com.kafkads.controller;

import com.kafkads.broker.Partition;
import com.kafkads.broker.Topic;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * Manages cluster metadata including topics, partitions, and broker assignments.
 */
public class MetadataManager {
    private final ConcurrentMap<String, TopicMetadata> topics = new ConcurrentHashMap<>();
    private final ConcurrentMap<Integer, BrokerMetadata> brokers = new ConcurrentHashMap<>();
    private final ConcurrentMap<String, PartitionAssignment> partitionAssignments = new ConcurrentHashMap<>();
    
    /**
     * Metadata for a topic.
     */
    public static class TopicMetadata {
        private final String name;
        private final int numPartitions;
        private final int replicationFactor;
        private final long createdAt;
        
        public TopicMetadata(String name, int numPartitions, int replicationFactor) {
            this.name = name;
            this.numPartitions = numPartitions;
            this.replicationFactor = replicationFactor;
            this.createdAt = System.currentTimeMillis();
        }
        
        public String getName() { return name; }
        public int getNumPartitions() { return numPartitions; }
        public int getReplicationFactor() { return replicationFactor; }
        public long getCreatedAt() { return createdAt; }
    }
    
    /**
     * Metadata for a broker.
     */
    public static class BrokerMetadata {
        private final int brokerId;
        private final String host;
        private final int port;
        private volatile boolean alive = true;
        private volatile long lastHeartbeat = System.currentTimeMillis();
        
        public BrokerMetadata(int brokerId, String host, int port) {
            this.brokerId = brokerId;
            this.host = host;
            this.port = port;
        }
        
        public int getBrokerId() { return brokerId; }
        public String getHost() { return host; }
        public int getPort() { return port; }
        public boolean isAlive() { return alive; }
        public void setAlive(boolean alive) { this.alive = alive; }
        public long getLastHeartbeat() { return lastHeartbeat; }
        public void updateHeartbeat() { this.lastHeartbeat = System.currentTimeMillis(); }
    }
    
    /**
     * Partition assignment information.
     */
    public static class PartitionAssignment {
        private final String topicName;
        private final int partitionId;
        private volatile int leaderBrokerId;
        private final List<Integer> replicaBrokerIds;
        
        public PartitionAssignment(String topicName, int partitionId, int leaderBrokerId, List<Integer> replicaBrokerIds) {
            this.topicName = topicName;
            this.partitionId = partitionId;
            this.leaderBrokerId = leaderBrokerId;
            this.replicaBrokerIds = new ArrayList<>(replicaBrokerIds);
        }
        
        public String getTopicName() { return topicName; }
        public int getPartitionId() { return partitionId; }
        public int getLeaderBrokerId() { return leaderBrokerId; }
        public void setLeaderBrokerId(int leaderBrokerId) { this.leaderBrokerId = leaderBrokerId; }
        public List<Integer> getReplicaBrokerIds() { return new ArrayList<>(replicaBrokerIds); }
    }
    
    /**
     * Registers a broker in the cluster.
     */
    public void registerBroker(int brokerId, String host, int port) {
        BrokerMetadata metadata = new BrokerMetadata(brokerId, host, port);
        brokers.put(brokerId, metadata);
    }
    
    /**
     * Updates broker heartbeat.
     */
    public void updateBrokerHeartbeat(int brokerId) {
        BrokerMetadata metadata = brokers.get(brokerId);
        if (metadata != null) {
            metadata.updateHeartbeat();
            metadata.setAlive(true);
        }
    }
    
    /**
     * Marks a broker as dead.
     */
    public void markBrokerDead(int brokerId) {
        BrokerMetadata metadata = brokers.get(brokerId);
        if (metadata != null) {
            metadata.setAlive(false);
        }
    }
    
    /**
     * Registers a topic.
     */
    public void registerTopic(String topicName, int numPartitions, int replicationFactor) {
        TopicMetadata metadata = new TopicMetadata(topicName, numPartitions, replicationFactor);
        topics.put(topicName, metadata);
    }
    
    /**
     * Gets topic metadata.
     */
    public TopicMetadata getTopicMetadata(String topicName) {
        return topics.get(topicName);
    }
    
    /**
     * Gets all topics.
     */
    public Collection<TopicMetadata> getAllTopics() {
        return topics.values();
    }
    
    /**
     * Assigns a partition to brokers.
     */
    public void assignPartition(String topicName, int partitionId, int leaderBrokerId, List<Integer> replicaBrokerIds) {
        String key = topicName + "-" + partitionId;
        PartitionAssignment assignment = new PartitionAssignment(topicName, partitionId, leaderBrokerId, replicaBrokerIds);
        partitionAssignments.put(key, assignment);
    }
    
    /**
     * Gets partition assignment.
     */
    public PartitionAssignment getPartitionAssignment(String topicName, int partitionId) {
        String key = topicName + "-" + partitionId;
        return partitionAssignments.get(key);
    }
    
    /**
     * Gets all alive brokers.
     */
    public List<BrokerMetadata> getAliveBrokers() {
        List<BrokerMetadata> alive = new ArrayList<>();
        for (BrokerMetadata broker : brokers.values()) {
            if (broker.isAlive()) {
                alive.add(broker);
            }
        }
        return alive;
    }
    
    /**
     * Gets broker metadata.
     */
    public BrokerMetadata getBrokerMetadata(int brokerId) {
        return brokers.get(brokerId);
    }
    
    /**
     * Gets all brokers.
     */
    public Collection<BrokerMetadata> getAllBrokers() {
        return brokers.values();
    }
}

