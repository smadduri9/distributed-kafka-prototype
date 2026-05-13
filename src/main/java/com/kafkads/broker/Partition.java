package com.kafkads.broker;

import com.kafkads.broker.storage.SegmentManager;

/**
 * Represents a partition within a topic.
 */
public class Partition {
    private final String topicName;
    private final int partitionId;
    private final SegmentManager segmentManager;
    private volatile int leaderBrokerId = -1;
    private volatile boolean isLeader = false;
    
    public Partition(String topicName, int partitionId) {
        this.topicName = topicName;
        this.partitionId = partitionId;
        this.segmentManager = new SegmentManager(topicName, partitionId);
    }
    
    public String getTopicName() {
        return topicName;
    }
    
    public int getPartitionId() {
        return partitionId;
    }
    
    public SegmentManager getSegmentManager() {
        return segmentManager;
    }
    
    public int getLeaderBrokerId() {
        return leaderBrokerId;
    }
    
    public void setLeaderBrokerId(int leaderBrokerId) {
        this.leaderBrokerId = leaderBrokerId;
    }
    
    public boolean isLeader() {
        return isLeader;
    }
    
    public void setLeader(boolean isLeader) {
        this.isLeader = isLeader;
    }
    
    public String getPartitionKey() {
        return topicName + "-" + partitionId;
    }
}

