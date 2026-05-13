package com.kafkads.replication;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;

/**
 * Defines the replication protocol for leader-follower communication.
 */
public class ReplicationProtocol {
    private static final Logger logger = LoggerFactory.getLogger(ReplicationProtocol.class);
    
    public enum ReplicationMode {
        SYNC,  // Wait for all replicas
        ASYNC  // Don't wait for replicas
    }
    
    /**
     * Replication request from leader to follower.
     */
    public static class ReplicationRequest {
        private final String topicName;
        private final int partitionId;
        private final long offset;
        private final byte[] message;
        private final long leaderEpoch;
        
        public ReplicationRequest(String topicName, int partitionId, long offset, 
                                 byte[] message, long leaderEpoch) {
            this.topicName = topicName;
            this.partitionId = partitionId;
            this.offset = offset;
            this.message = message;
            this.leaderEpoch = leaderEpoch;
        }
        
        public String getTopicName() { return topicName; }
        public int getPartitionId() { return partitionId; }
        public long getOffset() { return offset; }
        public byte[] getMessage() { return message; }
        public long getLeaderEpoch() { return leaderEpoch; }
    }
    
    /**
     * Replication response from follower to leader.
     */
    public static class ReplicationResponse {
        private final boolean success;
        private final long offset;
        private final String errorMessage;
        
        public ReplicationResponse(boolean success, long offset, String errorMessage) {
            this.success = success;
            this.offset = offset;
            this.errorMessage = errorMessage;
        }
        
        public boolean isSuccess() { return success; }
        public long getOffset() { return offset; }
        public String getErrorMessage() { return errorMessage; }
    }
    
    /**
     * Sends a replication request to a follower.
     * In a real implementation, this would send over the network.
     */
    public static ReplicationResponse sendReplicationRequest(int followerBrokerId, 
                                                             ReplicationRequest request) {
        // This is a placeholder - in production, would send TCP request to follower
        logger.debug("Sending replication request: follower={}, topic={}, partition={}, offset={}", 
            followerBrokerId, request.getTopicName(), request.getPartitionId(), request.getOffset());
        
        // Simulate network call
        return new ReplicationResponse(true, request.getOffset(), null);
    }
}

