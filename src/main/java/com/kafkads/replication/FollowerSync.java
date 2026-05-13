package com.kafkads.replication;

import com.kafkads.broker.storage.MessageStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Handles follower synchronization logic.
 * Followers receive replication requests from leaders and apply them to their local log.
 */
public class FollowerSync {
    private static final Logger logger = LoggerFactory.getLogger(FollowerSync.class);
    
    private final int followerBrokerId;
    private final MessageStore messageStore;
    private final AtomicLong highWaterMark = new AtomicLong(0);
    private volatile long leaderEpoch = 0;
    
    public FollowerSync(int followerBrokerId, MessageStore messageStore) {
        this.followerBrokerId = followerBrokerId;
        this.messageStore = messageStore;
    }
    
    /**
     * Handles a replication request from the leader.
     */
    public ReplicationProtocol.ReplicationResponse handleReplicationRequest(
            ReplicationProtocol.ReplicationRequest request) {
        try {
            // Validate leader epoch
            if (request.getLeaderEpoch() < leaderEpoch) {
                logger.warn("Stale leader epoch: received={}, current={}", 
                    request.getLeaderEpoch(), leaderEpoch);
                return new ReplicationProtocol.ReplicationResponse(
                    false, request.getOffset(), "Stale leader epoch");
            }
            
            // Update leader epoch
            if (request.getLeaderEpoch() > leaderEpoch) {
                leaderEpoch = request.getLeaderEpoch();
            }
            
            // Validate offset (should be sequential)
            long expectedOffset = highWaterMark.get();
            if (request.getOffset() < expectedOffset) {
                logger.warn("Duplicate or out-of-order offset: received={}, expected={}", 
                    request.getOffset(), expectedOffset);
                return new ReplicationProtocol.ReplicationResponse(
                    false, request.getOffset(), "Out-of-order offset");
            }
            
            // Append message to local log
            long appendedOffset = messageStore.append(
                request.getTopicName(),
                request.getPartitionId(),
                request.getMessage()
            );
            
            // Update high water mark
            if (appendedOffset == request.getOffset()) {
                highWaterMark.set(appendedOffset);
                logger.debug("Message replicated: topic={}, partition={}, offset={}", 
                    request.getTopicName(), request.getPartitionId(), appendedOffset);
                return new ReplicationProtocol.ReplicationResponse(true, appendedOffset, null);
            } else {
                logger.error("Offset mismatch: expected={}, actual={}", 
                    request.getOffset(), appendedOffset);
                return new ReplicationProtocol.ReplicationResponse(
                    false, appendedOffset, "Offset mismatch");
            }
        } catch (IOException e) {
            logger.error("Error replicating message: topic={}, partition={}, offset={}", 
                request.getTopicName(), request.getPartitionId(), request.getOffset(), e);
            return new ReplicationProtocol.ReplicationResponse(
                false, request.getOffset(), "IO error: " + e.getMessage());
        }
    }
    
    /**
     * Updates the high water mark (committed offset).
     */
    public void updateHighWaterMark(long offset) {
        long current = highWaterMark.get();
        if (offset > current) {
            highWaterMark.set(offset);
            logger.debug("High water mark updated: {}", offset);
        }
    }
    
    public long getHighWaterMark() {
        return highWaterMark.get();
    }
    
    public long getLeaderEpoch() {
        return leaderEpoch;
    }
    
    public void setLeaderEpoch(long leaderEpoch) {
        this.leaderEpoch = leaderEpoch;
    }
}

