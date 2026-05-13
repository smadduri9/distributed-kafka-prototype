package com.kafkads.replication;

import com.kafkads.broker.Partition;
import com.kafkads.broker.Topic;
import com.kafkads.controller.MetadataManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Manages replication of messages from leader to followers.
 */
public class ReplicationManager {
    private static final Logger logger = LoggerFactory.getLogger(ReplicationManager.class);
    
    private final int leaderBrokerId;
    private final MetadataManager metadataManager;
    private final Map<String, FollowerSync> followerSyncs = new ConcurrentHashMap<>();
    private final Map<String, AtomicLong> highWaterMarks = new ConcurrentHashMap<>();
    private final ReplicationProtocol.ReplicationMode replicationMode;
    
    public ReplicationManager(int leaderBrokerId, MetadataManager metadataManager, 
                             ReplicationProtocol.ReplicationMode replicationMode) {
        this.leaderBrokerId = leaderBrokerId;
        this.metadataManager = metadataManager;
        this.replicationMode = replicationMode;
    }
    
    /**
     * Replicates a message to followers.
     * Returns true if replication succeeded according to the replication mode.
     */
    public boolean replicateMessage(String topicName, int partitionId, long offset, 
                                   byte[] message, long leaderEpoch) {
        MetadataManager.PartitionAssignment assignment = 
            metadataManager.getPartitionAssignment(topicName, partitionId);
        
        if (assignment == null) {
            logger.warn("No partition assignment found: topic={}, partition={}", 
                topicName, partitionId);
            return false;
        }
        
        List<Integer> replicaBrokerIds = assignment.getReplicaBrokerIds();
        List<Integer> followerIds = new ArrayList<>();
        for (Integer brokerId : replicaBrokerIds) {
            if (brokerId != leaderBrokerId) {
                followerIds.add(brokerId);
            }
        }
        
        if (followerIds.isEmpty()) {
            // No followers, replication succeeds immediately
            return true;
        }
        
        ReplicationProtocol.ReplicationRequest request = 
            new ReplicationProtocol.ReplicationRequest(
                topicName, partitionId, offset, message, leaderEpoch
            );
        
        if (replicationMode == ReplicationProtocol.ReplicationMode.SYNC) {
            return replicateSync(followerIds, request);
        } else {
            return replicateAsync(followerIds, request);
        }
    }
    
    /**
     * Synchronous replication: waits for all replicas to acknowledge.
     */
    private boolean replicateSync(List<Integer> followerIds, 
                                  ReplicationProtocol.ReplicationRequest request) {
        CountDownLatch latch = new CountDownLatch(followerIds.size());
        List<Boolean> results = new ArrayList<>();
        
        for (Integer followerId : followerIds) {
            // In a real implementation, this would be async network calls
            ReplicationProtocol.ReplicationResponse response = 
                ReplicationProtocol.sendReplicationRequest(followerId, request);
            results.add(response.isSuccess());
            latch.countDown();
        }
        
        try {
            boolean completed = latch.await(5000, TimeUnit.MILLISECONDS);
            if (!completed) {
                logger.warn("Replication timeout: topic={}, partition={}, offset={}", 
                    request.getTopicName(), request.getPartitionId(), request.getOffset());
                return false;
            }
            
            // Check if all replications succeeded
            boolean allSuccess = results.stream().allMatch(Boolean::booleanValue);
            if (allSuccess) {
                updateHighWaterMark(request.getTopicName(), request.getPartitionId(), 
                    request.getOffset());
            }
            return allSuccess;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            logger.error("Replication interrupted", e);
            return false;
        }
    }
    
    /**
     * Asynchronous replication: doesn't wait for acknowledgments.
     */
    private boolean replicateAsync(List<Integer> followerIds, 
                                   ReplicationProtocol.ReplicationRequest request) {
        // Fire and forget - always return true
        for (Integer followerId : followerIds) {
            // In a real implementation, this would be async network calls
            ReplicationProtocol.sendReplicationRequest(followerId, request);
        }
        return true;
    }
    
    /**
     * Updates the high water mark for a partition.
     */
    private void updateHighWaterMark(String topicName, int partitionId, long offset) {
        String key = topicName + "-" + partitionId;
        highWaterMarks.computeIfAbsent(key, k -> new AtomicLong(0)).set(offset);
    }
    
    /**
     * Gets the high water mark for a partition.
     */
    public long getHighWaterMark(String topicName, int partitionId) {
        String key = topicName + "-" + partitionId;
        AtomicLong hwm = highWaterMarks.get(key);
        return hwm != null ? hwm.get() : 0;
    }
    
    /**
     * Registers a follower sync for a partition.
     */
    public void registerFollowerSync(String topicName, int partitionId, FollowerSync followerSync) {
        String key = topicName + "-" + partitionId;
        followerSyncs.put(key, followerSync);
    }
}

