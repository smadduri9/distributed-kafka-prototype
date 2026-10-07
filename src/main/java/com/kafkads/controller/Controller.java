package com.kafkads.controller;

import com.kafkads.config.ConfigLoader;
import com.kafkads.config.RedisConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Controller manages cluster metadata, partition assignments, and broker states.
 */
public class Controller {
    private static final Logger logger = LoggerFactory.getLogger(Controller.class);
    
    private final MetadataManager metadataManager;
    private final ClusterCache clusterCache;
    private final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(2);
    private volatile boolean running = false;
    static final long BROKER_TIMEOUT_MS = 10000; // 10 seconds
    static final int BROKER_TTL_SECONDS = (int) (BROKER_TIMEOUT_MS / 1000);
    
    public Controller() {
        this(ConfigLoader.loadConfig().toRedisConfig());
    }

    public Controller(RedisConfig redisConfig) {
        this(openCache(redisConfig));
    }

    public Controller(ClusterCache clusterCache) {
        this.metadataManager = new MetadataManager();
        this.clusterCache = clusterCache == null ? DisabledClusterCache.INSTANCE : clusterCache;
        MDC.put("brokerId", "controller");
    }

    private static ClusterCache openCache(RedisConfig redisConfig) {
        ClusterCache cache = RedisClusterCache.connect(redisConfig);
        return cache == null ? DisabledClusterCache.INSTANCE : cache;
    }
    
    /**
     * Starts the controller.
     */
    public void start() {
        if (running) {
            logger.warn("Controller is already running");
            return;
        }
        
        logger.info("Starting controller");
        running = true;
        
        // Start broker health checker
        scheduler.scheduleAtFixedRate(this::checkBrokerHealth, 
            5000, 5000, TimeUnit.MILLISECONDS);
    }
    
    /**
     * Stops the controller.
     */
    public void stop() {
        if (running) {
            logger.info("Stopping controller");
            running = false;
            scheduler.shutdown();
            try {
                if (!scheduler.awaitTermination(5, TimeUnit.SECONDS)) {
                    scheduler.shutdownNow();
                }
            } catch (InterruptedException e) {
                scheduler.shutdownNow();
                Thread.currentThread().interrupt();
            }
        }
        clusterCache.close();
    }
    
    /**
     * Registers a broker with the controller.
     */
    public void registerBroker(int brokerId, String host, int port) {
        metadataManager.registerBroker(brokerId, host, port);
        refreshBrokerKey(brokerId);
        logger.info("Broker registered: id={}, host={}, port={}", brokerId, host, port);
    }
    
    /**
     * Updates broker heartbeat and refreshes the Redis TTL key when Redis is up.
     */
    public void updateBrokerHeartbeat(int brokerId) {
        metadataManager.updateBrokerHeartbeat(brokerId);
        refreshBrokerKey(brokerId);
    }

    private void refreshBrokerKey(int brokerId) {
        if (!clusterCache.isEnabled()) {
            return;
        }
        MetadataManager.BrokerMetadata metadata = metadataManager.getBrokerMetadata(brokerId);
        if (metadata == null) {
            return;
        }
        try {
            clusterCache.touchBroker(metadata.getBrokerId(), metadata.getHost(), metadata.getPort(), BROKER_TTL_SECONDS);
        } catch (Exception e) {
            logger.warn("Redis heartbeat refresh failed for broker {}: {}", brokerId, e.getMessage());
        }
    }
    
    /**
     * Creates a topic and assigns partitions to brokers.
     */
    public boolean createTopic(String topicName, int numPartitions, int replicationFactor) {
        List<MetadataManager.BrokerMetadata> aliveBrokers = metadataManager.getAliveBrokers();
        
        if (aliveBrokers.size() < replicationFactor) {
            logger.warn("Not enough brokers for replication factor: required={}, available={}", 
                replicationFactor, aliveBrokers.size());
            return false;
        }
        
        // Register topic
        metadataManager.registerTopic(topicName, numPartitions, replicationFactor);
        
        // Assign partitions
        List<MetadataManager.PartitionAssignment> assignments = 
            PartitionAssignment.assignPartitions(topicName, numPartitions, replicationFactor, aliveBrokers);
        
        for (MetadataManager.PartitionAssignment assignment : assignments) {
            metadataManager.assignPartition(
                assignment.getTopicName(),
                assignment.getPartitionId(),
                assignment.getLeaderBrokerId(),
                assignment.getReplicaBrokerIds()
            );
            cacheAssignment(assignment);
        }
        
        logger.info("Topic created and partitions assigned: topic={}, partitions={}, replicationFactor={}", 
            topicName, numPartitions, replicationFactor);
        
        return true;
    }
    
    /**
     * Gets partition assignment for a topic and partition.
     */
    public MetadataManager.PartitionAssignment getPartitionAssignment(String topicName, int partitionId) {
        MetadataManager.PartitionAssignment local = metadataManager.getPartitionAssignment(topicName, partitionId);
        if (local != null || !clusterCache.isEnabled()) {
            return local;
        }
        try {
            ClusterCache.Assignment cached = clusterCache.getAssignment(topicName, partitionId);
            if (cached == null) {
                return null;
            }
            metadataManager.assignPartition(topicName, partitionId, cached.getLeaderBrokerId(), cached.getReplicaBrokerIds());
            return metadataManager.getPartitionAssignment(topicName, partitionId);
        } catch (Exception e) {
            logger.warn("Redis assignment read failed for {}-{}: {}", topicName, partitionId, e.getMessage());
            return null;
        }
    }

    private void cacheAssignment(MetadataManager.PartitionAssignment assignment) {
        if (!clusterCache.isEnabled()) {
            return;
        }
        try {
            clusterCache.putAssignment(
                assignment.getTopicName(),
                assignment.getPartitionId(),
                assignment.getLeaderBrokerId(),
                assignment.getReplicaBrokerIds()
            );
        } catch (Exception e) {
            logger.warn("Redis assignment cache write failed for {}-{}: {}",
                assignment.getTopicName(), assignment.getPartitionId(), e.getMessage());
        }
    }
    
    /**
     * Handles broker failure and reassigns partitions.
     */
    public void handleBrokerFailure(int brokerId) {
        logger.warn("Handling broker failure: brokerId={}", brokerId);
        metadataManager.markBrokerDead(brokerId);
        if (clusterCache.isEnabled()) {
            try {
                clusterCache.dropBroker(brokerId);
            } catch (Exception e) {
                logger.warn("Redis broker key delete failed for broker {}: {}", brokerId, e.getMessage());
            }
        }
        
        logger.info("Broker failure handled: brokerId={}", brokerId);
    }
    
    /**
     * Checks broker health and marks dead brokers.
     */
    private void checkBrokerHealth() {
        if (!running) {
            return;
        }
        detectDeadBrokers();
    }

    void detectDeadBrokers() {
        long currentTime = System.currentTimeMillis();
        for (MetadataManager.BrokerMetadata broker : metadataManager.getAllBrokers()) {
            if (!broker.isAlive()) {
                continue;
            }
            long timeSinceHeartbeat = currentTime - broker.getLastHeartbeat();
            boolean expiredLocally = timeSinceHeartbeat > BROKER_TIMEOUT_MS;
            boolean expiredInRedis = false;
            if (clusterCache.isEnabled()) {
                try {
                    expiredInRedis = !clusterCache.isBrokerAlive(broker.getBrokerId());
                } catch (Exception e) {
                    logger.warn("Redis liveness check failed for broker {}: {}", broker.getBrokerId(), e.getMessage());
                }
            }
            if (expiredLocally || expiredInRedis) {
                logger.warn("Broker timeout detected: brokerId={}, timeSinceHeartbeat={}ms, redisExpired={}",
                    broker.getBrokerId(), timeSinceHeartbeat, expiredInRedis);
                handleBrokerFailure(broker.getBrokerId());
            }
        }
    }
    
    public MetadataManager getMetadataManager() {
        return metadataManager;
    }
    
    public boolean isRunning() {
        return running;
    }
}

