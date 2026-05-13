package com.kafkads.controller;

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
    private final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(2);
    private volatile boolean running = false;
    private static final long BROKER_TIMEOUT_MS = 10000; // 10 seconds
    
    public Controller() {
        this.metadataManager = new MetadataManager();
        MDC.put("brokerId", "controller");
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
        if (!running) {
            return;
        }
        
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
    
    /**
     * Registers a broker with the controller.
     */
    public void registerBroker(int brokerId, String host, int port) {
        metadataManager.registerBroker(brokerId, host, port);
        logger.info("Broker registered: id={}, host={}, port={}", brokerId, host, port);
    }
    
    /**
     * Updates broker heartbeat.
     */
    public void updateBrokerHeartbeat(int brokerId) {
        metadataManager.updateBrokerHeartbeat(brokerId);
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
        }
        
        logger.info("Topic created and partitions assigned: topic={}, partitions={}, replicationFactor={}", 
            topicName, numPartitions, replicationFactor);
        
        return true;
    }
    
    /**
     * Gets partition assignment for a topic and partition.
     */
    public MetadataManager.PartitionAssignment getPartitionAssignment(String topicName, int partitionId) {
        return metadataManager.getPartitionAssignment(topicName, partitionId);
    }
    
    /**
     * Handles broker failure and reassigns partitions.
     */
    public void handleBrokerFailure(int brokerId) {
        logger.warn("Handling broker failure: brokerId={}", brokerId);
        metadataManager.markBrokerDead(brokerId);
        
        // Reassign partitions that were on the failed broker
        List<MetadataManager.BrokerMetadata> aliveBrokers = metadataManager.getAliveBrokers();
        
        // Get all partition assignments and reassign those affected by the failure
        // This is simplified - in production, would track which partitions need reassignment
        logger.info("Broker failure handled: brokerId={}", brokerId);
    }
    
    /**
     * Checks broker health and marks dead brokers.
     */
    private void checkBrokerHealth() {
        if (!running) {
            return;
        }
        
        long currentTime = System.currentTimeMillis();
        for (MetadataManager.BrokerMetadata broker : metadataManager.getAllBrokers()) {
            if (broker.isAlive()) {
                long timeSinceHeartbeat = currentTime - broker.getLastHeartbeat();
                if (timeSinceHeartbeat > BROKER_TIMEOUT_MS) {
                    logger.warn("Broker timeout detected: brokerId={}, timeSinceHeartbeat={}ms", 
                        broker.getBrokerId(), timeSinceHeartbeat);
                    handleBrokerFailure(broker.getBrokerId());
                }
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

