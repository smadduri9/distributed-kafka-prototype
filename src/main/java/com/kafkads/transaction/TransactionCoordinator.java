package com.kafkads.transaction;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Coordinates transactions across the cluster.
 * Implements two-phase commit protocol.
 */
public class TransactionCoordinator {
    private static final Logger logger = LoggerFactory.getLogger(TransactionCoordinator.class);
    
    private final TransactionManager transactionManager;
    private final ScheduledExecutorService scheduler;
    
    public TransactionCoordinator(TransactionManager transactionManager) {
        this.transactionManager = transactionManager;
        this.scheduler = Executors.newScheduledThreadPool(1);
    }
    
    /**
     * Starts the transaction coordinator.
     */
    public void start() {
        // Schedule cleanup of expired transactions
        scheduler.scheduleAtFixedRate(
            transactionManager::cleanupExpiredTransactions,
            60000, 60000, TimeUnit.MILLISECONDS
        );
        logger.info("Transaction coordinator started");
    }
    
    /**
     * Stops the transaction coordinator.
     */
    public void stop() {
        scheduler.shutdown();
        try {
            if (!scheduler.awaitTermination(5, TimeUnit.SECONDS)) {
                scheduler.shutdownNow();
            }
        } catch (InterruptedException e) {
            scheduler.shutdownNow();
            Thread.currentThread().interrupt();
        }
        logger.info("Transaction coordinator stopped");
    }
    
    /**
     * Begins a transaction.
     */
    public TransactionManager.Transaction beginTransaction(String transactionId, long timeoutMs) {
        return transactionManager.beginTransaction(transactionId, timeoutMs);
    }
    
    /**
     * Commits a transaction using two-phase commit.
     */
    public boolean commitTransaction(String transactionId) {
        // Phase 1: Prepare
        boolean prepared = transactionManager.prepareCommit(transactionId);
        if (!prepared) {
            logger.warn("Transaction prepare failed: id={}", transactionId);
            return false;
        }
        
        // Phase 2: Commit
        boolean committed = transactionManager.commitTransaction(transactionId);
        if (!committed) {
            logger.warn("Transaction commit failed: id={}", transactionId);
            return false;
        }
        
        logger.info("Transaction committed successfully: id={}", transactionId);
        return true;
    }
    
    /**
     * Aborts a transaction.
     */
    public boolean abortTransaction(String transactionId) {
        return transactionManager.abortTransaction(transactionId);
    }
    
    public TransactionManager getTransactionManager() {
        return transactionManager;
    }
}

