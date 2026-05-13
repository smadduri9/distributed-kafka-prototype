package com.kafkads.transaction;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Manages transaction state for transactional producers and consumers.
 */
public class TransactionManager {
    private static final Logger logger = LoggerFactory.getLogger(TransactionManager.class);
    
    public enum TransactionState {
        EMPTY,
        ONGOING,
        PREPARE_COMMIT,
        COMMITTING,
        COMMIT_COMPLETE,
        PREPARE_ABORT,
        ABORTING,
        ABORT_COMPLETE
    }
    
    public static class Transaction {
        private final String transactionId;
        private final long producerId;
        private final long producerEpoch;
        private volatile TransactionState state = TransactionState.EMPTY;
        private final List<TransactionRecord> records = new ArrayList<>();
        private final long timeoutMs;
        private final long startTime;
        
        public Transaction(String transactionId, long producerId, long producerEpoch, long timeoutMs) {
            this.transactionId = transactionId;
            this.producerId = producerId;
            this.producerEpoch = producerEpoch;
            this.timeoutMs = timeoutMs;
            this.startTime = System.currentTimeMillis();
        }
        
        public String getTransactionId() { return transactionId; }
        public long getProducerId() { return producerId; }
        public long getProducerEpoch() { return producerEpoch; }
        public TransactionState getState() { return state; }
        public void setState(TransactionState state) { this.state = state; }
        public List<TransactionRecord> getRecords() { return records; }
        public boolean isExpired() {
            return System.currentTimeMillis() - startTime > timeoutMs;
        }
    }
    
    public static class TransactionRecord {
        private final String topicName;
        private final int partitionId;
        private final long offset;
        private final byte[] message;
        
        public TransactionRecord(String topicName, int partitionId, long offset, byte[] message) {
            this.topicName = topicName;
            this.partitionId = partitionId;
            this.offset = offset;
            this.message = message;
        }
        
        public String getTopicName() { return topicName; }
        public int getPartitionId() { return partitionId; }
        public long getOffset() { return offset; }
        public byte[] getMessage() { return message; }
    }
    
    private final ConcurrentHashMap<String, Transaction> transactions = new ConcurrentHashMap<>();
    private final AtomicLong nextProducerId = new AtomicLong(1);
    
    /**
     * Begins a new transaction.
     */
    public Transaction beginTransaction(String transactionId, long timeoutMs) {
        long producerId = nextProducerId.getAndIncrement();
        long producerEpoch = 1;
        
        Transaction transaction = new Transaction(transactionId, producerId, producerEpoch, timeoutMs);
        transaction.setState(TransactionState.ONGOING);
        transactions.put(transactionId, transaction);
        
        logger.info("Transaction begun: id={}, producerId={}", transactionId, producerId);
        return transaction;
    }
    
    /**
     * Adds a record to a transaction.
     */
    public void addRecord(String transactionId, String topicName, int partitionId, 
                         long offset, byte[] message) {
        Transaction transaction = transactions.get(transactionId);
        if (transaction == null) {
            throw new IllegalStateException("Transaction not found: " + transactionId);
        }
        
        if (transaction.getState() != TransactionState.ONGOING) {
            throw new IllegalStateException("Transaction not in ONGOING state: " + transactionId);
        }
        
        TransactionRecord record = new TransactionRecord(topicName, partitionId, offset, message);
        transaction.getRecords().add(record);
        
        logger.debug("Record added to transaction: id={}, topic={}, partition={}, offset={}", 
            transactionId, topicName, partitionId, offset);
    }
    
    /**
     * Prepares a transaction for commit (two-phase commit).
     */
    public boolean prepareCommit(String transactionId) {
        Transaction transaction = transactions.get(transactionId);
        if (transaction == null) {
            return false;
        }
        
        transaction.setState(TransactionState.PREPARE_COMMIT);
        logger.info("Transaction prepared for commit: id={}", transactionId);
        return true;
    }
    
    /**
     * Commits a transaction.
     */
    public boolean commitTransaction(String transactionId) {
        Transaction transaction = transactions.get(transactionId);
        if (transaction == null) {
            return false;
        }
        
        transaction.setState(TransactionState.COMMITTING);
        // In a real implementation, would write commit marker to transaction log
        transaction.setState(TransactionState.COMMIT_COMPLETE);
        transactions.remove(transactionId);
        
        logger.info("Transaction committed: id={}", transactionId);
        return true;
    }
    
    /**
     * Aborts a transaction.
     */
    public boolean abortTransaction(String transactionId) {
        Transaction transaction = transactions.get(transactionId);
        if (transaction == null) {
            return false;
        }
        
        transaction.setState(TransactionState.ABORTING);
        // In a real implementation, would write abort marker to transaction log
        transaction.setState(TransactionState.ABORT_COMPLETE);
        transactions.remove(transactionId);
        
        logger.info("Transaction aborted: id={}", transactionId);
        return true;
    }
    
    /**
     * Gets a transaction by ID.
     */
    public Transaction getTransaction(String transactionId) {
        return transactions.get(transactionId);
    }
    
    /**
     * Cleans up expired transactions.
     */
    public void cleanupExpiredTransactions() {
        List<String> expired = new ArrayList<>();
        for (Map.Entry<String, Transaction> entry : transactions.entrySet()) {
            if (entry.getValue().isExpired()) {
                expired.add(entry.getKey());
            }
        }
        
        for (String transactionId : expired) {
            logger.warn("Transaction expired, aborting: id={}", transactionId);
            abortTransaction(transactionId);
        }
    }
}

