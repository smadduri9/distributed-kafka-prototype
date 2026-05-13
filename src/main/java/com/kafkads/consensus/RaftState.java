package com.kafkads.consensus;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * Represents the persistent state of a Raft node.
 */
public class RaftState {
    private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
    
    // Persistent state (updated on stable storage before responding to RPCs)
    private volatile long currentTerm = 0;
    private volatile Integer votedFor = null; // candidateId that received vote in current term
    private final List<LogEntry> log = new ArrayList<>();
    
    // Volatile state (reinitialized after election)
    private volatile long commitIndex = 0;
    private volatile long lastApplied = 0;
    
    /**
     * Log entry in Raft log.
     */
    public static class LogEntry {
        private final long term;
        private final byte[] command;
        
        public LogEntry(long term, byte[] command) {
            this.term = term;
            this.command = command;
        }
        
        public long getTerm() {
            return term;
        }
        
        public byte[] getCommand() {
            return command;
        }
    }
    
    public long getCurrentTerm() {
        lock.readLock().lock();
        try {
            return currentTerm;
        } finally {
            lock.readLock().unlock();
        }
    }
    
    public void setCurrentTerm(long term) {
        lock.writeLock().lock();
        try {
            this.currentTerm = term;
            this.votedFor = null; // Reset votedFor when term changes
        } finally {
            lock.writeLock().unlock();
        }
    }
    
    public Integer getVotedFor() {
        lock.readLock().lock();
        try {
            return votedFor;
        } finally {
            lock.readLock().unlock();
        }
    }
    
    public void setVotedFor(Integer candidateId) {
        lock.writeLock().lock();
        try {
            this.votedFor = candidateId;
        } finally {
            lock.writeLock().unlock();
        }
    }
    
    public List<LogEntry> getLog() {
        lock.readLock().lock();
        try {
            return new ArrayList<>(log);
        } finally {
            lock.readLock().unlock();
        }
    }
    
    public void appendLogEntry(LogEntry entry) {
        lock.writeLock().lock();
        try {
            log.add(entry);
        } finally {
            lock.writeLock().unlock();
        }
    }
    
    public LogEntry getLogEntry(int index) {
        lock.readLock().lock();
        try {
            if (index < 0 || index >= log.size()) {
                return null;
            }
            return log.get(index);
        } finally {
            lock.readLock().unlock();
        }
    }
    
    public int getLogSize() {
        lock.readLock().lock();
        try {
            return log.size();
        } finally {
            lock.readLock().unlock();
        }
    }
    
    public long getLastLogTerm() {
        lock.readLock().lock();
        try {
            if (log.isEmpty()) {
                return 0;
            }
            return log.get(log.size() - 1).getTerm();
        } finally {
            lock.readLock().unlock();
        }
    }
    
    public long getLastLogIndex() {
        lock.readLock().lock();
        try {
            return log.size() - 1;
        } finally {
            lock.readLock().unlock();
        }
    }
    
    public void truncateLogFrom(int index) {
        lock.writeLock().lock();
        try {
            if (index >= 0 && index < log.size()) {
                log.subList(index, log.size()).clear();
            }
        } finally {
            lock.writeLock().unlock();
        }
    }
    
    public long getCommitIndex() {
        return commitIndex;
    }
    
    public void setCommitIndex(long commitIndex) {
        this.commitIndex = commitIndex;
    }
    
    public long getLastApplied() {
        return lastApplied;
    }
    
    public void setLastApplied(long lastApplied) {
        this.lastApplied = lastApplied;
    }
}

