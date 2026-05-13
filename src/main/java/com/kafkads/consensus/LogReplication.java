package com.kafkads.consensus;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Handles log replication for Raft consensus.
 * Manages replication to followers and tracks match indices.
 */
public class LogReplication {
    private static final Logger logger = LoggerFactory.getLogger(LogReplication.class);
    
    private final RaftNode raftNode;
    private final Map<Integer, Long> nextIndex = new ConcurrentHashMap<>();
    private final Map<Integer, Long> matchIndex = new ConcurrentHashMap<>();
    
    public LogReplication(RaftNode raftNode) {
        this.raftNode = raftNode;
    }
    
    /**
     * Initializes replication state for all followers.
     */
    public void initializeReplicationState(List<Integer> followerIds) {
        RaftState state = raftNode.getState();
        long lastLogIndex = state.getLastLogIndex();
        
        for (Integer followerId : followerIds) {
            nextIndex.put(followerId, lastLogIndex + 1);
            matchIndex.put(followerId, 0L);
        }
        
        logger.debug("Initialized replication state: lastLogIndex={}, followers={}", 
            lastLogIndex, followerIds);
    }
    
    /**
     * Handles an AppendEntries RPC response from a follower.
     */
    public void handleAppendEntriesResponse(int followerId, long term, boolean success, long matchIndex) {
        RaftState state = raftNode.getState();
        
        // If term > currentTerm, become follower
        if (term > state.getCurrentTerm()) {
            state.setCurrentTerm(term);
            raftNode.becomeFollower();
            return;
        }
        
        if (!raftNode.isLeader()) {
            return;
        }
        
        if (success) {
            // Update nextIndex and matchIndex for follower
            this.matchIndex.put(followerId, matchIndex);
            this.nextIndex.put(followerId, matchIndex + 1);
            
            // Update commitIndex
            updateCommitIndex();
        } else {
            // Decrement nextIndex and retry
            Long currentNextIndex = this.nextIndex.get(followerId);
            if (currentNextIndex != null && currentNextIndex > 0) {
                this.nextIndex.put(followerId, currentNextIndex - 1);
            }
        }
    }
    
    /**
     * Updates commitIndex based on match indices of followers.
     */
    private void updateCommitIndex() {
        RaftState state = raftNode.getState();
        long lastLogIndex = state.getLastLogIndex();
        int majority = (raftNode.getClusterNodes().size() / 2) + 1;
        
        // Find the highest index that is replicated to a majority
        for (long index = state.getCommitIndex() + 1; index <= lastLogIndex; index++) {
            int replicatedCount = 1; // Leader always has the entry
            RaftState.LogEntry entry = state.getLogEntry((int) index);
            if (entry == null) {
                continue;
            }
            
            // Count followers that have replicated this entry
            for (Long matchIdx : matchIndex.values()) {
                if (matchIdx >= index) {
                    replicatedCount++;
                }
            }
            
            // If majority has replicated and entry is from current term, commit
            if (replicatedCount >= majority && entry.getTerm() == state.getCurrentTerm()) {
                state.setCommitIndex(index);
                logger.debug("Updated commitIndex: {}", index);
            }
        }
    }
    
    /**
     * Handles an AppendEntries RPC from a leader.
     */
    public boolean handleAppendEntries(long term, int leaderId, long prevLogIndex, 
                                      long prevLogTerm, List<RaftState.LogEntry> entries, 
                                      long leaderCommit) {
        RaftState state = raftNode.getState();
        
        // Reply false if term < currentTerm
        if (term < state.getCurrentTerm()) {
            return false;
        }
        
        // If term > currentTerm, update term and become follower
        if (term > state.getCurrentTerm()) {
            state.setCurrentTerm(term);
            raftNode.becomeFollower();
        }
        
        // Reset election timeout
        raftNode.getLeaderElection().resetElectionTimeout();
        
        // Reply false if log doesn't contain an entry at prevLogIndex with term prevLogTerm
        if (prevLogIndex >= 0) {
            RaftState.LogEntry prevEntry = state.getLogEntry((int) prevLogIndex);
            if (prevEntry == null || prevEntry.getTerm() != prevLogTerm) {
                return false;
            }
        }
        
        // If an existing entry conflicts with a new one, delete the entry and all that follow
        if (entries != null && !entries.isEmpty()) {
            int conflictIndex = -1;
            for (int i = 0; i < entries.size(); i++) {
                int logIndex = (int) (prevLogIndex + 1 + i);
                RaftState.LogEntry existingEntry = state.getLogEntry(logIndex);
                RaftState.LogEntry newEntry = entries.get(i);
                
                if (existingEntry != null && existingEntry.getTerm() != newEntry.getTerm()) {
                    conflictIndex = logIndex;
                    break;
                }
            }
            
            if (conflictIndex >= 0) {
                state.truncateLogFrom(conflictIndex);
            }
            
            // Append any new entries not already in the log
            for (RaftState.LogEntry entry : entries) {
                int logIndex = (int) (prevLogIndex + 1 + entries.indexOf(entry));
                RaftState.LogEntry existingEntry = state.getLogEntry(logIndex);
                if (existingEntry == null) {
                    state.appendLogEntry(entry);
                }
            }
        }
        
        // If leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index of last new entry)
        if (leaderCommit > state.getCommitIndex()) {
            long lastNewEntryIndex = prevLogIndex + (entries != null ? entries.size() : 0);
            state.setCommitIndex(Math.min(leaderCommit, lastNewEntryIndex));
        }
        
        return true;
    }
    
    public Map<Integer, Long> getNextIndex() {
        return new ConcurrentHashMap<>(nextIndex);
    }
    
    public Map<Integer, Long> getMatchIndex() {
        return new ConcurrentHashMap<>(matchIndex);
    }
}

