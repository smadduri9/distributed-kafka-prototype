package com.kafkads.consensus;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Handles leader election logic for Raft consensus.
 */
public class LeaderElection {
    private static final Logger logger = LoggerFactory.getLogger(LeaderElection.class);
    
    private final RaftNode raftNode;
    private final long electionTimeoutMs;
    private final AtomicLong lastHeartbeatTime = new AtomicLong(System.currentTimeMillis());
    private final AtomicBoolean electionInProgress = new AtomicBoolean(false);
    
    public LeaderElection(RaftNode raftNode, long electionTimeoutMs) {
        this.raftNode = raftNode;
        this.electionTimeoutMs = electionTimeoutMs;
    }
    
    /**
     * Starts the election process.
     * Called when election timeout expires.
     */
    public void startElection() {
        if (electionInProgress.compareAndSet(false, true)) {
            logger.info("Starting election: nodeId={}, term={}", 
                raftNode.getNodeId(), raftNode.getState().getCurrentTerm());
            
            // Increment current term
            long newTerm = raftNode.getState().getCurrentTerm() + 1;
            raftNode.getState().setCurrentTerm(newTerm);
            
            // Vote for self
            raftNode.getState().setVotedFor(raftNode.getNodeId());
            
            // Request votes from other nodes
            int votesReceived = 1; // Self vote
            int totalNodes = raftNode.getClusterNodes().size();
            int majority = (totalNodes / 2) + 1;
            
            // In a real implementation, this would send RequestVote RPCs to other nodes
            // For now, we'll simulate the election process
            logger.info("Election started: term={}, votesReceived={}, majority={}", 
                newTerm, votesReceived, majority);
            
            // If we have majority, become leader
            if (votesReceived >= majority) {
                raftNode.becomeLeader();
            } else {
                // Reset election in progress after timeout
                electionInProgress.set(false);
            }
        }
    }
    
    /**
     * Handles a RequestVote RPC from a candidate.
     */
    public boolean handleRequestVote(long term, int candidateId, long lastLogTerm, long lastLogIndex) {
        RaftState state = raftNode.getState();
        
        // Reply false if term < currentTerm
        if (term < state.getCurrentTerm()) {
            return false;
        }
        
        // If term > currentTerm, update term and become follower
        if (term > state.getCurrentTerm()) {
            state.setCurrentTerm(term);
            state.setVotedFor(null);
            raftNode.becomeFollower();
        }
        
        // Grant vote if:
        // 1. votedFor is null or candidateId
        // 2. candidate's log is at least as up-to-date as receiver's log
        Integer votedFor = state.getVotedFor();
        if ((votedFor == null || votedFor == candidateId) && 
            isCandidateLogUpToDate(lastLogTerm, lastLogIndex, state)) {
            state.setVotedFor(candidateId);
            resetElectionTimeout();
            return true;
        }
        
        return false;
    }
    
    /**
     * Checks if candidate's log is at least as up-to-date as receiver's log.
     */
    private boolean isCandidateLogUpToDate(long candidateLastLogTerm, long candidateLastLogIndex, 
                                          RaftState state) {
        long receiverLastLogTerm = state.getLastLogTerm();
        long receiverLastLogIndex = state.getLastLogIndex();
        
        return (candidateLastLogTerm > receiverLastLogTerm) ||
               (candidateLastLogTerm == receiverLastLogTerm && 
                candidateLastLogIndex >= receiverLastLogIndex);
    }
    
    /**
     * Resets the election timeout.
     */
    public void resetElectionTimeout() {
        lastHeartbeatTime.set(System.currentTimeMillis());
    }
    
    /**
     * Checks if election timeout has expired.
     */
    public boolean isElectionTimeoutExpired() {
        long elapsed = System.currentTimeMillis() - lastHeartbeatTime.get();
        return elapsed > electionTimeoutMs;
    }
    
    public void setElectionInProgress(boolean inProgress) {
        electionInProgress.set(inProgress);
    }
}

