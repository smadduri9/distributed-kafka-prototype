package com.kafkads.consensus;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Represents a Raft node in the consensus cluster.
 */
public class RaftNode {
    private static final Logger logger = LoggerFactory.getLogger(RaftNode.class);
    
    public enum NodeState {
        FOLLOWER,
        CANDIDATE,
        LEADER
    }
    
    private final int nodeId;
    private final List<Integer> clusterNodes;
    private final RaftState state;
    private final LeaderElection leaderElection;
    private final LogReplication logReplication;
    private final long heartbeatIntervalMs;
    
    private volatile NodeState nodeState = NodeState.FOLLOWER;
    private final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(2);
    private final AtomicBoolean running = new AtomicBoolean(false);
    
    public RaftNode(int nodeId, List<Integer> clusterNodes, long electionTimeoutMs, long heartbeatIntervalMs) {
        this.nodeId = nodeId;
        this.clusterNodes = clusterNodes;
        this.state = new RaftState();
        this.leaderElection = new LeaderElection(this, electionTimeoutMs);
        this.logReplication = new LogReplication(this);
        this.heartbeatIntervalMs = heartbeatIntervalMs;
        // Set MDC for logging
        MDC.put("nodeId", String.valueOf(nodeId));
    }
    
    /**
     * Starts the Raft node.
     */
    public void start() {
        if (running.compareAndSet(false, true)) {
            MDC.put("nodeId", String.valueOf(nodeId));
            logger.info("Starting Raft node: nodeId={}", nodeId);
            
            // Start election timeout checker (check every 50ms)
            scheduler.scheduleAtFixedRate(this::checkElectionTimeout, 
                50, 
                50, 
                TimeUnit.MILLISECONDS);
            
            // Start heartbeat sender (if leader)
            scheduler.scheduleAtFixedRate(this::sendHeartbeat, 
                heartbeatIntervalMs, 
                heartbeatIntervalMs, 
                TimeUnit.MILLISECONDS);
        }
    }
    
    /**
     * Stops the Raft node.
     */
    public void stop() {
        if (running.compareAndSet(true, false)) {
            logger.info("Stopping Raft node: nodeId={}", nodeId);
            MDC.remove("nodeId");
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
    }
    
    /**
     * Checks if election timeout has expired and starts election if needed.
     */
    private void checkElectionTimeout() {
        if (!running.get()) {
            return;
        }
        
        if (nodeState != NodeState.LEADER && leaderElection.isElectionTimeoutExpired()) {
            becomeCandidate();
            leaderElection.startElection();
        }
    }
    
    /**
     * Sends heartbeat if this node is the leader.
     */
    private void sendHeartbeat() {
        if (!running.get() || nodeState != NodeState.LEADER) {
            return;
        }
        
        // Send AppendEntries RPCs to all followers
        // In a real implementation, this would send network RPCs
        logger.debug("Sending heartbeat: nodeId={}, term={}", nodeId, state.getCurrentTerm());
    }
    
    /**
     * Transitions to candidate state and starts election.
     */
    public void becomeCandidate() {
        if (nodeState == NodeState.LEADER) {
            return;
        }
        
        nodeState = NodeState.CANDIDATE;
        logger.info("Became candidate: nodeId={}, term={}", nodeId, state.getCurrentTerm());
    }
    
    /**
     * Transitions to follower state.
     */
    public void becomeFollower() {
        nodeState = NodeState.FOLLOWER;
        leaderElection.resetElectionTimeout();
        leaderElection.setElectionInProgress(false);
        logger.info("Became follower: nodeId={}, term={}", nodeId, state.getCurrentTerm());
    }
    
    /**
     * Transitions to leader state.
     */
    public void becomeLeader() {
        nodeState = NodeState.LEADER;
        leaderElection.setElectionInProgress(false);
        
        // Initialize replication state for all followers
        List<Integer> followers = clusterNodes.stream()
            .filter(id -> id != nodeId)
            .toList();
        logReplication.initializeReplicationState(followers);
        
        logger.info("Became leader: nodeId={}, term={}", nodeId, state.getCurrentTerm());
    }
    
    /**
     * Appends a command to the log (leader only).
     */
    public boolean appendCommand(byte[] command) {
        if (nodeState != NodeState.LEADER) {
            logger.warn("Cannot append command: not leader");
            return false;
        }
        
        long term = state.getCurrentTerm();
        RaftState.LogEntry entry = new RaftState.LogEntry(term, command);
        state.appendLogEntry(entry);
        
        logger.debug("Appended command to log: nodeId={}, term={}, logSize={}", 
            nodeId, term, state.getLogSize());
        
        return true;
    }
    
    public int getNodeId() {
        return nodeId;
    }
    
    public List<Integer> getClusterNodes() {
        return clusterNodes;
    }
    
    public RaftState getState() {
        return state;
    }
    
    public LeaderElection getLeaderElection() {
        return leaderElection;
    }
    
    public LogReplication getLogReplication() {
        return logReplication;
    }
    
    public NodeState getNodeState() {
        return nodeState;
    }
    
    public boolean isLeader() {
        return nodeState == NodeState.LEADER;
    }
}

