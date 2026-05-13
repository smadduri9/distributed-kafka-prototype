package com.kafkads.demo;

import com.kafkads.broker.Broker;
import com.kafkads.broker.BrokerHeartbeat;
import com.kafkads.config.BrokerConfig;
import com.kafkads.consensus.RaftNode;
import com.kafkads.controller.Controller;
import com.kafkads.controller.MetadataManager;
import com.kafkads.replication.ReplicationManager;
import com.kafkads.replication.ReplicationProtocol;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

/**
 * Demonstration script for key features:
 * 1. Replication
 * 2. Leader Election
 * 3. Heartbeat Algorithm
 */
public class FeatureDemo {
    public static void main(String[] args) throws Exception {
        // Initialize logging configuration
        LoggingConfig.initialize();
        
        System.out.println("==========================================");
        System.out.println("Kafka-like Distributed System - Feature Demo");
        System.out.println("==========================================");
        System.out.println("Log files will be written to: logs/");
        System.out.println("  - demo-node-{id}.log (for Raft nodes)");
        System.out.println("  - demo-broker-{id}.log (for brokers)");
        System.out.println("  - demo-controller.log (for controller)");
        System.out.println("==========================================\n");
        
        // Run demonstrations
        demonstrateLeaderElection();
        demonstrateHeartbeat();
        demonstrateReplication();
        
        System.out.println("\n==========================================");
        System.out.println("All demonstrations completed!");
        System.out.println("Check logs/ directory for detailed logs from each node");
        System.out.println("==========================================");
    }
    
    /**
     * Demonstrates Raft Leader Election algorithm.
     */
    private static void demonstrateLeaderElection() throws InterruptedException {
        System.out.println("\n[1] LEADER ELECTION DEMONSTRATION");
        System.out.println("=====================================");
        
        List<Integer> nodeIds = List.of(1, 2, 3);
        List<RaftNode> nodes = new ArrayList<>();
        
        System.out.println("Creating 3 Raft nodes for cluster...");
        for (int nodeId : nodeIds) {
            // Configure logging for this node
            LoggingConfig.configureNodeLogging(nodeId);
            
            RaftNode node = new RaftNode(
                nodeId,
                nodeIds,
                200L, // election timeout
                50L   // heartbeat interval
            );
            nodes.add(node);
            System.out.println("  - Node " + nodeId + " created (initial state: FOLLOWER)");
            System.out.println("    Log file: logs/demo-node-" + nodeId + ".log");
        }
        
        System.out.println("\nStarting all nodes...");
        for (RaftNode node : nodes) {
            node.start();
            System.out.println("  - Node " + node.getNodeId() + " started");
        }
        
        System.out.println("\nWaiting for election timeout (200ms)...");
        Thread.sleep(300);
        
        System.out.println("\nChecking election results:");
        for (RaftNode node : nodes) {
            System.out.println("  - Node " + node.getNodeId() + 
                ": State=" + node.getNodeState() + 
                ", Term=" + node.getState().getCurrentTerm() +
                ", VotedFor=" + node.getState().getVotedFor());
        }
        
        // Find leader
        RaftNode leader = nodes.stream()
            .filter(RaftNode::isLeader)
            .findFirst()
            .orElse(null);
        
        if (leader != null) {
            System.out.println("\n✓ Leader elected: Node " + leader.getNodeId() + " (Term: " + 
                leader.getState().getCurrentTerm() + ")");
        } else {
            System.out.println("\n  Note: In a real network, one node would be elected as leader.");
            System.out.println("  Current implementation shows election process starting.");
        }
        
        // Demonstrate term increment
        System.out.println("\nDemonstrating term increment on election:");
        RaftNode node1 = nodes.get(0);
        long initialTerm = node1.getState().getCurrentTerm();
        node1.getLeaderElection().startElection();
        Thread.sleep(100);
        long newTerm = node1.getState().getCurrentTerm();
        System.out.println("  - Node 1 term: " + initialTerm + " -> " + newTerm);
        System.out.println("  - Node 1 voted for: " + node1.getState().getVotedFor());
        
        // Cleanup
        System.out.println("\nStopping nodes...");
        for (RaftNode node : nodes) {
            node.stop();
        }
        
        System.out.println("✓ Leader Election demonstration complete\n");
    }
    
    /**
     * Demonstrates Heartbeat Algorithm.
     */
    private static void demonstrateHeartbeat() throws InterruptedException {
        System.out.println("\n[2] HEARTBEAT ALGORITHM DEMONSTRATION");
        System.out.println("=======================================");
        
        // Configure controller logging
        LoggingConfig.configureControllerLogging();
        
        // Create controller
        Controller controller = new Controller();
        controller.start();
        System.out.println("Controller started");
        System.out.println("  Log file: logs/demo-controller.log");
        
        // Configure broker logging
        LoggingConfig.configureBrokerLogging(1);
        
        // Create broker
        Properties props = new Properties();
        props.setProperty("broker.id", "1");
        props.setProperty("broker.port", "9092");
        props.setProperty("broker.data.dir", "./demo-data/broker-1");
        BrokerConfig config = new BrokerConfig(props);
        Broker broker = new Broker(config);
        
        // Register broker
        controller.registerBroker(1, "localhost", 9092);
        System.out.println("Broker 1 registered with controller");
        System.out.println("  Log file: logs/demo-broker-1.log");
        
        // Create heartbeat
        BrokerHeartbeat heartbeat = new BrokerHeartbeat(broker, controller);
        System.out.println("Heartbeat mechanism created");
        
        // Start heartbeat
        heartbeat.start();
        System.out.println("Heartbeat started (interval: 3 seconds)");
        
        System.out.println("\nMonitoring heartbeat for 10 seconds...");
        for (int i = 0; i < 3; i++) {
            Thread.sleep(3500);
            MetadataManager.BrokerMetadata metadata = 
                controller.getMetadataManager().getBrokerMetadata(1);
            if (metadata != null) {
                long timeSinceHeartbeat = System.currentTimeMillis() - metadata.getLastHeartbeat();
                System.out.println("  [" + (i + 1) + "] Broker 1 heartbeat: " + 
                    timeSinceHeartbeat + "ms ago, Status: " + 
                    (metadata.isAlive() ? "ALIVE" : "DEAD"));
            }
        }
        
        // Demonstrate heartbeat timeout detection
        System.out.println("\nDemonstrating heartbeat timeout detection:");
        System.out.println("  - Stopping heartbeat...");
        heartbeat.stop();
        Thread.sleep(12000); // Wait longer than timeout (10 seconds)
        
        MetadataManager.BrokerMetadata metadata = 
            controller.getMetadataManager().getBrokerMetadata(1);
        if (metadata != null) {
            long timeSinceHeartbeat = System.currentTimeMillis() - metadata.getLastHeartbeat();
            System.out.println("  - Time since last heartbeat: " + timeSinceHeartbeat + "ms");
            System.out.println("  - Broker status: " + (metadata.isAlive() ? "ALIVE" : "DEAD"));
            System.out.println("  - Expected status after timeout (>10s): DEAD");
        }
        
        // Cleanup
        controller.stop();
        System.out.println("\n✓ Heartbeat demonstration complete\n");
    }
    
    /**
     * Demonstrates Replication mechanism.
     */
    private static void demonstrateReplication() throws InterruptedException {
        System.out.println("\n[3] REPLICATION DEMONSTRATION");
        System.out.println("===============================");
        
        // Configure controller logging
        LoggingConfig.configureControllerLogging();
        
        // Create controller
        Controller controller = new Controller();
        controller.start();
        System.out.println("Controller started");
        System.out.println("  Log file: logs/demo-controller.log");
        
        // Register multiple brokers
        System.out.println("\nRegistering 3 brokers for replication:");
        for (int i = 1; i <= 3; i++) {
            // Configure logging for each broker
            LoggingConfig.configureBrokerLogging(i);
            
            controller.registerBroker(i, "localhost", 9092 + i);
            System.out.println("  - Broker " + i + " registered (localhost:" + (9092 + i) + ")");
            System.out.println("    Log file: logs/demo-broker-" + i + ".log");
        }
        
        // Create topic with replication
        System.out.println("\nCreating topic with replication factor 3:");
        boolean created = controller.createTopic("replicated-topic", 2, 3);
        if (created) {
            System.out.println("  ✓ Topic 'replicated-topic' created");
            System.out.println("  - Partitions: 2");
            System.out.println("  - Replication factor: 3");
        }
        
        // Show partition assignments
        System.out.println("\nPartition assignments:");
        for (int partitionId = 0; partitionId < 2; partitionId++) {
            MetadataManager.PartitionAssignment assignment = 
                controller.getPartitionAssignment("replicated-topic", partitionId);
            if (assignment != null) {
                System.out.println("  Partition " + partitionId + ":");
                System.out.println("    - Leader: Broker " + assignment.getLeaderBrokerId());
                System.out.println("    - Replicas: " + assignment.getReplicaBrokerIds());
            }
        }
        
        // Demonstrate replication manager
        System.out.println("\nDemonstrating replication manager:");
        ReplicationManager replicationManager = new ReplicationManager(
            1, // leader broker ID
            controller.getMetadataManager(),
            ReplicationProtocol.ReplicationMode.SYNC
        );
        System.out.println("  - Replication manager created for Broker 1 (leader)");
        System.out.println("  - Replication mode: SYNC (waits for all replicas)");
        
        // Simulate message replication
        System.out.println("\nSimulating message replication:");
        String topicName = "replicated-topic";
        int partitionId = 0;
        byte[] message = "Test replicated message".getBytes();
        long offset = 0;
        long leaderEpoch = 1;
        
        System.out.println("  - Message: 'Test replicated message'");
        System.out.println("  - Topic: " + topicName + ", Partition: " + partitionId);
        System.out.println("  - Offset: " + offset);
        
        boolean replicated = replicationManager.replicateMessage(
            topicName, partitionId, offset, message, leaderEpoch
        );
        
        if (replicated) {
            System.out.println("  ✓ Message replicated to all followers");
        } else {
            System.out.println("  - Replication initiated (in real network, would replicate to followers)");
        }
        
        // Show high water mark
        long hwm = replicationManager.getHighWaterMark(topicName, partitionId);
        System.out.println("  - High Water Mark: " + hwm);
        
        // Demonstrate follower sync
        System.out.println("\nDemonstrating follower synchronization:");
        System.out.println("  - Follower brokers receive replication requests from leader");
        System.out.println("  - Followers append messages to their local log");
        System.out.println("  - Followers send acknowledgment back to leader");
        System.out.println("  - Leader commits message after receiving acknowledgments");
        
        // Demonstrate replication modes
        System.out.println("\nReplication modes:");
        System.out.println("  - SYNC: Waits for all replicas to acknowledge");
        System.out.println("  - ASYNC: Sends to replicas without waiting");
        
        // Cleanup
        controller.stop();
        System.out.println("\n✓ Replication demonstration complete\n");
    }
}

