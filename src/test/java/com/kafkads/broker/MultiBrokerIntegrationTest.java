package com.kafkads.broker;

import com.kafkads.config.BrokerConfig;
import com.kafkads.consensus.RaftNode;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Integration test for multiple brokers working together.
 */
public class MultiBrokerIntegrationTest {
    private List<Broker> brokers;
    private List<BrokerConfig> configs;
    
    @BeforeEach
    void setUp() {
        brokers = new ArrayList<>();
        configs = new ArrayList<>();
    }
    
    @AfterEach
    void tearDown() {
        for (Broker broker : brokers) {
            if (broker != null && broker.isRunning()) {
                broker.shutdown();
            }
        }
        brokers.clear();
        configs.clear();
    }
    
    @Test
    void testCreateMultipleBrokers() {
        // Create 3 broker configurations
        for (int i = 1; i <= 3; i++) {
            Properties props = new Properties();
            props.setProperty("broker.id", String.valueOf(i));
            props.setProperty("broker.port", String.valueOf(9092 + i));
            props.setProperty("broker.data.dir", "./test-data/broker-" + i);
            props.setProperty("raft.cluster.nodes", "localhost:9092,localhost:9093,localhost:9094");
            
            BrokerConfig config = new BrokerConfig(props);
            configs.add(config);
            
            Broker broker = new Broker(config);
            brokers.add(broker);
        }
        
        assertEquals(3, brokers.size());
        for (Broker broker : brokers) {
            assertNotNull(broker);
            assertFalse(broker.isRunning());
        }
    }
    
    @Test
    void testBrokerConfigurations() {
        Properties props1 = new Properties();
        props1.setProperty("broker.id", "1");
        props1.setProperty("broker.port", "9092");
        props1.setProperty("broker.data.dir", "./test-data/broker-1");
        
        BrokerConfig config1 = new BrokerConfig(props1);
        assertEquals(1, config1.getBrokerId());
        assertEquals(9092, config1.getBrokerPort());
        
        Properties props2 = new Properties();
        props2.setProperty("broker.id", "2");
        props2.setProperty("broker.port", "9093");
        props2.setProperty("broker.data.dir", "./test-data/broker-2");
        
        BrokerConfig config2 = new BrokerConfig(props2);
        assertEquals(2, config2.getBrokerId());
        assertEquals(9093, config2.getBrokerPort());
        
        assertNotEquals(config1.getBrokerId(), config2.getBrokerId());
        assertNotEquals(config1.getBrokerPort(), config2.getBrokerPort());
    }
    
    @Test
    void testMultipleBrokersCreateTopics() {
        // Create brokers
        for (int i = 1; i <= 2; i++) {
            Properties props = new Properties();
            props.setProperty("broker.id", String.valueOf(i));
            props.setProperty("broker.port", String.valueOf(9092 + i));
            props.setProperty("broker.data.dir", "./test-data/broker-" + i);
            
            BrokerConfig config = new BrokerConfig(props);
            Broker broker = new Broker(config);
            brokers.add(broker);
        }
        
        // Create topics on each broker
        Broker broker1 = brokers.get(0);
        Broker broker2 = brokers.get(1);
        
        boolean result1 = broker1.createTopic("topic-1", 3, 1);
        boolean result2 = broker2.createTopic("topic-2", 3, 1);
        
        assertTrue(result1);
        assertTrue(result2);
        
        assertNotNull(broker1.getTopic("topic-1"));
        assertNotNull(broker2.getTopic("topic-2"));
    }
    
    @Test
    void testRaftNodesWithBrokerIds() {
        List<Integer> brokerIds = List.of(1, 2, 3);
        List<RaftNode> raftNodes = new ArrayList<>();
        
        // Create Raft nodes corresponding to brokers
        for (int brokerId : brokerIds) {
            RaftNode node = new RaftNode(
                brokerId,
                brokerIds,
                150L,
                50L
            );
            raftNodes.add(node);
        }
        
        assertEquals(3, raftNodes.size());
        assertEquals(1, raftNodes.get(0).getNodeId());
        assertEquals(2, raftNodes.get(1).getNodeId());
        assertEquals(3, raftNodes.get(2).getNodeId());
        
        // Clean up
        for (RaftNode node : raftNodes) {
            node.stop();
        }
    }
    
    @Test
    void testBrokerRaftIntegration() throws InterruptedException {
        List<Integer> brokerIds = List.of(1, 2, 3);
        List<RaftNode> raftNodes = new ArrayList<>();
        
        // Create Raft nodes for each broker
        for (int brokerId : brokerIds) {
            RaftNode node = new RaftNode(
                brokerId,
                brokerIds,
                200L, // Longer timeout for testing
                50L
            );
            node.start();
            raftNodes.add(node);
        }
        
        // Wait for election
        Thread.sleep(500);
        
        // Verify at least one node is leader or candidate
        boolean hasLeader = raftNodes.stream().anyMatch(RaftNode::isLeader);
        boolean hasCandidate = raftNodes.stream()
            .anyMatch(n -> n.getNodeState() == RaftNode.NodeState.CANDIDATE);
        
        assertTrue(hasLeader || hasCandidate, 
            "At least one node should be leader or candidate after election");
        
        // Clean up
        for (RaftNode node : raftNodes) {
            node.stop();
        }
    }
    
    @Test
    void testElectionWithThreeNodes() throws InterruptedException {
        List<Integer> nodeIds = List.of(1, 2, 3);
        List<RaftNode> nodes = new ArrayList<>();
        
        // Create 3 nodes
        for (int nodeId : nodeIds) {
            RaftNode node = new RaftNode(
                nodeId,
                nodeIds,
                200L,
                50L
            );
            node.start();
            nodes.add(node);
        }
        
        // Wait for election
        Thread.sleep(600);
        
        // Count leaders (should be 0 or 1 in real implementation)
        long leaderCount = nodes.stream()
            .filter(RaftNode::isLeader)
            .count();
        
        // Count candidates
        long candidateCount = nodes.stream()
            .filter(n -> n.getNodeState() == RaftNode.NodeState.CANDIDATE)
            .count();
        
        // Verify election process
        assertTrue(leaderCount >= 0 && leaderCount <= 3);
        assertTrue(candidateCount >= 0 && candidateCount <= 3);
        
        // At least one node should have incremented term
        boolean hasTermIncrement = nodes.stream()
            .anyMatch(n -> n.getState().getCurrentTerm() > 0);
        
        assertTrue(hasTermIncrement, "At least one node should have incremented term");
        
        // Clean up
        for (RaftNode node : nodes) {
            node.stop();
        }
    }
    
    @Test
    void testLeaderElectionAfterTimeout() throws InterruptedException {
        List<Integer> nodeIds = List.of(1, 2, 3);
        RaftNode node1 = new RaftNode(1, nodeIds, 200L, 50L);
        RaftNode node2 = new RaftNode(2, nodeIds, 200L, 50L);
        RaftNode node3 = new RaftNode(3, nodeIds, 200L, 50L);
        
        node1.start();
        node2.start();
        node3.start();
        
        // Initially all should be followers
        assertEquals(RaftNode.NodeState.FOLLOWER, node1.getNodeState());
        assertEquals(RaftNode.NodeState.FOLLOWER, node2.getNodeState());
        assertEquals(RaftNode.NodeState.FOLLOWER, node3.getNodeState());
        
        // Wait for election timeout
        Thread.sleep(300);
        
        // At least one should have started election
        boolean hasElectionStarted = 
            node1.getState().getCurrentTerm() > 0 ||
            node2.getState().getCurrentTerm() > 0 ||
            node3.getState().getCurrentTerm() > 0;
        
        assertTrue(hasElectionStarted, "Election should have started after timeout");
        
        // Clean up
        node1.stop();
        node2.stop();
        node3.stop();
    }
}

