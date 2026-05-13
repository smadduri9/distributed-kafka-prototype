package com.kafkads.broker;

import com.kafkads.config.BrokerConfig;
import com.kafkads.config.ConfigLoader;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Properties;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Basic tests for the Broker.
 */
public class BrokerTest {
    private Broker broker;
    private BrokerConfig config;
    
    @BeforeEach
    void setUp() {
        // Create a test configuration
        Properties props = new Properties();
        props.setProperty("broker.id", "1");
        props.setProperty("broker.port", "9092");
        props.setProperty("broker.data.dir", "./test-data/broker");
        config = new BrokerConfig(props);
        broker = new Broker(config);
    }
    
    @AfterEach
    void tearDown() {
        if (broker != null && broker.isRunning()) {
            broker.shutdown();
        }
    }
    
    @Test
    void testBrokerCreation() {
        assertNotNull(broker);
        assertFalse(broker.isRunning());
    }
    
    @Test
    void testTopicCreation() {
        boolean result = broker.createTopic("test-topic", 3, 1);
        assertTrue(result);
        
        Topic topic = broker.getTopic("test-topic");
        assertNotNull(topic);
        assertEquals("test-topic", topic.getName());
        assertEquals(3, topic.getNumPartitions());
    }
    
    @Test
    void testDuplicateTopicCreation() {
        broker.createTopic("test-topic", 3, 1);
        boolean result = broker.createTopic("test-topic", 3, 1);
        assertFalse(result); // Should fail for duplicate
    }
    
    @Test
    void testProduceMessage() throws Exception {
        broker.createTopic("test-topic", 1, 1);
        
        byte[] message = "Hello, Kafka!".getBytes();
        long offset = broker.produce("test-topic", 0, message);
        
        assertTrue(offset >= 0);
    }
    
    @Test
    void testProduceToNonExistentTopic() {
        byte[] message = "Hello".getBytes();
        assertThrows(IllegalArgumentException.class, () -> {
            broker.produce("non-existent", 0, message);
        });
    }
    
    @Test
    void testFetchMessages() throws Exception {
        broker.createTopic("test-topic", 1, 1);
        
        // Produce a message
        byte[] message = "Test message".getBytes();
        long offset = broker.produce("test-topic", 0, message);
        
        // Fetch messages
        var messages = broker.fetch("test-topic", 0, offset, 10);
        assertNotNull(messages);
        assertFalse(messages.isEmpty());
    }
    
    @Test
    void testGetAllTopicNames() {
        broker.createTopic("topic1", 1, 1);
        broker.createTopic("topic2", 1, 1);
        
        var topicNames = broker.getAllTopicNames();
        assertEquals(2, topicNames.size());
        assertTrue(topicNames.contains("topic1"));
        assertTrue(topicNames.contains("topic2"));
    }
}

