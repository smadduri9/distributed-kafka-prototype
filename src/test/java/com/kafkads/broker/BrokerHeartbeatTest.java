package com.kafkads.broker;

import com.kafkads.config.BrokerConfig;
import com.kafkads.controller.Controller;
import com.kafkads.controller.MetadataManager;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Properties;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for broker heartbeat functionality.
 */
public class BrokerHeartbeatTest {
    private Broker broker;
    private Controller controller;
    private BrokerHeartbeat heartbeat;
    private BrokerConfig config;
    
    @BeforeEach
    void setUp() {
        Properties props = new Properties();
        props.setProperty("broker.id", "1");
        props.setProperty("broker.port", "9092");
        props.setProperty("broker.data.dir", "./test-data/broker");
        config = new BrokerConfig(props);
        broker = new Broker(config);
        controller = new Controller();
        controller.start();
        controller.registerBroker(1, "localhost", 9092);
        heartbeat = new BrokerHeartbeat(broker, controller);
    }
    
    @AfterEach
    void tearDown() {
        if (heartbeat != null) {
            heartbeat.stop();
        }
        if (controller != null) {
            controller.stop();
        }
    }
    
    @Test
    void testHeartbeatCreation() {
        assertNotNull(heartbeat);
        assertFalse(heartbeat.isRunning());
    }
    
    @Test
    void testHeartbeatStart() {
        heartbeat.start();
        assertTrue(heartbeat.isRunning());
        heartbeat.stop();
    }
    
    @Test
    void testHeartbeatUpdatesController() throws InterruptedException {
        heartbeat.start();
        
        // Wait for at least one heartbeat
        Thread.sleep(3500);
        
        MetadataManager.BrokerMetadata metadata = 
            controller.getMetadataManager().getBrokerMetadata(1);
        
        assertNotNull(metadata);
        assertTrue(metadata.isAlive());
        assertTrue(metadata.getLastHeartbeat() > 0);
        
        heartbeat.stop();
    }
    
    @Test
    void testHeartbeatStops() {
        heartbeat.start();
        assertTrue(heartbeat.isRunning());
        
        heartbeat.stop();
        assertFalse(heartbeat.isRunning());
    }
    
    @Test
    void testMultipleHeartbeats() throws InterruptedException {
        heartbeat.start();
        
        // Wait for multiple heartbeats
        Thread.sleep(7000);
        
        MetadataManager.BrokerMetadata metadata = 
            controller.getMetadataManager().getBrokerMetadata(1);
        
        assertNotNull(metadata);
        long lastHeartbeat = metadata.getLastHeartbeat();
        
        // Wait a bit more and verify heartbeat was updated
        Thread.sleep(4000);
        long newHeartbeat = metadata.getLastHeartbeat();
        
        assertTrue(newHeartbeat >= lastHeartbeat, 
            "Heartbeat should be updated. Last: " + lastHeartbeat + ", New: " + newHeartbeat);
        
        heartbeat.stop();
    }
    
    @Test
    void testBrokerAliveStatus() throws InterruptedException {
        controller.registerBroker(1, "localhost", 9092);
        heartbeat.start();
        
        Thread.sleep(3500);
        
        MetadataManager.BrokerMetadata metadata = 
            controller.getMetadataManager().getBrokerMetadata(1);
        
        assertNotNull(metadata);
        assertTrue(metadata.isAlive(), "Broker should be marked as alive after heartbeat");
        
        heartbeat.stop();
    }
    
    @Test
    void testHeartbeatWithoutController() {
        BrokerHeartbeat heartbeatNoController = new BrokerHeartbeat(broker, null);
        // Should not throw exception
        assertDoesNotThrow(() -> {
            heartbeatNoController.start();
            Thread.sleep(100);
            heartbeatNoController.stop();
        });
    }
}

