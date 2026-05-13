package com.kafkads.broker;

import com.kafkads.config.BrokerConfig;
import com.kafkads.controller.Controller;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Manages broker heartbeat to the controller.
 */
public class BrokerHeartbeat {
    private static final Logger logger = LoggerFactory.getLogger(BrokerHeartbeat.class);
    
    private final Broker broker;
    private final BrokerConfig config;
    private final Controller controller;
    private final ScheduledExecutorService scheduler;
    private volatile boolean running = false;
    private static final long HEARTBEAT_INTERVAL_MS = 3000; // 3 seconds
    
    public BrokerHeartbeat(Broker broker, Controller controller) {
        this.broker = broker;
        this.config = broker.getConfig();
        this.controller = controller;
        this.scheduler = Executors.newScheduledThreadPool(1);
        MDC.put("brokerId", String.valueOf(config.getBrokerId()));
    }
    
    /**
     * Starts sending heartbeats to the controller.
     */
    public void start() {
        if (running) {
            logger.warn("Heartbeat already running");
            return;
        }
        
        running = true;
        scheduler.scheduleAtFixedRate(
            this::sendHeartbeat,
            HEARTBEAT_INTERVAL_MS,
            HEARTBEAT_INTERVAL_MS,
            TimeUnit.MILLISECONDS
        );
        
        logger.info("Broker heartbeat started: brokerId={}, interval={}ms", 
            config.getBrokerId(), HEARTBEAT_INTERVAL_MS);
    }
    
    /**
     * Stops sending heartbeats.
     */
    public void stop() {
        if (!running) {
            return;
        }
        
        running = false;
        scheduler.shutdown();
        try {
            if (!scheduler.awaitTermination(2, TimeUnit.SECONDS)) {
                scheduler.shutdownNow();
            }
        } catch (InterruptedException e) {
            scheduler.shutdownNow();
            Thread.currentThread().interrupt();
        }
        
        logger.info("Broker heartbeat stopped: brokerId={}", config.getBrokerId());
    }
    
    /**
     * Sends a heartbeat to the controller.
     */
    private void sendHeartbeat() {
        if (!running || !broker.isRunning()) {
            return;
        }
        
        try {
            if (controller != null) {
                controller.updateBrokerHeartbeat(config.getBrokerId());
                logger.debug("Heartbeat sent: brokerId={}", config.getBrokerId());
            }
        } catch (Exception e) {
            logger.error("Error sending heartbeat: brokerId={}", config.getBrokerId(), e);
        }
    }
    
    /**
     * Checks if the heartbeat is running.
     */
    public boolean isRunning() {
        return running;
    }
}

