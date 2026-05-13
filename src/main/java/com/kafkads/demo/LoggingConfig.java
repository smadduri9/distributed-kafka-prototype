package com.kafkads.demo;

import ch.qos.logback.classic.LoggerContext;
import ch.qos.logback.classic.joran.JoranConfigurator;
import ch.qos.logback.core.joran.spi.JoranException;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.InputStream;

/**
 * Configures logging for demo with separate log files per node.
 */
public class LoggingConfig {
    private static boolean initialized = false;
    
    /**
     * Initializes logging configuration for demo.
     */
    public static void initialize() {
        if (initialized) {
            return;
        }
        ensureLogsDirectory();
        loadDemoLogbackConfig();
        initialized = true;
    }
    
    /**
     * Configures logging for a specific node.
     */
    public static void configureNodeLogging(int nodeId) {
        System.setProperty("nodeId", String.valueOf(nodeId));
        reloadLogbackConfig();
    }
    
    /**
     * Configures logging for a specific broker.
     */
    public static void configureBrokerLogging(int brokerId) {
        System.setProperty("brokerId", String.valueOf(brokerId));
        reloadLogbackConfig();
    }
    
    /**
     * Configures logging for controller.
     */
    public static void configureControllerLogging() {
        System.setProperty("brokerId", "controller");
        reloadLogbackConfig();
    }
    
    /**
     * Loads the demo logback configuration.
     */
    private static void loadDemoLogbackConfig() {
        try {
            LoggerContext context = (LoggerContext) LoggerFactory.getILoggerFactory();
            context.reset();
            
            JoranConfigurator configurator = new JoranConfigurator();
            configurator.setContext(context);
            
            InputStream configStream = LoggingConfig.class.getResourceAsStream("/logback-demo.xml");
            if (configStream != null) {
                configurator.doConfigure(configStream);
            } else {
                // Fallback to default
                configStream = LoggingConfig.class.getResourceAsStream("/logback.xml");
                if (configStream != null) {
                    configurator.doConfigure(configStream);
                }
            }
        } catch (JoranException e) {
            System.err.println("Error loading demo logback config: " + e.getMessage());
        }
    }
    
    /**
     * Reloads logback configuration.
     */
    private static void reloadLogbackConfig() {
        try {
            LoggerContext context = (LoggerContext) LoggerFactory.getILoggerFactory();
            context.reset();
            
            JoranConfigurator configurator = new JoranConfigurator();
            configurator.setContext(context);
            
            InputStream configStream = LoggingConfig.class.getResourceAsStream("/logback-demo.xml");
            if (configStream != null) {
                configurator.doConfigure(configStream);
            }
        } catch (JoranException e) {
            System.err.println("Error reloading logback config: " + e.getMessage());
        }
    }
    
    /**
     * Ensures logs directory exists.
     */
    public static void ensureLogsDirectory() {
        File logsDir = new File("logs");
        if (!logsDir.exists()) {
            logsDir.mkdirs();
        }
    }
}

