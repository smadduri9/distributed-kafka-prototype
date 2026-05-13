package com.kafkads.config;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.InputStream;
import java.util.Properties;

/**
 * Utility class to load configuration from properties file.
 */
public class ConfigLoader {
    private static final Logger logger = LoggerFactory.getLogger(ConfigLoader.class);
    private static final String CONFIG_FILE = "/application.properties";
    
    /**
     * Loads configuration from application.properties file.
     * Environment variables override file properties.
     * 
     * @return BrokerConfig instance
     */
    public static BrokerConfig loadConfig() {
        Properties properties = new Properties();
        
        try (InputStream inputStream = ConfigLoader.class.getResourceAsStream(CONFIG_FILE)) {
            if (inputStream != null) {
                properties.load(inputStream);
                logger.info("Configuration file loaded from {}", CONFIG_FILE);
            } else {
                logger.warn("Configuration file {} not found, using defaults", CONFIG_FILE);
            }
        } catch (Exception e) {
            logger.error("Error loading configuration file: {}", e.getMessage(), e);
        }
        
        return new BrokerConfig(properties);
    }
}

