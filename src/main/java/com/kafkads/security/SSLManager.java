package com.kafkads.security;

import com.kafkads.config.BrokerConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.net.ssl.*;
import java.io.FileInputStream;
import java.io.IOException;
import java.security.KeyStore;

/**
 * Manages SSL/TLS configuration for secure connections.
 */
public class SSLManager {
    private static final Logger logger = LoggerFactory.getLogger(SSLManager.class);
    
    private final BrokerConfig config;
    private SSLContext sslContext;
    
    public SSLManager(BrokerConfig config) {
        this.config = config;
        if (config.isSslEnabled()) {
            initializeSSL();
        }
    }
    
    /**
     * Initializes SSL context from keystore and truststore.
     */
    private void initializeSSL() {
        try {
            KeyStore keyStore = KeyStore.getInstance("JKS");
            keyStore.load(
                new FileInputStream(config.getSslKeystoreLocation()),
                config.getSslKeystorePassword().toCharArray()
            );
            
            KeyStore trustStore = KeyStore.getInstance("JKS");
            trustStore.load(
                new FileInputStream(config.getSslTruststoreLocation()),
                config.getSslTruststorePassword().toCharArray()
            );
            
            KeyManagerFactory kmf = KeyManagerFactory.getInstance("SunX509");
            kmf.init(keyStore, config.getSslKeystorePassword().toCharArray());
            
            TrustManagerFactory tmf = TrustManagerFactory.getInstance("SunX509");
            tmf.init(trustStore);
            
            sslContext = SSLContext.getInstance("TLS");
            sslContext.init(kmf.getKeyManagers(), tmf.getTrustManagers(), null);
            
            logger.info("SSL context initialized");
        } catch (Exception e) {
            logger.error("Failed to initialize SSL", e);
            throw new RuntimeException("SSL initialization failed", e);
        }
    }
    
    /**
     * Gets the SSL context.
     */
    public SSLContext getSSLContext() {
        return sslContext;
    }
    
    /**
     * Checks if SSL is enabled.
     */
    public boolean isEnabled() {
        return config.isSslEnabled() && sslContext != null;
    }
}

