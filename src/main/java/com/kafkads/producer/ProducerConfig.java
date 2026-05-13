package com.kafkads.producer;

import java.util.Properties;

/**
 * Configuration for a producer client.
 */
public class ProducerConfig {
    private final String brokerHost;
    private final int brokerPort;
    private final int acks;
    private final int retries;
    private final int batchSize;
    private final long lingerMs;
    private final String compressionType;
    private final int requestTimeoutMs;
    
    public ProducerConfig(Properties properties) {
        this.brokerHost = properties.getProperty("bootstrap.servers", "localhost:9092").split(":")[0];
        String portStr = properties.getProperty("bootstrap.servers", "localhost:9092").split(":")[1];
        this.brokerPort = Integer.parseInt(portStr);
        this.acks = Integer.parseInt(properties.getProperty("acks", "1"));
        this.retries = Integer.parseInt(properties.getProperty("retries", "3"));
        this.batchSize = Integer.parseInt(properties.getProperty("batch.size", "16384"));
        this.lingerMs = Long.parseLong(properties.getProperty("linger.ms", "0"));
        this.compressionType = properties.getProperty("compression.type", "none");
        this.requestTimeoutMs = Integer.parseInt(properties.getProperty("request.timeout.ms", "30000"));
    }
    
    public String getBrokerHost() { return brokerHost; }
    public int getBrokerPort() { return brokerPort; }
    public int getAcks() { return acks; }
    public int getRetries() { return retries; }
    public int getBatchSize() { return batchSize; }
    public long getLingerMs() { return lingerMs; }
    public String getCompressionType() { return compressionType; }
    public int getRequestTimeoutMs() { return requestTimeoutMs; }
}

