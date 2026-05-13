package com.kafkads.consumer;

import java.util.Properties;

/**
 * Configuration for a consumer client.
 */
public class ConsumerConfig {
    private final String brokerHost;
    private final int brokerPort;
    private final String groupId;
    private final String autoOffsetReset;
    private final boolean enableAutoCommit;
    private final long autoCommitIntervalMs;
    private final int fetchMinBytes;
    private final long fetchMaxWaitMs;
    
    public ConsumerConfig(Properties properties) {
        String servers = properties.getProperty("bootstrap.servers", "localhost:9092");
        this.brokerHost = servers.split(":")[0];
        String portStr = servers.split(":")[1];
        this.brokerPort = Integer.parseInt(portStr);
        this.groupId = properties.getProperty("group.id", "default-group");
        this.autoOffsetReset = properties.getProperty("auto.offset.reset", "earliest");
        this.enableAutoCommit = Boolean.parseBoolean(properties.getProperty("enable.auto.commit", "true"));
        this.autoCommitIntervalMs = Long.parseLong(properties.getProperty("auto.commit.interval.ms", "5000"));
        this.fetchMinBytes = Integer.parseInt(properties.getProperty("fetch.min.bytes", "1"));
        this.fetchMaxWaitMs = Long.parseLong(properties.getProperty("fetch.max.wait.ms", "500"));
    }
    
    public String getBrokerHost() { return brokerHost; }
    public int getBrokerPort() { return brokerPort; }
    public String getGroupId() { return groupId; }
    public String getAutoOffsetReset() { return autoOffsetReset; }
    public boolean isEnableAutoCommit() { return enableAutoCommit; }
    public long getAutoCommitIntervalMs() { return autoCommitIntervalMs; }
    public int getFetchMinBytes() { return fetchMinBytes; }
    public long getFetchMaxWaitMs() { return fetchMaxWaitMs; }
}

