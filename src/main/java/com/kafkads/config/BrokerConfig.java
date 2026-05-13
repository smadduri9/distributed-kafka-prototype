package com.kafkads.config;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

/**
 * Configuration class for broker settings.
 * Loads configuration from properties file and environment variables.
 */
public class BrokerConfig {
    private static final Logger logger = LoggerFactory.getLogger(BrokerConfig.class);
    
    private final Properties properties;
    
    // Broker settings
    private final int brokerId;
    private final String brokerHost;
    private final int brokerPort;
    private final String dataDir;
    
    // Controller settings
    private final String controllerHost;
    private final int controllerPort;
    
    // Log segment settings
    private final long logSegmentSizeBytes;
    private final long logSegmentMaxAgeHours;
    private final long logRetentionHours;
    private final long logRetentionBytes;
    
    // Replication settings
    private final int replicationFactor;
    private final int minInsyncReplicas;
    
    // Producer settings
    private final int producerAcks;
    private final int producerRetries;
    private final int producerBatchSize;
    private final long producerLingerMs;
    private final String producerCompressionType;
    
    // Consumer settings
    private final String consumerGroupId;
    private final String consumerAutoOffsetReset;
    private final boolean consumerEnableAutoCommit;
    private final long consumerAutoCommitIntervalMs;
    private final int consumerFetchMinBytes;
    private final long consumerFetchMaxWaitMs;
    
    // Raft settings
    private final long raftElectionTimeoutMs;
    private final long raftHeartbeatIntervalMs;
    private final String raftClusterNodes;
    
    // Network settings
    private final int networkThreads;
    private final int ioThreads;
    
    // SSL settings
    private final boolean sslEnabled;
    private final String sslKeystoreLocation;
    private final String sslKeystorePassword;
    private final String sslTruststoreLocation;
    private final String sslTruststorePassword;
    
    // ACL settings
    private final boolean aclEnabled;
    
    public BrokerConfig(Properties properties) {
        this.properties = properties;
        
        // Broker settings
        this.brokerId = getIntProperty("broker.id", 1);
        this.brokerHost = getStringProperty("broker.host", "localhost");
        this.brokerPort = getIntProperty("broker.port", 9092);
        this.dataDir = getStringProperty("broker.data.dir", "./data/broker");
        
        // Controller settings
        this.controllerHost = getStringProperty("controller.host", "localhost");
        this.controllerPort = getIntProperty("controller.port", 9093);
        
        // Log segment settings
        this.logSegmentSizeBytes = getLongProperty("log.segment.size.bytes", 1073741824L);
        this.logSegmentMaxAgeHours = getLongProperty("log.segment.max.age.hours", 168L);
        this.logRetentionHours = getLongProperty("log.retention.hours", 168L);
        this.logRetentionBytes = getLongProperty("log.retention.bytes", -1L);
        
        // Replication settings
        this.replicationFactor = getIntProperty("replication.factor", 1);
        this.minInsyncReplicas = getIntProperty("min.insync.replicas", 1);
        
        // Producer settings
        this.producerAcks = getIntProperty("producer.acks", 1);
        this.producerRetries = getIntProperty("producer.retries", 3);
        this.producerBatchSize = getIntProperty("producer.batch.size", 16384);
        this.producerLingerMs = getLongProperty("producer.linger.ms", 0L);
        this.producerCompressionType = getStringProperty("producer.compression.type", "none");
        
        // Consumer settings
        this.consumerGroupId = getStringProperty("consumer.group.id", "default-group");
        this.consumerAutoOffsetReset = getStringProperty("consumer.auto.offset.reset", "earliest");
        this.consumerEnableAutoCommit = getBooleanProperty("consumer.enable.auto.commit", true);
        this.consumerAutoCommitIntervalMs = getLongProperty("consumer.auto.commit.interval.ms", 5000L);
        this.consumerFetchMinBytes = getIntProperty("consumer.fetch.min.bytes", 1);
        this.consumerFetchMaxWaitMs = getLongProperty("consumer.fetch.max.wait.ms", 500L);
        
        // Raft settings
        this.raftElectionTimeoutMs = getLongProperty("raft.election.timeout.ms", 150L);
        this.raftHeartbeatIntervalMs = getLongProperty("raft.heartbeat.interval.ms", 50L);
        this.raftClusterNodes = getStringProperty("raft.cluster.nodes", "localhost:9092");
        
        // Network settings
        this.networkThreads = getIntProperty("network.threads", 3);
        this.ioThreads = getIntProperty("io.threads", 8);
        
        // SSL settings
        this.sslEnabled = getBooleanProperty("ssl.enabled", false);
        this.sslKeystoreLocation = getStringProperty("ssl.keystore.location", "");
        this.sslKeystorePassword = getStringProperty("ssl.keystore.password", "");
        this.sslTruststoreLocation = getStringProperty("ssl.truststore.location", "");
        this.sslTruststorePassword = getStringProperty("ssl.truststore.password", "");
        
        // ACL settings
        this.aclEnabled = getBooleanProperty("acl.enabled", false);
        
        logger.info("Broker configuration loaded: brokerId={}, port={}, dataDir={}", brokerId, brokerPort, dataDir);
    }
    
    private String getStringProperty(String key, String defaultValue) {
        String envKey = key.toUpperCase().replace('.', '_');
        String envValue = System.getenv(envKey);
        if (envValue != null) {
            return envValue;
        }
        return properties.getProperty(key, defaultValue);
    }
    
    private int getIntProperty(String key, int defaultValue) {
        String value = getStringProperty(key, String.valueOf(defaultValue));
        try {
            return Integer.parseInt(value);
        } catch (NumberFormatException e) {
            logger.warn("Invalid integer value for property {}: {}, using default: {}", key, value, defaultValue);
            return defaultValue;
        }
    }
    
    private long getLongProperty(String key, long defaultValue) {
        String value = getStringProperty(key, String.valueOf(defaultValue));
        try {
            return Long.parseLong(value);
        } catch (NumberFormatException e) {
            logger.warn("Invalid long value for property {}: {}, using default: {}", key, value, defaultValue);
            return defaultValue;
        }
    }
    
    private boolean getBooleanProperty(String key, boolean defaultValue) {
        String value = getStringProperty(key, String.valueOf(defaultValue));
        return Boolean.parseBoolean(value);
    }
    
    // Getters
    public int getBrokerId() { return brokerId; }
    public String getBrokerHost() { return brokerHost; }
    public int getBrokerPort() { return brokerPort; }
    public String getDataDir() { return dataDir; }
    public String getControllerHost() { return controllerHost; }
    public int getControllerPort() { return controllerPort; }
    public long getLogSegmentSizeBytes() { return logSegmentSizeBytes; }
    public long getLogSegmentMaxAgeHours() { return logSegmentMaxAgeHours; }
    public long getLogRetentionHours() { return logRetentionHours; }
    public long getLogRetentionBytes() { return logRetentionBytes; }
    public int getReplicationFactor() { return replicationFactor; }
    public int getMinInsyncReplicas() { return minInsyncReplicas; }
    public int getProducerAcks() { return producerAcks; }
    public int getProducerRetries() { return producerRetries; }
    public int getProducerBatchSize() { return producerBatchSize; }
    public long getProducerLingerMs() { return producerLingerMs; }
    public String getProducerCompressionType() { return producerCompressionType; }
    public String getConsumerGroupId() { return consumerGroupId; }
    public String getConsumerAutoOffsetReset() { return consumerAutoOffsetReset; }
    public boolean isConsumerEnableAutoCommit() { return consumerEnableAutoCommit; }
    public long getConsumerAutoCommitIntervalMs() { return consumerAutoCommitIntervalMs; }
    public int getConsumerFetchMinBytes() { return consumerFetchMinBytes; }
    public long getConsumerFetchMaxWaitMs() { return consumerFetchMaxWaitMs; }
    public long getRaftElectionTimeoutMs() { return raftElectionTimeoutMs; }
    public long getRaftHeartbeatIntervalMs() { return raftHeartbeatIntervalMs; }
    public String getRaftClusterNodes() { return raftClusterNodes; }
    public int getNetworkThreads() { return networkThreads; }
    public int getIoThreads() { return ioThreads; }
    public boolean isSslEnabled() { return sslEnabled; }
    public String getSslKeystoreLocation() { return sslKeystoreLocation; }
    public String getSslKeystorePassword() { return sslKeystorePassword; }
    public String getSslTruststoreLocation() { return sslTruststoreLocation; }
    public String getSslTruststorePassword() { return sslTruststorePassword; }
    public boolean isAclEnabled() { return aclEnabled; }
}

