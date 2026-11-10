package com.kafkads.config;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.Set;

/** Immutable broker configuration loaded from a properties file. */
public final class BrokerConfig {

    private final int nodeId;
    private final String host;
    private final int port;
    private final List<PeerConfig> peers;
    private final String logDir;
    private final int partitionCount;
    private final int segmentBytes;
    private final int indexIntervalBytes;
    private final int electionTimeoutMinMs;
    private final int electionTimeoutMaxMs;
    private final int heartbeatIntervalMs;
    private final int replicationFactor;
    private final String partitionAssignment;
    private final int produceTimeoutMs;
    private final String discoveryMode;
    private final String discoverySrvName;
    private final String discoveryHostnamePrefix;
    private final int discoveryTtlMinMs;
    private final int discoveryTtlMaxMs;
    private final String discoveryIpPreference;

    private BrokerConfig(Properties p) {
        this.nodeId = requireInt(p, "node.id");
        this.host = p.getProperty("host", "127.0.0.1").trim();
        this.port = requireInt(p, "port");
        this.peers = parsePeers(p.getProperty("peers", ""));
        this.logDir = p.getProperty("log.dir", "data/broker-" + nodeId).trim();
        this.partitionCount = intOrDefault(p, "partition.count", 1);
        this.segmentBytes = intOrDefault(p, "segment.bytes", 16 * 1024 * 1024);
        this.indexIntervalBytes = intOrDefault(p, "index.interval.bytes", 4096);
        this.electionTimeoutMinMs = intOrDefault(p, "raft.election.timeout.min.ms", 500);
        this.electionTimeoutMaxMs = intOrDefault(p, "raft.election.timeout.max.ms", 1000);
        this.heartbeatIntervalMs = intOrDefault(p, "raft.heartbeat.interval.ms", 150);
        this.replicationFactor = intOrDefault(p, "replication.factor", 3);
        this.partitionAssignment = p.getProperty("partition.assignment", "").trim();
        this.produceTimeoutMs = intOrDefault(p, "produce.timeout.ms", 3000);
        if (partitionCount < 1 || replicationFactor < 1) {
            throw new IllegalArgumentException("partition.count and replication.factor must be >= 1");
        }
        if (electionTimeoutMinMs <= 0 || electionTimeoutMaxMs < electionTimeoutMinMs
                || heartbeatIntervalMs <= 0 || heartbeatIntervalMs >= electionTimeoutMinMs) {
            throw new IllegalArgumentException(
                    "Raft timing must satisfy 0 < heartbeat < election.min <= election.max");
        }
        this.discoveryMode = p.getProperty("raft.discovery.mode", "static").trim();
        this.discoverySrvName = p.getProperty("raft.discovery.srv.name", "").trim();
        this.discoveryHostnamePrefix = p.getProperty("raft.discovery.hostname.prefix", "broker-").trim();
        this.discoveryTtlMinMs = intOrDefault(p, "raft.discovery.ttl.min.ms", 1000);
        this.discoveryTtlMaxMs = intOrDefault(p, "raft.discovery.ttl.max.ms", 300_000);
        this.discoveryIpPreference = p.getProperty("raft.discovery.ip.preference", "ipv4").trim();
        if (!Set.of("static", "dns-hostname", "dns-srv").contains(discoveryMode)) {
            throw new IllegalArgumentException("Invalid raft.discovery.mode: " + discoveryMode
                    + " (expected static, dns-hostname, or dns-srv)");
        }
        if ("dns-srv".equals(discoveryMode) && discoverySrvName.isEmpty()) {
            throw new IllegalArgumentException("raft.discovery.mode=dns-srv requires raft.discovery.srv.name");
        }
    }

    public static BrokerConfig load(Path path) throws IOException {
        Properties p = new Properties();
        try (InputStream in = Files.newInputStream(path)) {
            p.load(in);
        }
        return new BrokerConfig(p);
    }

    public static BrokerConfig fromProperties(Properties p) {
        return new BrokerConfig(p);
    }

    private static List<PeerConfig> parsePeers(String raw) {
        List<PeerConfig> result = new ArrayList<>();
        if (raw == null || raw.isBlank()) {
            return result;
        }
        for (String part : raw.split(",")) {
            String spec = part.trim();
            if (!spec.isEmpty()) {
                result.add(PeerConfig.parse(spec));
            }
        }
        return result;
    }

    private static int requireInt(Properties p, String key) {
        String v = p.getProperty(key);
        if (v == null) {
            throw new IllegalArgumentException("Missing required config: " + key);
        }
        return Integer.parseInt(v.trim());
    }

    private static int intOrDefault(Properties p, String key, int def) {
        String v = p.getProperty(key);
        return v == null ? def : Integer.parseInt(v.trim());
    }

    public int nodeId() {
        return nodeId;
    }

    public String host() {
        return host;
    }

    public int port() {
        return port;
    }

    public List<PeerConfig> peers() {
        return peers;
    }

    public String logDir() {
        return logDir;
    }

    public int partitionCount() {
        return partitionCount;
    }

    public int segmentBytes() {
        return segmentBytes;
    }

    public int indexIntervalBytes() {
        return indexIntervalBytes;
    }

    public int electionTimeoutMinMs() {
        return electionTimeoutMinMs;
    }

    public int electionTimeoutMaxMs() {
        return electionTimeoutMaxMs;
    }

    public int heartbeatIntervalMs() {
        return heartbeatIntervalMs;
    }

    /**
     * Desired replicas per partition. The effective value is capped at the number of
     * brokers in the cluster.
     */
    public int replicationFactor() {
        return replicationFactor;
    }

    /**
     * Optional explicit assignment, e.g. {@code 0:1,2,3;1:2,3,1}: partition id, then its
     * replica broker ids with the preferred leader first. Empty means "derive it".
     */
    public String partitionAssignment() {
        return partitionAssignment;
    }

    /** How long a produce with acks=QUORUM or acks=ALL waits before answering "uncertain". */
    public int produceTimeoutMs() {
        return produceTimeoutMs;
    }

    /** Peer discovery mode: {@code static}, {@code dns-hostname}, or {@code dns-srv}. */
    public String discoveryMode() {
        return discoveryMode;
    }

    public String discoverySrvName() {
        return discoverySrvName;
    }

    public String discoveryHostnamePrefix() {
        return discoveryHostnamePrefix;
    }

    public int discoveryTtlMinMs() {
        return discoveryTtlMinMs;
    }

    public int discoveryTtlMaxMs() {
        return discoveryTtlMaxMs;
    }

    /** {@code ipv4} or {@code ipv6}: which address family to try first when dual-stack. */
    public String discoveryIpPreference() {
        return discoveryIpPreference;
    }
}
