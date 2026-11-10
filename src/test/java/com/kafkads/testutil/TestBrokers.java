package com.kafkads.testutil;

import com.kafkads.broker.Broker;
import com.kafkads.broker.PartitionReplica;
import com.kafkads.config.BrokerConfig;
import com.kafkads.storage.Log;
import com.kafkads.storage.Record;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.TreeMap;
import java.util.function.BooleanSupplier;

/**
 * Several {@link Broker} objects in this JVM, talking to each other over real loopback
 * sockets. This is not a multi-process cluster: "stopping" a broker here is a graceful
 * {@code close()}. Abrupt process death is covered by {@code MultiProcessFailoverTest}.
 */
public final class TestBrokers implements AutoCloseable {

    private final Path root;
    private final int basePort;
    private final int brokerCount;
    private final int partitions;
    private final int replicationFactor;
    private final Properties extra = new Properties();
    private final Map<Integer, Broker> running = new TreeMap<>();

    public TestBrokers(Path root, int basePort, int brokerCount, int partitions, int replicationFactor) {
        this.root = root;
        this.basePort = basePort;
        this.brokerCount = brokerCount;
        this.partitions = partitions;
        this.replicationFactor = replicationFactor;
    }

    public TestBrokers with(String key, String value) {
        extra.setProperty(key, value);
        return this;
    }

    public int port(int id) {
        return basePort + id - 1;
    }

    public Properties properties(int id) {
        Properties p = new Properties();
        p.setProperty("node.id", Integer.toString(id));
        p.setProperty("host", "127.0.0.1");
        p.setProperty("port", Integer.toString(port(id)));
        StringBuilder peers = new StringBuilder();
        for (int other = 1; other <= brokerCount; other++) {
            if (other != id) {
                if (peers.length() > 0) {
                    peers.append(',');
                }
                peers.append(other).append("@127.0.0.1:").append(port(other));
            }
        }
        p.setProperty("peers", peers.toString());
        p.setProperty("log.dir", root.resolve("broker-" + id).toString());
        p.setProperty("partition.count", Integer.toString(partitions));
        p.setProperty("replication.factor", Integer.toString(replicationFactor));
        p.setProperty("raft.election.timeout.min.ms", "150");
        p.setProperty("raft.election.timeout.max.ms", "300");
        p.setProperty("raft.heartbeat.interval.ms", "50");
        p.setProperty("produce.timeout.ms", "1500");
        p.putAll(extra);
        return p;
    }

    public Path logDir(int id, int partition) {
        return root.resolve("broker-" + id).resolve("partition-" + partition).resolve("log");
    }

    public Path partitionDir(int id, int partition) {
        return root.resolve("broker-" + id).resolve("partition-" + partition);
    }

    /** Starts every broker: all server sockets first, then the election timers. */
    public TestBrokers startAll() throws InterruptedException {
        for (int id = 1; id <= brokerCount; id++) {
            Broker b = new Broker(BrokerConfig.fromProperties(properties(id)));
            running.put(id, b);
            b.start();
        }
        for (Broker b : running.values()) {
            b.startRaft();
        }
        return this;
    }

    public Broker start(int id) throws InterruptedException {
        Broker b = new Broker(BrokerConfig.fromProperties(properties(id)));
        running.put(id, b);
        b.start();
        b.startRaft();
        return b;
    }

    /** Graceful shutdown of one broker. */
    public void stop(int id) {
        Broker b = running.remove(id);
        if (b != null) {
            b.close();
        }
    }

    public Broker broker(int id) {
        return running.get(id);
    }

    public List<Integer> runningIds() {
        return new ArrayList<>(running.keySet());
    }

    public String bootstrap() {
        StringBuilder sb = new StringBuilder();
        for (int id = 1; id <= brokerCount; id++) {
            if (sb.length() > 0) {
                sb.append(',');
            }
            sb.append("127.0.0.1:").append(port(id));
        }
        return sb.toString();
    }

    /** The running broker that currently leads {@code partition} and is ready to serve, or -1. */
    public int leaderOf(int partition) {
        for (Map.Entry<Integer, Broker> e : running.entrySet()) {
            PartitionReplica r = e.getValue().partition(partition);
            if (r != null && r.isLeader() && r.raftNode().isLeaderReady()) {
                return e.getKey();
            }
        }
        return -1;
    }

    public int awaitLeader(int partition, long timeoutMs) throws InterruptedException {
        if (!await(() -> leaderOf(partition) >= 0, timeoutMs)) {
            throw new AssertionError("no ready leader for partition " + partition + " within " + timeoutMs + " ms");
        }
        return leaderOf(partition);
    }

    public void awaitAllLeaders(long timeoutMs) throws InterruptedException {
        for (int p = 0; p < partitions; p++) {
            awaitLeader(p, timeoutMs);
        }
    }

    public static boolean await(BooleanSupplier condition, long timeoutMs) throws InterruptedException {
        long deadline = System.currentTimeMillis() + timeoutMs;
        while (System.currentTimeMillis() < deadline) {
            if (condition.getAsBoolean()) {
                return true;
            }
            Thread.sleep(10);
        }
        return condition.getAsBoolean();
    }

    /** Reads client records straight from a stopped broker's partition directory. */
    public static List<String> dataOnDisk(Path logDir) throws IOException {
        List<String> out = new ArrayList<>();
        try (Log log = new Log(logDir, 16 * 1024 * 1024, 4096)) {
            long offset = 0;
            while (offset < log.logEndOffset()) {
                List<Record> batch = log.read(offset, 1 << 20, log.logEndOffset());
                for (Record r : batch) {
                    if (r.isData()) {
                        out.add(new String(r.value(), StandardCharsets.UTF_8));
                    }
                    offset = r.offset() + 1;
                }
            }
        }
        return out;
    }

    public static List<byte[]> values(String prefix, int from, int count) {
        List<byte[]> v = new ArrayList<>(count);
        for (int i = from; i < from + count; i++) {
            v.add((prefix + i).getBytes(StandardCharsets.UTF_8));
        }
        return v;
    }

    public static List<String> strings(String prefix, int from, int count) {
        List<String> v = new ArrayList<>(count);
        for (int i = from; i < from + count; i++) {
            v.add(prefix + i);
        }
        return v;
    }

    @Override
    public void close() {
        for (Broker b : new ArrayList<>(running.values())) {
            b.close();
        }
        running.clear();
    }
}
