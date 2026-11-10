package com.kafkads.broker;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

/**
 * The static mapping from each partition to the brokers that replicate it. The first
 * broker in a partition's replica list is its <em>preferred leader</em>.
 *
 * <p>This mapping is what the broker executes, not a description of it:
 * <ul>
 *   <li>a broker creates storage and a Raft group only for partitions listed for it;</li>
 *   <li>a partition's Raft peers, and therefore its commit quorum, are exactly its
 *       replica list;</li>
 *   <li>the preferred leader starts its first election before the other replicas;</li>
 *   <li>requests for a partition sent to a broker outside its replica list are refused
 *       with {@code NOT_REPLICA}.</li>
 * </ul>
 *
 * Every broker must be configured with the same broker ids, partition count and
 * replication factor (or the same explicit assignment), because each computes this
 * mapping locally. Membership is static: there is no reassignment at runtime. On
 * failover the replica list does not change; only the current leader within it does.
 */
public final class PartitionAssignment {

    private final TreeMap<Integer, List<Integer>> replicas = new TreeMap<>();

    private PartitionAssignment(Map<Integer, List<Integer>> replicas) {
        replicas.forEach((p, r) -> this.replicas.put(p, List.copyOf(r)));
    }

    /**
     * Deterministic round-robin: with brokers sorted by id, partition {@code p} gets
     * {@code replicationFactor} consecutive brokers starting at position
     * {@code p mod brokerCount}. Preferred leadership is therefore spread evenly.
     */
    public static PartitionAssignment roundRobin(Collection<Integer> brokerIds, int partitionCount,
                                                 int replicationFactor) {
        List<Integer> sorted = new ArrayList<>(new TreeSet<>(brokerIds));
        if (sorted.isEmpty()) {
            throw new IllegalArgumentException("No brokers to assign partitions to");
        }
        int rf = Math.min(replicationFactor, sorted.size());
        Map<Integer, List<Integer>> result = new TreeMap<>();
        for (int p = 0; p < partitionCount; p++) {
            List<Integer> r = new ArrayList<>(rf);
            for (int i = 0; i < rf; i++) {
                r.add(sorted.get((p + i) % sorted.size()));
            }
            result.put(p, r);
        }
        return new PartitionAssignment(result);
    }

    /**
     * Parses an explicit assignment such as {@code 0:1,2,3;1:2,3,1}. Every partition in
     * {@code [0, partitionCount)} must appear exactly once, with distinct, known brokers.
     */
    public static PartitionAssignment parse(String spec, Set<Integer> brokerIds, int partitionCount) {
        Map<Integer, List<Integer>> result = new TreeMap<>();
        for (String part : spec.split(";")) {
            String entry = part.trim();
            if (entry.isEmpty()) {
                continue;
            }
            int colon = entry.indexOf(':');
            if (colon < 0) {
                throw new IllegalArgumentException("Invalid assignment entry: " + entry);
            }
            int partition = Integer.parseInt(entry.substring(0, colon).trim());
            List<Integer> r = new ArrayList<>();
            for (String id : entry.substring(colon + 1).split(",")) {
                int broker = Integer.parseInt(id.trim());
                if (!brokerIds.contains(broker)) {
                    throw new IllegalArgumentException("Partition " + partition
                            + " is assigned to unknown broker " + broker);
                }
                r.add(broker);
            }
            if (r.isEmpty() || new HashSet<>(r).size() != r.size()) {
                throw new IllegalArgumentException("Partition " + partition
                        + " needs a non-empty list of distinct brokers");
            }
            if (partition < 0 || partition >= partitionCount || result.put(partition, r) != null) {
                throw new IllegalArgumentException("Partition " + partition
                        + " is out of range or listed twice");
            }
        }
        if (result.size() != partitionCount) {
            throw new IllegalArgumentException("Assignment covers " + result.size()
                    + " partitions but partition.count is " + partitionCount);
        }
        return new PartitionAssignment(result);
    }

    public int partitionCount() {
        return replicas.size();
    }

    public boolean hasPartition(int partition) {
        return replicas.containsKey(partition);
    }

    /** Replica broker ids for a partition, preferred leader first. */
    public List<Integer> replicas(int partition) {
        List<Integer> r = replicas.get(partition);
        if (r == null) {
            throw new IllegalArgumentException("Unknown partition " + partition);
        }
        return r;
    }

    public int preferredLeader(int partition) {
        return replicas(partition).get(0);
    }

    public boolean isReplica(int partition, int brokerId) {
        return hasPartition(partition) && replicas.get(partition).contains(brokerId);
    }

    /** Partitions hosted by a broker, ascending. */
    public List<Integer> partitionsOf(int brokerId) {
        List<Integer> out = new ArrayList<>();
        replicas.forEach((p, r) -> {
            if (r.contains(brokerId)) {
                out.add(p);
            }
        });
        return out;
    }

    @Override
    public String toString() {
        return replicas.toString();
    }
}
