package com.kafkads.controller;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Shared cluster view for data that is current and short-lived:
 * broker liveness, and the latest partition assignment map.
 */
public interface ClusterCache {
    boolean isEnabled();

    void touchBroker(int brokerId, String host, int port, int ttlSeconds);

    boolean isBrokerAlive(int brokerId);

    void dropBroker(int brokerId);

    void putAssignment(String topicName, int partitionId, int leaderBrokerId, List<Integer> replicaBrokerIds);

    Assignment getAssignment(String topicName, int partitionId);

    void close();

    final class Assignment {
        private final int leaderBrokerId;
        private final List<Integer> replicaBrokerIds;

        public Assignment(int leaderBrokerId, List<Integer> replicaBrokerIds) {
            this.leaderBrokerId = leaderBrokerId;
            this.replicaBrokerIds = replicaBrokerIds == null
                ? Collections.emptyList()
                : new ArrayList<>(replicaBrokerIds);
        }

        public int getLeaderBrokerId() { return leaderBrokerId; }
        public List<Integer> getReplicaBrokerIds() { return new ArrayList<>(replicaBrokerIds); }
    }
}
