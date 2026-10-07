package com.kafkads.controller;

import java.util.List;

/**
 * Used when Redis is disabled or unreachable. The controller then keeps
 * liveness and assignments in memory only.
 */
public final class DisabledClusterCache implements ClusterCache {
    public static final DisabledClusterCache INSTANCE = new DisabledClusterCache();

    private DisabledClusterCache() {
    }

    @Override
    public boolean isEnabled() {
        return false;
    }

    @Override
    public void touchBroker(int brokerId, String host, int port, int ttlSeconds) {
    }

    @Override
    public boolean isBrokerAlive(int brokerId) {
        return false;
    }

    @Override
    public void dropBroker(int brokerId) {
    }

    @Override
    public void putAssignment(String topicName, int partitionId, int leaderBrokerId, List<Integer> replicaBrokerIds) {
    }

    @Override
    public Assignment getAssignment(String topicName, int partitionId) {
        return null;
    }

    @Override
    public void close() {
    }
}
