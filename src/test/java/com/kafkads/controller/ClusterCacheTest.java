package com.kafkads.controller;

import com.kafkads.config.RedisConfig;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class ClusterCacheTest {
    @Test
    void missingRedisKeyMarksBrokerDead() {
        FakeClusterCache cache = new FakeClusterCache();
        Controller controller = new Controller(cache);
        controller.registerBroker(1, "localhost", 9092);
        controller.updateBrokerHeartbeat(1);

        assertTrue(controller.getMetadataManager().getBrokerMetadata(1).isAlive());
        assertTrue(cache.isBrokerAlive(1));

        cache.expire(1);
        controller.detectDeadBrokers();

        assertFalse(controller.getMetadataManager().getBrokerMetadata(1).isAlive());
        controller.stop();
    }

    @Test
    void assignmentIsCachedAndReadableByAnotherController() {
        FakeClusterCache cache = new FakeClusterCache();
        Controller writer = new Controller(cache);
        writer.registerBroker(1, "localhost", 9092);
        writer.registerBroker(2, "localhost", 9093);
        assertTrue(writer.createTopic("orders", 1, 2));

        ClusterCache.Assignment cached = cache.getAssignment("orders", 0);
        assertEquals(1, cached.getLeaderBrokerId());
        assertEquals(List.of(1, 2), cached.getReplicaBrokerIds());

        Controller reader = new Controller(cache);
        MetadataManager.PartitionAssignment assignment = reader.getPartitionAssignment("orders", 0);
        assertEquals(1, assignment.getLeaderBrokerId());
        assertEquals(List.of(1, 2), assignment.getReplicaBrokerIds());

        writer.stop();
        reader.stop();
    }

    @Test
    void disabledCacheKeepsAssignmentInMemoryOnly() {
        Controller controller = new Controller(DisabledClusterCache.INSTANCE);
        controller.registerBroker(1, "localhost", 9092);
        assertTrue(controller.createTopic("orders", 1, 1));
        assertEquals(1, controller.getPartitionAssignment("orders", 0).getLeaderBrokerId());
        controller.stop();
    }

    @Test
    void unreachableRedisFallsBackToDisabledCache() {
        assertNull(RedisClusterCache.connect(new RedisConfig(true, "127.0.0.1", 1, "", 200)));
    }

    @Test
    void redisKeysAreStable() {
        assertEquals("kafkads:broker:3", RedisClusterCache.brokerKey(3));
        assertEquals("orders-1", RedisClusterCache.assignmentField("orders", 1));
    }

    private static final class FakeClusterCache implements ClusterCache {
        private final Map<Integer, Boolean> brokers = new HashMap<>();
        private final Map<String, Assignment> assignments = new HashMap<>();

        @Override
        public boolean isEnabled() {
            return true;
        }

        @Override
        public void touchBroker(int brokerId, String host, int port, int ttlSeconds) {
            brokers.put(brokerId, true);
        }

        void expire(int brokerId) {
            brokers.put(brokerId, false);
        }

        @Override
        public boolean isBrokerAlive(int brokerId) {
            return Boolean.TRUE.equals(brokers.get(brokerId));
        }

        @Override
        public void dropBroker(int brokerId) {
            brokers.put(brokerId, false);
        }

        @Override
        public void putAssignment(String topicName, int partitionId, int leaderBrokerId, List<Integer> replicaBrokerIds) {
            assignments.put(RedisClusterCache.assignmentField(topicName, partitionId),
                new Assignment(leaderBrokerId, new ArrayList<>(replicaBrokerIds)));
        }

        @Override
        public Assignment getAssignment(String topicName, int partitionId) {
            return assignments.get(RedisClusterCache.assignmentField(topicName, partitionId));
        }

        @Override
        public void close() {
        }
    }
}
