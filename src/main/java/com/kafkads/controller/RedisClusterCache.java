package com.kafkads.controller;

import com.kafkads.config.RedisConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.DefaultJedisClientConfig;
import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

/**
 * Redis backing for broker liveness and the partition assignment cache.
 * Each broker key expires on its own. Assignments have no TTL; they are overwritten
 * when the controller changes them.
 */
public final class RedisClusterCache implements ClusterCache {
    private static final Logger logger = LoggerFactory.getLogger(RedisClusterCache.class);
    static final String BROKER_KEY_PREFIX = "kafkads:broker:";
    static final String BROKER_SET_KEY = "kafkads:brokers";
    static final String ASSIGNMENT_KEY = "kafkads:assignments";

    private final JedisPool pool;

    private RedisClusterCache(JedisPool pool) {
        this.pool = pool;
    }

    public static String brokerKey(int brokerId) {
        return BROKER_KEY_PREFIX + brokerId;
    }

    public static String assignmentField(String topicName, int partitionId) {
        return topicName + "-" + partitionId;
    }

    /**
     * Opens a pool and pings Redis. Returns null when Redis is disabled or unreachable.
     */
    public static RedisClusterCache connect(RedisConfig config) {
        if (config == null || !config.isEnabled()) {
            return null;
        }

        JedisPool pool = null;
        try {
            JedisPoolConfig poolConfig = new JedisPoolConfig();
            poolConfig.setMaxTotal(8);
            poolConfig.setMaxWait(Duration.ofMillis(config.getTimeoutMs()));

            DefaultJedisClientConfig.Builder clientConfig = DefaultJedisClientConfig.builder()
                .connectionTimeoutMillis(config.getTimeoutMs())
                .socketTimeoutMillis(config.getTimeoutMs());
            if (config.getPassword() != null && !config.getPassword().isEmpty()) {
                clientConfig.password(config.getPassword());
            }

            pool = new JedisPool(poolConfig, new HostAndPort(config.getHost(), config.getPort()), clientConfig.build());
            try (Jedis jedis = pool.getResource()) {
                String pong = jedis.ping();
                if (!"PONG".equalsIgnoreCase(pong)) {
                    pool.close();
                    return null;
                }
            }
            logger.info("Redis cluster cache connected at {}:{}", config.getHost(), config.getPort());
            return new RedisClusterCache(pool);
        } catch (Exception e) {
            logger.warn("Redis unavailable at {}:{} ({}); controller will track liveness in memory",
                config.getHost(), config.getPort(), e.getMessage());
            if (pool != null) {
                pool.close();
            }
            return null;
        }
    }

    @Override
    public boolean isEnabled() {
        return true;
    }

    @Override
    public void touchBroker(int brokerId, String host, int port, int ttlSeconds) {
        try (Jedis jedis = pool.getResource()) {
            jedis.setex(brokerKey(brokerId), ttlSeconds, host + ":" + port);
            jedis.sadd(BROKER_SET_KEY, Integer.toString(brokerId));
        }
    }

    @Override
    public boolean isBrokerAlive(int brokerId) {
        try (Jedis jedis = pool.getResource()) {
            return jedis.exists(brokerKey(brokerId));
        }
    }

    @Override
    public void dropBroker(int brokerId) {
        try (Jedis jedis = pool.getResource()) {
            jedis.del(brokerKey(brokerId));
            jedis.srem(BROKER_SET_KEY, Integer.toString(brokerId));
        }
    }

    @Override
    public void putAssignment(String topicName, int partitionId, int leaderBrokerId, List<Integer> replicaBrokerIds) {
        StringBuilder replicas = new StringBuilder();
        if (replicaBrokerIds != null) {
            for (int i = 0; i < replicaBrokerIds.size(); i++) {
                if (i > 0) {
                    replicas.append(',');
                }
                replicas.append(replicaBrokerIds.get(i));
            }
        }
        try (Jedis jedis = pool.getResource()) {
            jedis.hset(ASSIGNMENT_KEY, assignmentField(topicName, partitionId),
                leaderBrokerId + "|" + replicas);
        }
    }

    @Override
    public Assignment getAssignment(String topicName, int partitionId) {
        String value;
        try (Jedis jedis = pool.getResource()) {
            value = jedis.hget(ASSIGNMENT_KEY, assignmentField(topicName, partitionId));
        }
        if (value == null || value.isEmpty()) {
            return null;
        }
        String[] parts = value.split("\\|", 2);
        int leaderBrokerId = Integer.parseInt(parts[0]);
        List<Integer> replicas = new ArrayList<>();
        if (parts.length > 1 && !parts[1].isEmpty()) {
            for (String id : parts[1].split(",")) {
                replicas.add(Integer.parseInt(id));
            }
        }
        return new Assignment(leaderBrokerId, replicas);
    }

    @Override
    public void close() {
        pool.close();
    }
}
