package com.kafkads.config;

/**
 * Connection settings for the Redis store that holds committed consumer offsets.
 */
public final class RedisConfig {
    private final boolean enabled;
    private final String host;
    private final int port;
    private final String password;
    private final int timeoutMs;

    public RedisConfig(boolean enabled, String host, int port, String password, int timeoutMs) {
        this.enabled = enabled;
        this.host = host;
        this.port = port;
        this.password = password == null ? "" : password;
        this.timeoutMs = timeoutMs;
    }

    public boolean isEnabled() { return enabled; }
    public String getHost() { return host; }
    public int getPort() { return port; }
    public String getPassword() { return password; }
    public int getTimeoutMs() { return timeoutMs; }
}
