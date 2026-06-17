package com.kafkads.raft;

/**
 * Drives a {@link RaftNode}'s timers in real time by calling {@link RaftNode#tick(long)}
 * on a fixed cadence. Election timeouts and heartbeats fire as their deadlines pass.
 */
public final class RaftScheduler implements AutoCloseable {

    private final RaftNode node;
    private final long tickIntervalMs;
    private Thread thread;
    private volatile boolean running;

    public RaftScheduler(RaftNode node, long tickIntervalMs) {
        this.node = node;
        this.tickIntervalMs = tickIntervalMs;
    }

    public void start() {
        running = true;
        node.initTiming(System.currentTimeMillis());
        thread = new Thread(this::loop, "raft-scheduler-" + node.hashCode());
        thread.setDaemon(true);
        thread.start();
    }

    private void loop() {
        while (running) {
            try {
                node.tick(System.currentTimeMillis());
                Thread.sleep(tickIntervalMs);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            }
        }
    }

    @Override
    public void close() {
        running = false;
        if (thread != null) {
            thread.interrupt();
        }
    }
}
