package com.kafkads.replication;

import com.kafkads.broker.Partition;
import com.kafkads.config.PeerConfig;
import com.kafkads.network.Client;
import com.kafkads.protocol.Message;
import com.kafkads.protocol.messages.ReplicateAck;
import com.kafkads.protocol.messages.ReplicateRequest;
import com.kafkads.storage.Record;

import java.util.Collection;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * Leader-side replication. Runs one loop per follower that continuously ships the
 * records the follower is missing and applies the returned acknowledgement to the
 * partition's high-watermark. Followers acknowledge with their log end offset, so a
 * follower that falls behind (or dies) simply holds the high-watermark back, which
 * is exactly what acks=all relies on.
 */
public final class ReplicationManager implements AutoCloseable {

    private static final int MAX_REPLICATE_BYTES = 1 << 20;

    private final int leaderId;
    private final List<PeerConfig> followers;
    private final Collection<Partition> partitions;
    private final int pollIntervalMs;

    private final Thread[] threads;
    private volatile boolean running;

    public ReplicationManager(int leaderId, List<PeerConfig> followers,
                              Collection<Partition> partitions, int pollIntervalMs) {
        this.leaderId = leaderId;
        this.followers = followers;
        this.partitions = partitions;
        this.pollIntervalMs = pollIntervalMs;
        this.threads = new Thread[followers.size()];
    }

    public void start() {
        running = true;
        for (int i = 0; i < followers.size(); i++) {
            PeerConfig follower = followers.get(i);
            Thread t = new Thread(() -> replicateTo(follower), "replicator-" + follower.nodeId());
            t.setDaemon(true);
            threads[i] = t;
            t.start();
        }
    }

    private void replicateTo(PeerConfig follower) {
        Client client = null;
        while (running) {
            try {
                if (client == null) {
                    client = new Client();
                    client.connect(follower.host(), follower.port());
                }
                boolean sentAnything = false;
                for (Partition partition : partitions) {
                    long from = partition.followerEndOffset(follower.nodeId());
                    List<Record> records = partition.readForReplication(from, MAX_REPLICATE_BYTES);
                    long hw = partition.highWatermark();
                    Message resp = client.sendAndWait(
                            new ReplicateRequest(partition.id(), leaderId, hw, records),
                            5, TimeUnit.SECONDS);
                    if (resp instanceof ReplicateAck ack && ack.errorCode() == ReplicateAck.OK) {
                        partition.updateFollowerEndOffset(follower.nodeId(), ack.followerLogEndOffset());
                    }
                    if (!records.isEmpty()) {
                        sentAnything = true;
                    }
                }
                if (!sentAnything) {
                    Thread.sleep(pollIntervalMs);
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            } catch (Exception e) {
                // Follower unreachable: drop the connection and retry after a pause.
                if (client != null) {
                    client.close();
                    client = null;
                }
                sleepQuietly(pollIntervalMs);
            }
        }
        if (client != null) {
            client.close();
        }
    }

    private static void sleepQuietly(long ms) {
        try {
            Thread.sleep(ms);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    @Override
    public void close() {
        running = false;
        for (Thread t : threads) {
            if (t != null) {
                t.interrupt();
            }
        }
    }
}
