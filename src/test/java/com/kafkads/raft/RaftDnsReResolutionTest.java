package com.kafkads.raft;

import com.kafkads.broker.Broker;
import com.kafkads.config.BrokerConfig;
import com.kafkads.discovery.PeerResolutionException;
import com.kafkads.discovery.PeerResolver;
import com.kafkads.discovery.ResolvedPeer;
import com.kafkads.network.Server;
import com.kafkads.protocol.Frame;
import com.kafkads.raft.messages.RequestVoteRequest;
import com.kafkads.raft.messages.RequestVoteResponse;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.net.InetSocketAddress;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BooleanSupplier;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Proves DNS re-resolution on reconnect: when a peer moves to a new address
 * (simulating a Kubernetes pod reschedule), the transport's drop -> invalidate ->
 * re-resolve path must pick up the new address. Resolution changes are simulated
 * with a mutable in-memory {@link PeerResolver}, since CI has no controllable DNS
 * server; the network path itself is the real Netty stack.
 */
class RaftDnsReResolutionTest {

    @TempDir
    Path tempDir;

    private final List<AutoCloseable> toClose = new ArrayList<>();

    @AfterEach
    void tearDown() throws Exception {
        for (AutoCloseable c : toClose) {
            c.close();
        }
        toClose.clear();
    }

    /** Mutable in-memory resolver standing in for DNS; records resolve/invalidate calls. */
    static final class FakePeerResolver implements PeerResolver {
        private final ConcurrentHashMap<Integer, InetSocketAddress> peers = new ConcurrentHashMap<>();
        private final ConcurrentHashMap<Integer, AtomicInteger> resolveCounts = new ConcurrentHashMap<>();
        private final ConcurrentHashMap<Integer, AtomicInteger> invalidateCounts = new ConcurrentHashMap<>();

        FakePeerResolver(Map<Integer, Integer> idToPort) {
            idToPort.forEach((id, port) -> peers.put(id, new InetSocketAddress("127.0.0.1", port)));
        }

        /** Simulates the peer's DNS record changing (pod moved to a new address). */
        void move(int peerId, int newPort) {
            peers.put(peerId, new InetSocketAddress("127.0.0.1", newPort));
        }

        int resolves(int peerId) {
            AtomicInteger c = resolveCounts.get(peerId);
            return c == null ? 0 : c.get();
        }

        int invalidates(int peerId) {
            AtomicInteger c = invalidateCounts.get(peerId);
            return c == null ? 0 : c.get();
        }

        @Override
        public Set<Integer> peerIds() {
            return Set.copyOf(peers.keySet());
        }

        @Override
        public ResolvedPeer resolve(int peerId) throws PeerResolutionException {
            resolveCounts.computeIfAbsent(peerId, k -> new AtomicInteger()).incrementAndGet();
            InetSocketAddress address = peers.get(peerId);
            if (address == null) {
                throw new PeerResolutionException("Unknown peer id: " + peerId);
            }
            return new ResolvedPeer(peerId, List.of(address), Long.MAX_VALUE);
        }

        @Override
        public void invalidate(int peerId) {
            invalidateCounts.computeIfAbsent(peerId, k -> new AtomicInteger()).incrementAndGet();
        }
    }

    private static boolean awaitTrue(BooleanSupplier cond, long timeoutMs) throws InterruptedException {
        long deadline = System.currentTimeMillis() + timeoutMs;
        while (System.currentTimeMillis() < deadline) {
            if (cond.getAsBoolean()) {
                return true;
            }
            Thread.sleep(20);
        }
        return cond.getAsBoolean();
    }

    // ---- transport-level: the strict proof of drop -> invalidate -> re-resolve ----

    /** A server-side handler that grants every vote request. */
    private static SimpleChannelInboundHandler<Frame> voteGranter() {
        return new SimpleChannelInboundHandler<>() {
            @Override
            protected void channelRead0(ChannelHandlerContext ctx, Frame frame) {
                if (frame.message() instanceof RequestVoteRequest req) {
                    ctx.writeAndFlush(new Frame(frame.correlationId(),
                            new RequestVoteResponse(req.term(), true)));
                }
            }
        };
    }

    @Test
    void transportReResolvesAfterPeerMovesToNewPort() throws Exception {
        int portA = 19401;
        int portB = 19402;

        Server serverA = new Server(portA, RaftDnsReResolutionTest::voteGranter);
        serverA.start();
        toClose.add(serverA);

        FakePeerResolver resolver = new FakePeerResolver(Map.of(2, portA));
        NetworkRaftTransport transport = new NetworkRaftTransport(resolver);
        toClose.add(transport);

        RequestVoteRequest req = new RequestVoteRequest(0, 1, 1, 0, 0);
        RequestVoteResponse first = transport.requestVote(2, req).get(5, TimeUnit.SECONDS);
        assertTrue(first.voteGranted());
        assertEquals(1, resolver.resolves(2));

        // The peer "pod" restarts on a different port; its DNS record changes.
        serverA.close();
        Server serverB = new Server(portB, RaftDnsReResolutionTest::voteGranter);
        serverB.start();
        toClose.add(serverB);
        resolver.move(2, portB);

        // The first RPC after the move fails on the dead cached connection, which
        // triggers drop() -> invalidate(); a subsequent RPC must re-resolve and
        // reach the new port. The old port has nothing listening, so success is
        // only possible through re-resolution.
        RequestVoteResponse recovered = null;
        for (int attempt = 0; attempt < 20 && recovered == null; attempt++) {
            try {
                recovered = transport.requestVote(2, req).get(2, TimeUnit.SECONDS);
            } catch (Exception retryable) {
                Thread.sleep(50);
            }
        }
        assertNotNull(recovered, "transport must reconnect to the peer's new address");
        assertTrue(recovered.voteGranted());
        assertTrue(resolver.resolves(2) >= 2, "a fresh resolution must have happened after the move");
        assertTrue(resolver.invalidates(2) >= 1, "the stale entry must have been invalidated");
    }

    // ---- cluster-level: a follower moves ports and rejoins via leader heartbeats ----

    private final List<Broker> brokers = new ArrayList<>();

    private BrokerConfig cfg(int id, int port, int electionMinMs, int electionMaxMs) {
        Properties p = new Properties();
        p.setProperty("node.id", Integer.toString(id));
        p.setProperty("port", Integer.toString(port));
        p.setProperty("log.dir", tempDir.resolve("broker-" + id).toString());
        p.setProperty("partition.count", "1");
        p.setProperty("raft.enabled", "true");
        p.setProperty("raft.election.timeout.min.ms", Integer.toString(electionMinMs));
        p.setProperty("raft.election.timeout.max.ms", Integer.toString(electionMaxMs));
        p.setProperty("raft.heartbeat.interval.ms", "50");
        return BrokerConfig.fromProperties(p);
    }

    private Optional<Broker> currentLeader() {
        return brokers.stream().filter(Broker::isLeader).findFirst();
    }

    @Test
    void clusterReconvergesAfterFollowerMovesToNewPort() throws Exception {
        int p1 = 19411, p2 = 19412, p3 = 19413;
        Map<Integer, Integer> ports = Map.of(1, p1, 2, p2, 3, p3);

        Map<Integer, FakePeerResolver> resolvers = new ConcurrentHashMap<>();
        for (int id = 1; id <= 3; id++) {
            Map<Integer, Integer> others = new ConcurrentHashMap<>(ports);
            others.remove(id);
            resolvers.put(id, new FakePeerResolver(others));
        }
        for (int id = 1; id <= 3; id++) {
            Broker b = new Broker(cfg(id, ports.get(id), 150, 300), resolvers.get(id));
            brokers.add(b);
            toClose.add(b);
            b.start();
        }
        for (Broker b : brokers) {
            b.startRaft();
        }

        assertTrue(awaitTrue(() -> currentLeader().isPresent(), 5000), "a leader should be elected");
        Broker leader = currentLeader().orElseThrow();
        int leaderId = leader.config().nodeId();

        // Pick a follower and "reschedule" it: kill it and restart it with the same
        // node id and Raft state but on a NEW port.
        Broker follower = brokers.stream()
                .filter(b -> b.config().nodeId() != leaderId)
                .findFirst()
                .orElseThrow();
        int followerId = follower.config().nodeId();
        int newPort = 19414;
        follower.close();
        brokers.remove(follower);
        toClose.remove(follower);

        int resolvesBeforeMove = resolvers.get(leaderId).resolves(followerId);

        // Restart with huge election timeouts so it cannot self-elect: the ONLY way
        // it can learn the current leader is by receiving AppendEntries at its new
        // port, which requires the leader to re-resolve the moved address.
        Map<Integer, Integer> othersOfFollower = new ConcurrentHashMap<>(ports);
        othersOfFollower.remove(followerId);
        FakePeerResolver restartedResolver = new FakePeerResolver(othersOfFollower);
        Broker restarted = new Broker(cfg(followerId, newPort, 60_000, 61_000), restartedResolver);
        brokers.add(restarted);
        toClose.add(restarted);
        restarted.start();
        restarted.startRaft();

        // Publish the "DNS change" to the surviving nodes.
        for (Broker b : brokers) {
            int id = b.config().nodeId();
            if (id != followerId) {
                resolvers.get(id).move(followerId, newPort);
            }
        }

        assertTrue(awaitTrue(() -> restarted.raftNode().leaderId() == leaderId, 10_000),
                "the restarted follower must receive heartbeats at its new address");
        assertFalse(restarted.isLeader(), "the moved follower must have rejoined as a follower");
        assertTrue(resolvers.get(leaderId).resolves(followerId) > resolvesBeforeMove,
                "the leader must have re-resolved the moved peer");
        assertTrue(resolvers.get(leaderId).invalidates(followerId) >= 1,
                "the leader must have invalidated the stale address on connection failure");
    }
}
