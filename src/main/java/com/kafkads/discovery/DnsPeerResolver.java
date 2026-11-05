package com.kafkads.discovery;

import com.kafkads.config.PeerConfig;

import java.net.Inet4Address;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.LongSupplier;

/**
 * DNS-backed peer discovery with a TTL-aware cache, periodic background refresh,
 * exponential-backoff negative caching, and dual-stack (IPv4/IPv6) address ordering.
 *
 * <p>Two lookup modes:
 * <ul>
 *   <li><b>SRV</b>: query a single SRV name (Kubernetes headless-service style,
 *       e.g. {@code _raft._tcp.kafkads-headless.ns.svc.cluster.local}). Each SRV
 *       target hostname must follow the StatefulSet convention
 *       {@code <prefix><nodeId>.<...>} so node ids can be derived; targets are then
 *       A/AAAA-resolved.</li>
 *   <li><b>Hostname</b>: keep the static {@code id@host:port} peer list but resolve
 *       each host explicitly to A/AAAA records instead of relying on implicit
 *       resolution inside the connect call.</li>
 * </ul>
 *
 * <p>Failure handling: lookup failures serve the last-known-good entry when one
 * exists (stale-on-error); otherwise {@link #resolve(int)} throws. After a failure,
 * re-queries for that peer are suppressed until an exponential backoff elapses
 * (starting at {@value #BACKOFF_INITIAL_MS} ms, capped at {@value #BACKOFF_MAX_MS} ms)
 * so sub-second Raft heartbeats cannot cause a DNS query storm.
 */
public final class DnsPeerResolver implements PeerResolver {

    public enum Mode { SRV, HOSTNAME }

    public enum IpPreference { IPV4, IPV6 }

    private static final long BACKOFF_INITIAL_MS = 100;
    private static final long BACKOFF_MAX_MS = 5_000;

    private final DnsLookup lookup;
    private final Mode mode;
    private final String srvName;
    private final String hostnamePrefix;
    private final Map<Integer, PeerConfig> hostnamePeers = new HashMap<>();
    /** Own node id in SRV mode, excluded from the discovered peer set. */
    private final int selfNodeId;
    private final long ttlMinMs;
    private final long ttlMaxMs;
    private final IpPreference ipPreference;
    private final LongSupplier clock;
    private final ScheduledExecutorService refresher;

    private final ConcurrentHashMap<Integer, ResolvedPeer> cache = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<Integer, Backoff> failures = new ConcurrentHashMap<>();
    /** SRV targets discovered for each node id (target hostname + port). */
    private final ConcurrentHashMap<Integer, DnsLookup.SrvTarget> srvTargets = new ConcurrentHashMap<>();

    private volatile Set<Integer> discoveredIds;

    private record Backoff(int consecutiveFailures, long nextAttemptAtMs) {
    }

    /** SRV-mode resolver. */
    public static DnsPeerResolver forSrv(DnsLookup lookup, String srvName, String hostnamePrefix,
                                         int selfNodeId, long ttlMinMs, long ttlMaxMs,
                                         IpPreference ipPreference) {
        return new DnsPeerResolver(lookup, Mode.SRV, srvName, hostnamePrefix, List.of(), selfNodeId,
                ttlMinMs, ttlMaxMs, ipPreference, System::currentTimeMillis, true);
    }

    /** Hostname-mode resolver over the configured {@code id@host:port} peer list. */
    public static DnsPeerResolver forHostnames(DnsLookup lookup, List<PeerConfig> peers,
                                               long ttlMinMs, long ttlMaxMs,
                                               IpPreference ipPreference) {
        return new DnsPeerResolver(lookup, Mode.HOSTNAME, null, null, peers, -1,
                ttlMinMs, ttlMaxMs, ipPreference, System::currentTimeMillis, true);
    }

    /** Full constructor; tests inject a fake clock and disable the refresh thread. */
    public DnsPeerResolver(DnsLookup lookup, Mode mode, String srvName, String hostnamePrefix,
                           List<PeerConfig> hostnamePeers, int selfNodeId,
                           long ttlMinMs, long ttlMaxMs, IpPreference ipPreference,
                           LongSupplier clock, boolean backgroundRefresh) {
        if (mode == Mode.SRV && (srvName == null || srvName.isBlank())) {
            throw new IllegalArgumentException("SRV mode requires a srv name");
        }
        this.lookup = lookup;
        this.mode = mode;
        this.srvName = srvName;
        this.hostnamePrefix = hostnamePrefix == null ? "" : hostnamePrefix;
        for (PeerConfig p : hostnamePeers) {
            this.hostnamePeers.put(p.nodeId(), p);
        }
        this.selfNodeId = selfNodeId;
        this.ttlMinMs = ttlMinMs;
        this.ttlMaxMs = ttlMaxMs;
        this.ipPreference = ipPreference;
        this.clock = clock;
        if (backgroundRefresh) {
            this.refresher = Executors.newSingleThreadScheduledExecutor(r -> {
                Thread t = new Thread(r, "dns-refresh");
                t.setDaemon(true);
                return t;
            });
            long cadence = Math.max(500, ttlMinMs / 2);
            this.refresher.scheduleAtFixedRate(() -> refreshExpiring(2 * cadence),
                    cadence, cadence, TimeUnit.MILLISECONDS);
        } else {
            this.refresher = null;
        }
    }

    @Override
    public Set<Integer> peerIds() {
        if (mode == Mode.HOSTNAME) {
            return Set.copyOf(hostnamePeers.keySet());
        }
        Set<Integer> ids = discoveredIds;
        if (ids != null) {
            return ids;
        }
        try {
            refreshSrvTargets();
        } catch (PeerResolutionException e) {
            throw new IllegalStateException(
                    "Initial SRV discovery failed for " + srvName
                            + "; cannot determine cluster membership", e);
        }
        return discoveredIds;
    }

    @Override
    public ResolvedPeer resolve(int peerId) throws PeerResolutionException {
        long now = clock.getAsLong();
        ResolvedPeer cached = cache.get(peerId);
        if (cached != null && now < cached.expiresAtMs()) {
            return cached;
        }
        Backoff backoff = failures.get(peerId);
        if (backoff != null && now < backoff.nextAttemptAtMs()) {
            if (cached != null) {
                return cached; // stale-on-error while backing off
            }
            throw new PeerResolutionException(
                    "Resolution for peer " + peerId + " is backing off after "
                            + backoff.consecutiveFailures() + " failure(s)");
        }
        try {
            ResolvedPeer fresh = doResolve(peerId, now);
            cache.put(peerId, fresh);
            failures.remove(peerId);
            return fresh;
        } catch (PeerResolutionException e) {
            recordFailure(peerId, now);
            if (cached != null) {
                return cached; // serve last-known-good on lookup failure
            }
            throw e;
        }
    }

    @Override
    public void invalidate(int peerId) {
        // Keep the entry as last-known-good but mark it expired, and clear the
        // backoff so a connection-failure-triggered re-resolve happens immediately.
        cache.computeIfPresent(peerId, (id, old) ->
                new ResolvedPeer(old.nodeId(), old.addresses(), 0));
        failures.remove(peerId);
    }

    @Override
    public void close() {
        if (refresher != null) {
            refresher.shutdownNow();
        }
    }

    // ---- resolution ----

    private ResolvedPeer doResolve(int peerId, long now) throws PeerResolutionException {
        String host;
        int port;
        if (mode == Mode.HOSTNAME) {
            PeerConfig cfg = hostnamePeers.get(peerId);
            if (cfg == null) {
                throw new PeerResolutionException("Unknown peer id: " + peerId);
            }
            host = cfg.host();
            port = cfg.port();
        } else {
            DnsLookup.SrvTarget target = srvTargets.get(peerId);
            if (target == null) {
                refreshSrvTargets();
                target = srvTargets.get(peerId);
                if (target == null) {
                    throw new PeerResolutionException(
                            "No SRV target for peer " + peerId + " under " + srvName);
                }
            }
            host = target.target();
            port = target.port();
        }

        List<DnsLookup.AddressAnswer> answers = lookup.lookupAddresses(host);
        if (answers.isEmpty()) {
            throw new PeerResolutionException("No A/AAAA records for " + host);
        }
        List<InetSocketAddress> addresses = orderByPreference(answers, port);
        long ttlMs = answers.stream()
                .mapToLong(a -> a.ttlSeconds() * 1000)
                .min()
                .orElse(ttlMinMs);
        ttlMs = Math.max(ttlMinMs, Math.min(ttlMs, ttlMaxMs));
        return new ResolvedPeer(peerId, addresses, now + ttlMs);
    }

    /**
     * Re-queries the SRV name and rebuilds the peerId -> target map. In SRV mode a
     * stale/missing target also triggers this, so a rescheduled pod whose SRV target
     * changed is picked up.
     */
    private synchronized void refreshSrvTargets() throws PeerResolutionException {
        List<DnsLookup.SrvTarget> targets = new ArrayList<>(lookup.lookupSrv(srvName));
        if (targets.isEmpty()) {
            throw new PeerResolutionException("SRV lookup for " + srvName + " returned no records");
        }
        // Deterministic preference order per RFC 2782 intent: priority asc, weight desc.
        targets.sort(Comparator.comparingInt(DnsLookup.SrvTarget::priority)
                .thenComparing(Comparator.comparingInt(DnsLookup.SrvTarget::weight).reversed()));
        Map<Integer, DnsLookup.SrvTarget> fresh = new HashMap<>();
        for (DnsLookup.SrvTarget target : targets) {
            int id = nodeIdFromTarget(target.target());
            if (id < 0 || id == selfNodeId) {
                continue;
            }
            fresh.putIfAbsent(id, target); // best target per node id wins
        }
        srvTargets.keySet().retainAll(fresh.keySet());
        srvTargets.putAll(fresh);
        discoveredIds = Set.copyOf(fresh.keySet());
    }

    /** Extracts the node id from a target like {@code broker-2.svc.cluster.local.}. */
    private int nodeIdFromTarget(String target) {
        String firstLabel = target;
        int dot = target.indexOf('.');
        if (dot >= 0) {
            firstLabel = target.substring(0, dot);
        }
        if (!firstLabel.startsWith(hostnamePrefix)) {
            return -1;
        }
        try {
            return Integer.parseInt(firstLabel.substring(hostnamePrefix.length()));
        } catch (NumberFormatException e) {
            return -1;
        }
    }

    /** Orders answers by IP-family preference, preserving DNS answer order within a family. */
    private List<InetSocketAddress> orderByPreference(List<DnsLookup.AddressAnswer> answers, int port) {
        List<InetSocketAddress> preferred = new ArrayList<>();
        List<InetSocketAddress> fallback = new ArrayList<>();
        for (DnsLookup.AddressAnswer answer : answers) {
            boolean isV4 = answer.address() instanceof Inet4Address;
            boolean prefer = (ipPreference == IpPreference.IPV4) == isV4;
            (prefer ? preferred : fallback).add(new InetSocketAddress(answer.address(), port));
        }
        preferred.addAll(fallback);
        return preferred;
    }

    private void recordFailure(int peerId, long now) {
        failures.compute(peerId, (id, prev) -> {
            int count = prev == null ? 1 : prev.consecutiveFailures() + 1;
            long delay = Math.min(BACKOFF_MAX_MS, BACKOFF_INITIAL_MS << Math.min(20, count - 1));
            return new Backoff(count, now + delay);
        });
    }

    /** Background refresh: re-resolves cached entries expiring within {@code windowMs}. */
    private void refreshExpiring(long windowMs) {
        long now = clock.getAsLong();
        for (Map.Entry<Integer, ResolvedPeer> entry : cache.entrySet()) {
            if (entry.getValue().expiresAtMs() - now > windowMs) {
                continue;
            }
            int peerId = entry.getKey();
            Backoff backoff = failures.get(peerId);
            if (backoff != null && now < backoff.nextAttemptAtMs()) {
                continue;
            }
            try {
                cache.put(peerId, doResolve(peerId, now));
                failures.remove(peerId);
            } catch (PeerResolutionException e) {
                recordFailure(peerId, now);
            }
        }
    }
}
