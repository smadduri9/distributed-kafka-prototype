package com.kafkads.discovery;

import com.kafkads.config.PeerConfig;
import org.junit.jupiter.api.Test;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/** Unit tests for the DNS resolver against an in-memory fake DNS (no real lookups). */
class DnsPeerResolverTest {

    private static final String SRV_NAME = "_raft._tcp.kafkads-headless.default.svc.cluster.local";

    /** In-memory DNS: mutable answers, per-type query counters, and injectable failures. */
    static final class FakeDnsLookup implements DnsLookup {
        final Map<String, List<SrvTarget>> srv = new HashMap<>();
        final Map<String, List<AddressAnswer>> addresses = new HashMap<>();
        final Set<String> failing = new HashSet<>();
        int srvQueries;
        int addressQueries;

        @Override
        public List<SrvTarget> lookupSrv(String name) throws PeerResolutionException {
            srvQueries++;
            if (failing.contains(name)) {
                throw new PeerResolutionException("NXDOMAIN (fake): " + name);
            }
            return srv.getOrDefault(name, List.of());
        }

        @Override
        public List<AddressAnswer> lookupAddresses(String name) throws PeerResolutionException {
            addressQueries++;
            if (failing.contains(name)) {
                throw new PeerResolutionException("NXDOMAIN (fake): " + name);
            }
            List<AddressAnswer> answers = addresses.get(name);
            if (answers == null) {
                throw new PeerResolutionException("NXDOMAIN (fake): " + name);
            }
            return answers;
        }
    }

    private static DnsLookup.AddressAnswer v4(String ip, long ttl) throws Exception {
        return new DnsLookup.AddressAnswer(InetAddress.getByName(ip), ttl);
    }

    private static DnsLookup.AddressAnswer v6(String ip, long ttl) throws Exception {
        return new DnsLookup.AddressAnswer(InetAddress.getByName(ip), ttl);
    }

    private static DnsPeerResolver srvResolver(FakeDnsLookup dns, AtomicLong clock) {
        return new DnsPeerResolver(dns, DnsPeerResolver.Mode.SRV, SRV_NAME, "broker-",
                List.of(), 1, 1000, 300_000, DnsPeerResolver.IpPreference.IPV4,
                clock::get, false);
    }

    private static DnsPeerResolver hostnameResolver(FakeDnsLookup dns, AtomicLong clock,
                                                    DnsPeerResolver.IpPreference pref) {
        return new DnsPeerResolver(dns, DnsPeerResolver.Mode.HOSTNAME, null, null,
                List.of(PeerConfig.parse("2@broker-2.example.com:9092")),
                -1, 1000, 300_000, pref, clock::get, false);
    }

    @Test
    void srvModeDiscoversPeerIdsAndResolvesTargets() throws Exception {
        FakeDnsLookup dns = new FakeDnsLookup();
        dns.srv.put(SRV_NAME, List.of(
                new DnsLookup.SrvTarget("broker-1.kafkads-headless.default.svc.cluster.local", 9091, 0, 10, 30),
                new DnsLookup.SrvTarget("broker-2.kafkads-headless.default.svc.cluster.local", 9092, 0, 10, 30),
                new DnsLookup.SrvTarget("broker-3.kafkads-headless.default.svc.cluster.local", 9093, 0, 10, 30)));
        dns.addresses.put("broker-2.kafkads-headless.default.svc.cluster.local",
                List.of(v4("10.0.0.2", 30)));
        AtomicLong clock = new AtomicLong(0);
        DnsPeerResolver resolver = srvResolver(dns, clock); // selfNodeId = 1

        assertEquals(Set.of(2, 3), resolver.peerIds(), "self must be excluded from discovered peers");

        ResolvedPeer p2 = resolver.resolve(2);
        assertEquals(List.of(new InetSocketAddress(InetAddress.getByName("10.0.0.2"), 9092)),
                p2.addresses(), "address from A record, port from SRV record");
    }

    @Test
    void dualStackOrderingRespectsIpPreference() throws Exception {
        List<DnsLookup.AddressAnswer> mixed = List.of(
                v6("fd00::2", 30), v4("10.0.0.2", 30), v4("10.0.0.3", 30));

        FakeDnsLookup dns4 = new FakeDnsLookup();
        dns4.addresses.put("broker-2.example.com", mixed);
        ResolvedPeer v4First = hostnameResolver(dns4, new AtomicLong(0),
                DnsPeerResolver.IpPreference.IPV4).resolve(2);
        assertEquals(List.of(
                        new InetSocketAddress(InetAddress.getByName("10.0.0.2"), 9092),
                        new InetSocketAddress(InetAddress.getByName("10.0.0.3"), 9092),
                        new InetSocketAddress(InetAddress.getByName("fd00::2"), 9092)),
                v4First.addresses(), "IPv4 first, DNS answer order preserved within family");

        FakeDnsLookup dns6 = new FakeDnsLookup();
        dns6.addresses.put("broker-2.example.com", mixed);
        ResolvedPeer v6First = hostnameResolver(dns6, new AtomicLong(0),
                DnsPeerResolver.IpPreference.IPV6).resolve(2);
        assertEquals(new InetSocketAddress(InetAddress.getByName("fd00::2"), 9092),
                v6First.addresses().get(0));
    }

    @Test
    void cachedEntryIsServedUntilTtlExpires() throws Exception {
        FakeDnsLookup dns = new FakeDnsLookup();
        dns.addresses.put("broker-2.example.com", List.of(v4("10.0.0.2", 30)));
        AtomicLong clock = new AtomicLong(0);
        DnsPeerResolver resolver = hostnameResolver(dns, clock, DnsPeerResolver.IpPreference.IPV4);

        resolver.resolve(2);
        resolver.resolve(2);
        assertEquals(1, dns.addressQueries, "second resolve within TTL must hit the cache");

        clock.addAndGet(31_000); // past the 30s record TTL
        resolver.resolve(2);
        assertEquals(2, dns.addressQueries, "expired entry must be re-resolved");
    }

    @Test
    void ttlIsClampedToConfiguredBounds() throws Exception {
        FakeDnsLookup dns = new FakeDnsLookup();
        dns.addresses.put("broker-2.example.com", List.of(v4("10.0.0.2", 0))); // 0-TTL record
        AtomicLong clock = new AtomicLong(0);
        DnsPeerResolver resolver = hostnameResolver(dns, clock, DnsPeerResolver.IpPreference.IPV4);

        ResolvedPeer resolved = resolver.resolve(2);
        assertEquals(1000, resolved.expiresAtMs(), "0-TTL record must be floored to ttlMinMs");
        resolver.resolve(2);
        assertEquals(1, dns.addressQueries, "floor prevents a query per call on 0-TTL records");
    }

    @Test
    void invalidateForcesFreshLookupBeforeTtlExpiry() throws Exception {
        FakeDnsLookup dns = new FakeDnsLookup();
        dns.addresses.put("broker-2.example.com", List.of(v4("10.0.0.2", 300)));
        AtomicLong clock = new AtomicLong(0);
        DnsPeerResolver resolver = hostnameResolver(dns, clock, DnsPeerResolver.IpPreference.IPV4);

        resolver.resolve(2);
        resolver.invalidate(2);

        // Simulate the pod moving to a new IP.
        dns.addresses.put("broker-2.example.com", List.of(v4("10.0.0.9", 300)));
        ResolvedPeer after = resolver.resolve(2);
        assertEquals(2, dns.addressQueries, "invalidate must bypass the unexpired cache entry");
        assertEquals(InetAddress.getByName("10.0.0.9"), after.addresses().get(0).getAddress());
    }

    @Test
    void lookupFailureServesLastKnownGood() throws Exception {
        FakeDnsLookup dns = new FakeDnsLookup();
        dns.addresses.put("broker-2.example.com", List.of(v4("10.0.0.2", 30)));
        AtomicLong clock = new AtomicLong(0);
        DnsPeerResolver resolver = hostnameResolver(dns, clock, DnsPeerResolver.IpPreference.IPV4);

        ResolvedPeer good = resolver.resolve(2);
        clock.addAndGet(31_000); // expire the entry
        dns.failing.add("broker-2.example.com");

        ResolvedPeer stale = resolver.resolve(2);
        assertEquals(good.addresses(), stale.addresses(), "stale-on-error must serve last-known-good");
    }

    @Test
    void lookupFailureWithoutCacheThrowsAndBacksOff() throws Exception {
        FakeDnsLookup dns = new FakeDnsLookup();
        dns.failing.add("broker-2.example.com");
        AtomicLong clock = new AtomicLong(0);
        DnsPeerResolver resolver = hostnameResolver(dns, clock, DnsPeerResolver.IpPreference.IPV4);

        assertThrows(PeerResolutionException.class, () -> resolver.resolve(2));
        assertEquals(1, dns.addressQueries);

        // Within the initial 100ms backoff window: no new query.
        clock.addAndGet(50);
        assertThrows(PeerResolutionException.class, () -> resolver.resolve(2));
        assertEquals(1, dns.addressQueries, "resolve during backoff must not re-query DNS");

        // After the backoff elapses: exactly one retry.
        clock.addAndGet(100);
        assertThrows(PeerResolutionException.class, () -> resolver.resolve(2));
        assertEquals(2, dns.addressQueries);

        // Recovery: DNS answers again after the (now longer) backoff.
        dns.failing.clear();
        dns.addresses.put("broker-2.example.com", List.of(v4("10.0.0.2", 30)));
        clock.addAndGet(5_000);
        ResolvedPeer recovered = resolver.resolve(2);
        assertEquals(InetAddress.getByName("10.0.0.2"), recovered.addresses().get(0).getAddress());
    }

    @Test
    void partialResolutionOnePeerFailingDoesNotAffectOthers() throws Exception {
        FakeDnsLookup dns = new FakeDnsLookup();
        dns.srv.put(SRV_NAME, List.of(
                new DnsLookup.SrvTarget("broker-2.svc", 9092, 0, 10, 30),
                new DnsLookup.SrvTarget("broker-3.svc", 9093, 0, 10, 30)));
        dns.addresses.put("broker-2.svc", List.of(v4("10.0.0.2", 30)));
        dns.failing.add("broker-3.svc"); // NXDOMAIN for one peer only
        AtomicLong clock = new AtomicLong(0);
        DnsPeerResolver resolver = srvResolver(dns, clock);

        assertEquals(Set.of(2, 3), resolver.peerIds());
        assertEquals(9092, resolver.resolve(2).addresses().get(0).getPort());
        assertThrows(PeerResolutionException.class, () -> resolver.resolve(3),
                "unresolvable peer fails independently; Raft treats it as a down peer");
        // Peer 2 remains resolvable (from cache) while 3 is failing.
        assertEquals(9092, resolver.resolve(2).addresses().get(0).getPort());
    }

    @Test
    void srvTargetsOrderedByPriorityThenWeight() throws Exception {
        FakeDnsLookup dns = new FakeDnsLookup();
        // Two SRV records map to the same node id; the lower-priority one must win.
        dns.srv.put(SRV_NAME, List.of(
                new DnsLookup.SrvTarget("broker-2.backup.svc", 19092, 10, 100, 30),
                new DnsLookup.SrvTarget("broker-2.primary.svc", 9092, 0, 10, 30)));
        dns.addresses.put("broker-2.primary.svc", List.of(v4("10.0.0.2", 30)));
        dns.addresses.put("broker-2.backup.svc", List.of(v4("10.9.9.2", 30)));
        AtomicLong clock = new AtomicLong(0);
        DnsPeerResolver resolver = srvResolver(dns, clock);

        assertEquals(Set.of(2), resolver.peerIds());
        ResolvedPeer resolved = resolver.resolve(2);
        assertEquals(InetAddress.getByName("10.0.0.2"), resolved.addresses().get(0).getAddress(),
                "the priority-0 (primary) target must be preferred");
        assertEquals(9092, resolved.addresses().get(0).getPort());
    }
}
