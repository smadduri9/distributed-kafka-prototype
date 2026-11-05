package com.kafkads.discovery;

import com.kafkads.config.PeerConfig;
import org.junit.jupiter.api.Test;

import java.net.InetSocketAddress;
import java.util.List;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** The static resolver must behave exactly like the legacy id@host:port path. */
class StaticPeerResolverTest {

    @Test
    void resolvesConfiguredPeers() throws Exception {
        StaticPeerResolver resolver = new StaticPeerResolver(List.of(
                PeerConfig.parse("2@127.0.0.1:9092"),
                PeerConfig.parse("3@broker-3.example.com:9093")));

        assertEquals(Set.of(2, 3), resolver.peerIds());

        ResolvedPeer p2 = resolver.resolve(2);
        assertEquals(2, p2.nodeId());
        assertEquals(1, p2.addresses().size());
        InetSocketAddress addr = p2.addresses().get(0);
        assertEquals("127.0.0.1", addr.getHostString());
        assertEquals(9092, addr.getPort());
        assertTrue(addr.isUnresolved(), "static resolver must defer resolution to Netty, as before");

        ResolvedPeer p3 = resolver.resolve(3);
        assertEquals("broker-3.example.com", p3.addresses().get(0).getHostString());
        assertEquals(9093, p3.addresses().get(0).getPort());
    }

    @Test
    void entriesNeverExpireAndInvalidateIsANoOp() throws Exception {
        StaticPeerResolver resolver = new StaticPeerResolver(List.of(PeerConfig.parse("2@localhost:1234")));
        ResolvedPeer before = resolver.resolve(2);
        assertEquals(Long.MAX_VALUE, before.expiresAtMs());
        resolver.invalidate(2);
        assertEquals(before, resolver.resolve(2));
    }

    @Test
    void unknownPeerThrows() {
        StaticPeerResolver resolver = new StaticPeerResolver(List.of(PeerConfig.parse("2@localhost:1234")));
        assertThrows(PeerResolutionException.class, () -> resolver.resolve(9));
    }
}
