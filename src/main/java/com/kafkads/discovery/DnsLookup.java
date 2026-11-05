package com.kafkads.discovery;

import org.xbill.DNS.AAAARecord;
import org.xbill.DNS.ARecord;
import org.xbill.DNS.Name;
import org.xbill.DNS.Record;
import org.xbill.DNS.SRVRecord;
import org.xbill.DNS.Type;
import org.xbill.DNS.lookup.LookupResult;
import org.xbill.DNS.lookup.LookupSession;

import java.net.InetAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * Thin, mockable interface over raw DNS queries. The production implementation
 * wraps dnsjava's {@link LookupSession}; unit tests supply an in-memory fake.
 * Answers carry the record TTL (seconds) so callers can build TTL-aware caches.
 */
public interface DnsLookup {

    /** One SRV answer: {@code target:port} plus SRV priority/weight and the record TTL. */
    record SrvTarget(String target, int port, int priority, int weight, long ttlSeconds) {
    }

    /** One A/AAAA answer with the record TTL. */
    record AddressAnswer(InetAddress address, long ttlSeconds) {
    }

    /** Queries SRV records for {@code name}. */
    List<SrvTarget> lookupSrv(String name) throws PeerResolutionException;

    /** Queries both A and AAAA records for {@code name}. */
    List<AddressAnswer> lookupAddresses(String name) throws PeerResolutionException;

    /** A dnsjava-backed lookup using the system's configured resolvers. */
    static DnsLookup system() {
        return new DnsJavaLookup();
    }

    /** Production implementation over dnsjava's async {@link LookupSession}. */
    final class DnsJavaLookup implements DnsLookup {

        private static final long QUERY_TIMEOUT_MS = 5_000;

        private final LookupSession session = LookupSession.defaultBuilder().build();

        @Override
        public List<SrvTarget> lookupSrv(String name) throws PeerResolutionException {
            List<SrvTarget> out = new ArrayList<>();
            for (Record r : query(name, Type.SRV)) {
                if (r instanceof SRVRecord srv) {
                    out.add(new SrvTarget(srv.getTarget().toString(true), srv.getPort(),
                            srv.getPriority(), srv.getWeight(), srv.getTTL()));
                }
            }
            return out;
        }

        @Override
        public List<AddressAnswer> lookupAddresses(String name) throws PeerResolutionException {
            List<AddressAnswer> out = new ArrayList<>();
            PeerResolutionException failure = null;
            for (int type : new int[] {Type.A, Type.AAAA}) {
                try {
                    for (Record r : query(name, type)) {
                        if (r instanceof ARecord a) {
                            out.add(new AddressAnswer(a.getAddress(), r.getTTL()));
                        } else if (r instanceof AAAARecord aaaa) {
                            out.add(new AddressAnswer(aaaa.getAddress(), r.getTTL()));
                        }
                    }
                } catch (PeerResolutionException e) {
                    // One address family failing (e.g. no AAAA records) must not fail
                    // the lookup if the other family answered.
                    failure = e;
                }
            }
            if (out.isEmpty() && failure != null) {
                throw failure;
            }
            return out;
        }

        private List<Record> query(String name, int type) throws PeerResolutionException {
            try {
                Name qname = Name.fromString(name, Name.root);
                LookupResult result = session.lookupAsync(qname, type)
                        .toCompletableFuture()
                        .get(QUERY_TIMEOUT_MS, TimeUnit.MILLISECONDS);
                return result.getRecords();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new PeerResolutionException("DNS lookup interrupted for " + name, e);
            } catch (Exception e) {
                throw new PeerResolutionException(
                        "DNS lookup failed for " + name + " (type " + Type.string(type) + ")", e);
            }
        }
    }
}
