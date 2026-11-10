package com.kafkads.cluster;

import com.kafkads.testutil.TestBrokers;

import java.net.InetSocketAddress;
import java.util.List;

final class ClusterAddresses {

    private ClusterAddresses() {
    }

    /** Bootstrap list containing a single broker. */
    static List<InetSocketAddress> of(TestBrokers cluster, int brokerId) {
        return List.of(new InetSocketAddress("127.0.0.1", cluster.port(brokerId)));
    }
}
