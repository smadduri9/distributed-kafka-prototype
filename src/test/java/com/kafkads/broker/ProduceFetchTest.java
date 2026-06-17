package com.kafkads.broker;

import com.kafkads.config.BrokerConfig;
import com.kafkads.network.Client;
import com.kafkads.protocol.Acks;
import com.kafkads.protocol.messages.FetchRequest;
import com.kafkads.protocol.messages.FetchResponse;
import com.kafkads.protocol.messages.ProduceRequest;
import com.kafkads.protocol.messages.ProduceResponse;
import com.kafkads.storage.Record;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Phase 1 gate: end-to-end produce then fetch over the wire on a single broker. */
class ProduceFetchTest {

    @TempDir
    Path tempDir;

    private BrokerConfig config(int port) {
        Properties p = new Properties();
        p.setProperty("node.id", "1");
        p.setProperty("port", Integer.toString(port));
        p.setProperty("log.dir", tempDir.toString());
        p.setProperty("partition.count", "1");
        return BrokerConfig.fromProperties(p);
    }

    @Test
    void produceThenFetch() throws Exception {
        int port = 19101;
        try (Broker broker = new Broker(config(port))) {
            broker.start();
            try (Client client = new Client()) {
                client.connect("127.0.0.1", port);

                List<byte[]> values = List.of(
                        "alpha".getBytes(StandardCharsets.UTF_8),
                        "beta".getBytes(StandardCharsets.UTF_8),
                        "gamma".getBytes(StandardCharsets.UTF_8));
                ProduceResponse pr = (ProduceResponse) client.sendAndWait(
                        new ProduceRequest(0, Acks.LEADER, values), 5, TimeUnit.SECONDS);
                assertTrue(pr.isOk());
                assertEquals(0, pr.baseOffset());

                FetchResponse fr = (FetchResponse) client.sendAndWait(
                        new FetchRequest(0, 0, 1 << 20), 5, TimeUnit.SECONDS);
                assertEquals(FetchResponse.OK, fr.errorCode());
                assertEquals(3, fr.highWatermark());
                List<Record> records = fr.records();
                assertEquals(3, records.size());
                assertArrayEquals("alpha".getBytes(StandardCharsets.UTF_8), records.get(0).value());
                assertArrayEquals("gamma".getBytes(StandardCharsets.UTF_8), records.get(2).value());
            }
        }
    }
}
