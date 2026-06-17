package com.kafkads.network;

import com.kafkads.broker.Broker;
import com.kafkads.config.BrokerConfig;
import com.kafkads.protocol.Message;
import com.kafkads.protocol.messages.PingRequest;
import com.kafkads.protocol.messages.PingResponse;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;

/** Phase 0 gate: a client PING is framed, sent, handled, and echoed back. */
class PingRoundTripTest {

    @TempDir
    Path tempDir;

    private BrokerConfig configOnPort(int port) {
        Properties p = new Properties();
        p.setProperty("node.id", "1");
        p.setProperty("port", Integer.toString(port));
        p.setProperty("log.dir", tempDir.toString());
        return BrokerConfig.fromProperties(p);
    }

    @Test
    void pingRoundTrips() throws Exception {
        int port = 19091;
        try (Broker broker = new Broker(configOnPort(port))) {
            broker.start();

            try (Client client = new Client()) {
                client.connect("127.0.0.1", port);
                long nonce = 123456789L;
                Message response = client.sendAndWait(new PingRequest(nonce), 5, TimeUnit.SECONDS);

                assertInstanceOf(PingResponse.class, response);
                assertEquals(nonce, ((PingResponse) response).nonce());
            }
        }
    }
}
