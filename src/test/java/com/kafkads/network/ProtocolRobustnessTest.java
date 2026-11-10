package com.kafkads.network;

import com.kafkads.broker.Broker;
import com.kafkads.config.BrokerConfig;
import com.kafkads.protocol.Acks;
import com.kafkads.protocol.ApiKey;
import com.kafkads.protocol.ErrorCode;
import com.kafkads.protocol.Message;
import com.kafkads.protocol.messages.ErrorResponse;
import com.kafkads.protocol.messages.PingRequest;
import com.kafkads.protocol.messages.PingResponse;
import com.kafkads.protocol.messages.ProduceRequest;
import com.kafkads.protocol.messages.ProduceResponse;
import com.kafkads.testutil.TestBrokers;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.net.Socket;
import java.nio.file.Path;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Hostile and malformed input sent over a raw socket to a real broker. Each case must
 * end in a bounded outcome: an {@code INVALID_REQUEST} error frame and a closed
 * connection, with no large allocation, and a broker that still serves other clients.
 */
class ProtocolRobustnessTest {

    private static final int PORT = 20300;

    @TempDir
    Path tmp;

    private Broker broker;

    @BeforeEach
    void startBroker() throws Exception {
        Properties p = new Properties();
        p.setProperty("node.id", "1");
        p.setProperty("port", Integer.toString(PORT));
        p.setProperty("log.dir", tmp.toString());
        p.setProperty("raft.election.timeout.min.ms", "100");
        p.setProperty("raft.election.timeout.max.ms", "200");
        p.setProperty("raft.heartbeat.interval.ms", "30");
        broker = new Broker(BrokerConfig.fromProperties(p));
        broker.start();
        broker.startRaft();
        assertTrue(TestBrokers.await(() -> broker.isLeader(0) && broker.raftNode().isLeaderReady(), 5000));
    }

    @AfterEach
    void stopBroker() {
        broker.close();
    }

    private interface Body {
        void write(DataOutputStream out) throws IOException;
    }

    /** Builds [length][apiKey][correlationId][body]. */
    private static byte[] frame(int apiKey, int correlationId, Body body) throws IOException {
        ByteArrayOutputStream payload = new ByteArrayOutputStream();
        DataOutputStream out = new DataOutputStream(payload);
        out.writeByte(apiKey);
        out.writeInt(correlationId);
        body.write(out);
        ByteArrayOutputStream framed = new ByteArrayOutputStream();
        new DataOutputStream(framed).writeInt(payload.size());
        framed.write(payload.toByteArray());
        return framed.toByteArray();
    }

    private record Reply(int apiKey, int correlationId, short errorCode) {
    }

    /** Sends raw bytes, reads one response frame, then checks the server closed the connection. */
    private static Reply sendExpectingErrorAndClose(byte[] bytes) throws IOException {
        try (Socket socket = new Socket("127.0.0.1", PORT)) {
            socket.setSoTimeout(5000);
            socket.getOutputStream().write(bytes);
            socket.getOutputStream().flush();
            DataInputStream in = new DataInputStream(socket.getInputStream());
            int length = in.readInt();
            assertTrue(length > 0 && length < 4096, "error replies are small");
            int apiKey = in.readByte();
            int correlationId = in.readInt();
            short code = in.readShort();
            in.skipBytes(length - 7);
            assertThrows(EOFException.class, in::readByte, "the connection must be closed after a protocol error");
            return new Reply(apiKey, correlationId, code);
        }
    }

    private void assertBrokerStillServes() throws Exception {
        try (Client client = new Client()) {
            client.connect("127.0.0.1", PORT);
            Message pong = client.sendAndWait(new PingRequest(7), 5, TimeUnit.SECONDS);
            assertEquals(7, assertInstanceOf(PingResponse.class, pong).nonce());
            ProduceResponse ok = (ProduceResponse) client.sendAndWait(
                    new ProduceRequest(0, Acks.QUORUM, TestBrokers.values("still-works-", 0, 1)), 5, TimeUnit.SECONDS);
            assertEquals(ErrorCode.OK, ok.errorCode());
        }
    }

    private void assertRejected(byte[] bytes, int expectedCorrelationId) throws Exception {
        long usedBefore = usedHeap();
        Reply reply = sendExpectingErrorAndClose(bytes);
        assertEquals(ApiKey.ERROR.code(), reply.apiKey());
        assertEquals(ErrorCode.INVALID_REQUEST, reply.errorCode());
        assertEquals(expectedCorrelationId, reply.correlationId());
        assertTrue(usedHeap() - usedBefore < 64L * 1024 * 1024, "no large allocation for a tiny frame");
        assertBrokerStillServes();
    }

    private static long usedHeap() {
        Runtime rt = Runtime.getRuntime();
        return rt.totalMemory() - rt.freeMemory();
    }

    @Test
    void unknownApiKeyIsRejectedWithItsCorrelationId() throws Exception {
        assertRejected(frame(99, 41, out -> { }), 41);
    }

    @Test
    void recordCountLargerThanTheFrameIsRejectedBeforeAllocation() throws Exception {
        // 14 bytes claiming two billion records.
        assertRejected(frame(ApiKey.PRODUCE.code(), 42, out -> {
            out.writeInt(0);
            out.writeByte(Acks.QUORUM.code());
            out.writeInt(Integer.MAX_VALUE);
        }), 42);
        // A count that would allocate ~200 MB of references if trusted.
        assertRejected(frame(ApiKey.PRODUCE.code(), 43, out -> {
            out.writeInt(0);
            out.writeByte(Acks.QUORUM.code());
            out.writeInt(50_000_000);
        }), 43);
    }

    @Test
    void valueLengthLargerThanTheFrameOrNegativeIsRejected() throws Exception {
        assertRejected(frame(ApiKey.PRODUCE.code(), 44, out -> {
            out.writeInt(0);
            out.writeByte(Acks.QUORUM.code());
            out.writeInt(1);
            out.writeInt(1_000_000_000); // value length with no bytes behind it
        }), 44);
        assertRejected(frame(ApiKey.PRODUCE.code(), 45, out -> {
            out.writeInt(0);
            out.writeByte(Acks.QUORUM.code());
            out.writeInt(1);
            out.writeInt(-1);
        }), 45);
    }

    @Test
    void truncatedBodyTrailingBytesAndBadEnumAreRejected() throws Exception {
        assertRejected(frame(ApiKey.FETCH.code(), 46, out -> out.writeInt(0)), 46); // offset and maxBytes missing
        assertRejected(frame(ApiKey.PING.code(), 47, out -> {
            out.writeLong(1L);
            out.writeBytes("garbage after a complete message");
        }), 47);
        assertRejected(frame(ApiKey.PRODUCE.code(), 48, out -> {
            out.writeInt(0);
            out.writeByte(77); // no such acks mode
            out.writeInt(0);
        }), 48);
        // A frame too short to even hold the 5-byte header.
        assertRejected(new byte[]{0, 0, 0, 2, 1, 1}, 0);
    }

    @Test
    void oversizedFrameIsRejectedFromItsLengthPrefixAlone() throws Exception {
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        new DataOutputStream(bytes).writeInt(Framing.MAX_FRAME_BYTES + 1);
        bytes.write(new byte[64]); // the server must not wait for the other 16 MB
        assertRejected(bytes.toByteArray(), 0);
    }

    @Test
    void semanticallyInvalidRequestsGetAnErrorWithoutClosingTheConnection() throws Exception {
        try (Client client = new Client()) {
            client.connect("127.0.0.1", PORT);
            ProduceResponse empty = (ProduceResponse) client.sendAndWait(
                    new ProduceRequest(0, Acks.QUORUM, List.of()), 5, TimeUnit.SECONDS);
            assertEquals(ErrorCode.INVALID_REQUEST, empty.errorCode());
            ProduceResponse unknown = (ProduceResponse) client.sendAndWait(
                    new ProduceRequest(5, Acks.QUORUM, TestBrokers.values("v", 0, 1)), 5, TimeUnit.SECONDS);
            assertEquals(ErrorCode.UNKNOWN_PARTITION, unknown.errorCode());
            // A response-type message sent as a request is answered, not ignored.
            Message reply = client.sendAndWait(new ProduceResponse(ErrorCode.OK, 0, 0), 5, TimeUnit.SECONDS);
            assertEquals(ErrorCode.ERROR, assertInstanceOf(ErrorResponse.class, reply).errorCode());
            // The connection survived all three.
            assertInstanceOf(PingResponse.class, client.sendAndWait(new PingRequest(1), 5, TimeUnit.SECONDS));
        }
    }

    @Test
    void aFrameSplitAcrossManyTcpWritesIsReassembled() throws Exception {
        byte[] first = frame(ApiKey.PING.code(), 5, out -> out.writeLong(111L));
        byte[] second = frame(ApiKey.PING.code(), 6, out -> out.writeLong(222L));
        try (Socket socket = new Socket("127.0.0.1", PORT)) {
            socket.setTcpNoDelay(true);
            socket.setSoTimeout(5000);
            // First frame one byte at a time, then the tail of it glued to a whole second frame.
            for (int i = 0; i < first.length - 3; i++) {
                socket.getOutputStream().write(first[i]);
                socket.getOutputStream().flush();
            }
            ByteArrayOutputStream glued = new ByteArrayOutputStream();
            glued.write(first, first.length - 3, 3);
            glued.write(second);
            socket.getOutputStream().write(glued.toByteArray());
            socket.getOutputStream().flush();

            DataInputStream in = new DataInputStream(socket.getInputStream());
            for (long expected : new long[]{111L, 222L}) {
                assertEquals(13, in.readInt());
                assertEquals(ApiKey.PING.code(), in.readByte());
                int correlationId = in.readInt();
                assertEquals(expected == 111L ? 5 : 6, correlationId);
                assertEquals(expected, in.readLong());
            }
        }
    }
}
