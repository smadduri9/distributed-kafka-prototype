package com.kafkads.network;

import com.kafkads.network.RequestFailedException.Kind;
import com.kafkads.protocol.Frame;
import com.kafkads.protocol.Message;
import com.kafkads.protocol.messages.PingRequest;
import com.kafkads.protocol.messages.PingResponse;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The client's request bookkeeping, tested against scripted servers on real sockets:
 * responses out of order, responses that never come, connections that drop, and more
 * requests than the client allows to be outstanding.
 */
class ClientCorrelationTest {

    private final List<AutoCloseable> toClose = new ArrayList<>();

    @AfterEach
    void tearDown() throws Exception {
        for (AutoCloseable c : toClose) {
            c.close();
        }
    }

    private Server server(int port, BiConsumer<ChannelHandlerContext, Frame> behaviour) throws Exception {
        Server server = new Server(port, () -> new SimpleChannelInboundHandler<Frame>() {
            @Override
            protected void channelRead0(ChannelHandlerContext ctx, Frame frame) {
                behaviour.accept(ctx, frame);
            }
        });
        server.start();
        toClose.add(server);
        return server;
    }

    private Client client(int port, int maxPending) throws Exception {
        Client client = new Client(new io.netty.channel.nio.NioEventLoopGroup(1), true, maxPending);
        client.connect("127.0.0.1", port);
        toClose.add(client);
        return client;
    }

    private static long nonce(Message m) {
        return assertInstanceOf(PingResponse.class, m).nonce();
    }

    private static RequestFailedException failure(CompletableFuture<Message> future) {
        ExecutionException e = assertThrows(ExecutionException.class, () -> future.get(10, TimeUnit.SECONDS));
        return assertInstanceOf(RequestFailedException.class, e.getCause());
    }

    @Test
    void responsesArrivingInReverseOrderReachTheRightCallers() throws Exception {
        int batch = 50;
        ConcurrentLinkedQueue<Frame> held = new ConcurrentLinkedQueue<>();
        server(20401, (ctx, frame) -> {
            held.add(frame);
            if (held.size() == batch) {
                List<Frame> all = new ArrayList<>(held);
                for (int i = all.size() - 1; i >= 0; i--) { // answer the last request first
                    Frame f = all.get(i);
                    ctx.write(new Frame(f.correlationId(), new PingResponse(((PingRequest) f.message()).nonce())));
                }
                ctx.flush();
            }
        });
        Client client = client(20401, 1024);

        List<CompletableFuture<Message>> futures = new ArrayList<>();
        for (int i = 0; i < batch; i++) {
            futures.add(client.send(new PingRequest(1000 + i)));
        }
        for (int i = 0; i < batch; i++) {
            assertEquals(1000 + i, nonce(futures.get(i).get(10, TimeUnit.SECONDS)));
        }
        assertEquals(0, client.pendingRequests());
    }

    @Test
    void manyThreadsSharingOneConnectionEachGetTheirOwnResponse() throws Exception {
        // The server answers from a scheduler with a per-request delay, so responses interleave.
        server(20402, (ctx, frame) -> {
            long nonce = ((PingRequest) frame.message()).nonce();
            ctx.executor().schedule(
                    () -> ctx.writeAndFlush(new Frame(frame.correlationId(), new PingResponse(nonce))),
                    nonce % 7, TimeUnit.MILLISECONDS);
        });
        Client client = client(20402, 4096);

        int threads = 16;
        int perThread = 200;
        ExecutorService pool = Executors.newFixedThreadPool(threads);
        try {
            List<Future<Integer>> results = new ArrayList<>();
            for (int t = 0; t < threads; t++) {
                final int thread = t;
                results.add(pool.submit(() -> {
                    int mismatches = 0;
                    for (int i = 0; i < perThread; i++) {
                        long expected = thread * 1_000_000L + i;
                        if (nonce(client.sendAndWait(new PingRequest(expected), 10, TimeUnit.SECONDS)) != expected) {
                            mismatches++;
                        }
                    }
                    return mismatches;
                }));
            }
            for (Future<Integer> f : results) {
                assertEquals(0, f.get(60, TimeUnit.SECONDS), "a caller received someone else's response");
            }
        } finally {
            pool.shutdownNow();
        }
        assertEquals(0, client.pendingRequests());
    }

    @Test
    void aRequestWithNoResponseTimesOutAndReleasesItsSlot() throws Exception {
        List<Frame> swallowed = new ArrayList<>();
        server(20403, (ctx, frame) -> {
            long nonce = ((PingRequest) frame.message()).nonce();
            if (nonce == 1) {
                synchronized (swallowed) {
                    swallowed.add(frame); // never answered in time
                }
                // Answer much later: the client must ignore it rather than mis-deliver it.
                ctx.executor().schedule(
                        () -> ctx.writeAndFlush(new Frame(frame.correlationId(), new PingResponse(nonce))),
                        600, TimeUnit.MILLISECONDS);
            } else {
                ctx.writeAndFlush(new Frame(frame.correlationId(), new PingResponse(nonce)));
            }
        });
        Client client = client(20403, 1024);

        long start = System.nanoTime();
        RequestFailedException e = failure(client.send(new PingRequest(1), 200));
        long elapsedMs = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start);
        assertEquals(Kind.TIMEOUT, e.kind());
        assertTrue(e.requestMayHaveBeenProcessed(), "a timed-out request may have been processed");
        assertTrue(elapsedMs >= 150 && elapsedMs < 2000, "timed out after " + elapsedMs + " ms");
        assertEquals(0, client.pendingRequests());

        // The late response to request 1 arrives while request 2 is the only one outstanding.
        Thread.sleep(600);
        assertEquals(2, nonce(client.sendAndWait(new PingRequest(2), 5, TimeUnit.SECONDS)));
        assertEquals(3, nonce(client.sendAndWait(new PingRequest(3), 5, TimeUnit.SECONDS)));
    }

    @Test
    void pendingRequestsAreBoundedAndFailFast() throws Exception {
        server(20404, (ctx, frame) -> { }); // never answers
        Client client = client(20404, 8);

        List<CompletableFuture<Message>> accepted = new ArrayList<>();
        for (int i = 0; i < 8; i++) {
            accepted.add(client.send(new PingRequest(i), 500));
        }
        CompletableFuture<Message> rejected = client.send(new PingRequest(99), 500);
        assertTrue(rejected.isCompletedExceptionally(), "the ninth request is refused immediately");
        RequestFailedException e = failure(rejected);
        assertEquals(Kind.NOT_SENT, e.kind());
        assertFalse(e.requestMayHaveBeenProcessed());
        assertEquals(8, client.pendingRequests());

        for (CompletableFuture<Message> f : accepted) {
            assertEquals(Kind.TIMEOUT, failure(f).kind());
        }
        assertEquals(0, client.pendingRequests(), "timeouts release every slot");
        assertFalse(client.send(new PingRequest(100), 200).isCompletedExceptionally(), "capacity is back");
    }

    @Test
    void connectionLossFailsEveryOutstandingRequest() throws Exception {
        server(20405, (ctx, frame) -> {
            if (((PingRequest) frame.message()).nonce() == 3) {
                ctx.close(); // drop the connection with three requests unanswered
            }
        });
        Client client = client(20405, 1024);

        List<CompletableFuture<Message>> futures = new ArrayList<>();
        for (int i = 1; i <= 3; i++) {
            futures.add(client.send(new PingRequest(i), 30_000));
        }
        for (CompletableFuture<Message> f : futures) {
            RequestFailedException e = failure(f);
            assertEquals(Kind.CONNECTION_LOST, e.kind());
            assertTrue(e.requestMayHaveBeenProcessed());
        }
        assertEquals(0, client.pendingRequests());
        assertTrue(TestAwait.until(() -> !client.isActive(), 2000));

        RequestFailedException afterwards = failure(client.send(new PingRequest(4)));
        assertEquals(Kind.NOT_SENT, afterwards.kind(), "a closed connection refuses new requests without sending");
    }

    @Test
    void connectingToNothingFailsWithinTheConnectTimeout() {
        Client client = new Client();
        toClose.add(client);
        long start = System.nanoTime();
        assertThrows(IllegalStateException.class, () -> client.connect("127.0.0.1", 20499));
        assertTrue(TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start) < 5000);
        assertEquals(Kind.NOT_SENT, failure(client.send(new PingRequest(1))).kind());
    }

    private static final class TestAwait {
        static boolean until(java.util.function.BooleanSupplier c, long timeoutMs) throws InterruptedException {
            return com.kafkads.testutil.TestBrokers.await(c, timeoutMs);
        }
    }
}
