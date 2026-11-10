package com.kafkads.network;

import com.kafkads.network.RequestFailedException.Kind;
import com.kafkads.protocol.Frame;
import com.kafkads.protocol.Message;
import com.kafkads.protocol.Side;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * One connection to a broker. Requests may be issued concurrently from any thread; each
 * carries a fresh correlation id and its response is matched by that id, so responses
 * can arrive in any order.
 *
 * <p>Bounds: at most {@code maxPending} requests may be outstanding (further sends fail
 * immediately), and every request has a timeout after which its future fails and its
 * bookkeeping is released. When the connection closes, all outstanding requests fail.
 */
public class Client implements AutoCloseable {

    public static final int DEFAULT_MAX_PENDING = 1024;
    public static final long DEFAULT_REQUEST_TIMEOUT_MS = 5_000;
    public static final int DEFAULT_CONNECT_TIMEOUT_MS = 2_000;

    private record Pending(CompletableFuture<Message> future, ScheduledFuture<?> timer) {
    }

    private final EventLoopGroup group;
    private final boolean ownsGroup;
    private final int maxPending;
    private final ConcurrentHashMap<Integer, Pending> pending = new ConcurrentHashMap<>();
    private final AtomicInteger pendingCount = new AtomicInteger();
    private final AtomicInteger nextCorrelationId = new AtomicInteger(1);
    private volatile Channel channel;

    /** A client with its own single I/O thread. */
    public Client() {
        this(new NioEventLoopGroup(1), true, DEFAULT_MAX_PENDING);
    }

    /** A client sharing an existing event loop group, which the caller keeps ownership of. */
    public Client(EventLoopGroup sharedGroup) {
        this(sharedGroup, false, DEFAULT_MAX_PENDING);
    }

    public Client(EventLoopGroup group, boolean ownsGroup, int maxPending) {
        this.group = group;
        this.ownsGroup = ownsGroup;
        this.maxPending = maxPending;
    }

    /** Blocking connect; throws {@link IllegalStateException} if the broker is unreachable. */
    public void connect(String host, int port) throws InterruptedException {
        connect(List.of(new InetSocketAddress(host, port)));
    }

    public void connect(List<InetSocketAddress> addresses) throws InterruptedException {
        try {
            connectAsync(addresses, DEFAULT_CONNECT_TIMEOUT_MS).get();
        } catch (ExecutionException e) {
            throw new IllegalStateException("Failed to connect to any of " + addresses, e.getCause());
        }
    }

    /**
     * Connects to the first reachable address, trying each in order (dual-stack and
     * multi-A fallback) without blocking the caller.
     */
    public CompletableFuture<Void> connectAsync(List<InetSocketAddress> addresses, int connectTimeoutMs) {
        if (addresses.isEmpty()) {
            return CompletableFuture.failedFuture(new IllegalArgumentException("No addresses to connect to"));
        }
        Bootstrap bootstrap = new Bootstrap();
        bootstrap.group(group)
                .channel(NioSocketChannel.class)
                .option(ChannelOption.TCP_NODELAY, true)
                .option(ChannelOption.CONNECT_TIMEOUT_MILLIS, connectTimeoutMs)
                .handler(new ChannelInitializer<SocketChannel>() {
                    @Override
                    protected void initChannel(SocketChannel ch) {
                        Framing.configure(ch.pipeline(), Side.CLIENT, new InboundHandler());
                    }
                });
        CompletableFuture<Void> result = new CompletableFuture<>();
        tryConnect(bootstrap, new ArrayList<>(addresses), 0, result);
        return result;
    }

    private void tryConnect(Bootstrap bootstrap, List<InetSocketAddress> addresses, int i,
                            CompletableFuture<Void> result) {
        InetSocketAddress address = addresses.get(i);
        if (address.isUnresolved()) {
            address = new InetSocketAddress(address.getHostString(), address.getPort());
        }
        ChannelFuture future = bootstrap.connect(address);
        future.addListener(f -> {
            if (f.isSuccess()) {
                channel = future.channel();
                result.complete(null);
            } else if (i + 1 < addresses.size()) {
                tryConnect(bootstrap, addresses, i + 1, result);
            } else {
                result.completeExceptionally(f.cause());
            }
        });
    }

    public boolean isActive() {
        Channel ch = channel;
        return ch != null && ch.isActive();
    }

    public int pendingRequests() {
        return pendingCount.get();
    }

    public CompletableFuture<Message> send(Message request) {
        return send(request, DEFAULT_REQUEST_TIMEOUT_MS);
    }

    /**
     * Sends a request. The returned future completes with the response, or fails with a
     * {@link RequestFailedException} whose kind says whether the request may have been
     * processed.
     */
    public CompletableFuture<Message> send(Message request, long timeoutMs) {
        Channel ch = channel;
        if (ch == null || !ch.isActive()) {
            return CompletableFuture.failedFuture(
                    new RequestFailedException(Kind.NOT_SENT, "not connected", null));
        }
        if (pendingCount.incrementAndGet() > maxPending) {
            pendingCount.decrementAndGet();
            return CompletableFuture.failedFuture(new RequestFailedException(
                    Kind.NOT_SENT, "more than " + maxPending + " requests outstanding", null));
        }
        int id = nextCorrelationId.getAndIncrement();
        CompletableFuture<Message> future = new CompletableFuture<>();
        ScheduledFuture<?> timer;
        try {
            timer = ch.eventLoop().schedule(
                    () -> fail(id, new RequestFailedException(Kind.TIMEOUT,
                            "no response within " + timeoutMs + " ms", null)),
                    timeoutMs, TimeUnit.MILLISECONDS);
        } catch (RejectedExecutionException e) {
            pendingCount.decrementAndGet();
            return CompletableFuture.failedFuture(
                    new RequestFailedException(Kind.NOT_SENT, "client is shutting down", e));
        }
        pending.put(id, new Pending(future, timer));
        ch.writeAndFlush(new Frame(id, request)).addListener(f -> {
            if (!f.isSuccess()) {
                fail(id, new RequestFailedException(Kind.NOT_SENT, "write failed", f.cause()));
            }
        });
        return future;
    }

    public Message sendAndWait(Message request, long timeout, TimeUnit unit) throws Exception {
        try {
            return send(request, unit.toMillis(timeout)).get();
        } catch (ExecutionException e) {
            if (e.getCause() instanceof Exception cause) {
                throw cause;
            }
            throw e;
        }
    }

    private void complete(int id, Message response) {
        Pending p = pending.remove(id);
        if (p != null) {
            pendingCount.decrementAndGet();
            p.timer.cancel(false);
            p.future.complete(response);
        }
        // else: a response that arrived after its request timed out; drop it.
    }

    private void fail(int id, Throwable cause) {
        Pending p = pending.remove(id);
        if (p != null) {
            pendingCount.decrementAndGet();
            p.timer.cancel(false);
            p.future.completeExceptionally(cause);
        }
    }

    private void failAll(Throwable cause) {
        for (Integer id : new ArrayList<>(pending.keySet())) {
            fail(id, cause);
        }
    }

    @Override
    public void close() {
        Channel ch = channel;
        if (ch != null) {
            ch.close();
        }
        failAll(new RequestFailedException(Kind.CONNECTION_LOST, "client closed", null));
        if (ownsGroup) {
            group.shutdownGracefully(0, 100, TimeUnit.MILLISECONDS);
        }
    }

    private class InboundHandler extends SimpleChannelInboundHandler<Frame> {
        @Override
        protected void channelRead0(ChannelHandlerContext ctx, Frame frame) {
            complete(frame.correlationId(), frame.message());
        }

        @Override
        public void channelInactive(ChannelHandlerContext ctx) {
            failAll(new RequestFailedException(Kind.CONNECTION_LOST, "connection closed by peer", null));
            ctx.fireChannelInactive();
        }

        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
            ctx.close();
        }
    }
}
