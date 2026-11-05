package com.kafkads.network;

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
import java.util.List;
import java.util.Queue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.TimeUnit;

/**
 * A minimal blocking request/response client for the KafkaDS protocol. Responses
 * on a single connection are matched to requests in FIFO order.
 */
public class Client implements AutoCloseable {

    private final EventLoopGroup group = new NioEventLoopGroup();
    private final Queue<CompletableFuture<Message>> pending = new ConcurrentLinkedQueue<>();
    private Channel channel;

    public void connect(String host, int port) throws InterruptedException {
        connect(List.of(InetSocketAddress.createUnresolved(host, port)));
    }

    /**
     * Connects to the first reachable address, trying each in order (dual-stack /
     * multi-A fallback). Throws with the last failure if none are reachable.
     */
    public void connect(List<InetSocketAddress> addresses) throws InterruptedException {
        if (addresses.isEmpty()) {
            throw new IllegalArgumentException("No addresses to connect to");
        }
        Bootstrap bootstrap = new Bootstrap();
        bootstrap.group(group)
                .channel(NioSocketChannel.class)
                .option(ChannelOption.TCP_NODELAY, true)
                .handler(new ChannelInitializer<SocketChannel>() {
                    @Override
                    protected void initChannel(SocketChannel ch) {
                        Framing.configure(ch.pipeline(), Side.CLIENT, new InboundHandler());
                    }
                });
        Throwable lastFailure = null;
        for (InetSocketAddress address : addresses) {
            ChannelFuture future = bootstrap.connect(address);
            future.await();
            if (future.isSuccess()) {
                channel = future.channel();
                return;
            }
            lastFailure = future.cause();
        }
        throw new IllegalStateException("Failed to connect to any of " + addresses, lastFailure);
    }

    public CompletableFuture<Message> send(Message request) {
        CompletableFuture<Message> future = new CompletableFuture<>();
        pending.add(future);
        channel.writeAndFlush(request).addListener(f -> {
            if (!f.isSuccess()) {
                pending.remove(future);
                future.completeExceptionally(f.cause());
            }
        });
        return future;
    }

    public Message sendAndWait(Message request, long timeout, TimeUnit unit) throws Exception {
        return send(request).get(timeout, unit);
    }

    @Override
    public void close() {
        try {
            if (channel != null) {
                channel.close().sync();
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } finally {
            group.shutdownGracefully();
        }
    }

    private class InboundHandler extends SimpleChannelInboundHandler<Message> {
        @Override
        protected void channelRead0(ChannelHandlerContext ctx, Message msg) {
            CompletableFuture<Message> future = pending.poll();
            if (future != null) {
                future.complete(msg);
            }
        }

        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
            CompletableFuture<Message> future = pending.poll();
            if (future != null) {
                future.completeExceptionally(cause);
            }
            ctx.close();
        }
    }
}
