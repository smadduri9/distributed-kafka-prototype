package com.kafkads.network;

import com.kafkads.protocol.Side;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;

import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

/**
 * A Netty TCP server speaking the KafkaDS binary protocol. A fresh business handler is
 * created per connection via the supplied factory; it receives decoded
 * {@link com.kafkads.protocol.Frame}s.
 */
public class Server implements AutoCloseable {

    private final int port;
    private final Supplier<ChannelHandler> handlerFactory;
    private EventLoopGroup bossGroup;
    private EventLoopGroup workerGroup;
    private Channel channel;
    private boolean closed;

    public Server(int port, Supplier<ChannelHandler> handlerFactory) {
        this.port = port;
        this.handlerFactory = handlerFactory;
    }

    public void start() throws InterruptedException {
        bossGroup = new NioEventLoopGroup(1);
        workerGroup = new NioEventLoopGroup(Math.min(4, Runtime.getRuntime().availableProcessors()));
        ServerBootstrap bootstrap = new ServerBootstrap();
        bootstrap.group(bossGroup, workerGroup)
                .channel(NioServerSocketChannel.class)
                .option(ChannelOption.SO_BACKLOG, 128)
                .option(ChannelOption.SO_REUSEADDR, true)
                .childOption(ChannelOption.TCP_NODELAY, true)
                .childHandler(new ChannelInitializer<SocketChannel>() {
                    @Override
                    protected void initChannel(SocketChannel ch) {
                        Framing.configure(ch.pipeline(), Side.SERVER, handlerFactory.get());
                    }
                });
        channel = bootstrap.bind(port).sync().channel();
    }

    public int port() {
        return port;
    }

    @Override
    public synchronized void close() {
        if (closed) {
            return;
        }
        closed = true;
        try {
            if (channel != null) {
                channel.close().sync();
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } finally {
            // Shutting the worker group down closes every accepted connection.
            if (bossGroup != null) {
                bossGroup.shutdownGracefully(0, 200, TimeUnit.MILLISECONDS);
            }
            if (workerGroup != null) {
                try {
                    workerGroup.shutdownGracefully(0, 200, TimeUnit.MILLISECONDS).sync();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
        }
    }
}
