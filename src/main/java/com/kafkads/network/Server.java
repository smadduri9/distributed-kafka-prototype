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

import java.util.function.Supplier;

/**
 * A Netty TCP server speaking the KafkaDS binary protocol. A fresh business
 * handler is created per connection via the supplied factory.
 */
public class Server implements AutoCloseable {

    private final int port;
    private final Supplier<ChannelHandler> handlerFactory;
    private EventLoopGroup bossGroup;
    private EventLoopGroup workerGroup;
    private Channel channel;

    public Server(int port, Supplier<ChannelHandler> handlerFactory) {
        this.port = port;
        this.handlerFactory = handlerFactory;
    }

    public void start() throws InterruptedException {
        bossGroup = new NioEventLoopGroup(1);
        workerGroup = new NioEventLoopGroup();
        ServerBootstrap bootstrap = new ServerBootstrap();
        bootstrap.group(bossGroup, workerGroup)
                .channel(NioServerSocketChannel.class)
                .option(ChannelOption.SO_BACKLOG, 128)
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
    public void close() {
        try {
            if (channel != null) {
                channel.close().sync();
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } finally {
            if (bossGroup != null) {
                bossGroup.shutdownGracefully();
            }
            if (workerGroup != null) {
                workerGroup.shutdownGracefully();
            }
        }
    }
}
