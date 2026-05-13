package com.kafkads.producer;

import com.kafkads.protocol.RequestParser;
import com.kafkads.protocol.ResponseBuilder;
import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

/**
 * Producer client for sending messages to the broker.
 */
public class Producer {
    private static final Logger logger = LoggerFactory.getLogger(Producer.class);
    
    private final ProducerConfig config;
    private final EventLoopGroup group;
    private Channel channel;
    private volatile boolean connected = false;
    
    public Producer(Properties properties) {
        this.config = new ProducerConfig(properties);
        this.group = new NioEventLoopGroup();
    }
    
    /**
     * Connects to the broker.
     */
    public void connect() throws InterruptedException {
        Bootstrap bootstrap = new Bootstrap();
        bootstrap.group(group)
            .channel(NioSocketChannel.class)
            .handler(new ChannelInitializer<SocketChannel>() {
                @Override
                protected void initChannel(SocketChannel ch) {
                    ch.pipeline().addLast(new ProducerHandler());
                }
            })
            .option(ChannelOption.SO_KEEPALIVE, true);
        
        ChannelFuture future = bootstrap.connect(config.getBrokerHost(), config.getBrokerPort()).sync();
        channel = future.channel();
        connected = true;
        logger.info("Producer connected to broker: {}:{}", config.getBrokerHost(), config.getBrokerPort());
    }
    
    /**
     * Sends a message to a topic partition.
     */
    public CompletableFuture<Long> send(String topicName, int partitionId, byte[] message) {
        if (!connected) {
            CompletableFuture<Long> future = new CompletableFuture<>();
            future.completeExceptionally(new IllegalStateException("Producer not connected"));
            return future;
        }
        
        CompletableFuture<Long> result = new CompletableFuture<>();
        
        // Build produce request
        ByteBuffer buffer = ByteBuffer.allocate(1024);
        
        // Request header
        buffer.put(RequestParser.RequestType.PRODUCE.getCode());
        buffer.put((byte) 1); // version
        
        // Request body
        byte[] topicBytes = topicName.getBytes();
        int bodyLength = 4 + topicBytes.length + 4 + 1 + 4 + 4 + message.length;
        buffer.putInt(bodyLength);
        
        buffer.putInt(topicBytes.length);
        buffer.put(topicBytes);
        buffer.putInt(partitionId);
        buffer.put((byte) config.getAcks());
        buffer.putInt(config.getRequestTimeoutMs());
        buffer.putInt(message.length);
        buffer.put(message);
        
        buffer.flip();
        byte[] requestBytes = new byte[buffer.remaining()];
        buffer.get(requestBytes);
        
        // Send with retry logic
        sendWithRetry(requestBytes, result, 0);
        
        return result;
    }
    
    private void sendWithRetry(byte[] requestBytes, CompletableFuture<Long> result, int attempt) {
        if (attempt >= config.getRetries()) {
            result.completeExceptionally(new RuntimeException("Failed after " + config.getRetries() + " retries"));
            return;
        }
        
        ProducerResponseHandler handler = new ProducerResponseHandler(result, requestBytes, attempt);
        channel.pipeline().addLast(handler);
        
        ByteBuf requestBuf = Unpooled.wrappedBuffer(requestBytes);
        channel.writeAndFlush(requestBuf).addListener((ChannelFuture future) -> {
            if (!future.isSuccess()) {
                if (attempt < config.getRetries()) {
                    logger.warn("Send failed, retrying: attempt={}", attempt + 1);
                    channel.eventLoop().schedule(() -> sendWithRetry(requestBytes, result, attempt + 1), 
                        100, TimeUnit.MILLISECONDS);
                } else {
                    result.completeExceptionally(future.cause());
                }
            }
        });
    }
    
    /**
     * Closes the producer connection.
     */
    public void close() {
        if (channel != null) {
            channel.close();
        }
        group.shutdownGracefully();
        connected = false;
        logger.info("Producer closed");
    }
    
    /**
     * Handler for producer responses.
     */
    private static class ProducerResponseHandler extends ChannelInboundHandlerAdapter {
        private final CompletableFuture<Long> result;
        private final byte[] requestBytes;
        private final int attempt;
        
        public ProducerResponseHandler(CompletableFuture<Long> result, byte[] requestBytes, int attempt) {
            this.result = result;
            this.requestBytes = requestBytes;
            this.attempt = attempt;
        }
        
        @Override
        public void channelRead(ChannelHandlerContext ctx, Object msg) {
            ByteBuf in = (ByteBuf) msg;
            try {
                byte[] responseBytes = new byte[in.readableBytes()];
                in.readBytes(responseBytes);
                
                // Parse response (simplified)
                ByteBuffer buffer = ByteBuffer.wrap(responseBytes);
                byte responseType = buffer.get();
                short errorCode = buffer.getShort();
                int bodyLength = buffer.getInt();
                
                if (errorCode == 0 && bodyLength >= 8) {
                    long offset = buffer.getLong();
                    result.complete(offset);
                } else {
                    result.completeExceptionally(new RuntimeException("Produce failed with error code: " + errorCode));
                }
            } finally {
                in.release();
                ctx.pipeline().remove(this);
            }
        }
    }
    
    /**
     * Handler for producer channel.
     */
    private static class ProducerHandler extends ChannelInboundHandlerAdapter {
        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
            logger.error("Producer channel error", cause);
            ctx.close();
        }
    }
}

