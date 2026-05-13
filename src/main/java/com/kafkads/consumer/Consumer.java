package com.kafkads.consumer;

import com.kafkads.protocol.RequestParser;
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
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Consumer client for fetching messages from the broker.
 */
public class Consumer {
    private static final Logger logger = LoggerFactory.getLogger(Consumer.class);
    
    private final ConsumerConfig config;
    private final OffsetManager offsetManager;
    private final EventLoopGroup group;
    private final ScheduledExecutorService scheduler;
    private Channel channel;
    private volatile boolean connected = false;
    
    public Consumer(Properties properties) {
        this.config = new ConsumerConfig(properties);
        this.offsetManager = new OffsetManager(config.getGroupId(), "./data");
        this.group = new NioEventLoopGroup();
        this.scheduler = Executors.newScheduledThreadPool(1);
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
                    ch.pipeline().addLast(new ConsumerHandler());
                }
            })
            .option(ChannelOption.SO_KEEPALIVE, true);
        
        ChannelFuture future = bootstrap.connect(config.getBrokerHost(), config.getBrokerPort()).sync();
        channel = future.channel();
        connected = true;
        
        // Start auto-commit if enabled
        if (config.isEnableAutoCommit()) {
            scheduler.scheduleAtFixedRate(this::autoCommit, 
                config.getAutoCommitIntervalMs(), 
                config.getAutoCommitIntervalMs(), 
                TimeUnit.MILLISECONDS);
        }
        
        logger.info("Consumer connected to broker: {}:{}", config.getBrokerHost(), config.getBrokerPort());
    }
    
    /**
     * Fetches messages from a topic partition.
     */
    public CompletableFuture<List<MessageRecord>> fetch(String topicName, int partitionId, 
                                                        long offset, int maxBytes) {
        if (!connected) {
            CompletableFuture<List<MessageRecord>> future = new CompletableFuture<>();
            future.completeExceptionally(new IllegalStateException("Consumer not connected"));
            return future;
        }
        
        CompletableFuture<List<MessageRecord>> result = new CompletableFuture<>();
        
        // Build fetch request
        ByteBuffer buffer = ByteBuffer.allocate(1024);
        
        // Request header
        buffer.put(RequestParser.RequestType.FETCH.getCode());
        buffer.put((byte) 1); // version
        
        // Request body
        byte[] topicBytes = topicName.getBytes();
        int bodyLength = 4 + topicBytes.length + 4 + 8 + 4 + 4;
        buffer.putInt(bodyLength);
        
        buffer.putInt(topicBytes.length);
        buffer.put(topicBytes);
        buffer.putInt(partitionId);
        buffer.putLong(offset);
        buffer.putInt(maxBytes);
        buffer.putInt((int) config.getFetchMaxWaitMs());
        
        buffer.flip();
        byte[] requestBytes = new byte[buffer.remaining()];
        buffer.get(requestBytes);
        
        ConsumerResponseHandler handler = new ConsumerResponseHandler(result);
        channel.pipeline().addLast(handler);
        
        ByteBuf requestBuf = Unpooled.wrappedBuffer(requestBytes);
        channel.writeAndFlush(requestBuf);
        
        return result;
    }
    
    /**
     * Commits the offset for a topic partition.
     */
    public void commitOffset(String topicName, int partitionId, long offset) {
        offsetManager.commitOffset(topicName, partitionId, offset);
    }
    
    /**
     * Gets the committed offset for a topic partition.
     */
    public Long getCommittedOffset(String topicName, int partitionId) {
        return offsetManager.getCommittedOffset(topicName, partitionId);
    }
    
    /**
     * Auto-commit offsets.
     */
    private void autoCommit() {
        // In a real implementation, would commit all current offsets
        logger.debug("Auto-commit triggered");
    }
    
    /**
     * Closes the consumer connection.
     */
    public void close() {
        if (channel != null) {
            channel.close();
        }
        scheduler.shutdown();
        group.shutdownGracefully();
        offsetManager.close();
        connected = false;
        logger.info("Consumer closed");
    }
    
    /**
     * Represents a consumed message record.
     */
    public static class MessageRecord {
        private final long offset;
        private final byte[] value;
        
        public MessageRecord(long offset, byte[] value) {
            this.offset = offset;
            this.value = value;
        }
        
        public long getOffset() { return offset; }
        public byte[] getValue() { return value; }
    }
    
    /**
     * Handler for consumer responses.
     */
    private static class ConsumerResponseHandler extends ChannelInboundHandlerAdapter {
        private final CompletableFuture<List<MessageRecord>> result;
        
        public ConsumerResponseHandler(CompletableFuture<List<MessageRecord>> result) {
            this.result = result;
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
                
                List<MessageRecord> messages = new ArrayList<>();
                if (errorCode == 0 && bodyLength > 0) {
                    int messageCount = buffer.getInt();
                    for (int i = 0; i < messageCount; i++) {
                        long offset = buffer.getLong();
                        int messageLength = buffer.getInt();
                        byte[] message = new byte[messageLength];
                        buffer.get(message);
                        messages.add(new MessageRecord(offset, message));
                    }
                }
                
                result.complete(messages);
            } finally {
                in.release();
                ctx.pipeline().remove(this);
            }
        }
    }
    
    /**
     * Handler for consumer channel.
     */
    private static class ConsumerHandler extends ChannelInboundHandlerAdapter {
        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
            logger.error("Consumer channel error", cause);
            ctx.close();
        }
    }
}

