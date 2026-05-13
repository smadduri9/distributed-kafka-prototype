package com.kafkads.broker;

import com.kafkads.broker.storage.LogSegment;
import com.kafkads.broker.storage.MessageStore;
import com.kafkads.config.BrokerConfig;
import com.kafkads.config.ConfigLoader;
import com.kafkads.protocol.RequestHandler;
import com.kafkads.protocol.RequestParser;
import com.kafkads.protocol.ResponseBuilder;
import com.kafkads.util.ConcurrentUtils;
import com.kafkads.util.ErrorHandler;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;

/**
 * Main broker class that manages topics, handles client requests, and coordinates operations.
 */
public class Broker {
    private static final Logger logger = LoggerFactory.getLogger(Broker.class);
    
    private final BrokerConfig config;
    private final MessageStore messageStore;
    private final ConcurrentMap<String, Topic> topics = new ConcurrentHashMap<>();
    private final RequestHandler requestHandler;
    
    private EventLoopGroup bossGroup;
    private EventLoopGroup workerGroup;
    private Channel serverChannel;
    private ExecutorService executorService;
    private volatile boolean running = false;
    
    public Broker(BrokerConfig config) {
        this.config = config;
        MDC.put("brokerId", String.valueOf(config.getBrokerId()));
        this.messageStore = new MessageStore(config);
        this.requestHandler = new RequestHandler(this);
    }
    
    /**
     * Starts the broker server.
     */
    public void start() throws InterruptedException {
        if (running) {
            logger.warn("Broker is already running");
            return;
        }
        
        logger.info("Starting broker: id={}, port={}", config.getBrokerId(), config.getBrokerPort());
        
        bossGroup = new NioEventLoopGroup(config.getNetworkThreads());
        workerGroup = new NioEventLoopGroup(config.getIoThreads());
        executorService = ConcurrentUtils.createThreadPool(config.getIoThreads(), "broker-worker");
        
        try {
            ServerBootstrap bootstrap = new ServerBootstrap();
            bootstrap.group(bossGroup, workerGroup)
                .channel(NioServerSocketChannel.class)
                .childHandler(new ChannelInitializer<SocketChannel>() {
                    @Override
                    protected void initChannel(SocketChannel ch) {
                        ch.pipeline().addLast(new BrokerServerHandler());
                    }
                })
                .option(ChannelOption.SO_BACKLOG, 128)
                .childOption(ChannelOption.SO_KEEPALIVE, true);
            
            serverChannel = bootstrap.bind(config.getBrokerPort()).sync().channel();
            running = true;
            
            logger.info("Broker started successfully on port {}", config.getBrokerPort());
        } catch (Exception e) {
            logger.error("Failed to start broker", e);
            shutdown();
            throw e;
        }
    }
    
    /**
     * Stops the broker server.
     */
    public void shutdown() {
        if (!running) {
            return;
        }
        
        logger.info("Shutting down broker...");
        running = false;
        
        if (serverChannel != null) {
            serverChannel.close();
        }
        
        if (bossGroup != null) {
            bossGroup.shutdownGracefully();
        }
        
        if (workerGroup != null) {
            workerGroup.shutdownGracefully();
        }
        
        ConcurrentUtils.shutdownExecutor(executorService, 5);
        
        try {
            messageStore.close();
        } catch (IOException e) {
            logger.error("Error closing message store", e);
        }
        
        logger.info("Broker shut down complete");
    }
    
    /**
     * Creates a new topic.
     */
    public boolean createTopic(String topicName, int numPartitions, int replicationFactor) {
        if (topics.containsKey(topicName)) {
            logger.warn("Topic already exists: {}", topicName);
            return false;
        }
        
        if (numPartitions <= 0 || replicationFactor <= 0) {
            logger.warn("Invalid topic parameters: partitions={}, replicationFactor={}", 
                numPartitions, replicationFactor);
            return false;
        }
        
        Topic topic = new Topic(topicName, numPartitions, replicationFactor);
        topics.put(topicName, topic);
        
        logger.info("Topic created: name={}, partitions={}, replicationFactor={}", 
            topicName, numPartitions, replicationFactor);
        
        return true;
    }
    
    /**
     * Produces a message to a topic partition.
     */
    public long produce(String topicName, int partitionId, byte[] message) throws IOException {
        Topic topic = topics.get(topicName);
        if (topic == null) {
            throw new IllegalArgumentException("Topic not found: " + topicName);
        }
        
        Partition partition = topic.getPartition(partitionId);
        if (partition == null) {
            throw new IllegalArgumentException("Partition not found: " + topicName + "-" + partitionId);
        }
        
        // Check if this broker is the leader for this partition
        if (!partition.isLeader() && partition.getLeaderBrokerId() != -1) {
            throw new IllegalStateException("Not leader for partition: " + topicName + "-" + partitionId);
        }
        
        long offset = messageStore.append(topicName, partitionId, message);
        logger.debug("Message produced: topic={}, partition={}, offset={}", topicName, partitionId, offset);
        
        return offset;
    }
    
    /**
     * Fetches messages from a topic partition.
     */
    public List<LogSegment.MessageRecord> fetch(String topicName, int partitionId, long offset, int maxMessages) 
            throws IOException {
        Topic topic = topics.get(topicName);
        if (topic == null) {
            throw new IllegalArgumentException("Topic not found: " + topicName);
        }
        
        Partition partition = topic.getPartition(partitionId);
        if (partition == null) {
            throw new IllegalArgumentException("Partition not found: " + topicName + "-" + partitionId);
        }
        
        List<LogSegment.MessageRecord> messages = messageStore.readFromOffset(
            topicName, partitionId, offset, maxMessages
        );
        
        logger.debug("Messages fetched: topic={}, partition={}, offset={}, count={}", 
            topicName, partitionId, offset, messages.size());
        
        return messages;
    }
    
    /**
     * Gets all topic names.
     */
    public List<String> getAllTopicNames() {
        return new ArrayList<>(topics.keySet());
    }
    
    /**
     * Gets a topic by name.
     */
    public Topic getTopic(String topicName) {
        return topics.get(topicName);
    }
    
    public BrokerConfig getConfig() {
        return config;
    }
    
    public boolean isRunning() {
        return running;
    }
    
    /**
     * Netty channel handler for processing client requests.
     */
    private class BrokerServerHandler extends ChannelInboundHandlerAdapter {
        private final ByteBuffer readBuffer = ByteBuffer.allocate(8192);
        
        @Override
        public void channelRead(ChannelHandlerContext ctx, Object msg) {
            ByteBuf in = (ByteBuf) msg;
            
            try {
                // Read available data into buffer
                int readableBytes = in.readableBytes();
                if (readBuffer.remaining() < readableBytes) {
                    // Resize buffer if needed
                    ByteBuffer newBuffer = ByteBuffer.allocate(readBuffer.capacity() * 2);
                    readBuffer.flip();
                    newBuffer.put(readBuffer);
                    readBuffer.clear();
                    readBuffer.put(newBuffer.array(), 0, newBuffer.position());
                }
                
                byte[] bytes = new byte[readableBytes];
                in.readBytes(bytes);
                readBuffer.put(bytes);
                
                // Process complete requests
                readBuffer.flip();
                while (readBuffer.remaining() >= 6) {
                    int position = readBuffer.position();
                    RequestParser.ProtocolRequest request = RequestParser.parse(readBuffer);
                    
                    if (request == null) {
                        // Not enough data, reset position and wait for more
                        readBuffer.position(position);
                        readBuffer.compact();
                        break;
                    }
                    
                    // Process request asynchronously
                    processRequest(ctx, request);
                    
                    // Compact buffer
                    if (readBuffer.hasRemaining()) {
                        ByteBuffer remaining = ByteBuffer.allocate(readBuffer.remaining());
                        remaining.put(readBuffer);
                        readBuffer.clear();
                        readBuffer.put(remaining.array(), 0, remaining.position());
                    } else {
                        readBuffer.clear();
                    }
                }
                
                readBuffer.compact();
            } finally {
                in.release();
            }
        }
        
        private void processRequest(ChannelHandlerContext ctx, RequestParser.ProtocolRequest request) {
            executorService.execute(() -> {
                try {
                    byte[] response = requestHandler.handleRequest(request);
                    ByteBuf responseBuf = Unpooled.wrappedBuffer(response);
                    ctx.writeAndFlush(responseBuf);
                } catch (Exception e) {
                    logger.error("Error processing request", e);
                    ErrorHandler.logError(ErrorHandler.ErrorCode.UNKNOWN, "Request processing", e);
                    byte[] errorResponse = ResponseBuilder.buildErrorResponse(
                        ErrorHandler.ErrorCode.UNKNOWN,
                        "Internal error: " + e.getMessage()
                    );
                    ctx.writeAndFlush(Unpooled.wrappedBuffer(errorResponse));
                }
            });
        }
        
        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
            logger.error("Channel error", cause);
            ctx.close();
        }
    }
    
    /**
     * Main entry point for running the broker.
     */
    public static void main(String[] args) {
        BrokerConfig config = ConfigLoader.loadConfig();
        Broker broker = new Broker(config);
        
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            logger.info("Shutdown hook triggered");
            broker.shutdown();
        }));
        
        try {
            broker.start();
            // Keep the main thread alive
            Thread.currentThread().join();
        } catch (InterruptedException e) {
            logger.error("Broker interrupted", e);
            broker.shutdown();
        }
    }
}

