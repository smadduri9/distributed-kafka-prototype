package com.kafkads.protocol;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

/**
 * Parses incoming protocol requests.
 * Protocol format:
 * [Request Type (1 byte)][Request Version (1 byte)][Request Body Length (4 bytes)][Request Body]
 */
public class RequestParser {
    private static final Logger logger = LoggerFactory.getLogger(RequestParser.class);
    
    public enum RequestType {
        CREATE_TOPIC(1),
        PRODUCE(2),
        FETCH(3),
        METADATA(4),
        OFFSET_COMMIT(5),
        OFFSET_FETCH(6),
        HEARTBEAT(7),
        UNKNOWN(0);
        
        private final byte code;
        
        RequestType(int code) {
            this.code = (byte) code;
        }
        
        public byte getCode() {
            return code;
        }
        
        public static RequestType fromCode(byte code) {
            for (RequestType type : values()) {
                if (type.code == code) {
                    return type;
                }
            }
            return UNKNOWN;
        }
    }
    
    /**
     * Parses a request from a byte buffer.
     */
    public static ProtocolRequest parse(ByteBuffer buffer) {
        if (buffer.remaining() < 6) {
            return null; // Not enough data
        }
        
        byte requestTypeCode = buffer.get();
        byte version = buffer.get();
        int bodyLength = buffer.getInt();
        
        if (buffer.remaining() < bodyLength) {
            return null; // Not enough data for body
        }
        
        RequestType requestType = RequestType.fromCode(requestTypeCode);
        byte[] body = new byte[bodyLength];
        buffer.get(body);
        
        return new ProtocolRequest(requestType, version, body);
    }
    
    /**
     * Parses a CREATE_TOPIC request body.
     */
    public static CreateTopicRequest parseCreateTopic(byte[] body) {
        ByteBuffer buffer = ByteBuffer.wrap(body);
        
        int topicNameLength = buffer.getInt();
        byte[] topicNameBytes = new byte[topicNameLength];
        buffer.get(topicNameBytes);
        String topicName = new String(topicNameBytes, StandardCharsets.UTF_8);
        
        int numPartitions = buffer.getInt();
        int replicationFactor = buffer.getInt();
        
        return new CreateTopicRequest(topicName, numPartitions, replicationFactor);
    }
    
    /**
     * Parses a PRODUCE request body.
     */
    public static ProduceRequest parseProduce(byte[] body) {
        ByteBuffer buffer = ByteBuffer.wrap(body);
        
        int topicNameLength = buffer.getInt();
        byte[] topicNameBytes = new byte[topicNameLength];
        buffer.get(topicNameBytes);
        String topicName = new String(topicNameBytes, StandardCharsets.UTF_8);
        
        int partitionId = buffer.getInt();
        int acks = buffer.get();
        int timeoutMs = buffer.getInt();
        
        int messageLength = buffer.getInt();
        byte[] message = new byte[messageLength];
        buffer.get(message);
        
        return new ProduceRequest(topicName, partitionId, acks, timeoutMs, message);
    }
    
    /**
     * Parses a FETCH request body.
     */
    public static FetchRequest parseFetch(byte[] body) {
        ByteBuffer buffer = ByteBuffer.wrap(body);
        
        int topicNameLength = buffer.getInt();
        byte[] topicNameBytes = new byte[topicNameLength];
        buffer.get(topicNameBytes);
        String topicName = new String(topicNameBytes, StandardCharsets.UTF_8);
        
        int partitionId = buffer.getInt();
        long offset = buffer.getLong();
        int maxBytes = buffer.getInt();
        int maxWaitMs = buffer.getInt();
        
        return new FetchRequest(topicName, partitionId, offset, maxBytes, maxWaitMs);
    }
    
    /**
     * Parses a HEARTBEAT request body.
     */
    public static HeartbeatRequest parseHeartbeat(byte[] body) {
        ByteBuffer buffer = ByteBuffer.wrap(body);
        int brokerId = buffer.getInt();
        return new HeartbeatRequest(brokerId);
    }
    
    /**
     * Represents a parsed protocol request.
     */
    public static class ProtocolRequest {
        private final RequestType requestType;
        private final byte version;
        private final byte[] body;
        
        public ProtocolRequest(RequestType requestType, byte version, byte[] body) {
            this.requestType = requestType;
            this.version = version;
            this.body = body;
        }
        
        public RequestType getRequestType() {
            return requestType;
        }
        
        public byte getVersion() {
            return version;
        }
        
        public byte[] getBody() {
            return body;
        }
    }
    
    public static class CreateTopicRequest {
        private final String topicName;
        private final int numPartitions;
        private final int replicationFactor;
        
        public CreateTopicRequest(String topicName, int numPartitions, int replicationFactor) {
            this.topicName = topicName;
            this.numPartitions = numPartitions;
            this.replicationFactor = replicationFactor;
        }
        
        public String getTopicName() { return topicName; }
        public int getNumPartitions() { return numPartitions; }
        public int getReplicationFactor() { return replicationFactor; }
    }
    
    public static class ProduceRequest {
        private final String topicName;
        private final int partitionId;
        private final int acks;
        private final int timeoutMs;
        private final byte[] message;
        
        public ProduceRequest(String topicName, int partitionId, int acks, int timeoutMs, byte[] message) {
            this.topicName = topicName;
            this.partitionId = partitionId;
            this.acks = acks;
            this.timeoutMs = timeoutMs;
            this.message = message;
        }
        
        public String getTopicName() { return topicName; }
        public int getPartitionId() { return partitionId; }
        public int getAcks() { return acks; }
        public int getTimeoutMs() { return timeoutMs; }
        public byte[] getMessage() { return message; }
    }
    
    public static class FetchRequest {
        private final String topicName;
        private final int partitionId;
        private final long offset;
        private final int maxBytes;
        private final int maxWaitMs;
        
        public FetchRequest(String topicName, int partitionId, long offset, int maxBytes, int maxWaitMs) {
            this.topicName = topicName;
            this.partitionId = partitionId;
            this.offset = offset;
            this.maxBytes = maxBytes;
            this.maxWaitMs = maxWaitMs;
        }
        
        public String getTopicName() { return topicName; }
        public int getPartitionId() { return partitionId; }
        public long getOffset() { return offset; }
        public int getMaxBytes() { return maxBytes; }
        public int getMaxWaitMs() { return maxWaitMs; }
    }
    
    public static class HeartbeatRequest {
        private final int brokerId;
        
        public HeartbeatRequest(int brokerId) {
            this.brokerId = brokerId;
        }
        
        public int getBrokerId() { return brokerId; }
    }
}

