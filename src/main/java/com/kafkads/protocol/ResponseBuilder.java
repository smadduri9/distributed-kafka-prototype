package com.kafkads.protocol;

import com.kafkads.broker.storage.LogSegment;
import com.kafkads.util.ErrorHandler;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.List;

/**
 * Builds protocol responses.
 * Response format:
 * [Response Type (1 byte)][Error Code (2 bytes)][Response Body Length (4 bytes)][Response Body]
 */
public class ResponseBuilder {
    
    public enum ResponseType {
        CREATE_TOPIC_RESPONSE(1),
        PRODUCE_RESPONSE(2),
        FETCH_RESPONSE(3),
        METADATA_RESPONSE(4),
        OFFSET_COMMIT_RESPONSE(5),
        OFFSET_FETCH_RESPONSE(6),
        HEARTBEAT_RESPONSE(7),
        ERROR_RESPONSE(0);
        
        private final byte code;
        
        ResponseType(int code) {
            this.code = (byte) code;
        }
        
        public byte getCode() {
            return code;
        }
    }
    
    /**
     * Builds a CREATE_TOPIC response.
     */
    public static byte[] buildCreateTopicResponse(boolean success, String errorMessage) {
        ByteBuffer buffer = ByteBuffer.allocate(1024);
        
        buffer.put(ResponseType.CREATE_TOPIC_RESPONSE.getCode());
        buffer.putShort((short) (success ? ErrorHandler.ErrorCode.NONE.getCode() : ErrorHandler.ErrorCode.INVALID_REQUEST.getCode()));
        
        byte[] errorBytes = errorMessage != null ? errorMessage.getBytes(StandardCharsets.UTF_8) : new byte[0];
        buffer.putInt(errorBytes.length);
        buffer.put(errorBytes);
        
        buffer.flip();
        byte[] result = new byte[buffer.remaining()];
        buffer.get(result);
        return result;
    }
    
    /**
     * Builds a PRODUCE response.
     */
    public static byte[] buildProduceResponse(long offset, ErrorHandler.ErrorCode errorCode) {
        ByteBuffer buffer = ByteBuffer.allocate(32);
        
        buffer.put(ResponseType.PRODUCE_RESPONSE.getCode());
        buffer.putShort((short) errorCode.getCode());
        
        // Response body: [offset (8 bytes)]
        buffer.putInt(8);
        buffer.putLong(offset);
        
        buffer.flip();
        byte[] result = new byte[buffer.remaining()];
        buffer.get(result);
        return result;
    }
    
    /**
     * Builds a FETCH response.
     */
    public static byte[] buildFetchResponse(List<LogSegment.MessageRecord> messages, ErrorHandler.ErrorCode errorCode) {
        // Calculate size
        int bodySize = 4; // message count
        for (LogSegment.MessageRecord record : messages) {
            bodySize += 8; // offset
            bodySize += 4; // message length
            bodySize += record.getMessage().length;
        }
        
        ByteBuffer buffer = ByteBuffer.allocate(7 + bodySize);
        
        buffer.put(ResponseType.FETCH_RESPONSE.getCode());
        buffer.putShort((short) errorCode.getCode());
        buffer.putInt(bodySize);
        
        // Response body
        buffer.putInt(messages.size());
        for (LogSegment.MessageRecord record : messages) {
            buffer.putLong(record.getOffset());
            buffer.putInt(record.getMessage().length);
            buffer.put(record.getMessage());
        }
        
        buffer.flip();
        byte[] result = new byte[buffer.remaining()];
        buffer.get(result);
        return result;
    }
    
    /**
     * Builds an error response.
     */
    public static byte[] buildErrorResponse(ErrorHandler.ErrorCode errorCode, String message) {
        byte[] messageBytes = message != null ? message.getBytes(StandardCharsets.UTF_8) : new byte[0];
        ByteBuffer buffer = ByteBuffer.allocate(7 + messageBytes.length);
        
        buffer.put(ResponseType.ERROR_RESPONSE.getCode());
        buffer.putShort((short) errorCode.getCode());
        buffer.putInt(messageBytes.length);
        buffer.put(messageBytes);
        
        buffer.flip();
        byte[] result = new byte[buffer.remaining()];
        buffer.get(result);
        return result;
    }
    
    /**
     * Builds a METADATA response.
     */
    public static byte[] buildMetadataResponse(List<String> topics, ErrorHandler.ErrorCode errorCode) {
        // Calculate size
        int bodySize = 4; // topic count
        for (String topic : topics) {
            bodySize += 4; // topic name length
            bodySize += topic.getBytes(StandardCharsets.UTF_8).length;
        }
        
        ByteBuffer buffer = ByteBuffer.allocate(7 + bodySize);
        
        buffer.put(ResponseType.METADATA_RESPONSE.getCode());
        buffer.putShort((short) errorCode.getCode());
        buffer.putInt(bodySize);
        
        // Response body
        buffer.putInt(topics.size());
        for (String topic : topics) {
            byte[] topicBytes = topic.getBytes(StandardCharsets.UTF_8);
            buffer.putInt(topicBytes.length);
            buffer.put(topicBytes);
        }
        
        buffer.flip();
        byte[] result = new byte[buffer.remaining()];
        buffer.get(result);
        return result;
    }
    
    /**
     * Builds a HEARTBEAT response.
     */
    public static byte[] buildHeartbeatResponse(boolean success, ErrorHandler.ErrorCode errorCode) {
        ByteBuffer buffer = ByteBuffer.allocate(8);
        
        buffer.put(ResponseType.HEARTBEAT_RESPONSE.getCode());
        buffer.putShort((short) errorCode.getCode());
        buffer.putInt(1); // body length
        buffer.put((byte) (success ? 1 : 0));
        
        buffer.flip();
        byte[] result = new byte[buffer.remaining()];
        buffer.get(result);
        return result;
    }
}

