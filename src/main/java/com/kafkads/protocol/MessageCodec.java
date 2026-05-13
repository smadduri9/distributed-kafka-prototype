package com.kafkads.protocol;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

/**
 * Encodes and decodes messages for the protocol.
 */
public class MessageCodec {
    
    /**
     * Encodes a message with key and value.
     */
    public static byte[] encode(String key, byte[] value) {
        byte[] keyBytes = key != null ? key.getBytes(StandardCharsets.UTF_8) : new byte[0];
        int totalLength = 4 + keyBytes.length + 4 + value.length;
        
        ByteBuffer buffer = ByteBuffer.allocate(totalLength);
        buffer.putInt(keyBytes.length);
        buffer.put(keyBytes);
        buffer.putInt(value.length);
        buffer.put(value);
        
        return buffer.array();
    }
    
    /**
     * Decodes a message into key and value.
     */
    public static Message decode(byte[] data) {
        ByteBuffer buffer = ByteBuffer.wrap(data);
        
        int keyLength = buffer.getInt();
        byte[] keyBytes = new byte[keyLength];
        buffer.get(keyBytes);
        String key = keyLength > 0 ? new String(keyBytes, StandardCharsets.UTF_8) : null;
        
        int valueLength = buffer.getInt();
        byte[] value = new byte[valueLength];
        buffer.get(value);
        
        return new Message(key, value);
    }
    
    /**
     * Represents a decoded message.
     */
    public static class Message {
        private final String key;
        private final byte[] value;
        
        public Message(String key, byte[] value) {
            this.key = key;
            this.value = value;
        }
        
        public String getKey() {
            return key;
        }
        
        public byte[] getValue() {
            return value;
        }
    }
}

