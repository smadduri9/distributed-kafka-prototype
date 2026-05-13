package com.kafkads.compression;

/**
 * Interface for compression codecs.
 */
public interface CompressionCodec {
    /**
     * Compresses data.
     */
    byte[] compress(byte[] data) throws CompressionException;
    
    /**
     * Decompresses data.
     */
    byte[] decompress(byte[] data) throws CompressionException;
    
    /**
     * Gets the compression type name.
     */
    String getType();
    
    /**
     * Exception thrown during compression/decompression.
     */
    class CompressionException extends Exception {
        public CompressionException(String message) {
            super(message);
        }
        
        public CompressionException(String message, Throwable cause) {
            super(message, cause);
        }
    }
}

