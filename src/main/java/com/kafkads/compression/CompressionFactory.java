package com.kafkads.compression;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Factory for creating compression codecs.
 */
public class CompressionFactory {
    private static final Logger logger = LoggerFactory.getLogger(CompressionFactory.class);
    
    /**
     * Creates a compression codec by type name.
     */
    public static CompressionCodec create(String type) {
        if (type == null || type.equalsIgnoreCase("none")) {
            return new NoOpCompression();
        }
        
        switch (type.toLowerCase()) {
            case "gzip":
                return new GzipCompression();
            case "snappy":
                return new SnappyCompression();
            default:
                logger.warn("Unknown compression type: {}, using none", type);
                return new NoOpCompression();
        }
    }
    
    /**
     * No-op compression codec (pass-through).
     */
    private static class NoOpCompression implements CompressionCodec {
        @Override
        public byte[] compress(byte[] data) throws CompressionException {
            return data;
        }
        
        @Override
        public byte[] decompress(byte[] data) throws CompressionException {
            return data;
        }
        
        @Override
        public String getType() {
            return "none";
        }
    }
}

