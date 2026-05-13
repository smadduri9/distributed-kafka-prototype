package com.kafkads.compression;

import org.xerial.snappy.Snappy;

import java.io.IOException;

/**
 * Snappy compression codec implementation.
 */
public class SnappyCompression implements CompressionCodec {
    
    @Override
    public byte[] compress(byte[] data) throws CompressionException {
        try {
            return Snappy.compress(data);
        } catch (IOException e) {
            throw new CompressionException("Snappy compression failed", e);
        }
    }
    
    @Override
    public byte[] decompress(byte[] data) throws CompressionException {
        try {
            return Snappy.uncompress(data);
        } catch (IOException e) {
            throw new CompressionException("Snappy decompression failed", e);
        }
    }
    
    @Override
    public String getType() {
        return "snappy";
    }
}

