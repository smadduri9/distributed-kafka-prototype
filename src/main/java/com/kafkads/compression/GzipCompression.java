package com.kafkads.compression;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

/**
 * Gzip compression codec implementation.
 */
public class GzipCompression implements CompressionCodec {
    
    @Override
    public byte[] compress(byte[] data) throws CompressionException {
        try (ByteArrayOutputStream baos = new ByteArrayOutputStream();
             GZIPOutputStream gzos = new GZIPOutputStream(baos)) {
            gzos.write(data);
            gzos.finish();
            return baos.toByteArray();
        } catch (IOException e) {
            throw new CompressionException("Gzip compression failed", e);
        }
    }
    
    @Override
    public byte[] decompress(byte[] data) throws CompressionException {
        try (ByteArrayInputStream bais = new ByteArrayInputStream(data);
             GZIPInputStream gzis = new GZIPInputStream(bais);
             ByteArrayOutputStream baos = new ByteArrayOutputStream()) {
            byte[] buffer = new byte[8192];
            int len;
            while ((len = gzis.read(buffer)) != -1) {
                baos.write(buffer, 0, len);
            }
            return baos.toByteArray();
        } catch (IOException e) {
            throw new CompressionException("Gzip decompression failed", e);
        }
    }
    
    @Override
    public String getType() {
        return "gzip";
    }
}

