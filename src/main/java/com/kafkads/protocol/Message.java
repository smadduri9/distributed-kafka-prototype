package com.kafkads.protocol;

import io.netty.buffer.ByteBuf;

import java.nio.charset.StandardCharsets;

/**
 * A protocol message that can identify its api key and serialize its payload.
 * Framing, the api-key byte and the correlation id are written by the network layer;
 * implementations only encode and decode their own payload.
 *
 * <p>The static readers validate every length and count against the bytes actually
 * remaining in the frame <em>before</em> allocating, so a small malicious frame cannot
 * request a large allocation.
 */
public interface Message {

    ApiKey apiKey();

    void encode(ByteBuf out);

    static void writeBytes(ByteBuf out, byte[] value) {
        out.writeInt(value.length);
        out.writeBytes(value);
    }

    /** Reads a length-prefixed byte array; the length must fit in the rest of the frame. */
    static byte[] readBytes(ByteBuf in) {
        int len = readInt(in);
        if (len < 0 || len > in.readableBytes()) {
            throw new MalformedMessageException("Byte array length " + len + " exceeds remaining "
                    + in.readableBytes() + " bytes");
        }
        byte[] value = new byte[len];
        in.readBytes(value);
        return value;
    }

    /**
     * Reads an element count and checks that {@code count} elements of at least
     * {@code minElementBytes} each could fit in the rest of the frame.
     */
    static int readCount(ByteBuf in, int minElementBytes, int maxCount) {
        int count = readInt(in);
        if (count < 0 || count > maxCount || (long) count * minElementBytes > in.readableBytes()) {
            throw new MalformedMessageException("Element count " + count + " is not possible in "
                    + in.readableBytes() + " remaining bytes");
        }
        return count;
    }

    static void writeString(ByteBuf out, String s) {
        writeBytes(out, s.getBytes(StandardCharsets.UTF_8));
    }

    static String readString(ByteBuf in) {
        return new String(readBytes(in), StandardCharsets.UTF_8);
    }

    static void require(ByteBuf in, int bytes) {
        if (in.readableBytes() < bytes) {
            throw new MalformedMessageException("Message truncated: need " + bytes + " bytes, have "
                    + in.readableBytes());
        }
    }

    static byte readByte(ByteBuf in) {
        require(in, 1);
        return in.readByte();
    }

    static short readShort(ByteBuf in) {
        require(in, 2);
        return in.readShort();
    }

    static int readInt(ByteBuf in) {
        require(in, 4);
        return in.readInt();
    }

    static long readLong(ByteBuf in) {
        require(in, 8);
        return in.readLong();
    }
}
