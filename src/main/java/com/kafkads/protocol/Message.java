package com.kafkads.protocol;

import io.netty.buffer.ByteBuf;

/**
 * A protocol message that can identify its api key and serialize its payload.
 * The framing (4-byte length) and api-key byte are written by the network layer;
 * implementations only encode/decode their own payload.
 */
public interface Message {

    ApiKey apiKey();

    void encode(ByteBuf out);

    /** Helper to write a length-prefixed byte array. */
    static void writeBytes(ByteBuf out, byte[] value) {
        if (value == null) {
            out.writeInt(-1);
            return;
        }
        out.writeInt(value.length);
        out.writeBytes(value);
    }

    /** Helper to read a length-prefixed byte array written by {@link #writeBytes}. */
    static byte[] readBytes(ByteBuf in) {
        int len = in.readInt();
        if (len < 0) {
            return null;
        }
        byte[] value = new byte[len];
        in.readBytes(value);
        return value;
    }
}
