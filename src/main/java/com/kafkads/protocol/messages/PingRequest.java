package com.kafkads.protocol.messages;

import com.kafkads.protocol.ApiKey;
import com.kafkads.protocol.Message;
import io.netty.buffer.ByteBuf;

public final class PingRequest implements Message {

    private final long nonce;

    public PingRequest(long nonce) {
        this.nonce = nonce;
    }

    public long nonce() {
        return nonce;
    }

    @Override
    public ApiKey apiKey() {
        return ApiKey.PING;
    }

    @Override
    public void encode(ByteBuf out) {
        out.writeLong(nonce);
    }

    public static PingRequest decode(ByteBuf in) {
        return new PingRequest(Message.readLong(in));
    }
}
