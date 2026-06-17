package com.kafkads.protocol.messages;

import com.kafkads.protocol.ApiKey;
import com.kafkads.protocol.Message;
import io.netty.buffer.ByteBuf;

public final class ProduceResponse implements Message {

    public static final short OK = 0;
    public static final short NOT_LEADER = 1;
    public static final short ERROR = 2;
    public static final short NOT_ENOUGH_REPLICAS = 3;

    private final short errorCode;
    private final long baseOffset;

    public ProduceResponse(short errorCode, long baseOffset) {
        this.errorCode = errorCode;
        this.baseOffset = baseOffset;
    }

    public short errorCode() {
        return errorCode;
    }

    public long baseOffset() {
        return baseOffset;
    }

    public boolean isOk() {
        return errorCode == OK;
    }

    @Override
    public ApiKey apiKey() {
        return ApiKey.PRODUCE_RESP;
    }

    @Override
    public void encode(ByteBuf out) {
        out.writeShort(errorCode);
        out.writeLong(baseOffset);
    }

    public static ProduceResponse decode(ByteBuf in) {
        short errorCode = in.readShort();
        long baseOffset = in.readLong();
        return new ProduceResponse(errorCode, baseOffset);
    }
}
