package com.kafkads.protocol.messages;

import com.kafkads.protocol.ApiKey;
import com.kafkads.protocol.ErrorCode;
import com.kafkads.protocol.Message;
import io.netty.buffer.ByteBuf;

/**
 * Result of a produce request. {@code baseOffset} is the offset of the first record of
 * the batch when the records were appended (OK and the "uncertain" codes), else -1.
 * {@code leaderId} is the responding broker's view of the partition leader, or -1.
 */
public final class ProduceResponse implements Message {

    public static final short OK = ErrorCode.OK;
    public static final short NOT_LEADER = ErrorCode.NOT_LEADER;
    public static final short ERROR = ErrorCode.ERROR;
    public static final short NOT_ENOUGH_REPLICAS = ErrorCode.NOT_ENOUGH_REPLICAS;

    private final short errorCode;
    private final long baseOffset;
    private final int leaderId;

    public ProduceResponse(short errorCode, long baseOffset, int leaderId) {
        this.errorCode = errorCode;
        this.baseOffset = baseOffset;
        this.leaderId = leaderId;
    }

    public short errorCode() {
        return errorCode;
    }

    public boolean isOk() {
        return errorCode == OK;
    }

    public long baseOffset() {
        return baseOffset;
    }

    public int leaderId() {
        return leaderId;
    }

    @Override
    public ApiKey apiKey() {
        return ApiKey.PRODUCE_RESP;
    }

    @Override
    public void encode(ByteBuf out) {
        out.writeShort(errorCode);
        out.writeLong(baseOffset);
        out.writeInt(leaderId);
    }

    public static ProduceResponse decode(ByteBuf in) {
        return new ProduceResponse(Message.readShort(in), Message.readLong(in), Message.readInt(in));
    }

    @Override
    public String toString() {
        return "ProduceResponse{" + ErrorCode.name(errorCode) + ", baseOffset=" + baseOffset
                + ", leaderId=" + leaderId + "}";
    }
}
