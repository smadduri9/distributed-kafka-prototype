package com.kafkads.protocol.messages;

import com.kafkads.protocol.ApiKey;
import com.kafkads.protocol.Message;
import io.netty.buffer.ByteBuf;

/** Reads committed records of one partition starting at an explicit offset. */
public final class FetchRequest implements Message {

    private final int partition;
    private final long fromOffset;
    private final int maxBytes;

    public FetchRequest(int partition, long fromOffset, int maxBytes) {
        this.partition = partition;
        this.fromOffset = fromOffset;
        this.maxBytes = maxBytes;
    }

    public int partition() {
        return partition;
    }

    public long fromOffset() {
        return fromOffset;
    }

    public int maxBytes() {
        return maxBytes;
    }

    @Override
    public ApiKey apiKey() {
        return ApiKey.FETCH;
    }

    @Override
    public void encode(ByteBuf out) {
        out.writeInt(partition);
        out.writeLong(fromOffset);
        out.writeInt(maxBytes);
    }

    public static FetchRequest decode(ByteBuf in) {
        return new FetchRequest(Message.readInt(in), Message.readLong(in), Message.readInt(in));
    }
}
