package com.kafkads.protocol.messages;

import com.kafkads.protocol.Acks;
import com.kafkads.protocol.ApiKey;
import com.kafkads.protocol.MalformedMessageException;
import com.kafkads.protocol.Message;
import io.netty.buffer.ByteBuf;

import java.util.ArrayList;
import java.util.List;

/** Appends a batch of values to one partition. The batch is appended whole or not at all. */
public final class ProduceRequest implements Message {

    /** Upper bound on values per request. */
    public static final int MAX_RECORDS = 100_000;

    private final int partition;
    private final Acks acks;
    private final List<byte[]> values;

    public ProduceRequest(int partition, Acks acks, List<byte[]> values) {
        this.partition = partition;
        this.acks = acks;
        this.values = values;
    }

    public int partition() {
        return partition;
    }

    public Acks acks() {
        return acks;
    }

    public List<byte[]> values() {
        return values;
    }

    @Override
    public ApiKey apiKey() {
        return ApiKey.PRODUCE;
    }

    @Override
    public void encode(ByteBuf out) {
        out.writeInt(partition);
        out.writeByte(acks.code());
        out.writeInt(values.size());
        for (byte[] v : values) {
            Message.writeBytes(out, v);
        }
    }

    public static ProduceRequest decode(ByteBuf in) {
        int partition = Message.readInt(in);
        Acks acks;
        try {
            acks = Acks.fromCode(Message.readByte(in));
        } catch (IllegalArgumentException e) {
            throw new MalformedMessageException(e.getMessage());
        }
        int count = Message.readCount(in, 4, MAX_RECORDS);
        List<byte[]> values = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            values.add(Message.readBytes(in));
        }
        return new ProduceRequest(partition, acks, values);
    }
}
