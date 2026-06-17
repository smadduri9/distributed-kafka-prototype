package com.kafkads.protocol.messages;

import com.kafkads.protocol.Acks;
import com.kafkads.protocol.ApiKey;
import com.kafkads.protocol.Message;
import io.netty.buffer.ByteBuf;

import java.util.ArrayList;
import java.util.List;

public final class ProduceRequest implements Message {

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
        int partition = in.readInt();
        Acks acks = Acks.fromCode(in.readByte());
        int count = in.readInt();
        List<byte[]> values = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            values.add(Message.readBytes(in));
        }
        return new ProduceRequest(partition, acks, values);
    }
}
