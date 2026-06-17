package com.kafkads.protocol.messages;

import com.kafkads.protocol.ApiKey;
import com.kafkads.protocol.Message;
import com.kafkads.storage.Record;
import io.netty.buffer.ByteBuf;

import java.util.ArrayList;
import java.util.List;

/** Leader -> follower: records to append plus the leader's current high-watermark. */
public final class ReplicateRequest implements Message {

    private final int partition;
    private final int leaderId;
    private final long leaderHighWatermark;
    private final List<Record> records;

    public ReplicateRequest(int partition, int leaderId, long leaderHighWatermark, List<Record> records) {
        this.partition = partition;
        this.leaderId = leaderId;
        this.leaderHighWatermark = leaderHighWatermark;
        this.records = records;
    }

    public int partition() {
        return partition;
    }

    public int leaderId() {
        return leaderId;
    }

    public long leaderHighWatermark() {
        return leaderHighWatermark;
    }

    public List<Record> records() {
        return records;
    }

    @Override
    public ApiKey apiKey() {
        return ApiKey.REPLICATE;
    }

    @Override
    public void encode(ByteBuf out) {
        out.writeInt(partition);
        out.writeInt(leaderId);
        out.writeLong(leaderHighWatermark);
        out.writeInt(records.size());
        for (Record r : records) {
            out.writeLong(r.offset());
            Message.writeBytes(out, r.value());
        }
    }

    public static ReplicateRequest decode(ByteBuf in) {
        int partition = in.readInt();
        int leaderId = in.readInt();
        long hw = in.readLong();
        int count = in.readInt();
        List<Record> records = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            long offset = in.readLong();
            byte[] value = Message.readBytes(in);
            records.add(new Record(offset, value));
        }
        return new ReplicateRequest(partition, leaderId, hw, records);
    }
}
