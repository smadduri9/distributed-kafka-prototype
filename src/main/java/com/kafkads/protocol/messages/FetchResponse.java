package com.kafkads.protocol.messages;

import com.kafkads.protocol.ApiKey;
import com.kafkads.protocol.ErrorCode;
import com.kafkads.protocol.Message;
import com.kafkads.storage.Record;
import io.netty.buffer.ByteBuf;

import java.util.ArrayList;
import java.util.List;

/**
 * Committed records from one partition.
 *
 * <p>{@code highWatermark} is the exclusive upper bound of committed offsets.
 * {@code nextOffset} is where the consumer should continue; it can advance past
 * offsets that hold no client data (leader no-op entries), so a consumer must use it
 * rather than assuming offsets are dense.
 */
public final class FetchResponse implements Message {

    public static final short OK = ErrorCode.OK;
    public static final short NOT_LEADER = ErrorCode.NOT_LEADER;
    public static final short ERROR = ErrorCode.ERROR;

    private final short errorCode;
    private final long highWatermark;
    private final long nextOffset;
    private final int leaderId;
    private final List<Record> records;

    public FetchResponse(short errorCode, long highWatermark, long nextOffset, int leaderId,
                         List<Record> records) {
        this.errorCode = errorCode;
        this.highWatermark = highWatermark;
        this.nextOffset = nextOffset;
        this.leaderId = leaderId;
        this.records = records;
    }

    public short errorCode() {
        return errorCode;
    }

    public long highWatermark() {
        return highWatermark;
    }

    public long nextOffset() {
        return nextOffset;
    }

    public int leaderId() {
        return leaderId;
    }

    public List<Record> records() {
        return records;
    }

    @Override
    public ApiKey apiKey() {
        return ApiKey.FETCH_RESP;
    }

    @Override
    public void encode(ByteBuf out) {
        out.writeShort(errorCode);
        out.writeLong(highWatermark);
        out.writeLong(nextOffset);
        out.writeInt(leaderId);
        out.writeInt(records.size());
        for (Record r : records) {
            out.writeLong(r.offset());
            Message.writeBytes(out, r.value());
        }
    }

    public static FetchResponse decode(ByteBuf in) {
        short errorCode = Message.readShort(in);
        long highWatermark = Message.readLong(in);
        long nextOffset = Message.readLong(in);
        int leaderId = Message.readInt(in);
        int count = Message.readCount(in, 12, Integer.MAX_VALUE);
        List<Record> records = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            long offset = Message.readLong(in);
            records.add(new Record(offset, Message.readBytes(in)));
        }
        return new FetchResponse(errorCode, highWatermark, nextOffset, leaderId, records);
    }
}
