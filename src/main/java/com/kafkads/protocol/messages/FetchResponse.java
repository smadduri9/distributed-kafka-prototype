package com.kafkads.protocol.messages;

import com.kafkads.protocol.ApiKey;
import com.kafkads.protocol.Message;
import com.kafkads.storage.Record;
import io.netty.buffer.ByteBuf;

import java.util.ArrayList;
import java.util.List;

public final class FetchResponse implements Message {

    public static final short OK = 0;
    public static final short NOT_LEADER = 1;
    public static final short ERROR = 2;

    private final short errorCode;
    private final long highWatermark;
    private final List<Record> records;

    public FetchResponse(short errorCode, long highWatermark, List<Record> records) {
        this.errorCode = errorCode;
        this.highWatermark = highWatermark;
        this.records = records;
    }

    public short errorCode() {
        return errorCode;
    }

    public long highWatermark() {
        return highWatermark;
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
        out.writeInt(records.size());
        for (Record r : records) {
            out.writeLong(r.offset());
            Message.writeBytes(out, r.value());
        }
    }

    public static FetchResponse decode(ByteBuf in) {
        short errorCode = in.readShort();
        long highWatermark = in.readLong();
        int count = in.readInt();
        List<Record> records = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            long offset = in.readLong();
            byte[] value = Message.readBytes(in);
            records.add(new Record(offset, value));
        }
        return new FetchResponse(errorCode, highWatermark, records);
    }
}
