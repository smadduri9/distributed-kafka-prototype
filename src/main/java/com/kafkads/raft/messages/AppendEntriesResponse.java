package com.kafkads.raft.messages;

import com.kafkads.protocol.ApiKey;
import com.kafkads.protocol.Message;
import io.netty.buffer.ByteBuf;

public final class AppendEntriesResponse implements Message {

    private final int term;
    private final boolean success;
    private final long matchIndex;

    public AppendEntriesResponse(int term, boolean success, long matchIndex) {
        this.term = term;
        this.success = success;
        this.matchIndex = matchIndex;
    }

    public int term() {
        return term;
    }

    public boolean success() {
        return success;
    }

    public long matchIndex() {
        return matchIndex;
    }

    @Override
    public ApiKey apiKey() {
        return ApiKey.APPEND_ENTRIES_RESP;
    }

    @Override
    public void encode(ByteBuf out) {
        out.writeInt(term);
        out.writeBoolean(success);
        out.writeLong(matchIndex);
    }

    public static AppendEntriesResponse decode(ByteBuf in) {
        return new AppendEntriesResponse(in.readInt(), in.readBoolean(), in.readLong());
    }
}
