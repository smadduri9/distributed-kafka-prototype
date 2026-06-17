package com.kafkads.raft.messages;

import com.kafkads.protocol.ApiKey;
import com.kafkads.protocol.Message;
import io.netty.buffer.ByteBuf;

public final class RequestVoteResponse implements Message {

    private final int term;
    private final boolean voteGranted;

    public RequestVoteResponse(int term, boolean voteGranted) {
        this.term = term;
        this.voteGranted = voteGranted;
    }

    public int term() {
        return term;
    }

    public boolean voteGranted() {
        return voteGranted;
    }

    @Override
    public ApiKey apiKey() {
        return ApiKey.REQUEST_VOTE_RESP;
    }

    @Override
    public void encode(ByteBuf out) {
        out.writeInt(term);
        out.writeBoolean(voteGranted);
    }

    public static RequestVoteResponse decode(ByteBuf in) {
        return new RequestVoteResponse(in.readInt(), in.readBoolean());
    }
}
