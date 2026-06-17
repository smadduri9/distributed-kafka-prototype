package com.kafkads.raft.messages;

import com.kafkads.protocol.ApiKey;
import com.kafkads.protocol.Message;
import io.netty.buffer.ByteBuf;

public final class RequestVoteRequest implements Message {

    private final int term;
    private final int candidateId;
    private final long lastLogIndex;
    private final int lastLogTerm;

    public RequestVoteRequest(int term, int candidateId, long lastLogIndex, int lastLogTerm) {
        this.term = term;
        this.candidateId = candidateId;
        this.lastLogIndex = lastLogIndex;
        this.lastLogTerm = lastLogTerm;
    }

    public int term() {
        return term;
    }

    public int candidateId() {
        return candidateId;
    }

    public long lastLogIndex() {
        return lastLogIndex;
    }

    public int lastLogTerm() {
        return lastLogTerm;
    }

    @Override
    public ApiKey apiKey() {
        return ApiKey.REQUEST_VOTE;
    }

    @Override
    public void encode(ByteBuf out) {
        out.writeInt(term);
        out.writeInt(candidateId);
        out.writeLong(lastLogIndex);
        out.writeInt(lastLogTerm);
    }

    public static RequestVoteRequest decode(ByteBuf in) {
        return new RequestVoteRequest(in.readInt(), in.readInt(), in.readLong(), in.readInt());
    }
}
