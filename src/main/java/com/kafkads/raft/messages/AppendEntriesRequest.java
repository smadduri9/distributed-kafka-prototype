package com.kafkads.raft.messages;

import com.kafkads.protocol.ApiKey;
import com.kafkads.protocol.Message;
import com.kafkads.raft.LogEntry;
import io.netty.buffer.ByteBuf;

import java.util.ArrayList;
import java.util.List;

public final class AppendEntriesRequest implements Message {

    private final int term;
    private final int leaderId;
    private final long prevLogIndex;
    private final int prevLogTerm;
    private final List<LogEntry> entries;
    private final long leaderCommit;

    public AppendEntriesRequest(int term, int leaderId, long prevLogIndex, int prevLogTerm,
                                List<LogEntry> entries, long leaderCommit) {
        this.term = term;
        this.leaderId = leaderId;
        this.prevLogIndex = prevLogIndex;
        this.prevLogTerm = prevLogTerm;
        this.entries = entries;
        this.leaderCommit = leaderCommit;
    }

    public int term() {
        return term;
    }

    public int leaderId() {
        return leaderId;
    }

    public long prevLogIndex() {
        return prevLogIndex;
    }

    public int prevLogTerm() {
        return prevLogTerm;
    }

    public List<LogEntry> entries() {
        return entries;
    }

    public long leaderCommit() {
        return leaderCommit;
    }

    @Override
    public ApiKey apiKey() {
        return ApiKey.APPEND_ENTRIES;
    }

    @Override
    public void encode(ByteBuf out) {
        out.writeInt(term);
        out.writeInt(leaderId);
        out.writeLong(prevLogIndex);
        out.writeInt(prevLogTerm);
        out.writeLong(leaderCommit);
        out.writeInt(entries.size());
        for (LogEntry e : entries) {
            out.writeLong(e.index());
            out.writeInt(e.term());
            Message.writeBytes(out, e.command());
        }
    }

    public static AppendEntriesRequest decode(ByteBuf in) {
        int term = in.readInt();
        int leaderId = in.readInt();
        long prevLogIndex = in.readLong();
        int prevLogTerm = in.readInt();
        long leaderCommit = in.readLong();
        int count = in.readInt();
        List<LogEntry> entries = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            long index = in.readLong();
            int eterm = in.readInt();
            byte[] command = Message.readBytes(in);
            entries.add(new LogEntry(index, eterm, command));
        }
        return new AppendEntriesRequest(term, leaderId, prevLogIndex, prevLogTerm, entries, leaderCommit);
    }
}
