package com.kafkads.raft.messages;

import com.kafkads.protocol.ApiKey;
import com.kafkads.protocol.Message;
import com.kafkads.raft.LogEntry;
import io.netty.buffer.ByteBuf;

import java.util.ArrayList;
import java.util.List;

/** Raft AppendEntries. {@code groupId} is the partition whose Raft group this belongs to. */
public final class AppendEntriesRequest implements Message {

    private final int groupId;
    private final int term;
    private final int leaderId;
    private final long prevLogIndex;
    private final int prevLogTerm;
    private final List<LogEntry> entries;
    private final long leaderCommit;

    public AppendEntriesRequest(int groupId, int term, int leaderId, long prevLogIndex, int prevLogTerm,
                                List<LogEntry> entries, long leaderCommit) {
        this.groupId = groupId;
        this.term = term;
        this.leaderId = leaderId;
        this.prevLogIndex = prevLogIndex;
        this.prevLogTerm = prevLogTerm;
        this.entries = entries;
        this.leaderCommit = leaderCommit;
    }

    public int groupId() {
        return groupId;
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
        out.writeInt(groupId);
        out.writeInt(term);
        out.writeInt(leaderId);
        out.writeLong(prevLogIndex);
        out.writeInt(prevLogTerm);
        out.writeLong(leaderCommit);
        out.writeInt(entries.size());
        for (LogEntry e : entries) {
            out.writeLong(e.index());
            out.writeInt(e.term());
            out.writeByte(e.type());
            Message.writeBytes(out, e.command());
        }
    }

    public static AppendEntriesRequest decode(ByteBuf in) {
        int groupId = Message.readInt(in);
        int term = Message.readInt(in);
        int leaderId = Message.readInt(in);
        long prevLogIndex = Message.readLong(in);
        int prevLogTerm = Message.readInt(in);
        long leaderCommit = Message.readLong(in);
        int count = Message.readCount(in, 17, Integer.MAX_VALUE);
        List<LogEntry> entries = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            long index = Message.readLong(in);
            int eterm = Message.readInt(in);
            byte type = Message.readByte(in);
            entries.add(new LogEntry(index, eterm, type, Message.readBytes(in)));
        }
        return new AppendEntriesRequest(groupId, term, leaderId, prevLogIndex, prevLogTerm, entries, leaderCommit);
    }
}
