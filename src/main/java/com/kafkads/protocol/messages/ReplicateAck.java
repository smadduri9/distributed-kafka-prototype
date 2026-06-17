package com.kafkads.protocol.messages;

import com.kafkads.protocol.ApiKey;
import com.kafkads.protocol.Message;
import io.netty.buffer.ByteBuf;

/** Follower -> leader: acknowledges replication, reporting the follower's log end offset. */
public final class ReplicateAck implements Message {

    public static final short OK = 0;
    public static final short ERROR = 1;

    private final int partition;
    private final int followerId;
    private final short errorCode;
    private final long followerLogEndOffset;

    public ReplicateAck(int partition, int followerId, short errorCode, long followerLogEndOffset) {
        this.partition = partition;
        this.followerId = followerId;
        this.errorCode = errorCode;
        this.followerLogEndOffset = followerLogEndOffset;
    }

    public int partition() {
        return partition;
    }

    public int followerId() {
        return followerId;
    }

    public short errorCode() {
        return errorCode;
    }

    public long followerLogEndOffset() {
        return followerLogEndOffset;
    }

    @Override
    public ApiKey apiKey() {
        return ApiKey.REPLICATE_ACK;
    }

    @Override
    public void encode(ByteBuf out) {
        out.writeInt(partition);
        out.writeInt(followerId);
        out.writeShort(errorCode);
        out.writeLong(followerLogEndOffset);
    }

    public static ReplicateAck decode(ByteBuf in) {
        int partition = in.readInt();
        int followerId = in.readInt();
        short errorCode = in.readShort();
        long leo = in.readLong();
        return new ReplicateAck(partition, followerId, errorCode, leo);
    }
}
