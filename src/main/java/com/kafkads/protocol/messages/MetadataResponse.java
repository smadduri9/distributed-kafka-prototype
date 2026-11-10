package com.kafkads.protocol.messages;

import com.kafkads.protocol.ApiKey;
import com.kafkads.protocol.Message;
import io.netty.buffer.ByteBuf;

import java.util.ArrayList;
import java.util.List;

/**
 * The static cluster layout plus the answering broker's current view of each partition.
 *
 * <p>{@code replicas} and {@code preferredLeader} come from the partition assignment and
 * are the same on every broker. {@code leader}, {@code term}, {@code logEndOffset} and
 * {@code highWatermark} are this broker's local view and are -1 for partitions it does
 * not host.
 */
public final class MetadataResponse implements Message {

    public record BrokerInfo(int id, String host, int port) {
    }

    public record PartitionInfo(int partition, List<Integer> replicas, int preferredLeader,
                                int leader, int term, long logEndOffset, long highWatermark) {
    }

    private static final int MAX_ITEMS = 100_000;

    private final int respondingBroker;
    private final List<BrokerInfo> brokers;
    private final List<PartitionInfo> partitions;

    public MetadataResponse(int respondingBroker, List<BrokerInfo> brokers, List<PartitionInfo> partitions) {
        this.respondingBroker = respondingBroker;
        this.brokers = brokers;
        this.partitions = partitions;
    }

    public int respondingBroker() {
        return respondingBroker;
    }

    public List<BrokerInfo> brokers() {
        return brokers;
    }

    public List<PartitionInfo> partitions() {
        return partitions;
    }

    @Override
    public ApiKey apiKey() {
        return ApiKey.METADATA_RESP;
    }

    @Override
    public void encode(ByteBuf out) {
        out.writeInt(respondingBroker);
        out.writeInt(brokers.size());
        for (BrokerInfo b : brokers) {
            out.writeInt(b.id());
            Message.writeString(out, b.host());
            out.writeInt(b.port());
        }
        out.writeInt(partitions.size());
        for (PartitionInfo p : partitions) {
            out.writeInt(p.partition());
            out.writeInt(p.replicas().size());
            for (int r : p.replicas()) {
                out.writeInt(r);
            }
            out.writeInt(p.preferredLeader());
            out.writeInt(p.leader());
            out.writeInt(p.term());
            out.writeLong(p.logEndOffset());
            out.writeLong(p.highWatermark());
        }
    }

    public static MetadataResponse decode(ByteBuf in) {
        int responding = Message.readInt(in);
        int brokerCount = Message.readCount(in, 12, MAX_ITEMS);
        List<BrokerInfo> brokers = new ArrayList<>(brokerCount);
        for (int i = 0; i < brokerCount; i++) {
            brokers.add(new BrokerInfo(Message.readInt(in), Message.readString(in), Message.readInt(in)));
        }
        int partitionCount = Message.readCount(in, 36, MAX_ITEMS);
        List<PartitionInfo> partitions = new ArrayList<>(partitionCount);
        for (int i = 0; i < partitionCount; i++) {
            int partition = Message.readInt(in);
            int replicaCount = Message.readCount(in, 4, MAX_ITEMS);
            List<Integer> replicas = new ArrayList<>(replicaCount);
            for (int r = 0; r < replicaCount; r++) {
                replicas.add(Message.readInt(in));
            }
            partitions.add(new PartitionInfo(partition, replicas, Message.readInt(in), Message.readInt(in),
                    Message.readInt(in), Message.readLong(in), Message.readLong(in)));
        }
        return new MetadataResponse(responding, brokers, partitions);
    }
}
