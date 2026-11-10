package com.kafkads.protocol.messages;

import com.kafkads.protocol.ApiKey;
import com.kafkads.protocol.Message;
import io.netty.buffer.ByteBuf;

/** Asks a broker for the cluster's brokers and partition assignment. */
public final class MetadataRequest implements Message {

    @Override
    public ApiKey apiKey() {
        return ApiKey.METADATA;
    }

    @Override
    public void encode(ByteBuf out) {
    }

    public static MetadataRequest decode(ByteBuf in) {
        return new MetadataRequest();
    }
}
