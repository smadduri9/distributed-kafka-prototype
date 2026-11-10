package com.kafkads.network;

import com.kafkads.protocol.Frame;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToByteEncoder;

/** Writes {@code apiKey(1) correlationId(4) payload}; the length prefix is added downstream. */
public class MessageEncoder extends MessageToByteEncoder<Frame> {

    @Override
    protected void encode(ChannelHandlerContext ctx, Frame frame, ByteBuf out) {
        out.writeByte(frame.message().apiKey().code());
        out.writeInt(frame.correlationId());
        frame.message().encode(out);
    }
}
