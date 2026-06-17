package com.kafkads.network;

import com.kafkads.protocol.Message;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToByteEncoder;

/**
 * Writes [api-key byte][payload]. The 4-byte length prefix is added by a
 * LengthFieldPrepender placed earlier in the pipeline (toward the head).
 */
public class MessageEncoder extends MessageToByteEncoder<Message> {

    @Override
    protected void encode(ChannelHandlerContext ctx, Message msg, ByteBuf out) {
        out.writeByte(msg.apiKey().code());
        msg.encode(out);
    }
}
