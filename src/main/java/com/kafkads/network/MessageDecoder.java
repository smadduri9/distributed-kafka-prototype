package com.kafkads.network;

import com.kafkads.protocol.ApiKey;
import com.kafkads.protocol.Message;
import com.kafkads.protocol.MessageRegistry;
import com.kafkads.protocol.Side;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.ByteToMessageDecoder;

import java.util.List;

/**
 * Reads a single de-framed message: [api-key byte][payload]. The 4-byte length
 * prefix has already been stripped by a LengthFieldBasedFrameDecoder earlier in
 * the pipeline.
 */
public class MessageDecoder extends ByteToMessageDecoder {

    private final Side side;

    public MessageDecoder(Side side) {
        this.side = side;
    }

    @Override
    protected void decode(ChannelHandlerContext ctx, ByteBuf in, List<Object> out) {
        if (!in.isReadable()) {
            return;
        }
        ApiKey apiKey = ApiKey.fromCode(in.readByte());
        Message message = MessageRegistry.decode(apiKey, side, in);
        out.add(message);
    }
}
