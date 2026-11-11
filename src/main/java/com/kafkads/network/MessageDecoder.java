package com.kafkads.network;

import com.kafkads.protocol.ApiKey;
import com.kafkads.protocol.Frame;
import com.kafkads.protocol.MalformedMessageException;
import com.kafkads.protocol.Message;
import com.kafkads.protocol.MessageRegistry;
import com.kafkads.protocol.Side;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToMessageDecoder;

import java.util.List;

/**
 * Decodes exactly one {@link Frame} from each de-framed buffer:
 * {@code apiKey(1) correlationId(4) payload}. The whole buffer must be consumed; leftover
 * bytes are a protocol error rather than the start of another message.
 */
public class MessageDecoder extends MessageToMessageDecoder<ByteBuf> {

    private final Side side;

    public MessageDecoder(Side side) {
        this.side = side;
    }

    @Override
    protected void decode(ChannelHandlerContext ctx, ByteBuf in, List<Object> out) {
        if (in.readableBytes() < 5) {
            throw new MalformedMessageException("Frame shorter than its 5-byte header");
        }
        byte keyCode = in.readByte();
        int correlationId = in.readInt();
        try {
            ApiKey apiKey;
            try {
                apiKey = ApiKey.fromCode(keyCode);
            } catch (IllegalArgumentException e) {
                throw new MalformedMessageException(e.getMessage());
            }
            Message message = MessageRegistry.decode(apiKey, side, in);
            if (in.isReadable()) {
                throw new MalformedMessageException(in.readableBytes() + " unexpected trailing bytes in "
                        + apiKey + " frame");
            }
            out.add(new Frame(correlationId, message));
        } catch (MalformedMessageException e) {
            throw new MalformedMessageException(e.getMessage(), correlationId);
        } catch (IndexOutOfBoundsException | IllegalArgumentException e) {
            throw new MalformedMessageException("Undecodable frame: " + e.getMessage(), correlationId);
        }
    }
}
