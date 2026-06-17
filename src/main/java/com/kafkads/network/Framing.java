package com.kafkads.network;

import com.kafkads.protocol.Side;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelPipeline;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;
import io.netty.handler.codec.LengthFieldPrepender;

/**
 * Shared pipeline wiring for the length-prefixed binary protocol.
 *
 * Wire frame: [4-byte length][1-byte api key][payload].
 */
public final class Framing {

    public static final int MAX_FRAME_BYTES = 64 * 1024 * 1024;
    public static final int LENGTH_FIELD_BYTES = 4;

    private Framing() {
    }

    /**
     * Installs the framing + codec handlers, then the business handler last.
     */
    public static void configure(ChannelPipeline pipeline, Side side, ChannelHandler businessHandler) {
        pipeline.addLast(new LengthFieldBasedFrameDecoder(
                MAX_FRAME_BYTES, 0, LENGTH_FIELD_BYTES, 0, LENGTH_FIELD_BYTES));
        pipeline.addLast(new LengthFieldPrepender(LENGTH_FIELD_BYTES));
        pipeline.addLast(new MessageDecoder(side));
        pipeline.addLast(new MessageEncoder());
        pipeline.addLast(businessHandler);
    }
}
