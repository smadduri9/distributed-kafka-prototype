package com.kafkads.network;

import com.kafkads.protocol.Side;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelPipeline;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;
import io.netty.handler.codec.LengthFieldPrepender;

/**
 * Shared pipeline wiring for the length-prefixed binary protocol.
 *
 * <pre>frame := length(4) apiKey(1) correlationId(4) payload</pre>
 *
 * {@code length} counts everything after itself. Netty's length-field decoder
 * reassembles frames from partial or coalesced TCP reads and rejects frames larger than
 * {@link #MAX_FRAME_BYTES} before buffering them.
 */
public final class Framing {

    public static final int MAX_FRAME_BYTES = 16 * 1024 * 1024;
    public static final int LENGTH_FIELD_BYTES = 4;

    private Framing() {
    }

    /** Installs the framing and codec handlers, then the business handler last. */
    public static void configure(ChannelPipeline pipeline, Side side, ChannelHandler businessHandler) {
        pipeline.addLast(new LengthFieldBasedFrameDecoder(
                MAX_FRAME_BYTES, 0, LENGTH_FIELD_BYTES, 0, LENGTH_FIELD_BYTES));
        pipeline.addLast(new LengthFieldPrepender(LENGTH_FIELD_BYTES));
        pipeline.addLast(new MessageDecoder(side));
        pipeline.addLast(new MessageEncoder());
        pipeline.addLast(businessHandler);
    }
}
