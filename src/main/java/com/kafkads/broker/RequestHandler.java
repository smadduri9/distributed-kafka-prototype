package com.kafkads.broker;

import com.kafkads.protocol.Message;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;

/**
 * Per-connection server handler. Delegates each decoded request to the owning
 * {@link Broker}, which produces a response message that is written back.
 */
public class RequestHandler extends SimpleChannelInboundHandler<Message> {

    private final Broker broker;

    public RequestHandler(Broker broker) {
        this.broker = broker;
    }

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, Message request) {
        try {
            broker.handle(request).whenComplete((response, error) -> {
                if (response != null) {
                    ctx.writeAndFlush(response);
                } else {
                    ctx.close();
                }
            });
        } catch (Exception e) {
            ctx.close();
        }
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        ctx.close();
    }
}
