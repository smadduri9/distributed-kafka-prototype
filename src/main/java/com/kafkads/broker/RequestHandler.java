package com.kafkads.broker;

import com.kafkads.protocol.ErrorCode;
import com.kafkads.protocol.Frame;
import com.kafkads.protocol.MalformedMessageException;
import com.kafkads.protocol.messages.ErrorResponse;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.codec.TooLongFrameException;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * Per-connection server handler. It hands each decoded request to the {@link Broker},
 * which answers asynchronously, and writes the response back under the request's
 * correlation id. Responses are written as they complete, in any order.
 *
 * <p>A connection may have at most {@link #MAX_IN_FLIGHT} unanswered requests; further
 * requests are answered with {@code BUSY}. A malformed or oversized frame gets an error
 * response where possible and the connection is then closed, because the byte stream can
 * no longer be trusted.
 */
public class RequestHandler extends SimpleChannelInboundHandler<Frame> {

    public static final int MAX_IN_FLIGHT = 1024;

    private final Broker broker;
    private final AtomicInteger inFlight = new AtomicInteger();

    public RequestHandler(Broker broker) {
        this.broker = broker;
    }

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, Frame frame) {
        int id = frame.correlationId();
        if (inFlight.incrementAndGet() > MAX_IN_FLIGHT) {
            inFlight.decrementAndGet();
            ctx.writeAndFlush(new Frame(id, new ErrorResponse(ErrorCode.BUSY, "too many requests in flight")));
            return;
        }
        try {
            broker.handle(frame.message()).whenComplete((response, error) -> {
                inFlight.decrementAndGet();
                if (response != null) {
                    ctx.writeAndFlush(new Frame(id, response));
                } else {
                    ctx.writeAndFlush(new Frame(id, new ErrorResponse(ErrorCode.ERROR, String.valueOf(error))));
                }
            });
        } catch (RuntimeException e) {
            inFlight.decrementAndGet();
            ctx.writeAndFlush(new Frame(id, new ErrorResponse(ErrorCode.ERROR, String.valueOf(e))));
        }
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        Throwable root = cause;
        while (root.getCause() != null && !(root instanceof MalformedMessageException)
                && !(root instanceof TooLongFrameException)) {
            root = root.getCause();
        }
        if (root instanceof MalformedMessageException malformed) {
            int id = malformed.correlationId() == null ? 0 : malformed.correlationId();
            ctx.writeAndFlush(new Frame(id, new ErrorResponse(ErrorCode.INVALID_REQUEST, malformed.getMessage())))
                    .addListener(ChannelFutureListener.CLOSE);
        } else if (root instanceof TooLongFrameException) {
            ctx.writeAndFlush(new Frame(0, new ErrorResponse(ErrorCode.INVALID_REQUEST, "frame too large")))
                    .addListener(ChannelFutureListener.CLOSE);
        } else {
            ctx.close();
        }
    }
}
