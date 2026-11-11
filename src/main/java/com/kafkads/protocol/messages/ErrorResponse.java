package com.kafkads.protocol.messages;

import com.kafkads.protocol.ApiKey;
import com.kafkads.protocol.Message;
import io.netty.buffer.ByteBuf;

/** Generic failure response, used when a request could not be decoded or dispatched. */
public final class ErrorResponse implements Message {

    private static final int MAX_TEXT = 512;

    private final short errorCode;
    private final String text;

    public ErrorResponse(short errorCode, String text) {
        this.errorCode = errorCode;
        String t = text == null ? "" : text;
        this.text = t.length() > MAX_TEXT ? t.substring(0, MAX_TEXT) : t;
    }

    public short errorCode() {
        return errorCode;
    }

    public String text() {
        return text;
    }

    @Override
    public ApiKey apiKey() {
        return ApiKey.ERROR;
    }

    @Override
    public void encode(ByteBuf out) {
        out.writeShort(errorCode);
        Message.writeString(out, text);
    }

    public static ErrorResponse decode(ByteBuf in) {
        return new ErrorResponse(Message.readShort(in), Message.readString(in));
    }

    @Override
    public String toString() {
        return "ErrorResponse{" + com.kafkads.protocol.ErrorCode.name(errorCode) + ": " + text + "}";
    }
}
