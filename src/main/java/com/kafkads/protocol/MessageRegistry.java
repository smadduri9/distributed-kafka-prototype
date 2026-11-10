package com.kafkads.protocol;

import com.kafkads.protocol.messages.ErrorResponse;
import com.kafkads.protocol.messages.FetchRequest;
import com.kafkads.protocol.messages.FetchResponse;
import com.kafkads.protocol.messages.MetadataRequest;
import com.kafkads.protocol.messages.MetadataResponse;
import com.kafkads.protocol.messages.PingRequest;
import com.kafkads.protocol.messages.PingResponse;
import com.kafkads.protocol.messages.ProduceRequest;
import com.kafkads.protocol.messages.ProduceResponse;
import com.kafkads.raft.messages.AppendEntriesRequest;
import com.kafkads.raft.messages.AppendEntriesResponse;
import com.kafkads.raft.messages.RequestVoteRequest;
import com.kafkads.raft.messages.RequestVoteResponse;
import io.netty.buffer.ByteBuf;

/** Central decode dispatch from api key (and decoding side) to a concrete {@link Message}. */
public final class MessageRegistry {

    private MessageRegistry() {
    }

    public static Message decode(ApiKey apiKey, Side side, ByteBuf payload) {
        switch (apiKey) {
            case PING:
                return side == Side.SERVER ? PingRequest.decode(payload) : PingResponse.decode(payload);
            case PRODUCE:
                return ProduceRequest.decode(payload);
            case PRODUCE_RESP:
                return ProduceResponse.decode(payload);
            case FETCH:
                return FetchRequest.decode(payload);
            case FETCH_RESP:
                return FetchResponse.decode(payload);
            case METADATA:
                return MetadataRequest.decode(payload);
            case METADATA_RESP:
                return MetadataResponse.decode(payload);
            case REQUEST_VOTE:
                return RequestVoteRequest.decode(payload);
            case REQUEST_VOTE_RESP:
                return RequestVoteResponse.decode(payload);
            case APPEND_ENTRIES:
                return AppendEntriesRequest.decode(payload);
            case APPEND_ENTRIES_RESP:
                return AppendEntriesResponse.decode(payload);
            case ERROR:
                return ErrorResponse.decode(payload);
            default:
                throw new MalformedMessageException("No decoder registered for api key " + apiKey);
        }
    }
}
