package com.kafkads.raft;

import com.kafkads.raft.messages.AppendEntriesRequest;
import com.kafkads.raft.messages.AppendEntriesResponse;
import com.kafkads.raft.messages.RequestVoteRequest;
import com.kafkads.raft.messages.RequestVoteResponse;

import java.util.concurrent.CompletableFuture;

/** Sends Raft RPCs to peers. Implementations may be in-memory (tests) or networked. */
public interface RaftTransport {

    CompletableFuture<RequestVoteResponse> requestVote(int peerId, RequestVoteRequest request);

    CompletableFuture<AppendEntriesResponse> appendEntries(int peerId, AppendEntriesRequest request);
}
