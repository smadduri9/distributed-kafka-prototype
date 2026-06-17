package com.kafkads.raft;

import com.kafkads.raft.messages.AppendEntriesRequest;
import com.kafkads.raft.messages.AppendEntriesResponse;
import com.kafkads.raft.messages.RequestVoteRequest;
import com.kafkads.raft.messages.RequestVoteResponse;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.Consumer;

/**
 * A single Raft consensus node, following the Raft paper (Ongaro &amp; Ousterhout).
 * The logic is transport- and clock-agnostic: timeouts are driven externally via
 * {@link #tick(long)} (or {@link #startElection()} / {@link #broadcastHeartbeat()}
 * directly in tests), and RPCs go through a pluggable {@link RaftTransport}. This makes
 * the protocol deterministically testable in-memory while reusing the same code over
 * the network.
 *
 * All public methods are synchronized on this node, so RPC handlers, timer ticks, and
 * response callbacks never interleave.
 */
public final class RaftNode {

    private final int id;
    private final Set<Integer> peers;
    private final int clusterSize;
    private final RaftTransport transport;
    private final RaftPersistence persistence;
    private final Consumer<byte[]> applyCommand;
    private final int electionTimeoutMinMs;
    private final int electionTimeoutMaxMs;
    private final int heartbeatIntervalMs;

    // Persistent state.
    private int currentTerm;
    private Integer votedFor;
    private final List<LogEntry> log = new ArrayList<>();

    // Volatile state.
    private RaftRole role = RaftRole.FOLLOWER;
    private int leaderId = -1;
    private long commitIndex = 0;
    private long lastApplied = 0;

    // Candidate state.
    private int votesGranted = 0;

    // Leader state.
    private final Map<Integer, Long> nextIndex = new HashMap<>();
    private final Map<Integer, Long> matchIndex = new HashMap<>();

    // Timing.
    private long electionDeadline = Long.MAX_VALUE;
    private long nextHeartbeat = 0;

    public RaftNode(int id, Set<Integer> peers, RaftTransport transport, RaftPersistence persistence,
                    Consumer<byte[]> applyCommand,
                    int electionTimeoutMinMs, int electionTimeoutMaxMs, int heartbeatIntervalMs) {
        this.id = id;
        this.peers = Set.copyOf(peers);
        this.clusterSize = peers.size() + 1;
        this.transport = transport;
        this.persistence = persistence;
        this.applyCommand = applyCommand;
        this.electionTimeoutMinMs = electionTimeoutMinMs;
        this.electionTimeoutMaxMs = electionTimeoutMaxMs;
        this.heartbeatIntervalMs = heartbeatIntervalMs;
        try {
            RaftPersistence.State state = persistence.load();
            this.currentTerm = state.currentTerm();
            this.votedFor = state.votedFor();
            this.log.addAll(state.log());
        } catch (IOException e) {
            throw new UncheckedIOException("Failed to load Raft state", e);
        }
    }

    // ---- accessors (synchronized for visibility) ----

    public synchronized RaftRole role() {
        return role;
    }

    public synchronized boolean isLeader() {
        return role == RaftRole.LEADER;
    }

    public synchronized int currentTerm() {
        return currentTerm;
    }

    public synchronized int leaderId() {
        return leaderId;
    }

    public synchronized long commitIndex() {
        return commitIndex;
    }

    public synchronized long lastLogIndex() {
        return log.size();
    }

    public synchronized Integer votedFor() {
        return votedFor;
    }

    // ---- timer-driven entry points ----

    /** Initialize timers relative to {@code now}; call once before ticking. */
    public synchronized void initTiming(long now) {
        resetElectionDeadline(now);
    }

    /** Advances time. Triggers elections and heartbeats as deadlines pass. */
    public synchronized void tick(long now) {
        if (role == RaftRole.LEADER) {
            if (now >= nextHeartbeat) {
                broadcastHeartbeat();
                nextHeartbeat = now + heartbeatIntervalMs;
            }
        } else if (now >= electionDeadline) {
            startElection();
            resetElectionDeadline(now);
        }
    }

    private void resetElectionDeadline(long now) {
        int span = Math.max(1, electionTimeoutMaxMs - electionTimeoutMinMs);
        electionDeadline = now + electionTimeoutMinMs + ThreadLocalRandom.current().nextInt(span);
    }

    // ---- elections ----

    public synchronized void startElection() {
        currentTerm++;
        role = RaftRole.CANDIDATE;
        votedFor = id;
        leaderId = -1;
        votesGranted = 1; // vote for self
        persistMeta();

        int termForElection = currentTerm;
        long lastIdx = lastLogIndex();
        int lastTerm = termAt(lastIdx);
        RequestVoteRequest req = new RequestVoteRequest(termForElection, id, lastIdx, lastTerm);

        if (clusterSize == 1) {
            becomeLeader();
            return;
        }
        for (int peer : peers) {
            transport.requestVote(peer, req).whenComplete((resp, err) -> {
                if (resp != null) {
                    onVoteResponse(termForElection, resp);
                }
            });
        }
    }

    private synchronized void onVoteResponse(int termForElection, RequestVoteResponse resp) {
        if (resp.term() > currentTerm) {
            stepDown(resp.term());
            return;
        }
        if (role != RaftRole.CANDIDATE || currentTerm != termForElection) {
            return; // stale response
        }
        if (resp.voteGranted()) {
            votesGranted++;
            if (votesGranted > clusterSize / 2) {
                becomeLeader();
            }
        }
    }

    private void becomeLeader() {
        role = RaftRole.LEADER;
        leaderId = id;
        long next = lastLogIndex() + 1;
        nextIndex.clear();
        matchIndex.clear();
        for (int peer : peers) {
            nextIndex.put(peer, next);
            matchIndex.put(peer, 0L);
        }
        nextHeartbeat = 0; // send heartbeats immediately on next tick
        broadcastHeartbeat();
    }

    public synchronized RequestVoteResponse handleRequestVote(RequestVoteRequest req) {
        if (req.term() > currentTerm) {
            stepDown(req.term());
        }
        boolean grant = false;
        if (req.term() == currentTerm
                && (votedFor == null || votedFor == req.candidateId())
                && candidateLogUpToDate(req.lastLogIndex(), req.lastLogTerm())) {
            votedFor = req.candidateId();
            grant = true;
            persistMeta();
            resetElectionDeadline(System.currentTimeMillis());
        }
        return new RequestVoteResponse(currentTerm, grant);
    }

    private boolean candidateLogUpToDate(long candLastIndex, int candLastTerm) {
        long myLastIndex = lastLogIndex();
        int myLastTerm = termAt(myLastIndex);
        if (candLastTerm != myLastTerm) {
            return candLastTerm > myLastTerm;
        }
        return candLastIndex >= myLastIndex;
    }

    // ---- log replication ----

    /** Leader: append a client command to the log. Returns its index, or -1 if not leader. */
    public synchronized long propose(byte[] command) {
        if (role != RaftRole.LEADER) {
            return -1;
        }
        long index = lastLogIndex() + 1;
        LogEntry entry = new LogEntry(index, currentTerm, command);
        log.add(entry);
        persistAppend(entry);
        matchIndex.put(id, index);
        broadcastHeartbeat();
        // single-node cluster commits immediately
        maybeAdvanceCommitIndex();
        return index;
    }

    public synchronized void broadcastHeartbeat() {
        if (role != RaftRole.LEADER) {
            return;
        }
        for (int peer : peers) {
            sendAppendEntries(peer);
        }
    }

    private void sendAppendEntries(int peer) {
        long ni = nextIndex.getOrDefault(peer, 1L);
        long prevLogIndex = ni - 1;
        int prevLogTerm = termAt(prevLogIndex);
        List<LogEntry> entries = entriesFrom(ni);
        int termForRpc = currentTerm;
        AppendEntriesRequest req = new AppendEntriesRequest(
                termForRpc, id, prevLogIndex, prevLogTerm, entries, commitIndex);
        long lastSent = prevLogIndex + entries.size();
        transport.appendEntries(peer, req).whenComplete((resp, err) -> {
            if (resp != null) {
                onAppendResponse(peer, termForRpc, lastSent, resp);
            }
        });
    }

    private synchronized void onAppendResponse(int peer, int termForRpc, long lastSent,
                                               AppendEntriesResponse resp) {
        if (resp.term() > currentTerm) {
            stepDown(resp.term());
            return;
        }
        if (role != RaftRole.LEADER || currentTerm != termForRpc) {
            return; // stale
        }
        if (resp.success()) {
            matchIndex.put(peer, Math.max(matchIndex.getOrDefault(peer, 0L), lastSent));
            nextIndex.put(peer, lastSent + 1);
            maybeAdvanceCommitIndex();
        } else {
            // log inconsistency: back off and retry on the next heartbeat
            long ni = nextIndex.getOrDefault(peer, 1L);
            nextIndex.put(peer, Math.max(1, ni - 1));
        }
    }

    public synchronized AppendEntriesResponse handleAppendEntries(AppendEntriesRequest req) {
        if (req.term() > currentTerm) {
            stepDown(req.term());
        }
        if (req.term() < currentTerm) {
            return new AppendEntriesResponse(currentTerm, false, 0);
        }
        // valid leader for the current term
        role = RaftRole.FOLLOWER;
        leaderId = req.leaderId();
        resetElectionDeadline(System.currentTimeMillis());

        // consistency check against prevLogIndex/prevLogTerm
        if (req.prevLogIndex() > 0) {
            if (lastLogIndex() < req.prevLogIndex()
                    || termAt(req.prevLogIndex()) != req.prevLogTerm()) {
                return new AppendEntriesResponse(currentTerm, false, 0);
            }
        }

        // append entries, truncating on conflict
        boolean truncated = false;
        long index = req.prevLogIndex();
        for (LogEntry e : req.entries()) {
            index++;
            if (lastLogIndex() >= index) {
                if (termAt(index) != e.term()) {
                    truncateFrom(index); // delete this and all that follow
                    log.add(e);
                    truncated = true;
                }
                // else: already present and matching, skip
            } else {
                log.add(e);
                if (!truncated) {
                    persistAppend(e);
                }
            }
        }
        if (truncated) {
            persistRewrite();
        }

        if (req.leaderCommit() > commitIndex) {
            commitIndex = Math.min(req.leaderCommit(), lastLogIndex());
            applyCommitted();
        }
        return new AppendEntriesResponse(currentTerm, true, lastLogIndex());
    }

    private void maybeAdvanceCommitIndex() {
        for (long n = lastLogIndex(); n > commitIndex; n--) {
            if (termAt(n) != currentTerm) {
                continue; // Raft: only commit entries from the current term by counting
            }
            int count = 1; // self
            for (int peer : peers) {
                if (matchIndex.getOrDefault(peer, 0L) >= n) {
                    count++;
                }
            }
            if (count > clusterSize / 2) {
                commitIndex = n;
                applyCommitted();
                break;
            }
        }
    }

    private void applyCommitted() {
        while (lastApplied < commitIndex) {
            lastApplied++;
            LogEntry entry = log.get((int) (lastApplied - 1));
            applyCommand.accept(entry.command());
        }
    }

    private void stepDown(int newTerm) {
        currentTerm = newTerm;
        role = RaftRole.FOLLOWER;
        votedFor = null;
        leaderId = -1;
        persistMeta();
    }

    // ---- log helpers ----

    private int termAt(long index) {
        if (index <= 0 || index > log.size()) {
            return 0;
        }
        return log.get((int) (index - 1)).term();
    }

    private List<LogEntry> entriesFrom(long index) {
        if (index > log.size()) {
            return List.of();
        }
        return new ArrayList<>(log.subList((int) (index - 1), log.size()));
    }

    private void truncateFrom(long index) {
        while (log.size() >= index) {
            log.remove(log.size() - 1);
        }
    }

    // ---- persistence wrappers ----

    private void persistMeta() {
        try {
            persistence.saveMeta(currentTerm, votedFor);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private void persistAppend(LogEntry entry) {
        try {
            persistence.appendEntry(entry);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private void persistRewrite() {
        try {
            persistence.rewriteLog(new ArrayList<>(log));
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }
}
