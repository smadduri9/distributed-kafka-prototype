package com.kafkads.raft;

import com.kafkads.raft.messages.AppendEntriesRequest;
import com.kafkads.raft.messages.AppendEntriesResponse;
import com.kafkads.raft.messages.RequestVoteRequest;
import com.kafkads.raft.messages.RequestVoteResponse;
import com.kafkads.storage.Record;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.LongSupplier;

/**
 * One member of one Raft group, following the Raft paper (Ongaro &amp; Ousterhout).
 * In KafkaDS there is one group per partition and the group's log <em>is</em> the
 * partition log, so "committed" here is exactly "visible to consumers".
 *
 * <p>The logic is transport-, storage- and clock-agnostic: timeouts are driven by
 * {@link #tick(long)} (or {@link #startElection()} / {@link #broadcastHeartbeat()}
 * directly in tests), RPCs go through a {@link RaftTransport}, and the log and the
 * term/vote record go through {@link RaftLog} and {@link RaftMetaStore}. The same code
 * therefore runs deterministically in memory and over the network.
 *
 * <p>Implemented: terms, randomized election timeouts, a durable vote, the
 * up-to-date-log vote check, the AppendEntries consistency check, conflicting-suffix
 * truncation, commit by current-term majority, a no-op entry on election, and leader
 * step-down when a majority has been unreachable for an election timeout.
 * Not implemented: membership change, snapshots, pre-vote, leadership transfer.
 *
 * <p>All public methods are synchronized. Response callbacks are dispatched through the
 * supplied executor, so no RPC is ever awaited while holding the lock.
 */
public final class RaftNode {

    /** Notified (while the node's lock is held) after role, term, commit or match state changes. */
    public interface Listener {
        void onStateChanged();
    }

    /** Payload bytes per AppendEntries request. */
    static final int MAX_APPEND_BYTES = 1 << 20;

    private final int groupId;
    private final int id;
    private final Set<Integer> peers;
    private final int clusterSize;
    private final RaftTransport transport;
    private final RaftLog log;
    private final RaftMetaStore metaStore;
    private final Listener listener;
    private final Executor callbackExecutor;
    private final LongSupplier clock;
    private final int electionTimeoutMinMs;
    private final int electionTimeoutMaxMs;
    private final int heartbeatIntervalMs;
    private final int electionDelayMs;

    // Persistent state (besides the log).
    private int currentTerm;
    private Integer votedFor;

    // Volatile state. commitIndex deliberately restarts at 0: it is re-learned from a
    // leader, so having an entry on local disk never by itself makes it committed.
    private RaftRole role = RaftRole.FOLLOWER;
    private int leaderId = -1;
    private long commitIndex = 0;
    private boolean stopped;

    // Candidate state.
    private int votesGranted = 0;

    // Leader state.
    private final Map<Integer, Long> nextIndex = new HashMap<>();
    private final Map<Integer, Long> matchIndex = new HashMap<>();
    private final Map<Integer, Long> commitSent = new HashMap<>();
    private final Map<Integer, Long> lastAckMs = new HashMap<>();
    private final Set<Integer> inFlight = new HashSet<>();

    // Timing.
    private long electionDeadline = Long.MAX_VALUE;
    private long nextHeartbeat = 0;

    public static Builder builder(int id, Set<Integer> peers) {
        return new Builder(id, peers);
    }

    public static final class Builder {
        private final int id;
        private final Set<Integer> peers;
        private int groupId = 0;
        private RaftTransport transport;
        private RaftLog log = new InMemoryRaftLog();
        private RaftMetaStore metaStore = new InMemoryRaftMetaStore();
        private Listener listener = () -> { };
        private Executor callbackExecutor = Runnable::run;
        private LongSupplier clock = System::currentTimeMillis;
        private int electionTimeoutMinMs = 500;
        private int electionTimeoutMaxMs = 1000;
        private int heartbeatIntervalMs = 150;
        private int electionDelayMs = 0;

        private Builder(int id, Set<Integer> peers) {
            this.id = id;
            this.peers = peers;
        }

        public Builder groupId(int groupId) {
            this.groupId = groupId;
            return this;
        }

        public Builder transport(RaftTransport transport) {
            this.transport = transport;
            return this;
        }

        public Builder log(RaftLog log) {
            this.log = log;
            return this;
        }

        public Builder metaStore(RaftMetaStore metaStore) {
            this.metaStore = metaStore;
            return this;
        }

        public Builder listener(Listener listener) {
            this.listener = listener;
            return this;
        }

        /** Executor on which RPC response callbacks run. Defaults to the completing thread. */
        public Builder callbackExecutor(Executor executor) {
            this.callbackExecutor = executor;
            return this;
        }

        public Builder clock(LongSupplier clock) {
            this.clock = clock;
            return this;
        }

        public Builder timing(int electionTimeoutMinMs, int electionTimeoutMaxMs, int heartbeatIntervalMs) {
            this.electionTimeoutMinMs = electionTimeoutMinMs;
            this.electionTimeoutMaxMs = electionTimeoutMaxMs;
            this.heartbeatIntervalMs = heartbeatIntervalMs;
            return this;
        }

        /**
         * Extra delay added to this node's first election timeout after start-up. The
         * preferred leader of a partition uses 0 and the other replicas a positive value,
         * so the preferred replica normally times out and wins the first election.
         */
        public Builder electionDelayMs(int electionDelayMs) {
            this.electionDelayMs = electionDelayMs;
            return this;
        }

        public RaftNode build() {
            return new RaftNode(this);
        }
    }

    private RaftNode(Builder b) {
        this.groupId = b.groupId;
        this.id = b.id;
        // Sorted so that peers are always contacted in the same order (reproducible tests).
        this.peers = java.util.Collections.unmodifiableSortedSet(new java.util.TreeSet<>(b.peers));
        this.clusterSize = peers.size() + 1;
        this.transport = b.transport;
        this.log = b.log;
        this.metaStore = b.metaStore;
        this.listener = b.listener;
        this.callbackExecutor = b.callbackExecutor;
        this.clock = b.clock;
        this.electionTimeoutMinMs = b.electionTimeoutMinMs;
        this.electionTimeoutMaxMs = b.electionTimeoutMaxMs;
        this.heartbeatIntervalMs = b.heartbeatIntervalMs;
        this.electionDelayMs = b.electionDelayMs;
        try {
            RaftMetaStore.Meta meta = metaStore.load();
            if (!meta.exists() && log.lastIndex() > 0) {
                // A term is always persisted before the first entry of that term is written,
                // so a log without a term/vote record means the record was lost.
                throw new IllegalStateException("Raft group " + groupId + ": log has "
                        + log.lastIndex() + " entries but the term/vote record is missing");
            }
            if (meta.currentTerm() < log.termAt(log.lastIndex())) {
                throw new IllegalStateException("Raft group " + groupId + ": persisted term "
                        + meta.currentTerm() + " is older than the log's last term "
                        + log.termAt(log.lastIndex()));
            }
            this.currentTerm = meta.currentTerm();
            this.votedFor = meta.votedFor();
        } catch (IOException e) {
            throw new UncheckedIOException("Failed to load Raft state", e);
        }
    }

    // ---- accessors ----

    public synchronized RaftRole role() {
        return role;
    }

    public synchronized boolean isLeader() {
        return role == RaftRole.LEADER;
    }

    /**
     * True once this leader has committed an entry of its own term. Until then its commit
     * index may lag what the previous leader acknowledged, so it must not serve reads.
     */
    public synchronized boolean isLeaderReady() {
        return role == RaftRole.LEADER && commitIndex > 0 && log.termAt(commitIndex) == currentTerm;
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
        return log.lastIndex();
    }

    public synchronized Integer votedFor() {
        return votedFor;
    }

    /** Leader only: the highest index known to be durable on every replica. */
    public synchronized long minMatchIndex() {
        long min = log.lastIndex();
        for (int peer : peers) {
            min = Math.min(min, matchIndex.getOrDefault(peer, 0L));
        }
        return min;
    }

    // ---- timer-driven entry points ----

    /** Initialize timers relative to {@code now}; call once before ticking. */
    public synchronized void initTiming(long now) {
        resetElectionDeadline(now);
        // The start-up bias applies to the first election only, so it orders the initial
        // election without slowing down later failovers.
        electionDeadline += electionDelayMs;
    }

    /** Advances time. Triggers elections, heartbeats and leader step-down as deadlines pass. */
    public synchronized void tick(long now) {
        if (stopped) {
            return;
        }
        if (role == RaftRole.LEADER) {
            if (!majorityHeardWithin(now, electionTimeoutMaxMs)) {
                // A leader that cannot reach a majority cannot commit anything. Step down so
                // clients are redirected instead of timing out against a stale leader.
                role = RaftRole.FOLLOWER;
                leaderId = -1;
                resetElectionDeadline(now);
                listener.onStateChanged();
                return;
            }
            if (now >= nextHeartbeat) {
                broadcastHeartbeat();
                nextHeartbeat = now + heartbeatIntervalMs;
            }
        } else if (now >= electionDeadline) {
            startElection();
            resetElectionDeadline(now);
        }
    }

    private boolean majorityHeardWithin(long now, long windowMs) {
        int count = 1; // self
        for (int peer : peers) {
            if (now - lastAckMs.getOrDefault(peer, 0L) <= windowMs) {
                count++;
            }
        }
        return count > clusterSize / 2;
    }

    private void resetElectionDeadline(long now) {
        int span = Math.max(1, electionTimeoutMaxMs - electionTimeoutMinMs);
        electionDeadline = now + electionTimeoutMinMs + ThreadLocalRandom.current().nextInt(span);
    }

    /** Permanently stops this node from taking part (shutdown or unrecoverable storage error). */
    public synchronized void stop() {
        stopped = true;
        role = RaftRole.FOLLOWER;
        leaderId = -1;
    }

    // ---- elections ----

    public synchronized void startElection() {
        if (stopped) {
            return;
        }
        currentTerm++;
        role = RaftRole.CANDIDATE;
        votedFor = id;
        leaderId = -1;
        votesGranted = 1; // vote for self
        persistMeta();
        listener.onStateChanged();

        int termForElection = currentTerm;
        long lastIdx = log.lastIndex();
        RequestVoteRequest req = new RequestVoteRequest(
                groupId, termForElection, id, lastIdx, log.termAt(lastIdx));

        if (clusterSize == 1) {
            becomeLeader();
            return;
        }
        for (int peer : peers) {
            CompletableFuture<RequestVoteResponse> f;
            try {
                f = transport.requestVote(peer, req);
            } catch (RuntimeException e) {
                continue;
            }
            f.whenCompleteAsync((resp, err) -> {
                if (resp != null) {
                    onVoteResponse(termForElection, resp);
                }
            }, callbackExecutor);
        }
    }

    private synchronized void onVoteResponse(int termForElection, RequestVoteResponse resp) {
        if (stopped) {
            return;
        }
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
        long next = log.lastIndex() + 1;
        long now = clock.getAsLong();
        nextIndex.clear();
        matchIndex.clear();
        commitSent.clear();
        lastAckMs.clear();
        inFlight.clear();
        for (int peer : peers) {
            nextIndex.put(peer, next);
            matchIndex.put(peer, 0L);
            lastAckMs.put(peer, now);
        }
        nextHeartbeat = now + heartbeatIntervalMs;
        // Raft only commits entries of the leader's own term by counting replicas. Appending
        // a no-op immediately lets the new leader commit everything its predecessors left
        // behind without waiting for the next client write.
        appendAsLeader(Record.NOOP, List.of(new byte[0]));
        listener.onStateChanged();
    }

    public synchronized RequestVoteResponse handleRequestVote(RequestVoteRequest req) {
        if (stopped) {
            return new RequestVoteResponse(currentTerm, false);
        }
        if (req.term() > currentTerm) {
            stepDown(req.term());
        }
        boolean grant = false;
        if (req.term() == currentTerm
                && (votedFor == null || votedFor == req.candidateId())
                && candidateLogUpToDate(req.lastLogIndex(), req.lastLogTerm())) {
            votedFor = req.candidateId();
            grant = true;
            persistMeta(); // durable before the vote leaves this node
            resetElectionDeadline(clock.getAsLong());
        }
        return new RequestVoteResponse(currentTerm, grant);
    }

    private boolean candidateLogUpToDate(long candLastIndex, int candLastTerm) {
        long myLastIndex = log.lastIndex();
        int myLastTerm = log.termAt(myLastIndex);
        if (candLastTerm != myLastTerm) {
            return candLastTerm > myLastTerm;
        }
        return candLastIndex >= myLastIndex;
    }

    // ---- log replication ----

    /**
     * Leader: appends client commands as one durable batch and starts replicating them.
     * Returns the index of the first command, or -1 if this node is not the leader.
     * A returned index means "appended locally", not "committed".
     */
    public synchronized long propose(List<byte[]> commands) {
        if (role != RaftRole.LEADER || stopped) {
            return -1;
        }
        return appendAsLeader(Record.DATA, commands);
    }

    /** Convenience for a single command. */
    public synchronized long propose(byte[] command) {
        return propose(List.of(command));
    }

    private long appendAsLeader(byte type, List<byte[]> commands) {
        long first = log.lastIndex() + 1;
        List<LogEntry> entries = new ArrayList<>(commands.size());
        long index = first;
        for (byte[] c : commands) {
            entries.add(new LogEntry(index++, currentTerm, type, c));
        }
        try {
            log.append(entries); // durable on return, so the leader may count itself
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        maybeAdvanceCommitIndex(); // a single-node group commits immediately
        for (int peer : peers) {
            sendAppendEntries(peer);
        }
        return first;
    }

    public synchronized void broadcastHeartbeat() {
        if (role != RaftRole.LEADER || stopped) {
            return;
        }
        for (int peer : peers) {
            sendAppendEntries(peer);
        }
    }

    /** At most one AppendEntries is outstanding per peer; the next is sent when it resolves. */
    private void sendAppendEntries(int peer) {
        if (inFlight.contains(peer)) {
            return;
        }
        long ni = nextIndex.getOrDefault(peer, 1L);
        long prevLogIndex = ni - 1;
        int prevLogTerm = log.termAt(prevLogIndex);
        List<LogEntry> entries;
        try {
            entries = ni <= log.lastIndex() ? log.entries(ni, MAX_APPEND_BYTES) : List.of();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        int termForRpc = currentTerm;
        long commitAtSend = commitIndex;
        AppendEntriesRequest req = new AppendEntriesRequest(
                groupId, termForRpc, id, prevLogIndex, prevLogTerm, entries, commitAtSend);
        long lastSent = prevLogIndex + entries.size();
        inFlight.add(peer);
        CompletableFuture<AppendEntriesResponse> f;
        try {
            f = transport.appendEntries(peer, req);
        } catch (RuntimeException e) {
            f = CompletableFuture.failedFuture(e);
        }
        f.whenCompleteAsync(
                (resp, err) -> onAppendDone(peer, termForRpc, lastSent, commitAtSend, resp),
                callbackExecutor);
    }

    private synchronized void onAppendDone(int peer, int termForRpc, long lastSent, long commitAtSend,
                                           AppendEntriesResponse resp) {
        if (currentTerm == termForRpc) {
            inFlight.remove(peer);
        }
        if (stopped || resp == null) {
            return; // failed or timed out; the next heartbeat retries
        }
        if (resp.term() > currentTerm) {
            stepDown(resp.term());
            return;
        }
        if (role != RaftRole.LEADER || currentTerm != termForRpc) {
            return; // stale
        }
        lastAckMs.put(peer, clock.getAsLong());
        if (resp.success()) {
            long match = Math.max(matchIndex.getOrDefault(peer, 0L), lastSent);
            matchIndex.put(peer, match);
            nextIndex.put(peer, match + 1);
            commitSent.merge(peer, commitAtSend, Math::max);
            maybeAdvanceCommitIndex();
            listener.onStateChanged();
            if (match < log.lastIndex() || commitSent.get(peer) < commitIndex) {
                sendAppendEntries(peer); // more entries, or a newer commit index, to deliver
            }
        } else {
            // Log mismatch: move back, using the follower's log length to skip quickly.
            long ni = nextIndex.getOrDefault(peer, 1L);
            nextIndex.put(peer, Math.max(1, Math.min(ni - 1, resp.matchIndex() + 1)));
            sendAppendEntries(peer);
        }
    }

    public synchronized AppendEntriesResponse handleAppendEntries(AppendEntriesRequest req) {
        if (stopped) {
            return new AppendEntriesResponse(currentTerm, false, 0);
        }
        if (req.term() > currentTerm) {
            stepDown(req.term());
        }
        if (req.term() < currentTerm) {
            return new AppendEntriesResponse(currentTerm, false, log.lastIndex());
        }
        // A valid leader for the current term.
        boolean changed = role != RaftRole.FOLLOWER || leaderId != req.leaderId();
        role = RaftRole.FOLLOWER;
        leaderId = req.leaderId();
        resetElectionDeadline(clock.getAsLong());

        // Consistency check against prevLogIndex/prevLogTerm.
        if (req.prevLogIndex() > 0
                && (log.lastIndex() < req.prevLogIndex()
                || log.termAt(req.prevLogIndex()) != req.prevLogTerm())) {
            if (changed) {
                listener.onStateChanged();
            }
            return new AppendEntriesResponse(currentTerm, false, log.lastIndex());
        }

        try {
            // Skip entries already present; on the first conflict drop the local suffix.
            List<LogEntry> entries = req.entries();
            int firstNew = 0;
            long index = req.prevLogIndex();
            for (; firstNew < entries.size(); firstNew++) {
                index++;
                LogEntry e = entries.get(firstNew);
                if (e.index() != index) {
                    return new AppendEntriesResponse(currentTerm, false, log.lastIndex());
                }
                if (log.lastIndex() < index) {
                    break;
                }
                if (log.termAt(index) != e.term()) {
                    if (index <= commitIndex) {
                        throw new IllegalStateException("Raft group " + groupId
                                + ": leader asked to overwrite committed index " + index);
                    }
                    log.truncateFrom(index);
                    break;
                }
            }
            if (firstNew < entries.size()) {
                log.append(entries.subList(firstNew, entries.size())); // durable before the reply
            }
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }

        // Only what this request verified may be marked committed: entries beyond it could be
        // a stale suffix that the leader has not yet examined.
        long lastVerified = req.prevLogIndex() + req.entries().size();
        long newCommit = Math.min(req.leaderCommit(), lastVerified);
        if (newCommit > commitIndex) {
            commitIndex = newCommit;
            changed = true;
        }
        if (changed) {
            listener.onStateChanged();
        }
        return new AppendEntriesResponse(currentTerm, true, lastVerified);
    }

    private void maybeAdvanceCommitIndex() {
        for (long n = log.lastIndex(); n > commitIndex; n--) {
            if (log.termAt(n) != currentTerm) {
                break; // only entries of the current term are committed by counting replicas
            }
            int count = 1; // self: the leader's entries are durable before this is called
            for (int peer : peers) {
                if (matchIndex.getOrDefault(peer, 0L) >= n) {
                    count++;
                }
            }
            if (count > clusterSize / 2) {
                commitIndex = n;
                listener.onStateChanged();
                break;
            }
        }
    }

    private void stepDown(int newTerm) {
        currentTerm = newTerm;
        role = RaftRole.FOLLOWER;
        votedFor = null;
        leaderId = -1;
        persistMeta();
        listener.onStateChanged();
    }

    private void persistMeta() {
        try {
            metaStore.save(currentTerm, votedFor);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }
}
