# KafkaDS Design

This document describes the current design of KafkaDS: how partitions are assigned to
brokers, how each partition is replicated with Raft, how the partition log doubles as the
Raft log, the wire protocol, the on-disk format, the recovery rules, and the failure
behaviour. It describes the code as it is; where the code does not do something, the
document says so.

## Overview

KafkaDS is a message-broker prototype in Java 17 on Netty 4.1. A cluster has a fixed set
of brokers and a fixed number of partitions. Each partition is an ordered, append-only log
replicated across a subset of the brokers (its replica list). Each replica of a partition
is a Raft group member, and the partition's segmented log is the Raft log, so one copy of
the data is both the replicated log and the log that consumers read.

A write goes to the leader of the partition. The leader appends the batch to its local log
and fsyncs it, sends the entries to the other replicas with AppendEntries, and answers the
producer once the acknowledgment mode's condition holds. A record becomes visible to
consumers when it is committed, that is, when a majority of the partition's replicas hold it
in a term led by the current leader.

```
producer
   | ProduceRequest(partition, acks, values)
   v
leader broker (Netty event loop: decode and enqueue only)
   |
   v
PartitionReplica for the partition    thread: broker-<id>-partition-<p>
   |-- RaftNode.propose -> SegmentRaftLog -> Log -> Segment   (write, then fsync)
   |-- AppendEntries ----> the other replicas of the partition (over shared peer connections)
   v
commit index (exclusive high-watermark) reached -> ProduceResponse to the producer
```

## Design decision: one Raft group per partition

Two designs were considered for replicating a multi-partition cluster.

- Option A: one Raft group for the whole cluster. Every write, for any partition, is a
  command in that single log, tagged with its partition. With this option every partition
  has the same replicas and the same leader, so a per-partition assignment map would have no
  effect on where data is stored, which brokers form the quorum, or who leads.
- Option B: one Raft group per partition. Each partition has its own log, its own quorum
  and its own leader, drawn from its replica list.

Option B was chosen. With it, the partition assignment decides storage (which brokers host
a partition), the commit quorum (the partition's Raft peers are its replica list) and
leadership (the preferred leader starts the first election first). The assignment therefore
changes behaviour.

The cost of option B is one thread and one election per partition on each replica, and more
heartbeats, because each partition's leader sends its own AppendEntries heartbeats. All
partitions on a broker share one server socket and one outgoing connection per peer broker,
and Raft RPCs carry the partition id so the receiving broker can route them.

## Partition assignment

`broker/PartitionAssignment` maps each partition to an ordered list of broker ids. The first
broker in the list is the preferred leader.

Assignment is computed locally by every broker from its own configuration, so all brokers
must be configured with the same broker ids, `partition.count`, and `replication.factor` (or
the same explicit assignment). Two forms are accepted:

- Default: round-robin over the sorted broker ids. Partition `p` gets `replication.factor`
  consecutive brokers starting at position `p mod N`, where `N` is the number of brokers. The
  replication factor is capped at the number of brokers.
- Explicit: `partition.assignment=0:1,2,3;1:2,3,1`. Every partition in
  `[0, partition.count)` must appear exactly once, with distinct, known broker ids.

The assignment is executed, not only described:

1. A broker creates storage and a Raft group member only for the partitions that list it
   (`Broker` constructor, `PartitionAssignment.partitionsOf`).
2. A partition's Raft peers, and therefore its commit quorum, are exactly its replica list.
   Raft's majority is computed over that list (`RaftNode.clusterSize`).
3. The preferred leader (rank 0) starts its first election earlier than the other replicas.
   Each replica adds `rank * raft.election.timeout.max.ms` to its first election deadline
   only (`PartitionReplica` constructor, `RaftNode.initTiming`). This biases the initial
   election and does not affect later failovers.
4. A broker that is not in a partition's replica list answers `NOT_REPLICA`. A partition that
   is not in the assignment at all answers `UNKNOWN_PARTITION` (`Broker.missingPartitionCode`).

### Client routing

Metadata (`METADATA` request) is answered by any broker. For each partition it returns the
replica list, the preferred leader, and this broker's own view of the current leader, term,
log end offset and high-watermark. The view of a broker that does not host the partition
contains -1 values. The leader field is therefore one broker's view and can be stale.

The client (`client/ClusterClient.sendToLeader`) chooses its first target for a partition in
this order: the last leader hint it holds for that partition, then the leader reported by
metadata, then the preferred leader. It then follows responses:

- `NOT_LEADER` or `NOT_REPLICA` with a leader hint: the client switches to the hinted broker.
- `NOT_LEADER` or `NOT_REPLICA` without a hint, or a connection failure: the client moves to the
  next broker in the replica list, after a 50 ms pause.
- `LEADER_NOT_READY` or `BUSY`: the client pauses and retries the same target.
- Any other code is returned to the caller. After a successful `OK` the client remembers that
  broker as the leader hint for the partition.

Every call is bounded by an overall deadline (`deliveryTimeoutMs` for producers and
`fetchTimeoutMs` for consumers).

### Failover

On failover the replica list does not change; only the leader within it does. There is no
automatic return to the preferred leader and no reassignment at runtime. Membership is
static.

## The log is the Raft log

Each partition's log is a `storage/Log` of rolling segments. `raft/SegmentRaftLog` presents
that log to `RaftNode` as its Raft log, so each record is stored once.

- Raft index `i` is log offset `i - 1`. `SegmentRaftLog` performs the conversion in each
  direction (`termAt`, `append`, `truncateFrom`, `entries`).
- The Raft commit index is the partition's exclusive high-watermark. Fetch reads offsets
  below `RaftNode.commitIndex()` only (`PartitionReplica.doFetch`).
- There is no state machine. Committed records are not applied to anything; consumers read
  them from the log. There is therefore nothing to replay on restart and no applied-index file.
- The commit index is volatile. It starts at 0 after a restart and is re-learned from a leader.
  A record being present on local disk never by itself makes it visible.

Offsets are strictly increasing per partition. Every offset holds exactly one record on disk,
including the no-op record a new leader appends on election (`RaftNode.becomeLeader`), so the
sequence on disk has no holes. Fetch removes no-op records from its results, so the offsets a
consumer receives have gaps where no-ops are. Consumers continue from the `nextOffset` returned
by fetch and must not assume that offsets are contiguous.

## Request paths

### Produce (acks=QUORUM)

1. The producer sends a `PRODUCE` frame to the partition's leader. The Netty pipeline decodes
   it in `network/MessageDecoder`, which yields a `ProduceRequest`.
2. `broker/RequestHandler.channelRead0` checks the per-connection in-flight limit, then calls
   `Broker.handle`, which dispatches to `PartitionReplica.produce`.
3. `PartitionReplica.submitClient` checks the queue bound (`MAX_QUEUED_REQUESTS`) and submits the
   task to the partition thread. The Netty thread does no log or disk work.
4. On the partition thread, `PartitionReplica.doProduce` rejects an empty batch and any value
   larger than `Segment.MAX_RECORD_BYTES` with `INVALID_REQUEST`.
5. `RaftNode.propose` (leader only; otherwise `NOT_LEADER`) calls `appendAsLeader`, which calls
   `SegmentRaftLog.append`. That writes the whole batch through `Log.appendRecords` and
   `Segment.append`, then calls `Log.flush` (fsync).
6. `appendAsLeader` runs `maybeAdvanceCommitIndex` (a single-replica partition commits here) and
   calls `sendAppendEntries` for each peer. Each peer has at most one AppendEntries in flight.
7. `doProduce` registers a `PendingProduce` with a timer of `produce.timeout.ms`.
8. When an AppendEntries response arrives, `RaftNode.onAppendDone` updates the peer's match index
   and calls `maybeAdvanceCommitIndex`. Then `PartitionReplica.onRaftStateChanged` completes every
   pending produce whose last index is committed. The response is written back on the Netty
   thread by `RequestHandler`.
9. If the timer fires first, `PartitionReplica.timeOut` answers `COMMIT_TIMEOUT`. If leadership is
   lost or the term changes, `onRaftStateChanged` answers `LEADERSHIP_LOST`.

For `acks=LEADER`, step 7 is skipped and the response is sent after step 5. For `acks=ALL`, step 8
also requires every peer's match index to reach the last index (`RaftNode.minMatchIndex`).

### Fetch

1. The consumer sends a `FETCH` frame to a broker that it believes leads the partition.
2. `RequestHandler` and `Broker.handle` dispatch to `PartitionReplica.fetch`, which submits the
   task to the partition thread (`submitClient`).
3. `PartitionReplica.doFetch` reads `hw = RaftNode.commitIndex()`. It answers `NOT_LEADER` if this
   replica is not the leader, and `LEADER_NOT_READY` if `RaftNode.isLeaderReady()` is false.
   `isLeaderReady` requires the leader to have committed an entry of its own term.
4. `doFetch` caps `maxBytes` at `MAX_FETCH_BYTES` (4 MB), calls `Log.read(from, maxBytes, hw)`,
   which calls `Segment.read` for each segment, and keeps only data records.
5. The response carries the high-watermark, `nextOffset` (the offset after the last record scanned,
   or `from` if nothing was scanned), the leader hint and the records.

Fetch is leader-only. Followers do not serve reads.

## Acknowledgment contract

The acknowledgment mode is a field of each produce request (`protocol/Acks`).

| Mode | Wire code | OK is sent when | OK guarantees | OK does not guarantee |
|---|---|---|---|---|
| LEADER | 1 | The leader has appended the batch and fsynced it locally (`doProduce`). | The batch is durable on the leader's disk. | That the batch is committed or visible to consumers. If the leader is replaced before a majority holds the batch, the batch is discarded. |
| QUORUM (producer default) | 2 | The commit index (an exclusive bound) has passed the batch's last record (`onRaftStateChanged`). | The batch is committed: durable on a majority of the replicas in a term led by the leader. It is visible to consumers and survives the loss of any minority of the replicas. | That every replica has it. Repeating the send can store the batch twice, because there are no producer ids or sequence numbers. |
| ALL | -1 | The batch is committed and every peer's match index covers its last record. | The batch is committed and durable on every configured replica. | Nothing beyond QUORUM if the answer is an error. `NOT_ENOUGH_REPLICAS` is returned after `produce.timeout.ms` when a replica is unavailable, but the batch may still have been committed by the majority and become visible. This is not a rollback. |

A batch that is answered `OK` or with a rejected code was appended whole or not at all, with
consecutive offsets. The one exception is a leader that dies in the middle of writing a batch: a
prefix of that batch can survive on its disk and may later be committed. The producer never
received an answer for such a batch, so for the producer it is an uncertain write (crash window (a)
below).

### Error codes for produce

Error codes fall into two groups. The client treats them differently.

- Rejected: nothing was appended, so resending cannot create a duplicate. These are
  `NOT_LEADER`, `UNKNOWN_PARTITION`, `NOT_REPLICA`, `INVALID_REQUEST` and `BUSY`
  (`ErrorCode.produceWasRejected`).
- Uncertain: the records are in the leader's log and may or may not become committed. These are
  `COMMIT_TIMEOUT`, `NOT_ENOUGH_REPLICAS`, `LEADERSHIP_LOST`, and `ERROR` (an internal failure,
  for example a storage error).

The producer (`client/Producer.send`, `client/ClusterClient.sendToLeader`) handles each case as
follows.

- `OK`: returns `RecordMetadata` with the base offset.
- `NOT_LEADER` and `NOT_REPLICA`: the client follows the hint or moves to the next replica
  (see Client routing). `BUSY` is retried with a 50 ms pause. If the deadline passes first, the
  producer throws `RequestRejectedException`.
- `UNKNOWN_PARTITION` and `INVALID_REQUEST`: returned to the producer, which throws
  `RequestRejectedException` at once.
- Uncertain codes: not retried. The producer throws `UncertainWriteException` with the base
  offset.
- A transport failure that may have reached the broker (`TIMEOUT` or `CONNECTION_LOST`): not
  retried. `UncertainWriteException` is thrown. A failure that certainly did not reach the broker
  (`NOT_SENT`, for example not connected or a write that failed) is retried against the next
  replica.

The producer never retries an uncertain outcome. Delivery is at-least-once if the caller resends
an uncertain write and at-most-once if it does not. It is never exactly-once.

## Wire protocol

Every message is one frame:

```
frame   := length(4) apiKey(1) correlationId(4) payload
length  := number of bytes after the length field
```

`Framing.configure` installs a `LengthFieldBasedFrameDecoder` (reassembles partial and coalesced
reads and rejects frames larger than `Framing.MAX_FRAME_BYTES`, 16 MB, before buffering them) and a
`LengthFieldPrepender`. `MessageDecoder` decodes exactly one message from each de-framed buffer,
and treats leftover bytes as an error.

API keys (`protocol/ApiKey`): PING (0), PRODUCE (1), PRODUCE_RESP (2), FETCH (3), FETCH_RESP (4),
METADATA (5), METADATA_RESP (6), REQUEST_VOTE (7), REQUEST_VOTE_RESP (8), APPEND_ENTRIES (9),
APPEND_ENTRIES_RESP (10), ERROR (11).

The produce payload, as an example of the encoding:

```
ProduceRequest := partition(4) acks(1) count(4) value*
value          := length(4) bytes
```

Validation before allocation. Every decoded length or count is checked against the bytes that
remain in the frame before anything is allocated (`protocol/Message`):

- `readBytes` rejects a negative length, or a length greater than the remaining bytes.
- `readCount` rejects a negative count, a count above the caller's maximum (for example
  `ProduceRequest.MAX_RECORDS` = 100,000), or a count whose minimum encoded size exceeds the
  remaining bytes.
- A truncated read fails with `MalformedMessageException`.

Error response. A malformed or oversized frame gets `ErrorResponse(INVALID_REQUEST, text)`
carrying the request's correlation id when it could be read, after which the connection is
closed (`RequestHandler.exceptionCaught`). `ErrorResponse` is `errorCode(2)` followed by a
UTF-8 string whose text is limited to 512 characters. A request that the broker cannot dispatch
gets `ErrorResponse(ERROR)`.

Bounds:

- The server accepts at most 1024 unanswered requests per connection (`RequestHandler.MAX_IN_FLIGHT`).
  Further requests get `ErrorResponse(BUSY)`.
- Each partition accepts at most 10,000 client requests that are queued or waiting for commit
  (`PartitionReplica.MAX_QUEUED_REQUESTS`). Further requests get `BUSY`. Internal work (Raft RPCs
  and their callbacks) is not counted against this bound.
- A fetch returns at most 4 MB of payload (`PartitionReplica.MAX_FETCH_BYTES`). A fetch always
  returns at least one record when one is available.
- A single record payload is at most 8 MB (`Segment.MAX_RECORD_BYTES`).
- A Raft AppendEntries request carries at most 1 MB of payload (`RaftNode.MAX_APPEND_BYTES`), and
  always at least one entry.
- The client (`network/Client`) keeps at most 1024 pending requests per connection
  (`DEFAULT_MAX_PENDING`). Further sends fail at once with `NOT_SENT`. Each request has a timeout
  (default 5 s). When the connection closes, all pending requests fail. Responses are matched by
  correlation id, and a response that arrives after its request timed out is dropped.

The server has no TLS, no authentication and no idle-connection timeout. It does not use channel
writability for flow control.

## Storage format and recovery

### Files and records

A partition's log directory contains segment files named by their base offset:
`<20-digit base offset>.log`. A segment file has this layout:

```
file    := MAGIC "KDSLOG02" (8 bytes) record*
record  := offset(8) term(4) type(1) length(4) crc32(4) payload(length)
type    := 0 DATA | 1 NOOP
```

The CRC-32 covers the first 17 bytes of the record (offset, term, type and length) and the
payload. A record with a zero-filled or misdirected header therefore fails the check. A record
header is 21 bytes.

### Batch, roll and fsync invariants

- A batch is written with one `write` to the active segment, so a batch never spans segments.
  Rolling happens before a batch is written, never inside it (`Log.appendRecords` calls
  `maybeRoll` first).
- `Log.maybeRoll` rolls when the active segment's size is at least `segment.bytes` (default 16 MB).
  Because the check happens before a batch, a segment can exceed `segment.bytes` by up to one
  batch. Before rolling, the old segment is fsynced, and after creating the new file the directory
  is fsynced (best effort).
- `Segment.append` writes the batch but does not fsync it. The fsync is done by
  `SegmentRaftLog.append`, which calls `Log.flush` after `Log.appendRecords` returns. Because a
  batch is in one segment, one fsync covers the whole batch. A batch is acknowledged only after this
  fsync.
- If the write of a batch fails, `Segment.append` truncates the file back to its previous length
  and leaves the in-memory state unchanged.
- The sparse offset index (one entry per `index.interval.bytes`, default 4096) lives in memory only.
  It is rebuilt when the segment is opened. There is no index file.

### Recovery on open

On open, every record of every segment is validated: the header and length must be plausible,
offsets must be continuous with the previous record, and the CRC must match
(`Segment` constructor, `Segment.recover`). Segments must start at offset 0 and be contiguous
(`Log.recoverSegments`). Start-up time therefore grows with the size of the log.

- Last segment (tail): an incomplete trailing record, or an invalid trailing record, is truncated,
  and the file is fsynced. A tail file shorter than the 8-byte header (for example a file whose
  creation was interrupted) is reset to an empty segment with a fresh header.
- Earlier segments: any invalid or incomplete record, a truncated header, or an unknown file format
  fails the open with `CorruptRecordException`.
- Tail with damage in the middle: if a record has a complete header and an invalid CRC, and the
  record after it is valid, the open fails rather than truncating. Dropping that data would lose
  records that might have been acknowledged.
- A gap between segments, or a log that does not start at offset 0, fails the open.

### Term and vote file

Current term and vote are stored by `raft/FileRaftMetaStore` in `raft-meta` inside the
partition's `raft` directory:

```
file := magic(4, 0x4B44534D "KDSM") term(4) votedFor(4, -1 = none) crc32(4)
```

The file is replaced atomically. The new contents are written to `raft-meta.tmp`, that file is
fsynced, it is renamed over `raft-meta` (ATOMIC_MOVE), and then the directory is fsynced. On load,
a leftover `raft-meta.tmp` is deleted, since it is an interrupted save that never took effect. A
file whose length, magic or CRC is wrong fails start-up with `CorruptRecordException`.

Start-up also fails if the log has entries but no term/vote file exists, or if the persisted term is
lower than the term of the log's last entry (`RaftNode` constructor).

## Recovery invariants and crash windows

The table lists what a crash can leave on disk and why the result is safe. The rules that make it
safe are these: an acknowledgment is sent only after an fsync; the commit index is volatile; an
uncommitted entry may later be committed or truncated; and only entries above the commit index are
ever truncated (`RaftNode.handleAppendEntries` refuses to overwrite a committed index).

| Crash point | What is on disk | What happens on restart | Why it is safe |
|---|---|---|---|
| (a) Leader crashes after writing a batch, before fsync returns | Either all records of the batch, a prefix of them (a torn tail), or none. Page-cache loss is possible in principle; SIGKILL does not lose it (see limitations). | The tail scan of the last segment keeps the valid prefix and truncates an incomplete or invalid trailing record. The commit index starts at 0. | The leader sends OK only after `Log.flush` returns, so the producer never saw OK. The outcome is uncertain on the client side. Any surviving records are uncommitted, so they may later be committed (by a new leader's no-op entry) or truncated, and both are legal. |
| (b) Leader crashes after fsync, before any follower has the batch | The full batch, intact. No follower has it. | The batch is valid and is loaded. The commit index starts at 0, so nothing is visible. | The batch was not committed (no majority held it), so it is not visible and not acknowledged as OK. If a new leader lacks the batch, the old leader's conflicting suffix is truncated when it catches up, which is allowed because the suffix is above the commit index. If the batch survives, it may be committed later, which is also allowed. |
| (c) Follower crashes after fsync, before its reply is sent | The batch is intact on the follower. The leader has no reply for it. | The follower loads the batch with commit index 0. The leader's next AppendEntries, sent on a later heartbeat, starts from its next index for that follower. Entries the follower already has at the same index and term are skipped and not appended again. | Replication is idempotent by index and term, so the entry is not stored twice. The follower's extra copy is an entry the leader will either commit (with the majority) or truncate. The leader's commit only counts a replica's match index after it has replied. |
| (d) Crash while saving term/vote, after `raft-meta.tmp` is written but before the rename | `raft-meta` holds the previous term and vote. `raft-meta.tmp` holds the new, possibly partial, contents. | `FileRaftMetaStore.load` deletes the temporary file and loads the previous term and vote. | `persistMeta` runs before the node sends a vote or replies to a vote request, and before a candidate sends RequestVote. A new term or vote that was not saved was never used, so forgetting it cannot let the node vote twice. The rename is atomic, so a reader never sees a mixture. |
| (e) Crash in the middle of truncating a conflicting suffix | `Log.truncateTo` deletes whole later segments from the end first, then truncates the segment that contains the cut, then fsyncs the directory. A crash can leave some later segments removed, and the cut segment either full length or truncated to a record boundary. | The surviving segments form a contiguous prefix. Any remaining suffix is loaded as ordinary uncommitted entries. The next AppendEntries from the leader detects the conflict by prevLogIndex and prevLogTerm and repeats the truncation. | The truncation cannot go below the commit index. The committed prefix is never in the cut region, and the uncommitted suffix that remains is not visible because the commit index is 0 after restart. |
| (f) Crash right after creating a new segment file | Either no new file, or a file with a partial header, or a file with a complete header and no records. The previous segment was fsynced before the roll. | A new file shorter than the header is reset to an empty segment. A file with a complete header is an empty tail segment. | No record is written to the new segment before its header is fsynced, and the previous segment was fsynced before the roll. All earlier data is in the previous segment. Only the last segment may be incomplete, which is the rule the open applies. |

The table describes process crashes. It does not cover power loss, where additional states are
possible: for example, in window (e) a segment file deleted before the directory fsync could
reappear. If it reappears after a gap the open fails; if it directly follows the cut point it is
loaded as an uncommitted suffix and truncated again by the leader. None of this has been tested
(see Known limitations).

## Raft implementation

Implemented (`raft/RaftNode`):

- Terms and randomized election timeouts (`raft.election.timeout.min.ms` to `.max.ms`).
- A durable vote: the term and vote are saved before the vote reply is sent.
- The up-to-date-log check when granting a vote.
- The AppendEntries consistency check (prevLogIndex and prevLogTerm), and truncation of a
  conflicting suffix, never below the commit index.
- Commit by majority, counting only entries of the leader's current term.
- A no-op entry appended on election, so that the new leader can commit entries from earlier terms.
- Leader step-down (check-quorum): a leader that has not heard from a majority within
  `raft.election.timeout.max.ms` steps down.
- At most one AppendEntries in flight per peer, with at most 1 MB of payload per request.
- A follower advances its commit index only to what the current request verified.
- A new leader refuses fetches (`LEADER_NOT_READY`) until it has committed an entry of its own term.

Not implemented:

- Membership change.
- Snapshots and log compaction. The log grows without bound, and start-up scans all of it.
- Pre-vote. A broker that rejoins with a higher term can force an election.
- Leadership transfer.
- Linearizable reads from followers. Fetch is leader-only.

## Concurrency model

- Each partition replica owns one thread, `broker-<id>-partition-<p>` (a single-thread
  `ScheduledThreadPoolExecutor`). Every operation on that partition's log and Raft node runs on this
  thread: client requests (`submitClient`), Raft RPCs (`submitInternal`), RPC responses (routed back
  through `executeInternal`), timer ticks (every max(5 ms, heartbeat / 3), default 50 ms) and produce
  timeouts. `RaftNode` methods are also `synchronized`.
- Netty threads decode frames, enqueue work on the partition thread and write responses. They do no
  disk I/O and do not wait for locks held by the partition thread. The server has its own accept
  thread and up to four worker event-loop threads (`network/Server`). Outgoing connections to peer
  brokers use a separate two-thread group (`Broker.clientGroup`).
- Other threads read state through a `volatile View` (role, leader, term, high-watermark, log end
  offset and failed flag). `refreshView` publishes a new snapshot after each task on the partition
  thread.
- Metadata requests run on a separate single thread (`broker-admin`). Peer address resolution runs on
  `raft-connector`.
- Bounded queues: the per-partition client bound of 10,000 (see Wire protocol) and the per-connection
  in-flight bound of 1024. Other work queued on the partition thread is not bounded by these counters.

## Failure handling

Leader death. Followers stop hearing heartbeats and start an election after a randomized timeout in
`raft.election.timeout.min.ms` to `.max.ms` (defaults 500 to 1000 ms). The new leader appends a no-op
entry and serves fetches once it has committed an entry of its own term. The replica list does not
change. Producers that were waiting on the old leader see a lost connection or a timeout, which is an
uncertain outcome, and the producer does not resend automatically. Producers that reach the new leader
are routed by the NOT_LEADER hint or by moving through the replica list.

Minority or isolated leader. A leader that cannot reach a majority cannot commit. After
`raft.election.timeout.max.ms` without a majority of acknowledgments it steps down (check-quorum), and
each pending produce is answered `LEADERSHIP_LOST`. The step-down is checked on the partition's tick, so it
happens within one tick interval after the timeout. Later requests to that broker get `NOT_LEADER`. The
majority side can elect a new leader from its own log.

Slow or dead follower, by acknowledgment mode:

- LEADER: unaffected. The leader answers after its own fsync.
- QUORUM: unaffected while a majority of the replicas is available. The batch commits without the slow
  or dead replica.
- ALL: the produce waits for every replica. When one is down or slow, the produce fails with
  `NOT_ENOUGH_REPLICAS` after `produce.timeout.ms` (default 3000 ms). The batch may still be committed.

An AppendEntries to a dead or slow follower times out after `raft.election.timeout.max.ms`. The leader then
retries on its next heartbeat, and the outstanding-RPC guard keeps at most one AppendEntries in flight per
peer.

Storage error. An I/O or invariant error on a partition replica is fail-stop for that partition only. The
replica sets `failed`, stops its `RaftNode` (it no longer votes, leads or serves), fails its pending
produces with `ERROR`, and answers `ERROR` to later requests. The message is printed to standard error.
The broker process and its other partitions keep running. The fail-stop path has no test (see Known
limitations).

Malformed input. A frame that cannot be decoded, is oversized, or has trailing bytes gets
`ErrorResponse(INVALID_REQUEST)` and the connection is closed. A produce with an empty batch or an oversized
value gets `INVALID_REQUEST` without touching the log. A request for a partition this broker does not host
gets `NOT_REPLICA` or `UNKNOWN_PARTITION`. Protocol bounds (in-flight limit, queue limit, fetch size) return
`BUSY` or cap the size, without allocating from unchecked input.

## Known limitations

- Power loss or kernel crash is not tested. SIGKILL does not lose page-cache data, so the correctness of the
  fsync-based durability has not been verified under power loss.
- An I/O error during an append: the fail-stop path exists, but no test exercises it.
- Tested only on macOS (arm64, OpenJDK 25). The CI configuration targets JDK 17 on Linux, and the current code
  has not been run there.
- Long runs, large logs and more than four brokers have not been tested.
- Network partitions between live processes have not been tested. Minority tests stop brokers instead.
- Bit rot in the final record of the active segment cannot be told apart from a torn write, so it is truncated.
- No pre-vote. No snapshots: the log grows without bound and start-up scans all of it.
- Static membership only. No membership change and no reassignment at runtime.
- No consumer groups and no stored consumer offsets. Consumer positions are in memory only.
- No idempotent producer and no sequence numbers. Delivery is at-least-once or at-most-once, never exactly-once.
- No TLS, no authentication, no idle-connection timeout, and no flow control through channel writability.
- Metrics are four in-process counters with no exporter.
- The only benchmark is single-broker and unreplicated (results in `docs/HANDOFF.md`). No replicated benchmark has been run.
