# KafkaDS Design

This document describes how KafkaDS works: the architecture, the wire protocol, the
on-disk format, the replication and high-watermark semantics, the Raft implementation,
and the concurrency model. It also records the key "why" decisions, which are the most
useful thing to be able to explain.

## Architecture overview

```
            produce / fetch
client  ───────────────────────────▶  Broker (leader)
                                         │  append to Log
                                         │  replicate to followers
                                         ▼
                                   Broker (follower) × N
```

A `Broker` owns:
- a Netty `Server` speaking the binary protocol,
- one or more `Partition`s, each wrapping an append-only `Log` plus a high-watermark,
- on the leader, a `ReplicationManager` that ships records to followers,
- optionally a `RaftNode` (+ scheduler + network transport) that elects the leader.

All requests funnel through `Broker.handle(Message)`, which returns a
`CompletableFuture<Message>` so that slow paths (e.g. `acks=all` waiting for commit) never
block the Netty event loop.

## Wire protocol

Every frame is length-prefixed: `[4-byte length][1-byte api key][payload]`. The 4-byte
length is added/stripped by Netty's `LengthFieldPrepender` / `LengthFieldBasedFrameDecoder`;
the api key and payload are handled by `MessageEncoder` / `MessageDecoder`. All api keys are
reserved up front (`ApiKey`) so the framing layer never changes between build phases.

API keys: `PING`, `PRODUCE`/`PRODUCE_RESP`, `FETCH`/`FETCH_RESP`, `REPLICATE`/`REPLICATE_ACK`,
`REQUEST_VOTE`/`REQUEST_VOTE_RESP`, `APPEND_ENTRIES`/`APPEND_ENTRIES_RESP`.

Some api keys (e.g. `PING`) are shared by request and response; the decoding `Side`
(SERVER vs CLIENT) disambiguates which concrete type to build.

## On-disk log format

Each partition is a directory of rolling segments. A segment is a `.log` file plus a
sparse `.index` file, both named by the segment's base offset (e.g. `00000000000000000000`).

Record layout in the `.log` file:

```
[8-byte offset][4-byte payload length][4-byte CRC32][payload bytes]
```

The CRC covers the payload and is verified on every read (`CorruptRecordException` on
mismatch). The `.index` file holds `[4-byte relative offset][4-byte file position]` entries,
one roughly every `index.interval.bytes` of log data.

### Reads and the offset index

To read from a target offset, the index is consulted for the greatest indexed offset `<=`
the target (a floor lookup); the log is then scanned forward from that file position. This
is the same sparse-index strategy Kafka uses: it keeps the index small while bounding the
scan to one interval.

### Crash recovery

On open, a segment finds the last entry in its `.index`, seeks the `.log` to that position,
and scans forward, validating record headers, to rebuild the true end offset (and to add any
missing index entries). A partial trailing record from a crash mid-write is detected (its
declared length runs past end-of-file) and truncated, so the log is always left in a clean,
append-ready state. `MillionRecordRestartTest` exercises this at scale.

## Replication and the high-watermark

The **high-watermark (HWM)** is the exclusive upper bound of committed offsets. Consumers
never read at or beyond it, so they only ever observe replicated, durable records.

On the leader, the `ReplicationManager` runs one loop per follower. Each loop ships the
records the follower is missing (starting from the follower's last known log end offset) and
applies the follower's acknowledged offset back to the partition. The HWM is then recomputed
as the minimum log-end-offset across the leader and all followers:

```
highWatermark = min(leaderLEO, min over followers of followerLEO)
```

- `acks=1` (LEADER): the producer is acknowledged as soon as the leader appends locally.
  Replication still proceeds and the HWM advances asynchronously as followers catch up.
- `acks=all` (ALL): the producer blocks (via a commit future) until the HWM covers its
  records, i.e. all followers have replicated them. If a follower is down, the HWM cannot
  advance past it, the produce times out with `NOT_ENOUGH_REPLICAS`, and consumers never see
  the un-replicated records. This safety property is verified by `ReplicationTest`.

This is a deliberately simple model (no ISR shrink/expand): with a fixed replica set, a dead
follower holds the HWM back, which is the conservative, safe behavior.

## Raft

`RaftNode` implements the Raft consensus algorithm (Ongaro & Ousterhout) and is the most
involved part of the system. It is written to be transport- and clock-agnostic so the exact
same logic is testable deterministically in-memory and runnable over the network.

- **Persistent state**: `currentTerm`, `votedFor`, and the log survive restarts
  (`FileRaftPersistence`); `FileRaftPersistenceTest` covers recovery and truncation.
- **Elections**: randomized election timeouts; a candidate increments its term, votes for
  itself, and wins on a majority. The up-to-date-log check (`lastLogTerm`, then `lastLogIndex`)
  prevents a node with a stale log from becoming leader.
- **Log replication**: `AppendEntries` carries `prevLogIndex`/`prevLogTerm` for the log
  matching property; conflicting follower entries are truncated and replaced. The leader
  advances `commitIndex` only for entries from its current term once a majority has matched,
  then applies committed commands to the state machine.
- **Timing**: `tick(now)` drives timeouts; tests call `startElection()` /
  `broadcastHeartbeat()` directly for determinism, while `RaftScheduler` drives real time in
  production.

Safety properties are tested directly in `RaftConsensusTest`: single-leader election,
replication and commit, re-election after a leader crash, rejection of stale candidates, and
overwriting a conflicting follower tail. `RaftFailoverNetworkTest` proves election and
failover over the real Netty transport.

### Why Raft, not a heartbeat hack?

A simple "lowest live node id is leader" scheme cannot safely decide leadership under
partitions (two sides can each think they are in charge) and gives no guarantee that a new
leader has all committed data. Raft provides both: a single leader per term via majority
voting, and the guarantee (via the up-to-date-log vote check + commit rules) that a newly
elected leader's log contains every committed entry.

## Concurrency model

- **Netty event loops** handle all socket I/O. Business logic never blocks an event loop:
  `Broker.handle` returns a `CompletableFuture`, and `acks=all` completes that future later
  from a replication thread.
- **The append-only `Log` is synchronized**, so appends and reads are serialized per
  partition; offsets are assigned under that lock.
- **Replication** uses one dedicated thread per follower on the leader. Each follower has at
  most one in-flight request, which keeps offsets ordered and makes catch-up trivial.
- **`RaftNode` is guarded by a single intrinsic lock**, so RPC handlers, timer ticks, and RPC
  response callbacks (which arrive on Netty threads) never interleave. RPC *sends* return
  futures and do not block while holding the lock for the response.

## Known simplifications

- Fixed partition count and a fixed replica set (no dynamic membership changes).
- The leader fsyncs on each produce, which bounds single-record latency; batching amortizes it.
- Raft and the Phase 2 replication path coexist: Raft is used for leadership/failover, while
  the segmented-log replication path handles bulk data movement. Unifying produce fully under
  Raft would be the natural next step.
