# Design

This document explains how conclave is built, which problems were hard, and
which alternatives were considered and rejected. The README says what the
system does; this says why it is shaped the way it is.

## 1. The shape of the code

```
                         +--------------------- internal/node ----------------------+
 requests ──Submit──▶    |  proposals, ReadIndex reads, responses                    |
 messages ──Step────▶    |  +-------------------+   +-------------+  +-----------+   |
 ticks ─────Tick────▶    |  |  internal/raft    |──▶| raft.Storage|  | internal/ |   |
                         |  |  (pure: no I/O,   |   |  = wal.WAL  |  | kv (state |   |
 responses ◀──Ready──    |  |   no clock, no    |   +------┬------+  |  machine, |   |
                         |  |   goroutines)     |──▶ Transport      |  sessions)|   |
                         |  +-------------------+          │        +-----------+   |
                         +------------------------------------│------------------------+
                                       │                      │
               production (internal/server)           simulation (internal/sim)
               one goroutine, time.Ticker             one goroutine, virtual time
               TCP (internal/transport)               simulated network
               files + F_FULLFSYNC (internal/vfs)     simulated disk (internal/simdisk)
               HTTP API, client library               simulated clients, nemesis, checker
```

Everything above the line is the same code in both worlds. The Raft core
(`internal/raft`) is a state machine driven by four kinds of calls: `Tick`,
`Step(message)`, `Propose`/`ReadIndex`/`ProposeConfChange`/`TransferLeadership`,
and `Flush`. It never reads a clock, never starts a goroutine and never
performs I/O itself; storage, network and randomness arrive as interfaces.
`internal/node` adds the request path (turning a client request into a
proposal or a ReadIndex round, and deciding the response) and the key-value
state machine. The production server and the simulator each drive a
`node.Node` from a single goroutine.

The point of this split is that the simulator does not test a model of the
system: it runs the request path, the consensus core, the write-ahead log
and the state machine that the `conclave` binary runs. What it replaces is
only what is below the interfaces: the clock, the network and the disk.

## 2. The durability contract: `Flush`

All effects of a batch of calls are released by one `Flush`, in this order:

1. messages that do not promise anything about local state leave first
   (appends and heartbeats from a leader: the leader does not need its own
   copy on disk to replicate it);
2. one fsync covers every log and hard-state write of the batch;
3. only then do promises leave: votes and append acknowledgements, and only
   then does a leader count its own copy towards a majority.

This is the place where a Raft implementation most often goes wrong in
practice, and two of the injected bugs live exactly here
(`vote-not-persisted`, `ack-before-fsync`). The core's unit tests record the
order of storage and network calls and assert it.

Before the fsync, `Raft.Early` hands out what is already decided: entries
committed by followers' acknowledgements and confirmed reads. They are
already on a majority of durable logs, so the server applies them and
answers their clients before it syncs the next batch. Without this, every
answer also waited for the fsync of requests that arrived after it.

A server does all of a batch's I/O synchronously in its loop. Group commit
comes from the loop draining every queued message and request before calling
`Ready`, so one fsync covers as many requests as arrived during the previous
one.

## 3. Raft

What is implemented, and the one or two decisions behind each:

- **Leader election with pre-vote** (thesis §9.6) and leader stickiness: a
  server that heard from a leader within the minimum election timeout ignores
  vote requests, so a partitioned or removed server cannot depose a working
  leader. Check-quorum makes a leader that has not heard from a majority for
  an election timeout step down; without it, stickiness plus a one-way
  partition would block elections forever.
- **Log replication** with pipelining (up to 64 appends in flight per
  follower) and **fast backtracking**: a rejection carries the conflicting
  term and the first index of that term, so the leader skips a whole
  divergent term per round trip (`TestFastBacktracking` repairs 100
  divergent entries in at most 2 rejections).
- **Commitment** only by counting replicas of an entry of the leader's own
  term (§5.4.2, Figure 8); earlier entries commit indirectly. A new leader
  appends a no-op at once so this happens promptly.
- **Snapshots and InstallSnapshot.** The node snapshots the state machine
  every N applied entries; the core keeps a configurable number of entries
  behind the snapshot so that slightly lagging followers are caught up from
  the log. A follower whose next entry has been compacted gets the snapshot.
- **Membership change, one server at a time** (thesis §4.1), with the fix
  for the bug Ongaro reported on raft-dev in 2015: a leader may not propose a
  change before it has committed an entry of its own term. A configuration
  takes effect when appended, not when committed, and is rolled back if the
  entry is truncated. A leader that removes itself steps down once the
  removal commits and hands off with TimeoutNow.
- **Leadership transfer** (thesis §3.10): stop accepting proposals, bring
  the target up to date, send TimeoutNow; give up after an election timeout.
- **Linearizable reads with ReadIndex** (thesis §6.4): the leader records
  its commit index, confirms with a round of heartbeats that a majority
  still follows it, and serves the read once it has applied that index. A
  new leader refuses reads until it has committed an entry of its term.

Rejected: **joint consensus** (single-server changes are enough for a key-
value store and much simpler to get right); **lease-based reads** (they trade
a network round for a dependence on bounded clock drift, which the simulator
would then have to model and the reader would have to trust); **asynchronous
disk writes in the core** (a leader appending and fsyncing in parallel with
sending is already what step 1 of Flush achieves, and a fully asynchronous
storage interface would double the state space for little gain on a single
SSD).

## 4. The write-ahead log

A data directory holds segment files and at most one snapshot file. A
segment is a sequence of records `| length | crc32c | type | payload |`. The
checksum covers the segment's sequence number as well as the record, so a
record copied into the wrong segment does not verify. Every segment begins
with a header and a copy of the hard state (term and vote), which makes any
suffix of the segment list self-contained: segments whose entries are all
covered by the snapshot are deleted from the front.

**Torn writes.** On recovery, the first record of the last segment that is
incomplete or fails its checksum is taken to be a write torn by the crash: it
and everything after it are cut off. Those bytes were never synced, so
nothing acknowledged is lost. The same damage in any earlier segment, which a
crash cannot explain, stops the server.

**Truncation in an append-only file.** A follower that must overwrite a
suffix of its log just appends the new entries; recovery replays records in
order and an entry whose index is not past the end replaces the suffix from
that index.

**Installing a snapshot** from the leader replaces the whole log. The
snapshot file is written first (temporary file, fsync, rename, fsync of the
directory), then a reset record that voids the old log. *A bug lived here.*
The first version handled a crash between the two steps by dropping the stale
log in memory during recovery, but left it on disk. Everything appended
afterwards then followed the stale entries in the file, and the *next*
recovery either refused to start (a gap in the log) or, worse, took the stale
branch for the log again and silently dropped new, acknowledged entries.
Recovery now writes the reset record itself. The simulator found this while
the `commit-prior-term-by-count` mutation was being measured: a run failed
with a recovery error that had nothing to do with the mutation. The WAL test
for this crash window now appends and recovers a second time
(`TestCrashDuringInstallSnapshot`).

**macOS.** `fsync(2)` on macOS hands data to the drive but does not ask the
drive to flush its cache; `fcntl(F_FULLFSYNC)` does. The OS file system
implementation issues `F_FULLFSYNC` explicitly (and on the directory, falling
back to `fsync` where the file system refuses). `-fsync fsync` and
`-fsync none` exist for benchmarks and say what they give up.

## 5. Client sessions and exactly-once writes

A client registers a session (a log entry; its index is the session ID) and
numbers its writes 1, 2, 3... The state machine remembers, per session, the
highest sequence number applied and its result. A command carrying that
number again is a retry: the cached result is returned and nothing is
applied. A lower number is a write the client gave up on and is refused, so
an old retry that surfaces late cannot overwrite newer data. Sessions are
evicted least-recently-used by log position, which every replica computes
identically; a write on an evicted session returns "session expired", and
the client must treat its outcome as unknown.

The server answers every request with one of: done (with the result),
not-leader (definitely not executed), retry (not executed), or unknown (this
server lost leadership with the write in its log; it may still commit). The
client retries on unknown with the same sequence number. Only if it runs out
of time after an attempt that might have executed does it report
`ErrUnknown`.

One subtlety cost a false alarm in the simulator before it was right: a
"not executed" answer covers only the attempt it answers. A duplicated, late
refusal of an earlier attempt says nothing about the attempt in flight. The
first simulated client took such a refusal as final, gave up on a write that
the in-flight attempt did commit, and recorded it as "not executed". The
checker correctly rejected the history. Both the simulated client and the
real one now track every attempt that has not been refused.

## 6. The simulator

**Event loop.** A binary heap of events ordered by (virtual time, insertion
number), one goroutine, virtual time in microseconds. Every random decision
comes from one seed through `internal/prng` (xoshiro256\*\*, specified
bit-for-bit so that a seed means the same run on every Go release). Each
component draws from its own forked stream, so adding a random decision in one
place does not reshuffle all the others. There is no map iteration on any
path that affects behaviour. `TestDeterminism` runs seeds three times
concurrently and compares a digest of every event, the trace and the history.

**What it injects.** Each seed draws a fault profile (swarm testing): network
delay range, loss, duplication, delay spikes that reorder; partitions
(isolate one server, split, one-way in either direction, a bridge server
seeing two halves, random links); crashes between events and in the middle of
a disk write, sync or rename, with the unsynced tail of a file partly kept and
possibly torn; process pauses; membership changes; leadership transfers;
clock skew; message size limits; snapshot frequency; WAL segment size; session
limits; client timeouts.

Two kinds of fault were added because random timing almost never hits the
moments where Raft bugs live:

- **Crashes at protocol transitions.** With some probability a server
  crashes within a few milliseconds of granting a vote, becoming leader, or
  (rarely) advancing its commit index, and restarts quickly. Before this
  existed, `vote-not-persisted` was not detected in the first 200 seeds.
- **Lost send buffers.** Half the crashes are power losses, which also lose
  the messages the server sent in the last two milliseconds: a machine that
  loses power takes its socket buffers with it (a killed process does not, so
  the other half keep them). This is what lets a new leader die with its
  newest entries existing only in its own log, the shape of the Figure 8
  scenario. An earlier version lost the buffer on every crash, which hid the
  `ack-before-fsync` bug: its acknowledgements leave just before the fsync
  the crash interrupts, so they were always lost with it.

**What it checks.** During the run: at most one leader per term (election
safety) and the same entry applied at every index by every server (state
machine safety). Any panic in the server code (it asserts, for example, that
it is never asked to overwrite a committed entry) is reported. At the end:
all faults are healed and clients must complete operations again (liveness);
servers that applied the same prefix must hold identical state; and the
clients' history must be linearizable.

**Clients** run one operation at a time with think time, retry like the real
client, and record each operation's invocation and response times. Their
requests and answers can be delayed and lost but are never duplicated: the
real client speaks HTTP over TCP, where a request cannot reach the server
twice. An early version did duplicate them, and a 40,000-seed sweep failed
at seed 16800: a server proposed a write and crashed, a duplicate of the
same request reached the restarted server and was refused, and the client
took that refusal as the answer to its attempt. The checker was right to
reject the history; the model of the client's connection was wrong. A write
whose outcome they never learn enters the history as unknown, bounded by the
time the same session's next write completed (after that the session refuses
the older sequence number, so the abandoned write took effect before or
never).

**Failure reports** give the seed, the profile, every violation and a trace.
A linearizability violation is only known after the run, so the simulator
re-runs the seed with full tracing and prints the stretch of virtual time
around the operations the checker could not place. Determinism makes this
free.

## 7. The linearizability checker

`internal/lincheck` is a Wing–Gong–Lowe search: call and return events in a
doubly linked list; any operation whose call precedes the first remaining
return may be linearized next; backtrack when nothing works; a cache of
(linearized set, state) pairs, hashed incrementally with Zobrist keys, prunes
repeated configurations. Histories are partitioned by key, which is sound
because linearizability is local (Herlihy and Wing).

Operations with unknown outcomes are what make real histories expensive. A
single key's history from one simulated run had a burst of twenty timed-out
deletes during an outage; the plain search needed 266 million steps for it.
Four reductions, each with an exchange argument in the code, bring that to
16 thousand:

1. **Unobserved unknown writes are dropped.** Every result that depends on a
   key's state reveals it (a delete returns the value it removed, a failed CAS
   the current value). So if no operation ever mentions the value a timed-out
   put or CAS wrote, any linearization in which it took effect can be turned
   into one where it did not.
2. **No-op applications are skipped.** Applying an unknown operation where it
   does not change the state is never necessary; leaving it pending can do
   everything that can.
3. **Symmetry.** Among pending unknown operations with the same effect
   (`Model.EffectKey`), only the one with the earliest bound is applied.
4. **Subsumption.** A configuration is pruned if a failed one had linearized
   the same known operations, reached the same state and decided a subset of
   its unknown operations.

`TestAgreesWithBruteForce` compares the search with exhaustive enumeration on
random histories built to be mostly non-linearizable, with repeated values
and bounded and unbounded unknown operations; it was run with 300,000
histories of up to 9 operations while the reductions were developed.

Rejected: using an existing checker (Porcupine, Knossos). Writing it was
part of the point, and the reductions above depend on knowing exactly what
each operation's result reveals. A sibling project (`linproof`) has a
separately written, proof-checked checker; conclave does not depend on it.

## 8. Mutations: does the harness have teeth?

`internal/mutation` defines ten deliberately wrong versions of single lines of
the real code (voting without the log check, committing an old-term entry by
counting replicas, not persisting the vote, acknowledging before fsync,
serving reads without the heartbeat round, applying a retried write twice,
accepting WAL records without checking the checksum, skipping the directory
fsync after creating a segment, installing a snapshot without the reset
record, proposing a membership change before committing in the term). They
can only be selected from a test binary: the constructor panics otherwise,
and no flag, environment variable or file reaches them. `TestMutationsAreDetected`
runs seeds 1, 2, 3, ... until the simulator reports a safety violation,
checks that the same seed passes without the mutation, and fails if the
bound is exceeded. The README lists how many seeds each took.

## 9. Production mode

`internal/server` runs a node with a 10 ms tick, the TCP transport and the
WAL on the real file system. The transport prefixes frames with their length
and starts each connection with a hello frame naming the sender and its
listen address: a server that has just been added does not know the
membership yet, but must still answer the leader. Per-peer queues are bounded
and drop when full; Raft retransmits what matters.

Membership metadata stores each member's addresses, so a server records the
addresses it bound in its data directory and binds the same ones after a
restart. A cluster starts as a single bootstrapped server; the others start
empty and are added through the API, which exercises membership change on
every cluster start.
