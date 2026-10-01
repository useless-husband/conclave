# conclave

**A replicated, linearizable key-value store on a Raft written from scratch, tested by a deterministic simulator that runs the real server code under seeded faults and checks every run for linearizability.**

[繁體中文說明](README.zh-TW.md) · [Design](docs/DESIGN.md) · [導讀（中文入門）](docs/導讀.zh-TW.md)

There are hundreds of Raft implementations on GitHub. Most are tested by a
handful of scripted scenarios, which show that the code works when nothing
unusual happens. conclave is built around the evidence that it is correct
when things do go wrong:

- **The real code, under simulated failure.** The consensus core, the
  request path, the write-ahead log and the state machine have no goroutines,
  clocks or I/O of their own. The `conclave` binary drives them with TCP, a
  disk and a wall clock; the simulator drives *the same code* from one
  goroutine with a simulated network (delay, loss, duplication, reordering,
  symmetric and one-way partitions between servers), simulated disks (a crash keeps only
  synced bytes, may tear the last write, and can strike in the middle of a
  write, fsync or rename), crashes, pauses, membership changes and client
  load, all drawn from one seed. The same seed gives a bit-identical run.
- **Every run is checked.** Election safety and state-machine safety while it
  runs; liveness after all faults heal; replica convergence; and
  linearizability of every client operation, including writes whose outcome
  the client never learned, by a checker written for this project.
- **The checks have teeth.** Ten deliberately wrong versions of single lines
  of the real code (the classic Raft mistakes) can be switched on from tests
  only. The simulator catches every one of them within a bounded number of
  seeds; the table below says how many.
- **It found a real bug.** During development the simulator found a
  write-ahead-log recovery bug that could silently drop acknowledged writes
  two restarts after an unlucky crash ([details](docs/DESIGN.md#4-the-write-ahead-log)).
  It also exposed two modelling errors in its own simulated clients, each
  of which had produced a false alarm; every detection in the mutation table
  below is therefore re-run without the injected bug, and must pass.

Standard library only: about 9,000 lines of Go and 3,700 lines of tests.

## Try it

```console
$ scripts/cluster.sh start          # builds, starts 3 servers on 127.0.0.1 with ephemeral ports, joins them
cluster running: n1 pid 78069, n2 pid 78099, n3 pid 78117
client addresses: 127.0.0.1:61697,127.0.0.1:61699,127.0.0.1:61701
use it with:  . .cluster/env && .cluster/conclave put greeting hello && .cluster/conclave get greeting
$ . .cluster/env
$ .cluster/conclave put greeting hello
ok
$ .cluster/conclave get greeting
hello
$ .cluster/conclave cas greeting hello bonjour
ok
$ .cluster/conclave cas greeting hello hola
conclave: cas failed: current value is "bonjour"
$ .cluster/conclave delete greeting
deleted (was "bonjour")
$ scripts/cluster.sh kill 1         # SIGKILL the leader, by PID
killed n1 (pid 78069)
$ .cluster/conclave put survivor yes
ok
$ scripts/cluster.sh restart 1      # same data directory, same ports
n1 running again, pid 78176
$ scripts/cluster.sh stop
```

The same with curl (every error body carries a machine-readable `code`):

```console
$ curl -s -X PUT --data hello http://127.0.0.1:61828/v1/kv/greeting
{"index":4}
$ curl -s http://127.0.0.1:61828/v1/kv/greeting
{"found":true,"value":"hello","index":4}
$ curl -s -X POST -d '{"expect":"hi","value":"x"}' http://127.0.0.1:61828/v1/cas/greeting
{"found":true,"value":"hello","swapped":false,"index":5}
$ curl -s http://127.0.0.1:61830/v1/kv/greeting          # a follower
{"code":"not-leader","error":"this server is not the leader","leader":1,"leader_api":"127.0.0.1:61828"}
$ curl -s -X POST http://127.0.0.1:61828/v1/sessions
{"index":7,"session":7}
$ curl -s -X POST -H 'Conclave-Session: 7' -H 'Conclave-Seq: 1' -d '{"expect_absent":true,"value":"first"}' http://127.0.0.1:61828/v1/cas/k
{"found":false,"swapped":true,"index":8}
$ # the same write retried: the cached result, not a second (failing) application
$ curl -s -X POST -H 'Conclave-Session: 7' -H 'Conclave-Seq: 1' -d '{"expect_absent":true,"value":"first"}' http://127.0.0.1:61828/v1/cas/k
{"found":false,"swapped":true,"index":9}
```

And the simulator, replaying one seed (servers crash in the middle of disk
writes every 0.7 s on average, members are added and removed, leadership is
transferred):

```console
$ go run ./cmd/conclave sim -seed 17
seed 17: profile nodes=3 clients=6 keys=1 drop=0‰ dup=10‰ delay=0.000100s..0.001000s spike=50‰/1.000000s crash/0.700000s membership/2.000000s transfer/2.000000s midio=100% torn=0% skew=5% transition-crash=0% reconfigure-on-election maxents=64 snapevery=50 segment=16384 sessions=4096 timeout=0.030000s deadline=5.000000s think<=0.020000s
25.000000s simulated in 113ms: 96705 events, 55378 messages (0 dropped), 4549 client operations (0 unknown), 27 crashes (27 mid-I/O, 0 torn writes), 0 partitions, 0 pauses, 19 elections, max term 25, 12 snapshots installed, 3 members added, 2 removed
history: 4549 operations checked (0 dropped as unobservable), linearizable, 12775 search steps
digest ba6d09b8888aa8b2
$ go run ./cmd/conclave sim -seed 17 -trace | grep -E 'CRASH|restart|ADD|leader term|torn'
0.101551s  n2 leader term=1 last=0
0.102250s  ADD n4 requested at leader n2
0.102250s  ADD n4 at n2: retry
0.318293s  n2 CRASH (crashed in the middle of disk I/O)
0.721578s  n2 restart
0.721578s  n2 recovery cut a torn tail of 5 bytes
0.768317s  n1 leader term=2 last=43
0.769162s  ADD n4 requested at leader n1
0.771044s  ADD n4 at n1: ok
0.880049s  n1 CRASH (crashed in the middle of disk I/O)
...
```

## What a failure looks like

With the `read-without-quorum` bug switched on (a leader answers reads
without first confirming with a round of heartbeats that it is still
leader), seed 74 ends like this (abridged). A partition cuts the leader n2
off from n1 and n3 at 0.999 s; n3 wins term 2 and acknowledges two puts by
1.602 s; at 1.605 s a client asks n2, which still believes it leads (old,
delayed acknowledgements kept its check-quorum satisfied), and n2 answers
with the value from before the partition:

```
seed 74: FAILED with 1 violation(s)
profile: nodes=3 clients=5 keys=1 drop=0‰ dup=50‰ delay=0.000100s..0.005000s spike=50‰/1.000000s partition/0.700000s crash/3.000000s membership/2.000000s transfer/2.000000s ...
mutations: read-without-quorum
replay:  go run ./cmd/conclave sim -seed 74 -trace

[25.000000s] linearizability: key "k0" (3996 operations) is not linearizable
longest linearizable prefix found: 192 of 3996 operations, the last 12 of them:
  ...
  client 2 [1001657, 1601592] put("k0", "c2.34") -> ok  -> "c2.34"
  client 4 [1093977, 1599056] put("k0", "c4.44") -> ok  -> "c4.44"
  client 1 [1065373, 1612328] put("k0", "c1.40") -> ok  -> "c1.40"
  client 0 [997325, 1617696] cas("k0", "c0.31", "c0.32") -> cas-failed  -> "c1.40"
  client 4 [1605396, 1617183] cas("k0", "c4.44", "c4.45") -> cas-failed  -> "c1.40"
in state "c1.40" nothing can be linearized before the return of
  client 2 [1604659, 1608602] get("k0") -> "c0.31"
...
trace from 0.741610s to 1.628602s, around the operations the linearizability checker could not place:
...
0.998677s  PARTITION split n1-x>n2 n2-x>n1 n2-x>n3 n3-x>n2
...
1.542934s  n1 votes for n3 term=2
1.547030s  n3 leader term=2 last=121
1.599056s  client 4 put("k0", "c4.44") -> ok (index 123)
1.601592s  client 2 put("k0", "c2.34") -> ok (index 124)
1.604659s  client 2 sends get("k0") to n2 (request 440)
1.608602s  client 2 get("k0") -> ok "c0.31" (index 121)
...
```

The seed, the fault profile, the operations the checker could not place,
and the stretch of the run around them, recovered by replaying the seed:
that is all a developer needs to reproduce and debug the failure.

## Results

### Every injected bug is caught

`go test ./internal/sim -run TestMutationsAreDetected -v` runs seeds 1, 2,
3, ... with one bug switched on until the simulator reports a safety
violation, then re-runs that seed without the bug and requires it to pass.
The test fails if a bug survives past its bound (about twice the number
below).

| injected bug (one line of the real code changed) | first failing seed | caught by |
|---|---:|---|
| `vote-without-log-check`: grant a vote without the "candidate's log is at least as up to date" check (§5.4.1) | 2 | state-machine safety, linearizability |
| `commit-prior-term-by-count`: commit an old-term entry once a majority stores it (Figure 8) | 5,584 | state-machine safety |
| `vote-not-persisted`: answer a vote request without first writing the vote to disk | 383 | election safety (two leaders in one term) |
| `ack-before-fsync`: acknowledge appended entries (and count the leader's own copy) before fsync | 3 | state-machine safety |
| `read-without-quorum`: serve a read from a leader without the ReadIndex heartbeat round | 74 | linearizability (a deposed leader's stale read) |
| `duplicate-apply`: ignore the session table, so a retried write is applied again | 1 | linearizability |
| `skip-wal-checksum`: replay WAL records without verifying their checksum | 241 | state-machine safety (a torn record replayed as data) |
| `skip-dir-sync`: do not fsync the directory after creating a WAL segment | 5 | state-machine safety, election safety, linearizability |
| `truncate-without-marker`: install a leader's snapshot without the record that voids the old log | 1 | recovery (the WAL no longer opens) |
| `conf-change-before-term-commit`: propose a membership change before committing in the term (Ongaro, 2015) | 3,358 | state-machine safety |

All ten together took 314 s on four workers. The two expensive ones need a
precise interleaving. For Figure 8, an entry of an old term must reach a
majority only after a leader of a later term has written a different entry
at the same index, and the leader that counted those replicas must lose
leadership before any entry of its own term is stored on a majority.
The membership bug, which `TestOngaro2015MembershipBug` also replays step by
step, needs two overlapping reconfigurations by leaders of different terms.
Neither was found in 2,000 seeds until the simulator learned to lose a
crashing server's unsent messages (power loss) and to crash a leader just as
it advances its commit index.

### The unmodified code

{{SWEEP}}

### Real processes

`go test ./test/e2e -v` builds the binary, starts a 3-server cluster,
runs six concurrent clients against it for 20 seconds while a server is
killed with SIGKILL (by PID, half the time the leader) every 1 to 2.5
seconds and restarted on its data directory, and checks the recorded history
with the same checker. A copy of the history with one read altered must be
rejected, so the check cannot pass vacuously.

Three runs on this machine (seeds 1, 2, 3; 20 s of faults each, then 3 s
without). Half of the clients give up on an operation after 300 ms, so some
writes end with an unknown outcome while a leader is being replaced:

| seed | SIGKILLs | operations completed | writes with unknown outcome | verdict | check time |
|---|---|---|---|---|---|
| 1 | 8 | 7,616 | 6 | linearizable | 6 ms |
| 2 | 8 | 6,924 | 4 | linearizable | 6 ms |
| 3 | 8 | 7,498 | 4 | linearizable | 5 ms |

### Performance

{{BENCH}}

## How it works

```
                 +------------------------ internal/node -------------------------+
  requests ────▶ |  proposals, ReadIndex reads, client sessions, responses         |
  messages ────▶ |  internal/raft (pure state machine) + internal/wal + internal/kv |
  ticks ───────▶ |                                                                 |
                 +-----------------------------------------------------------------+
                       ▲ the same code, driven by one goroutine in either world ▲
   production: internal/server                    simulation: internal/sim
   wall clock, TCP, files + F_FULLFSYNC,          virtual time, simulated network and
   HTTP API, Go client                            disks, nemesis, clients, checkers
```

- **Raft** (`internal/raft`): leader election with pre-vote, leader
  stickiness and check-quorum; pipelined log replication with fast conflict
  backtracking; commitment of current-term entries only (Figure 8); snapshots
  and InstallSnapshot with log compaction; single-server membership change
  with the 2015 fix; leadership transfer; ReadIndex reads. The core starts no
  goroutines and reads no clock. One function, `Flush`, encodes the
  durability rule: appends may leave before the local fsync, votes and
  acknowledgements only after it.
- **Write-ahead log** (`internal/wal`): segmented, CRC32C over every record;
  recovery cuts a write torn by a crash off the end of the last segment and
  refuses to start on damage anywhere else; snapshots are written with the
  temp-file, fsync, rename, directory-fsync sequence; `F_FULLFSYNC` on macOS.
- **State machine** (`internal/kv`): get, put, delete, compare-and-swap, and
  client sessions that make a retried write take effect exactly once.
- **Simulator** (`internal/sim`): discrete-event, single-threaded, every
  decision from one seed; each seed draws its own fault profile. It crashes
  servers right after protocol transitions (granting a vote, becoming leader,
  a new leader's first commit) and, in power-loss crashes, drops the messages
  the server had just sent, because random timing rarely hits those windows.
- **Checker** (`internal/lincheck`): Wing–Gong–Lowe search with memoization,
  partitioned per key, with sound reductions for operations of unknown
  outcome; cross-checked against brute-force enumeration.

[docs/DESIGN.md](docs/DESIGN.md) explains each part, the hard problems and the
alternatives that were rejected.

## API

| Request | Effect |
|---|---|
| `GET /v1/kv/{key}` | linearizable read; 200 `{"found":true,"value":...}` or 404 |
| `PUT /v1/kv/{key}` (body = value) | set |
| `DELETE /v1/kv/{key}` | delete; returns the removed value |
| `POST /v1/cas/{key}` `{"expect":...,"expect_absent":false,"value":...}` | compare-and-swap; 409 with the current value on mismatch |
| `POST /v1/sessions` | open a client session |
| `GET /v1/status` | the server's view: role, term, leader, commit, members |
| `POST /v1/members` `{"id":N,"raft":...,"api":...}`, `DELETE /v1/members/{id}` | membership change |
| `POST /v1/transfer/{id}` | leadership transfer |

Writes may carry `Conclave-Session` and `Conclave-Seq` headers; a retry with
the same pair takes effect at most once. Errors distinguish *not executed*
(`not-leader`, `retry`) from *outcome unknown* (`unknown`, `timeout`,
`session-expired`). The Go client (`client`) follows leader hints, retries
writes with the same sequence number and returns `client.ErrUnknown` only
when a write may or may not have taken effect.

## Limitations

- **One machine.** All measurements are of three processes on one laptop
  sharing one SSD, whose cache flushes serialize; numbers on separate machines
  would differ. The machine was shared with other work while they were taken.
- **The simulator's coverage is statistical.** It found the injected bugs and
  one real one, but a bug that needs a fault sequence it does not generate, or
  more seeds than were run, would pass. The two hardest injected bugs took
  thousands of seeds.
- **No byzantine faults.** Disks may lose unsynced data and tear the last
  write, but silent corruption of synced data is detected only by checksums
  and stops the server; it is not repaired from other replicas.
- **Membership change is one server at a time** (no joint consensus), and a
  removed server's ID must never be reused.
- **No authentication or TLS** on either port. Bind them to trusted networks.
- **Reads are served by the leader only**, through ReadIndex; there are no
  follower reads or leases.
- **The state machine is in memory**; its size is bounded by RAM, and a
  snapshot is a full copy.
- **No dynamic reconfiguration of addresses**: a server must come back on the
  addresses it was added with.

## Related work

To my knowledge, few open-source Raft implementations ship a deterministic
simulator of their own production code together with an independent
linearizability checker and a measured mutation table; the ideas themselves
are well established:

- **FoundationDB** ([simulation testing](https://apple.github.io/foundationdb/testing.html))
  established deterministic simulation of a whole distributed database as an
  engineering practice; **TigerBeetle**'s VOPR applies it to a replicated
  ledger with injected storage faults. conclave applies the approach to a much
  smaller system and adds the per-run linearizability check and the mutation
  table.
- **etcd/raft** and **hashicorp/raft** are the production-grade Go Raft
  libraries. etcd/raft's design (a core driven by `Tick`/`Step`/`Ready`)
  inspired the shape of the core here; no code is shared. Both are far more
  complete and battle-tested.
- **Jepsen / Knossos** and **Porcupine** check histories of real systems for
  linearizability. conclave's checker is its own implementation of the same
  family of algorithms, with reductions tailored to its key-value model.
- **MIT 6.5840 (6.824)** labs test student Raft and key-value implementations
  over a simulated unreliable network with crashes, and the key-value lab
  checks histories with Porcupine. The schedule there comes from real
  goroutines and timers, so a failing run cannot be replayed from a seed.
- **MadSim** and **turmoil** (Rust) provide deterministic simulation runtimes;
  conclave instead keeps its core free of I/O so that no runtime is needed.
- A sibling project, **linproof**, is a separately written linearizability
  checker verified in Lean; conclave does not use it.

## Build and test

Requires Go 1.23 or later. No other dependencies.

```sh
make build        # bin/conclave
make short        # unit tests, a short seed sweep, a short kill/restart run
make test         # everything, including the mutation table (several minutes)
make race         # short tests under the race detector
make sweep SEEDS=1-10000 WORKERS=4
make mutations    # print the mutation table
make e2e          # kill/restart test with real processes
make bench        # scripts/bench.sh: the performance table above
make lint         # gofmt, go vet, staticcheck
```

Replay any simulator seed with `go run ./cmd/conclave sim -seed N -trace`.

## License

[MIT](LICENSE)
