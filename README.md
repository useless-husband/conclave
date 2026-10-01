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
  symmetric and one-way partitions), simulated disks (a crash keeps only
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
- **It found real bugs.** During development the simulator found a
  write-ahead-log recovery bug that could silently drop acknowledged writes
  two restarts after an unlucky crash ([details](docs/DESIGN.md#4-the-write-ahead-log)).

Standard library only. About 9,000 lines of Go plus 3,500 lines of tests.

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

And the simulator, replaying one seed:

```console
$ go run ./cmd/conclave sim -seed 42
seed 42: profile nodes=3 clients=3 keys=1 drop=30‰ dup=10‰ delay=0.000100s..0.005000s spike=0‰/1.000000s partition/3.000000s pause/2.000000s midio=50% torn=100% skew=5% transition-crash=0% maxents=1 snapevery=1000 segment=16384 sessions=4096 timeout=0.030000s deadline=0.300000s think<=0.001000s
25.000000s simulated in 568ms: 532295 events, 514097 messages (16694 dropped), 4381 client operations (21 unknown), 0 crashes (0 mid-I/O, 0 torn writes), 3 partitions, 15 pauses, 9 elections, max term 9, 0 snapshots installed, 0 members added, 0 removed
history: 4387 operations checked (15 dropped as unobservable), linearizable, 7608 search steps
digest df27f74703e53d2a
```

## What a failure looks like

With the `read-without-quorum` bug switched on (a leader answers reads
without first confirming with a round of heartbeats that it is still
leader), seed 74 ends like this (abridged):

```
{{FAILURE}}
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

{{MUTATIONS}}

### The unmodified code

{{SWEEP}}

### Real processes

`go test ./test/e2e -v` builds the binary, starts a 3-server cluster,
runs six concurrent clients against it for 20 seconds while a server is
killed with SIGKILL (by PID, half the time the leader) every 1 to 2.5
seconds and restarted on its data directory, and checks the recorded history
with the same checker. A copy of the history with one read altered must be
rejected, so the check cannot pass vacuously.

{{E2E}}

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
  servers right after protocol transitions (granting a vote, becoming leader)
  and drops the messages a crashing server had just sent, because random
  timing rarely hits those windows.
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
