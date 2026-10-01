# Changelog

## 0.1.0 (unreleased)

First complete version.

- Raft written from scratch as a deterministic state machine: pre-vote,
  leader stickiness and check-quorum, pipelined replication with fast
  backtracking, snapshots and InstallSnapshot, single-server membership
  change, leadership transfer, ReadIndex reads.
- Checksummed, segmented write-ahead log with torn-write recovery and
  `F_FULLFSYNC` on macOS.
- Key-value state machine (get, put, delete, compare-and-swap) with client
  sessions for exactly-once writes.
- Deterministic simulator running the real node code under seeded network,
  disk, crash, pause and membership faults, with election-safety,
  state-machine-safety, liveness, replica-convergence and linearizability
  checks.
- Linearizability checker (Wing–Gong–Lowe search with sound reductions for
  operations of unknown outcome).
- Ten injected bugs, each detected by the simulator within a bounded number
  of seeds.
- `conclave` binary: server, CLI client, benchmark, simulator; HTTP API; Go
  client; `scripts/cluster.sh`; kill/restart test with real processes.

### Fixed during development

- WAL: a crash between writing an installed snapshot and the reset record
  that voids the old log could, two restarts later, make the server refuse
  to start or silently drop acknowledged entries. Found by the simulator.
