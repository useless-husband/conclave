package sim

import (
	"fmt"
	"strings"

	"github.com/useless-husband/conclave/internal/prng"
)

// Time is virtual time in microseconds.
type Time int64

const (
	Microsecond Time = 1
	Millisecond      = 1000 * Microsecond
	Second           = 1000 * Millisecond
)

func (t Time) String() string { return fmt.Sprintf("%d.%06ds", t/Second, t%Second) }

// Profile is the fault and workload mix of one run. Each seed draws its own
// profile ("swarm testing", Groce et al. 2012): a run with only crashes, or
// only partitions, reaches states that a run with everything at once
// rarely does.
type Profile struct {
	Nodes   int // initial voters
	Clients int
	Keys    int

	// Network.
	DropPermille  int
	DupPermille   int
	DelayMin      Time
	DelayMax      Time
	SpikePermille int // chance of a much longer delay, which reorders
	SpikeMax      Time

	// Mean time between nemesis actions of each kind; zero disables it.
	PartitionEvery  Time
	CrashEvery      Time
	PauseEvery      Time
	MembershipEvery Time
	TransferEvery   Time

	// MidIOPercent is the share of crashes that strike in the middle of a
	// write, sync or rename instead of between two events.
	MidIOPercent int
	// TornPercent is the chance that the surviving part of an unsynced
	// write ends in garbage.
	TornPercent int
	// SkewPercent bounds how much faster or slower each node's clock
	// ticks.
	SkewPercent int
	// TransitionCrashPercent is the chance that a server crashes within
	// a few milliseconds of granting a vote or becoming leader, and
	// restarts within a few more. Protocol bugs live at these
	// transitions, and random crash times rarely hit them.
	TransitionCrashPercent int
	// ReconfigureOnElection makes the operator request a membership
	// change as soon as a new leader appears (when MembershipEvery is
	// set), as automation replacing a failed server would.
	ReconfigureOnElection bool

	// Server configuration.
	MaxEntriesPerMsg int
	SnapshotEvery    uint64
	SegmentSize      int64
	MaxSessions      int

	// Clients.
	ClientTimeout Time // per attempt
	OpDeadline    Time // per operation, across retries
	ThinkMax      Time
}

func pick[T any](rng *prng.Rand, xs ...T) T { return xs[rng.Intn(len(xs))] }

// RandomProfile draws a profile from rng.
func RandomProfile(rng *prng.Rand) Profile {
	p := Profile{
		Nodes:   pick(rng, 3, 3, 5),
		Clients: 2 + rng.Intn(5),
		Keys:    1 + rng.Intn(4),

		DropPermille:  pick(rng, 0, 5, 30, 100),
		DupPermille:   pick(rng, 0, 10, 50),
		DelayMin:      100 * Microsecond,
		DelayMax:      pick(rng, 1*Millisecond, 5*Millisecond, 30*Millisecond),
		SpikePermille: pick(rng, 0, 10, 50),
		SpikeMax:      pick(rng, 200*Millisecond, 1*Second),

		PartitionEvery:  pick(rng, 0, 3*Second, 700*Millisecond),
		CrashEvery:      pick(rng, 0, 3*Second, 700*Millisecond),
		PauseEvery:      pick(rng, 0, 0, 2*Second),
		MembershipEvery: pick(rng, 0, 0, 2*Second),
		TransferEvery:   pick(rng, 0, 0, 2*Second),

		MidIOPercent: pick(rng, 0, 50, 100),
		TornPercent:  pick(rng, 0, 50, 100),
		SkewPercent:  pick(rng, 0, 5, 20),

		TransitionCrashPercent: pick(rng, 0, 0, 10, 40),
		ReconfigureOnElection:  rng.Chance(1, 2),

		MaxEntriesPerMsg: pick(rng, 1, 4, 64),
		SnapshotEvery:    pick[uint64](rng, 8, 50, 1000),
		SegmentSize:      pick[int64](rng, 1<<10, 16<<10, 1<<20),

		ClientTimeout: pick(rng, 30*Millisecond, 150*Millisecond, 1*Second),
		ThinkMax:      pick(rng, 1*Millisecond, 20*Millisecond),
	}
	p.MaxSessions = pick(rng, p.Clients, 4096)
	// Clients keep retrying an operation for a while, as real ones do;
	// an operation they give up on enters the history with an unknown
	// outcome, which weakens the check.
	p.OpDeadline = max(p.ClientTimeout*3, pick(rng, 300*Millisecond, 2*Second, 5*Second))
	return p
}

func (p Profile) String() string {
	var b strings.Builder
	fmt.Fprintf(&b, "nodes=%d clients=%d keys=%d", p.Nodes, p.Clients, p.Keys)
	fmt.Fprintf(&b, " drop=%d‰ dup=%d‰ delay=%v..%v spike=%d‰/%v", p.DropPermille, p.DupPermille,
		p.DelayMin, p.DelayMax, p.SpikePermille, p.SpikeMax)
	every := func(name string, t Time) {
		if t > 0 {
			fmt.Fprintf(&b, " %s/%v", name, t)
		}
	}
	every("partition", p.PartitionEvery)
	every("crash", p.CrashEvery)
	every("pause", p.PauseEvery)
	every("membership", p.MembershipEvery)
	every("transfer", p.TransferEvery)
	fmt.Fprintf(&b, " midio=%d%% torn=%d%% skew=%d%% transition-crash=%d%%", p.MidIOPercent, p.TornPercent, p.SkewPercent, p.TransitionCrashPercent)
	if p.MembershipEvery > 0 && p.ReconfigureOnElection {
		b.WriteString(" reconfigure-on-election")
	}
	fmt.Fprintf(&b, " maxents=%d snapevery=%d segment=%d sessions=%d", p.MaxEntriesPerMsg, p.SnapshotEvery, p.SegmentSize, p.MaxSessions)
	fmt.Fprintf(&b, " timeout=%v deadline=%v think<=%v", p.ClientTimeout, p.OpDeadline, p.ThinkMax)
	return b.String()
}
