package sim

import (
	"flag"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/useless-husband/conclave/internal/mutation"
)

var (
	flagMutSeeds = flag.Int("sim.mutseeds", 0, "override the seed bound of TestMutationsAreDetected")
	flagReport   = flag.Bool("sim.report", false, "print the full failure report of each detection")
)

// mutationBound is the number of seeds within which the simulator must
// expose each injected bug. The bounds are about twice the number of seeds
// the detection actually took when the README table was produced, so that
// harmless changes to the simulator do not make the test flaky, while a
// harness that lost its teeth fails it.
var mutationBound = map[mutation.Mutation]int{
	mutation.VoteWithoutLogCheck:        200,
	mutation.CommitPriorTermByCount:     200,
	mutation.VoteNotPersisted:           200,
	mutation.AckBeforeFsync:             200,
	mutation.ReadWithoutQuorum:          200,
	mutation.DuplicateApply:             200,
	mutation.SkipWALChecksum:            200,
	mutation.SkipDirSync:                200,
	mutation.TruncateWithoutMarker:      200,
	mutation.ConfChangeBeforeTermCommit: 200,
}

// firstDetection runs seeds 1, 2, ... in parallel batches and returns the
// smallest seed whose run violates a safety property, or 0.
func firstDetection(t *testing.T, m mutation.Mutation, bound int) (uint64, *Result) {
	set := mutation.ForTest(t, m)
	const batch = 8
	for first := uint64(1); first <= uint64(bound); first += batch {
		n := min(batch, bound-int(first)+1)
		results := make([]*Result, n)
		var wg sync.WaitGroup
		var next atomic.Int64
		for w := 0; w < 4; w++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for {
					i := int(next.Add(1) - 1)
					if i >= n {
						return
					}
					results[i] = Run(Options{Seed: first + uint64(i), Mutations: set})
				}
			}()
		}
		wg.Wait()
		for _, r := range results {
			if r.SafetyViolated() {
				return r.Seed, r
			}
		}
	}
	return 0, nil
}

// TestMutationsAreDetected is the evidence that the simulator can tell
// correct code from subtly broken code: each injected bug must be caught
// within a bounded number of seeds.
func TestMutationsAreDetected(t *testing.T) {
	if testing.Short() {
		t.Skip("slow")
	}
	var table strings.Builder
	fmt.Fprintf(&table, "| mutation | first failing seed | detected by |\n|---|---|---|\n")
	for _, m := range mutation.All() {
		bound := mutationBound[m]
		if *flagMutSeeds > 0 {
			bound = *flagMutSeeds
		}
		t.Run(m.String(), func(t *testing.T) {
			seed, r := firstDetection(t, m, bound)
			if seed == 0 {
				fmt.Fprintf(&table, "| %v | not detected in %d seeds | |\n", m, bound)
				t.Errorf("%v: no violation in seeds 1..%d", m, bound)
				return
			}
			// The same seed without the bug must pass, or the "detection"
			// is a bug in the harness or in the unmutated code.
			if control := Run(Options{Seed: seed}); control.Failed() {
				t.Fatalf("seed %d also fails without the mutation:\n%s", seed, control.Report())
			}
			fmt.Fprintf(&table, "| %v | %d | %s |\n", m, seed, strings.Join(r.Kinds(), ", "))
			t.Logf("detected at seed %d: %v", seed, r.Kinds())
			if *flagReport {
				t.Log("\n" + r.Report())
			} else if testing.Verbose() {
				t.Log(firstLine(r.Violations[0].Detail))
			}
		})
	}
	t.Logf("\n%s", table.String())
}
