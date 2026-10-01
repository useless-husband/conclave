package sim

import (
	"flag"
	"fmt"
	"strings"
	"sync"
	"testing"

	"github.com/useless-husband/conclave/internal/mutation"
)

var (
	flagMutSeeds = flag.Int("sim.mutseeds", 0, "override the seed bound of TestMutationsAreDetected")
	flagReport   = flag.Bool("sim.report", false, "print the full failure report of each detection")
)

// mutationBound is the number of seeds within which the simulator must
// expose each injected bug. The bounds are about twice the seed at which
// detection happened when the README table was produced (at least 50), so
// that harmless changes to the simulator do not make the test flaky, while
// a harness that lost its teeth fails it.
var mutationBound = map[mutation.Mutation]int{
	mutation.VoteWithoutLogCheck:        50,    // measured: seed 2
	mutation.CommitPriorTermByCount:     12000, // measured: seed 5584
	mutation.VoteNotPersisted:           1000,  // measured: seed 383
	mutation.AckBeforeFsync:             50,    // measured: seed 3
	mutation.ReadWithoutQuorum:          200,   // measured: seed 74
	mutation.DuplicateApply:             50,    // measured: seed 1
	mutation.SkipWALChecksum:            600,   // measured: seed 241
	mutation.SkipDirSync:                50,    // measured: seed 5
	mutation.TruncateWithoutMarker:      50,    // measured: seed 1
	mutation.ConfChangeBeforeTermCommit: 7000,  // measured: seed 3358
}

// firstDetection runs seeds 1, 2, ... on four goroutines and returns the
// smallest seed whose run violates a safety property, or 0. Seeds are
// handed out in order and no seed above a detection is started, so when
// the workers stop every smaller seed has been run.
func firstDetection(t *testing.T, m mutation.Mutation, bound int) (uint64, *Result) {
	set := mutation.ForTest(t, m)
	var (
		mu   sync.Mutex
		next uint64 = 1
		best *Result
		wg   sync.WaitGroup
	)
	for w := 0; w < 4; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				mu.Lock()
				seed := next
				if seed > uint64(bound) || best != nil && seed > best.Seed {
					mu.Unlock()
					return
				}
				next++
				mu.Unlock()
				r := Run(Options{Seed: seed, Mutations: set})
				if r.SafetyViolated() {
					mu.Lock()
					if best == nil || r.Seed < best.Seed {
						best = r
					}
					mu.Unlock()
				}
			}
		}()
	}
	wg.Wait()
	if best == nil {
		return 0, nil
	}
	return best.Seed, best
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
