package sim

import (
	"flag"
	"fmt"
	"runtime"
	"sync"
	"testing"
	"time"
)

var (
	flagSeeds = flag.Int("sim.seeds", 40, "number of seeds for TestSeedSweep")
	flagFirst = flag.Uint64("sim.first", 1, "first seed for TestSeedSweep")
	flagSeed  = flag.Uint64("sim.seed", 0, "run only this seed in TestOneSeed and print its trace")
)

func TestOneSeed(t *testing.T) {
	if *flagSeed == 0 {
		t.Skip("set -sim.seed")
	}
	res := Run(Options{Seed: *flagSeed, TraceLines: -1})
	for _, l := range res.Trace {
		t.Log(l)
	}
	t.Logf("profile: %v", res.Profile)
	t.Logf("stats: %+v", res.Stats)
	if res.Failed() {
		t.Fatal(res.Report())
	}
}

// sweep runs seeds [first, first+n) on a few goroutines and returns the
// results in seed order.
func sweep(first uint64, n int, opt Options) []*Result {
	out := make([]*Result, n)
	workers := min(4, runtime.GOMAXPROCS(0))
	var wg sync.WaitGroup
	next := make(chan int)
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := range next {
				o := opt
				o.Seed = first + uint64(i)
				out[i] = Run(o)
			}
		}()
	}
	for i := 0; i < n; i++ {
		next <- i
	}
	close(next)
	wg.Wait()
	return out
}

func TestSeedSweep(t *testing.T) {
	n := *flagSeeds
	if testing.Short() {
		n = min(n, 8)
	}
	start := time.Now()
	results := sweep(*flagFirst, n, Options{})
	var simTime Time
	var ops, unknown, crashes, parts int
	for _, r := range results {
		simTime += r.SimTime
		ops += r.Stats.Ops
		unknown += r.Stats.Unknown
		crashes += r.Stats.Crashes
		parts += r.Stats.Partitions
		if r.Failed() {
			t.Errorf("%s", r.Report())
		}
		if r.Stats.LinInconclusive {
			t.Logf("seed %d: linearizability check inconclusive (budget)", r.Seed)
		}
	}
	wall := time.Since(start)
	t.Logf("%d seeds, %v simulated in %v wall (%.0f simulated s per wall s on 4 workers); %d ops (%d unknown), %d crashes, %d partitions",
		n, simTime, wall.Round(time.Millisecond), float64(simTime)/float64(Second)/wall.Seconds(), ops, unknown, crashes, parts)
}

func TestDeterminism(t *testing.T) {
	for _, seed := range []uint64{1, 2, 3, 4} {
		var wg sync.WaitGroup
		res := make([]*Result, 3)
		for i := range res {
			wg.Add(1)
			go func() {
				defer wg.Done()
				res[i] = Run(Options{Seed: seed, TraceLines: -1})
			}()
		}
		wg.Wait()
		for i := 1; i < len(res); i++ {
			if res[i].Digest != res[0].Digest {
				t.Fatalf("seed %d: run %d digest %x, run 0 digest %x", seed, i, res[i].Digest, res[0].Digest)
			}
			if fmt.Sprint(res[i].Trace) != fmt.Sprint(res[0].Trace) || fmt.Sprint(res[i].History) != fmt.Sprint(res[0].History) {
				t.Fatalf("seed %d: equal digests but different traces or histories", seed)
			}
			if res[i].Stats != res[0].Stats {
				t.Fatalf("seed %d: stats differ: %+v vs %+v", seed, res[i].Stats, res[0].Stats)
			}
		}
	}
	if Run(Options{Seed: 1}).Digest == Run(Options{Seed: 2}).Digest {
		t.Fatal("different seeds produced the same run")
	}
}
