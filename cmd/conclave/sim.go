package main

import (
	"errors"
	"flag"
	"fmt"
	"os"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/useless-husband/conclave/internal/sim"
)

// simCommand runs one seed, or sweeps a range of seeds and summarizes.
func simCommand(args []string) error {
	fs := flag.NewFlagSet("sim", flag.ExitOnError)
	seed := fs.Uint64("seed", 0, "run this seed")
	seeds := fs.String("seeds", "", "sweep a range of seeds, A-B (inclusive)")
	workers := fs.Int("workers", min(4, runtime.NumCPU()), "parallel runs for -seeds")
	trace := fs.Bool("trace", false, "print the whole trace of a single run")
	dur := fs.Duration("duration", 20*time.Second, "faulty phase, in virtual time")
	heal := fs.Duration("heal", 5*time.Second, "fault-free phase after it, in virtual time")
	fs.Parse(args)
	opt := sim.Options{
		Duration: sim.Time(dur.Microseconds()),
		Heal:     sim.Time(heal.Microseconds()),
	}
	switch {
	case *seed != 0 && *seeds == "":
		opt.Seed = *seed
		if *trace {
			opt.TraceLines = -1
		}
		start := time.Now()
		r := sim.Run(opt)
		if *trace {
			for _, l := range r.Trace {
				fmt.Println(l)
			}
		}
		fmt.Printf("seed %d: profile %v\n", r.Seed, r.Profile)
		fmt.Printf("%v simulated in %v: %d events, %d messages (%d dropped), %d client operations (%d unknown), %d crashes (%d mid-I/O, %d torn writes), %d partitions, %d pauses, %d elections, max term %d, %d snapshots installed, %d members added, %d removed\n",
			r.SimTime, time.Since(start).Round(time.Millisecond), r.Stats.Events, r.Stats.Messages, r.Stats.Dropped,
			r.Stats.Ops, r.Stats.Unknown, r.Stats.Crashes, r.Stats.MidIOCrashes, r.Stats.TornWrites, r.Stats.Partitions,
			r.Stats.Pauses, r.Stats.Elections, r.Stats.MaxTerm, r.Stats.SnapshotsRecvd, r.Stats.MembershipAdds, r.Stats.MembershipRems)
		fmt.Printf("history: %d operations checked (%d dropped as unobservable), %s, %d search steps\n",
			r.Lin.Ops, r.Lin.Dropped, r.Lin.Verdict, r.Stats.LinSteps)
		fmt.Printf("digest %016x\n", r.Digest)
		if r.Failed() {
			fmt.Print(r.Report())
			return errors.New("violations found")
		}
		return nil
	case *seeds != "":
		lo, hi, err := parseRange(*seeds)
		if err != nil {
			return err
		}
		return sweep(lo, hi, *workers, opt)
	}
	return errors.New("sim: give -seed N or -seeds A-B")
}

func parseRange(s string) (uint64, uint64, error) {
	a, b, ok := strings.Cut(s, "-")
	lo, err1 := strconv.ParseUint(a, 10, 64)
	hi, err2 := strconv.ParseUint(b, 10, 64)
	if !ok || err1 != nil || err2 != nil || lo == 0 || hi < lo {
		return 0, 0, fmt.Errorf("bad seed range %q (want A-B with 1 <= A <= B)", s)
	}
	return lo, hi, nil
}

func sweep(lo, hi uint64, workers int, opt sim.Options) error {
	start := time.Now()
	var (
		mu       sync.Mutex
		failed   []*sim.Result
		simTime  sim.Time
		tot      sim.Stats
		inconcl  int
		done     int
		next     = lo
		wg       sync.WaitGroup
		lastNote = time.Now()
	)
	n := int(hi - lo + 1)
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				mu.Lock()
				if next > hi {
					mu.Unlock()
					return
				}
				s := next
				next++
				mu.Unlock()
				o := opt
				o.Seed = s
				r := sim.Run(o)
				mu.Lock()
				done++
				simTime += r.SimTime
				tot.Events += r.Stats.Events
				tot.Messages += r.Stats.Messages
				tot.Ops += r.Stats.Ops
				tot.Unknown += r.Stats.Unknown
				tot.Crashes += r.Stats.Crashes
				tot.MidIOCrashes += r.Stats.MidIOCrashes
				tot.TornWrites += r.Stats.TornWrites
				tot.Partitions += r.Stats.Partitions
				tot.Pauses += r.Stats.Pauses
				tot.Elections += r.Stats.Elections
				tot.SnapshotsRecvd += r.Stats.SnapshotsRecvd
				tot.MembershipAdds += r.Stats.MembershipAdds
				tot.MembershipRems += r.Stats.MembershipRems
				if r.Stats.LinInconclusive {
					inconcl++
				}
				if r.Failed() {
					failed = append(failed, r)
					fmt.Fprintf(os.Stderr, "seed %d FAILED: %v\n", r.Seed, r.Kinds())
				}
				if time.Since(lastNote) > 30*time.Second {
					lastNote = time.Now()
					fmt.Fprintf(os.Stderr, "%d/%d seeds, %d failed, %v\n", done, n, len(failed), time.Since(start).Round(time.Second))
				}
				mu.Unlock()
			}
		}()
	}
	wg.Wait()
	wall := time.Since(start)
	hours := float64(simTime) / float64(sim.Second) / 3600
	fmt.Printf("seeds %d-%d: %d runs, %d failed, %d inconclusive checks\n", lo, hi, n, len(failed), inconcl)
	fmt.Printf("simulated %.1f hours in %v wall on %d workers (%.0f simulated seconds per wall second)\n",
		hours, wall.Round(time.Second), workers, float64(simTime)/float64(sim.Second)/wall.Seconds())
	fmt.Printf("%d events, %d messages, %d client operations checked for linearizability (%d with unknown outcome)\n",
		tot.Events, tot.Messages, tot.Ops, tot.Unknown)
	fmt.Printf("%d crashes (%d in the middle of disk I/O, %d torn writes), %d partitions, %d pauses, %d elections, %d snapshots installed, %d members added, %d removed\n",
		tot.Crashes, tot.MidIOCrashes, tot.TornWrites, tot.Partitions, tot.Pauses, tot.Elections, tot.SnapshotsRecvd, tot.MembershipAdds, tot.MembershipRems)
	for i, r := range failed {
		if i == 3 {
			fmt.Printf("... and %d more failures\n", len(failed)-3)
			break
		}
		fmt.Print(r.Report())
	}
	if len(failed) > 0 {
		return fmt.Errorf("%d seeds failed", len(failed))
	}
	return nil
}
