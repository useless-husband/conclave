package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"math/rand/v2"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/useless-husband/conclave/client"
)

// bench drives a cluster with closed-loop clients: each of -clients
// goroutines has its own session and issues its next request as soon as
// the previous one is answered. Latency is measured per request by the
// client, from just before the request to just after the answer.
func bench(args []string) error {
	fs := flag.NewFlagSet("bench", flag.ExitOnError)
	addr := addrFlag(fs)
	clients := fs.Int("clients", 16, "concurrent clients, each with its own session")
	dur := fs.Duration("duration", 10*time.Second, "measurement time")
	warm := fs.Duration("warmup", time.Second, "time to run before measuring")
	mix := fs.String("mix", "put", "put, get, or mixed (50% each)")
	keys := fs.Int("keys", 1000, "key space")
	vsize := fs.Int("value", 64, "value size in bytes")
	fs.Parse(args)
	if *addr == "" {
		return errors.New("no server address: use -addr or set CONCLAVE_ADDR")
	}
	if *mix != "put" && *mix != "get" && *mix != "mixed" {
		return fmt.Errorf("unknown -mix %q", *mix)
	}
	addrs := strings.Split(*addr, ",")
	value := strings.Repeat("x", *vsize)

	// Populate the keys read by get-only runs.
	if *mix != "put" {
		c := client.New(addrs, client.Options{})
		ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
		for k := 0; k < *keys; k++ {
			if err := c.Put(ctx, fmt.Sprintf("bench-%d", k), value); err != nil {
				cancel()
				return fmt.Errorf("populate: %w", err)
			}
		}
		cancel()
		c.Close()
	}

	var (
		wg      sync.WaitGroup
		mu      sync.Mutex
		lats    []time.Duration
		errs    int
		measure = time.Now().Add(*warm)
		end     = measure.Add(*dur)
	)
	for i := 0; i < *clients; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			c := client.New(addrs, client.Options{})
			defer c.Close()
			rng := rand.New(rand.NewPCG(uint64(i), 42))
			var mine []time.Duration
			nerr := 0
			for {
				now := time.Now()
				if now.After(end) {
					break
				}
				key := fmt.Sprintf("bench-%d", rng.IntN(*keys))
				ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
				var err error
				get := *mix == "get" || *mix == "mixed" && rng.IntN(2) == 0
				if get {
					_, err = c.Get(ctx, key)
				} else {
					err = c.Put(ctx, key, value)
				}
				cancel()
				took := time.Since(now)
				if now.Before(measure) {
					continue
				}
				if err != nil && !errors.Is(err, client.ErrNotFound) {
					nerr++
					continue
				}
				mine = append(mine, took)
			}
			mu.Lock()
			lats = append(lats, mine...)
			errs += nerr
			mu.Unlock()
		}(i)
	}
	wg.Wait()
	if len(lats) == 0 {
		return fmt.Errorf("no request completed (%d errors)", errs)
	}
	sort.Slice(lats, func(i, j int) bool { return lats[i] < lats[j] })
	pct := func(p float64) time.Duration { return lats[min(len(lats)-1, int(p*float64(len(lats))))] }
	ms := func(d time.Duration) string { return fmt.Sprintf("%.2f ms", float64(d.Microseconds())/1000) }
	fmt.Printf("mix=%s clients=%d duration=%v keys=%d value=%dB\n", *mix, *clients, *dur, *keys, *vsize)
	fmt.Printf("requests: %d ok, %d errors\n", len(lats), errs)
	fmt.Printf("throughput: %.0f req/s\n", float64(len(lats))/dur.Seconds())
	fmt.Printf("latency: p50 %s  p90 %s  p99 %s  p99.9 %s  max %s\n",
		ms(pct(0.50)), ms(pct(0.90)), ms(pct(0.99)), ms(pct(0.999)), ms(lats[len(lats)-1]))
	return nil
}
