package lincheck

import (
	"fmt"
	"strings"
	"testing"

	"github.com/useless-husband/conclave/internal/kv"
	"github.com/useless-husband/conclave/internal/prng"
)

func put(c int, call, ret int64, k, v string) Op {
	return Op{Client: c, Call: call, Return: ret, Cmd: kv.Command{Kind: kv.Put, Key: k, Value: v}, Result: kv.Result{Code: kv.OK}}
}

// found describes a key's state as a result reveals it; "<absent>" means
// the key did not exist.
func found(code kv.Code, v string) kv.Result {
	if v == "<absent>" {
		return kv.Result{Code: code}
	}
	return kv.Result{Code: code, Found: true, Value: v}
}

func get(c int, call, ret int64, k, v string) Op {
	o := Op{Client: c, Call: call, Return: ret, Cmd: kv.Command{Kind: kv.Get, Key: k}, Result: found(kv.OK, v)}
	if v == "<absent>" {
		o.Result.Code = kv.NotFound
	}
	return o
}

// del records a delete that found old ("<absent>" for not-found).
func del(c int, call, ret int64, k string, old string) Op {
	o := Op{Client: c, Call: call, Return: ret, Cmd: kv.Command{Kind: kv.Delete, Key: k}, Result: found(kv.OK, old)}
	if old == "<absent>" {
		o.Result.Code = kv.NotFound
	}
	return o
}

// cas records a successful cas, or with cur != "" a failed one that found
// cur.
func cas(c int, call, ret int64, k, expect, v string, cur string) Op {
	o := Op{Client: c, Call: call, Return: ret, Cmd: kv.Command{Kind: kv.CAS, Key: k, Expect: expect, Value: v}, Result: kv.Result{Code: kv.OK}}
	if expect == "<absent>" {
		o.Cmd.Expect, o.Cmd.ExpectAbsent = "", true
	}
	if cur != "" {
		o.Result = found(kv.CASFailed, cur)
	}
	return o
}

func bounded(o Op, by int64) Op {
	o = unknown(o)
	o.Return = by
	return o
}

func unknown(o Op) Op {
	o.Unknown = true
	o.Return = 0
	o.Result = kv.Result{}
	return o
}

func TestKnownHistories(t *testing.T) {
	cases := []struct {
		name string
		ops  []Op
		want Verdict
	}{
		{"empty", nil, Linearizable},
		{"sequential", []Op{put(1, 0, 1, "x", "a"), get(2, 2, 3, "x", "a"), del(1, 4, 5, "x", "a"), get(2, 6, 7, "x", "<absent>")}, Linearizable},
		{"delete must report the value it removed", []Op{put(1, 0, 1, "x", "a"), del(1, 4, 5, "x", "b")}, Violation},
		{"read of initial state", []Op{get(1, 0, 1, "x", "<absent>")}, Linearizable},
		{"read of a value never written", []Op{get(1, 0, 1, "x", "zzz")}, Violation},
		{"concurrent read may see the write", []Op{put(1, 0, 10, "x", "a"), get(2, 1, 2, "x", "a")}, Linearizable},
		{"concurrent read may miss the write", []Op{put(1, 0, 10, "x", "a"), get(2, 1, 2, "x", "<absent>")}, Linearizable},
		{"stale read after the write returned", []Op{put(1, 0, 1, "x", "a"), get(2, 2, 3, "x", "<absent>")}, Violation},
		{"lost update", []Op{put(1, 0, 1, "x", "a"), put(1, 2, 3, "x", "b"), get(2, 4, 5, "x", "a")}, Violation},
		{
			// Two reads during one write: once a read has returned the
			// new value, a later read may not return the old one.
			"new-old inversion",
			[]Op{put(1, 0, 1, "x", "old"), put(1, 2, 20, "x", "new"), get(2, 3, 4, "x", "new"), get(3, 5, 6, "x", "old")},
			Violation,
		},
		{"new-old overlapping reads are fine", []Op{put(1, 0, 1, "x", "old"), put(1, 2, 20, "x", "new"), get(2, 3, 6, "x", "new"), get(3, 5, 7, "x", "old")}, Linearizable},
		{"two cas from the same value both succeed", []Op{put(1, 0, 1, "x", "0"), cas(2, 2, 10, "x", "0", "a", ""), cas(3, 2, 10, "x", "0", "b", "")}, Violation},
		{"one of two cas succeeds", []Op{put(1, 0, 1, "x", "0"), cas(2, 2, 10, "x", "0", "a", ""), cas(3, 2, 10, "x", "0", "b", "a"), get(1, 11, 12, "x", "a")}, Linearizable},
		{"cas failure must be justified", []Op{put(1, 0, 1, "x", "0"), cas(2, 2, 3, "x", "0", "a", "0")}, Violation},
		{"cas failure must report the current value", []Op{put(1, 0, 1, "x", "0"), cas(2, 2, 3, "x", "9", "a", "1")}, Violation},
		{"cas on absent key", []Op{cas(1, 0, 1, "x", "<absent>", "a", ""), cas(2, 2, 3, "x", "<absent>", "b", "a"), get(1, 4, 5, "x", "a")}, Linearizable},
		{"delete of absent key must report not-found", []Op{del(1, 0, 1, "x", "a")}, Violation},
		{
			// The unknown cas can only have failed (nothing was ever
			// "0"), so the observed "b" must come from the put.
			"unobserved timed-out writes are dropped",
			[]Op{unknown(put(1, 0, 0, "x", "never-seen")), unknown(cas(2, 0, 0, "x", "0", "also-never", "")), put(3, 1, 2, "x", "b"), get(4, 3, 4, "x", "b")},
			Linearizable,
		},
		{"timed-out write observed later", []Op{unknown(put(1, 0, 0, "x", "a")), get(2, 5, 6, "x", "a")}, Linearizable},
		{"timed-out write never observed", []Op{unknown(put(1, 0, 0, "x", "a")), get(2, 5, 6, "x", "<absent>")}, Linearizable},
		{"timed-out write observed before it was issued", []Op{get(2, 0, 1, "x", "a"), unknown(put(1, 5, 0, "x", "a"))}, Violation},
		{"timed-out write cannot flicker", []Op{unknown(put(1, 0, 0, "x", "a")), get(2, 5, 6, "x", "a"), get(2, 7, 8, "x", "<absent>")}, Violation},
		{
			"timed-out write may land after a later write",
			[]Op{unknown(put(1, 0, 0, "x", "a")), put(2, 1, 2, "x", "b"), get(3, 3, 4, "x", "b"), get(3, 5, 6, "x", "a")},
			Linearizable,
		},
		{
			"bounded timed-out write observed after its bound",
			[]Op{bounded(put(1, 0, 0, "x", "a"), 3), get(2, 5, 6, "x", "<absent>"), get(2, 7, 8, "x", "a")},
			Violation,
		},
		{
			"bounded timed-out write observed before its bound",
			[]Op{bounded(put(1, 0, 0, "x", "a"), 3), get(2, 1, 2, "x", "a"), get(2, 7, 8, "x", "a")},
			Linearizable,
		},
		{
			"bounded timed-out write that never happened",
			[]Op{bounded(put(1, 0, 0, "x", "a"), 3), put(3, 4, 5, "x", "b"), get(2, 5, 6, "x", "b"), get(2, 7, 8, "x", "b")},
			Linearizable,
		},
		{"keys are independent", []Op{put(1, 0, 1, "x", "a"), get(2, 2, 3, "y", "<absent>"), get(2, 4, 5, "x", "a")}, Linearizable},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			rep := CheckKV(c.ops, 0)
			if rep.Verdict != c.want {
				t.Fatalf("verdict %v, want %v\n%v", rep.Verdict, c.want, rep)
			}
			if c.want == Violation && !strings.Contains(rep.String(), "longest linearizable prefix") {
				t.Fatalf("violation without explanation:\n%v", rep)
			}
		})
	}
}

func TestUnknownGetsAreDropped(t *testing.T) {
	g := unknown(get(1, 0, 0, "x", "a"))
	rep := CheckKV([]Op{g}, 0)
	if rep.Verdict != Linearizable || rep.Dropped != 1 || rep.Ops != 0 {
		t.Fatalf("%+v", rep)
	}
}

func TestBudget(t *testing.T) {
	// A counter with many concurrent increments of unknown outcome and a
	// final read no subset of them explains: the search has to try every
	// subset before it can give up.
	type op struct{ read int }
	m := Model[int, op]{
		Init: func() int { return 0 },
		Step: func(s int, o op, unknown bool) (bool, int) {
			if o.read != 0 {
				return o.read == s, s
			}
			return true, s + 1
		},
		Hash: func(s int) uint64 { return uint64(s) },
	}
	var evs []Event[op]
	for i := 0; i < 40; i++ {
		evs = append(evs, Event[op]{Call: 0, Unknown: true})
	}
	evs = append(evs, Event[op]{Op: op{read: -1}, Call: 1, Return: 2})
	if res := Check(m, evs, 1000); res.Verdict != Inconclusive {
		t.Fatalf("verdict %v", res.Verdict)
	}
	evs = append(evs[:12], Event[op]{Op: op{read: 7}, Call: 1, Return: 2})
	if res := Check(m, evs, 0); res.Verdict != Linearizable {
		t.Fatalf("verdict %v for a read of 7 after 12 possible increments", res.Verdict)
	}
}

// bruteForce decides linearizability by enumerating every subset of the
// unknown operations and every order of the chosen operations that respects
// real time.
func bruteForce(ops []Op) bool {
	var unk []int
	for i, o := range ops {
		if o.Unknown {
			unk = append(unk, i)
		}
	}
	for mask := 0; mask < 1<<len(unk); mask++ {
		skip := map[int]bool{}
		for j, i := range unk {
			if mask&(1<<j) == 0 {
				skip[i] = true
			}
		}
		used := make([]bool, len(ops))
		var dfs func(s register, done int) bool
		total := len(ops) - len(skip)
		dfs = func(s register, done int) bool {
			if done == total {
				return true
			}
			for i, o := range ops {
				if used[i] || skip[i] {
					continue
				}
				minimal := true
				for j, x := range ops {
					bounded := !x.Unknown || x.Return != 0
					if j != i && !used[j] && !skip[j] && bounded && x.Return < o.Call {
						minimal = false
						break
					}
				}
				if !minimal {
					continue
				}
				ok, next := stepKV(s, o, o.Unknown)
				if !ok {
					continue
				}
				used[i] = true
				if dfs(next, done+1) {
					return true
				}
				used[i] = false
			}
			return false
		}
		if dfs(register{}, 0) {
			return true
		}
	}
	return false
}

func randomOp(rng *prng.Rand, client int) Op {
	vals := []string{"a", "b", "c"}
	v := vals[rng.Intn(len(vals))]
	state := func() string {
		if rng.Chance(1, 3) {
			return "<absent>"
		}
		return vals[rng.Intn(len(vals))]
	}
	call := int64(rng.Intn(20))
	ret := call + int64(rng.Intn(8))
	var o Op
	switch rng.Intn(4) {
	case 0:
		o = put(client, call, ret, "k", v)
	case 1:
		o = get(client, call, ret, "k", state())
	case 2:
		o = del(client, call, ret, "k", state())
	default:
		e := state()
		cur := ""
		if rng.Chance(1, 2) {
			cur = state()
		}
		o = cas(client, call, ret, "k", e, v, cur)
	}
	if o.Cmd.Kind != kv.Get && rng.Chance(1, 5) {
		r := o.Return
		o = unknown(o)
		if rng.Chance(1, 2) {
			o.Return = r // bounded: took effect by r or never
		}
	}
	return o
}

// TestAgreesWithBruteForce compares the search with exhaustive enumeration
// on random small histories, most of which are not linearizable.
func TestAgreesWithBruteForce(t *testing.T) {
	const seed = 20261001
	rng := prng.New(seed)
	counts := map[bool]int{}
	for iter := 0; iter < 4000; iter++ {
		n := 1 + rng.Intn(7)
		ops := make([]Op, n)
		for i := range ops {
			ops[i] = randomOp(rng, i)
		}
		want := bruteForce(ops)
		got := CheckKV(ops, 0)
		if (got.Verdict == Linearizable) != want {
			t.Fatalf("seed %d iteration %d: search says %v, brute force says linearizable=%v\nops: %v",
				seed, iter, got.Verdict, want, ops)
		}
		counts[want]++
	}
	if counts[true] < 300 || counts[false] < 300 {
		t.Fatalf("unbalanced sample: %v", counts)
	}
}

// genLinearizable produces a history that is linearizable by construction:
// operations take effect atomically at distinct instants inside their
// intervals, against a sequential register.
func genLinearizable(rng *prng.Rand, n, clients int) []Op {
	var out []Op
	free := make([]int64, clients) // when each client may next call
	s := map[string]register{}
	t := int64(0)
	keys := []string{"x", "y"}
	for i := 0; i < n; i++ {
		c := rng.Intn(clients)
		call := max(free[c], t-int64(rng.Intn(10)))
		lin := max(call, t+1) + int64(rng.Intn(3))
		t = lin
		ret := lin + int64(rng.Intn(15))
		free[c] = ret + 1
		k := keys[rng.Intn(len(keys))]
		v := fmt.Sprint(i)
		cur := s[k]
		var o Op
		switch rng.Intn(4) {
		case 0:
			o = put(c, call, ret, k, v)
		case 1:
			o = get(c, call, ret, k, "<absent>")
			if cur.present {
				o = get(c, call, ret, k, cur.value)
			}
		case 2:
			o = del(c, call, ret, k, "<absent>")
			if cur.present {
				o = del(c, call, ret, k, cur.value)
			}
		default:
			e := cur.value
			if rng.Chance(1, 3) {
				e = "nope"
			}
			if !cur.present {
				e = "<absent>"
			}
			o = cas(c, call, ret, k, e, v, "")
		}
		skip := false
		if o.Cmd.Kind != kv.Get && rng.Chance(1, 6) {
			o = unknown(o)
			skip = rng.Chance(1, 2) // the write may never take effect
			free[c] = call + 1      // the client gave up and moved on
		}
		if !skip {
			_, next := stepKV(cur, o, o.Unknown)
			if !o.Unknown {
				ok, _ := stepKV(cur, o, false)
				if !ok {
					// The CAS did not match: record the failure.
					o.Result = kv.Result{Code: kv.CASFailed, Found: cur.present, Value: cur.value}
				}
			}
			s[k] = next
		}
		out = append(out, o)
	}
	return out
}

func TestRandomLinearizableHistories(t *testing.T) {
	for seed := uint64(1); seed <= 200; seed++ {
		rng := prng.New(seed)
		ops := genLinearizable(rng, 300, 5)
		rep := CheckKV(ops, 0)
		if rep.Verdict != Linearizable {
			t.Fatalf("seed %d: history linearizable by construction reported %v\n%v", seed, rep.Verdict, rep)
		}
		// Corrupt one read so that it returns a value nobody wrote.
		for i := range ops {
			if ops[i].Cmd.Kind == kv.Get && !ops[i].Unknown {
				ops[i].Result = kv.Result{Code: kv.OK, Found: true, Value: "never-written"}
				break
			}
		}
		if rep := CheckKV(ops, 0); rep.Verdict != Violation {
			t.Fatalf("seed %d: corrupted history reported %v", seed, rep.Verdict)
		}
	}
}

func BenchmarkCheckKV(b *testing.B) {
	ops := genLinearizable(prng.New(1), 5000, 8)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if CheckKV(ops, 0).Verdict != Linearizable {
			b.Fatal("not linearizable")
		}
	}
	b.ReportMetric(float64(len(ops)), "ops/history")
}
