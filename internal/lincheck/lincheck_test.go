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

func get(c int, call, ret int64, k, v string) Op {
	o := Op{Client: c, Call: call, Return: ret, Cmd: kv.Command{Kind: kv.Get, Key: k}, Result: kv.Result{Code: kv.OK, Value: v}}
	if v == "<absent>" {
		o.Result = kv.Result{Code: kv.NotFound}
	}
	return o
}

func del(c int, call, ret int64, k string, existed bool) Op {
	o := Op{Client: c, Call: call, Return: ret, Cmd: kv.Command{Kind: kv.Delete, Key: k}, Result: kv.Result{Code: kv.OK}}
	if !existed {
		o.Result.Code = kv.NotFound
	}
	return o
}

func cas(c int, call, ret int64, k, expect, v string, ok bool) Op {
	o := Op{Client: c, Call: call, Return: ret, Cmd: kv.Command{Kind: kv.CAS, Key: k, Expect: expect, Value: v}, Result: kv.Result{Code: kv.OK}}
	if expect == "<absent>" {
		o.Cmd.Expect, o.Cmd.ExpectAbsent = "", true
	}
	if !ok {
		o.Result.Code = kv.CASFailed
	}
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
		{"sequential", []Op{put(1, 0, 1, "x", "a"), get(2, 2, 3, "x", "a"), del(1, 4, 5, "x", true), get(2, 6, 7, "x", "<absent>")}, Linearizable},
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
		{"two cas from the same value both succeed", []Op{put(1, 0, 1, "x", "0"), cas(2, 2, 10, "x", "0", "a", true), cas(3, 2, 10, "x", "0", "b", true)}, Violation},
		{"one of two cas succeeds", []Op{put(1, 0, 1, "x", "0"), cas(2, 2, 10, "x", "0", "a", true), cas(3, 2, 10, "x", "0", "b", false), get(1, 11, 12, "x", "a")}, Linearizable},
		{"cas failure must be justified", []Op{put(1, 0, 1, "x", "0"), cas(2, 2, 3, "x", "0", "a", false)}, Violation},
		{"cas on absent key", []Op{cas(1, 0, 1, "x", "<absent>", "a", true), cas(2, 2, 3, "x", "<absent>", "b", false), get(1, 4, 5, "x", "a")}, Linearizable},
		{"delete of absent key must report not-found", []Op{del(1, 0, 1, "x", true)}, Violation},
		{"timed-out write observed later", []Op{unknown(put(1, 0, 0, "x", "a")), get(2, 5, 6, "x", "a")}, Linearizable},
		{"timed-out write never observed", []Op{unknown(put(1, 0, 0, "x", "a")), get(2, 5, 6, "x", "<absent>")}, Linearizable},
		{"timed-out write observed before it was issued", []Op{get(2, 0, 1, "x", "a"), unknown(put(1, 5, 0, "x", "a"))}, Violation},
		{"timed-out write cannot flicker", []Op{unknown(put(1, 0, 0, "x", "a")), get(2, 5, 6, "x", "a"), get(2, 7, 8, "x", "<absent>")}, Violation},
		{
			"timed-out write may land after a later write",
			[]Op{unknown(put(1, 0, 0, "x", "a")), put(2, 1, 2, "x", "b"), get(3, 3, 4, "x", "b"), get(3, 5, 6, "x", "a")},
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
	// Many concurrent unknown writes and a final read that matches none
	// of them: the search must explore every subset before giving up.
	var ops []Op
	for i := 0; i < 40; i++ {
		ops = append(ops, unknown(put(i, 0, 0, "x", fmt.Sprint(i))))
	}
	ops = append(ops, get(99, 1, 2, "x", "nope"))
	rep := CheckKV(ops, 1000)
	if rep.Verdict != Inconclusive {
		t.Fatalf("verdict %v", rep.Verdict)
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
					if j != i && !used[j] && !skip[j] && !x.Unknown && x.Return < o.Call {
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
	call := int64(rng.Intn(20))
	ret := call + int64(rng.Intn(8))
	var o Op
	switch rng.Intn(4) {
	case 0:
		o = put(client, call, ret, "k", v)
	case 1:
		if rng.Chance(1, 3) {
			v = "<absent>"
		}
		o = get(client, call, ret, "k", v)
	case 2:
		o = del(client, call, ret, "k", rng.Chance(1, 2))
	default:
		e := vals[rng.Intn(len(vals))]
		if rng.Chance(1, 4) {
			e = "<absent>"
		}
		o = cas(client, call, ret, "k", e, v, rng.Chance(1, 2))
	}
	if o.Cmd.Kind != kv.Get && rng.Chance(1, 5) {
		o = unknown(o)
	}
	return o
}

// TestAgreesWithBruteForce compares the search with exhaustive enumeration
// on random small histories, most of which are not linearizable.
func TestAgreesWithBruteForce(t *testing.T) {
	const seed = 20261001
	rng := prng.New(seed)
	counts := map[bool]int{}
	for iter := 0; iter < 3000; iter++ {
		n := 1 + rng.Intn(6)
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
			o = get(c, call, ret, k, cur.String())
			if cur.present {
				o.Result.Value = cur.value
			}
		case 2:
			o = del(c, call, ret, k, cur.present)
		default:
			e := cur.value
			if rng.Chance(1, 3) {
				e = "nope"
			}
			if !cur.present {
				e = "<absent>"
			}
			o = cas(c, call, ret, k, e, v, true)
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
					o.Result = kv.Result{Code: kv.CASFailed}
					if cur.present {
						o.Result.Value = cur.value
					}
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
				ops[i].Result = kv.Result{Code: kv.OK, Value: "never-written"}
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
