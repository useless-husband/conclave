// Package lincheck decides whether a concurrent history is linearizable.
//
// The search is the algorithm of Wing and Gong as improved by Lowe ("Testing
// for linearizability", 2017): keep the call and return events in a doubly
// linked list ordered by time; at each step any operation whose call comes
// before the first remaining return may be linearized next; when no choice
// works, backtrack. A cache of configurations already explored, each a set
// of linearized operations and a model state, prunes the search. Sets are
// hashed incrementally with Zobrist keys.
//
// Operations whose outcome is unknown (a write that timed out) may take
// effect at any point after their call, or not at all. If nothing bounds
// them their return is at infinity, and the search succeeds as soon as
// every operation with a known outcome has been linearized. An unknown
// operation can also carry a bound: "if it took effect at all, it did so
// before time R" (a client session provides one, see CheckKV). Reaching
// that bound without having linearized the operation is a move of its own:
// deciding that it never takes effect.
//
// Unknown operations are what makes real histories expensive: every one of
// them that is pending can be applied at any point, and the configurations
// that differ only in when an overwritten unknown write was applied are all
// distinct. Four refinements keep this in check, each justified by an
// exchange argument (every completion of the pruned branch maps to a
// completion of a branch that is kept):
//
//   - an unknown operation is never applied where it would not change the
//     state: leaving it pending can do everything applying it can;
//   - among pending unknown operations with the same effect (Model.EffectKey)
//     only the one with the earliest bound is applied: any use of another
//     one can be swapped for it;
//   - moves are tried with known operations first, so configurations that
//     have not spent an unknown operation are explored first;
//   - a configuration is pruned if a failed one had linearized the same
//     known operations, reached the same state and decided a subset of its
//     unknown operations.
//
// The KV checker (kv.go) partitions a history by key and checks each key
// separately, which is sound because linearizability is local (Herlihy and
// Wing, 1990): a history is linearizable if and only if its restriction to
// every object is.
package lincheck

import (
	"fmt"
	"math"
	"sort"
)

// Model is a sequential specification. S must be comparable; equal states
// must have equal hashes.
type Model[S comparable, O any] struct {
	Init func() S
	// Step applies op to s. It reports whether the operation's observed
	// outcome is possible in state s, and the resulting state. When
	// unknown is true the outcome was not observed and any outcome is
	// possible.
	Step func(s S, op O, unknown bool) (ok bool, next S)
	Hash func(s S) uint64
	// EffectKey, if set, returns a key such that two operations with the
	// same non-empty key have the same effect on every state. Among
	// interchangeable pending operations of unknown outcome only the one
	// with the earliest bound is ever applied, which removes the
	// symmetric branches of the search.
	EffectKey func(op O) string
	// DescribeState and DescribeOp render states and operations for
	// explanations.
	DescribeState func(s S) string
	DescribeOp    func(op O) string
}

// Event is one operation of a history. Call and Return are positions in a
// total order of events (timestamps or sequence numbers). Operation a
// precedes b in real time if a.Return < b.Call.
type Event[O any] struct {
	Op     O
	Client int
	Call   int64
	Return int64
	// Unknown marks an operation whose outcome was not observed. Its
	// Return, if not zero, bounds when it can have taken effect; zero
	// means no bound.
	Unknown bool
}

// Verdict is the outcome of a check.
type Verdict uint8

const (
	// Linearizable: a linearization was found.
	Linearizable Verdict = iota
	// Violation: no linearization exists.
	Violation
	// Inconclusive: the search budget ran out.
	Inconclusive
)

func (v Verdict) String() string {
	switch v {
	case Linearizable:
		return "linearizable"
	case Violation:
		return "NOT linearizable"
	}
	return "inconclusive"
}

// Result describes the outcome of checking one history.
type Result struct {
	Verdict Verdict
	// Steps is the number of moves tried.
	Steps int
	// Explanation is set for a violation: the end of the longest
	// linearizable prefix found and the operations that could not be
	// placed after it.
	Explanation string
	// From and To bound, for a violation, the stretch of the history the
	// explanation is about: from the earliest call among the operations
	// left pending to the return that could not be passed.
	From, To int64
}

type entry struct {
	op      int
	call    bool
	time    int64
	match   *entry
	prev    *entry
	next    *entry
	unknown bool
}

type bitset []uint64

func (b bitset) set(i int)   { b[i/64] |= 1 << (uint(i) % 64) }
func (b bitset) clear(i int) { b[i/64] &^= 1 << (uint(i) % 64) }

func (b bitset) equal(c bitset) bool {
	for i := range b {
		if b[i] != c[i] {
			return false
		}
	}
	return true
}

func zobrist(i int) uint64 {
	// splitmix64 of the operation index: fixed, so runs are repeatable.
	z := uint64(i+1) * 0x9e3779b97f4a7c15
	z = (z ^ (z >> 30)) * 0xbf58476d1ce4e5b9
	z = (z ^ (z >> 27)) * 0x94d049bb133111eb
	return z ^ (z >> 31)
}

// config is one explored configuration.
type config[S comparable] struct {
	lin   bitset
	state S
	hk    uint64 // hash of the known operations and the state
	// failed is set once every move from the configuration has been
	// explored without success.
	failed bool
}

// subsumeWindow is how many failed configurations with the same known
// operations and state a new configuration is compared with.
const subsumeWindow = 32

// subsumes reports whether a failed configuration with linearized set old
// makes one with set cur pointless: the same known operations, and a
// subset of cur's unknown ones.
func subsumes(old, cur, unknownMask bitset) bool {
	for i := range cur {
		if (old[i]^cur[i])&^unknownMask[i] != 0 || old[i]&unknownMask[i]&^cur[i] != 0 {
			return false
		}
	}
	return true
}

// move linearizes the operation of a call entry or, with skip, decides
// that an unknown operation never takes effect.
type move struct {
	call *entry
	skip bool
}

type frame[S comparable] struct {
	moves []move
	next  int
	state S
	taken move // the move that led to the child frame
	cfg   *config[S]
}

// Check searches for a linearization of events under model m, trying at
// most budget moves (no limit if budget <= 0).
func Check[S comparable, O any](m Model[S, O], events []Event[O], budget int) Result {
	n := len(events)
	if n == 0 {
		return Result{Verdict: Linearizable}
	}
	all := make([]*entry, 0, 2*n)
	unknownMask := make(bitset, (n+63)/64)
	for i, ev := range events {
		c := &entry{op: i, call: true, time: ev.Call, unknown: ev.Unknown}
		rt := ev.Return
		if ev.Unknown && rt == 0 {
			rt = math.MaxInt64
		}
		r := &entry{op: i, time: rt, unknown: ev.Unknown}
		c.match, r.match = r, c
		all = append(all, c, r)
		if ev.Unknown {
			unknownMask.set(i)
		}
	}
	// Calls sort before returns at equal times, which treats operations
	// that touch as concurrent: never a false alarm.
	sort.SliceStable(all, func(i, j int) bool {
		if all[i].time != all[j].time {
			return all[i].time < all[j].time
		}
		return all[i].call && !all[j].call
	})
	head := &entry{op: -1}
	prev := head
	for _, e := range all {
		prev.next, e.prev = e, prev
		prev = e
	}

	unlink := func(e *entry) {
		e.prev.next = e.next
		if e.next != nil {
			e.next.prev = e.prev
		}
	}
	relink := func(e *entry) {
		e.prev.next = e
		if e.next != nil {
			e.next.prev = e
		}
	}
	lift := func(c *entry) { unlink(c); unlink(c.match) }
	unlift := func(c *entry) { relink(c.match); relink(c) }

	// moves lists what may happen next: linearize an operation whose call
	// precedes the first remaining return, or, if that return is the
	// bound of an unknown operation, decide that it never happened. done
	// is set when every operation with a known outcome is linearized.
	moves := func() (out []move, done bool) {
		var unknown []move
		add := func(e *entry) {
			if m.EffectKey != nil {
				if k := m.EffectKey(events[e.op].Op); k != "" {
					for i, u := range unknown {
						if m.EffectKey(events[u.call.op].Op) == k {
							if e.match.time < u.call.match.time {
								unknown[i] = move{call: e}
							}
							return
						}
					}
				}
			}
			unknown = append(unknown, move{call: e})
		}
		for e := head.next; e != nil; e = e.next {
			if e.call {
				if e.unknown {
					add(e)
				} else {
					out = append(out, move{call: e})
				}
				continue
			}
			if e.unknown && e.time == math.MaxInt64 {
				return nil, true
			}
			if e.unknown {
				out = append(out, move{call: e.match, skip: true})
			}
			return append(out, unknown...), false
		}
		return nil, true
	}

	var (
		lin      = make(bitset, (n+63)/64)
		zobK     uint64 // Zobrist hash of the known operations in lin
		zobU     uint64 // ... and of the unknown ones
		exact    = map[uint64][]*config[S]{}
		failed   = map[uint64][]*config[S]{} // by known operations and state
		steps    int
		deepest  []move
		deepSt   S
		deepStop *entry
	)
	// prune reports whether the configuration (lin, s) has been explored
	// already or is subsumed by a failed one; otherwise it records it.
	prune := func(s S) (*config[S], bool) {
		hs := m.Hash(s)
		h := zobK ^ zobU ^ hs
		for _, c := range exact[h] {
			if c.state == s && c.lin.equal(lin) {
				return nil, true
			}
		}
		// Subsumption: only the most recent failures with the same known
		// operations and state are compared, which bounds the cost of a
		// step. Pruning less is always sound.
		fs := failed[zobK^hs]
		for i := len(fs) - 1; i >= 0 && i >= len(fs)-subsumeWindow; i-- {
			c := fs[i]
			if c.state == s && subsumes(c.lin, lin, unknownMask) {
				return nil, true
			}
		}
		c := &config[S]{lin: append(bitset(nil), lin...), state: s, hk: zobK ^ hs}
		exact[h] = append(exact[h], c)
		return c, false
	}
	mark := func(op int) {
		lin.set(op)
		if events[op].Unknown {
			zobU ^= zobrist(op)
		} else {
			zobK ^= zobrist(op)
		}
	}
	unmark := func(op int) {
		lin.clear(op)
		if events[op].Unknown {
			zobU ^= zobrist(op)
		} else {
			zobK ^= zobrist(op)
		}
	}

	rootMoves, done := moves()
	if done {
		return Result{Verdict: Linearizable}
	}
	rootCfg, _ := prune(m.Init())
	stack := []frame[S]{{moves: rootMoves, state: m.Init(), cfg: rootCfg}}
	for {
		f := &stack[len(stack)-1]
		if f.next == len(f.moves) {
			// Every move from here failed.
			f.cfg.failed = true
			failed[f.cfg.hk] = append(failed[f.cfg.hk], f.cfg)
			if len(stack)-1 > len(deepest) || deepest == nil {
				deepest = deepest[:0]
				for _, fr := range stack[:len(stack)-1] {
					deepest = append(deepest, fr.taken)
				}
				deepSt = f.state
				deepStop = firstReturn(head)
			}
			stack = stack[:len(stack)-1]
			if len(stack) == 0 {
				res := Result{Verdict: Violation, Steps: steps,
					Explanation: explain(m, events, deepest, deepStop, deepSt)}
				if deepStop != nil {
					res.From, res.To = deepStop.time, deepStop.time
					in := map[int]bool{}
					for _, mv := range deepest {
						in[mv.call.op] = true
					}
					for i, ev := range events {
						if !in[i] && ev.Call <= deepStop.time && ev.Call < res.From {
							res.From = ev.Call
						}
					}
				}
				return res
			}
			p := &stack[len(stack)-1]
			unlift(p.taken.call)
			unmark(p.taken.call.op)
			continue
		}
		steps++
		if budget > 0 && steps > budget {
			return Result{Verdict: Inconclusive, Steps: steps}
		}
		mv := f.moves[f.next]
		f.next++
		op := mv.call.op
		next := f.state
		if !mv.skip {
			ok, s := m.Step(f.state, events[op].Op, events[op].Unknown)
			if !ok || events[op].Unknown && s == f.state {
				continue
			}
			next = s
		}
		mark(op)
		cfg, pruned := prune(next)
		if pruned {
			unmark(op)
			continue
		}
		lift(mv.call)
		f.taken = mv
		ms, done := moves()
		if done {
			return Result{Verdict: Linearizable, Steps: steps}
		}
		stack = append(stack, frame[S]{moves: ms, state: next, cfg: cfg})
	}
}

func firstReturn(head *entry) *entry {
	for e := head.next; e != nil; e = e.next {
		if !e.call {
			return e
		}
	}
	return nil
}

func explain[S comparable, O any](m Model[S, O], events []Event[O], best []move, stop *entry, st S) string {
	describe := func(i int) string {
		ev := events[i]
		ret := fmt.Sprint(ev.Return)
		if ev.Unknown && ev.Return == 0 {
			ret = "?"
		} else if ev.Unknown {
			ret = "?<=" + ret
		}
		d := fmt.Sprintf("%v", ev.Op)
		if m.DescribeOp != nil {
			d = m.DescribeOp(ev.Op)
		}
		return fmt.Sprintf("client %d [%d, %s] %s", ev.Client, ev.Call, ret, d)
	}
	desc := func(s S) string {
		if m.DescribeState != nil {
			return m.DescribeState(s)
		}
		return fmt.Sprintf("%v", s)
	}
	var b []byte
	const show = 12
	b = fmt.Appendf(b, "longest linearizable prefix found: %d of %d operations", len(best), len(events))
	if len(best) > show {
		b = fmt.Appendf(b, ", the last %d of them", show)
	}
	b = append(b, ":\n"...)
	s := m.Init()
	in := map[int]bool{}
	for k, mv := range best {
		i := mv.call.op
		in[i] = true
		line := ""
		if mv.skip {
			line = fmt.Sprintf("  %s  never took effect\n", describe(i))
		} else {
			_, s = m.Step(s, events[i].Op, events[i].Unknown)
			line = fmt.Sprintf("  %s  -> %s\n", describe(i), desc(s))
		}
		if k >= len(best)-show {
			b = append(b, line...)
		}
	}
	if stop != nil {
		b = fmt.Appendf(b, "in state %s nothing can be linearized before the return of\n  %s\n", desc(st), describe(stop.op))
		b = fmt.Appendf(b, "operations invoked by then and not linearized:\n")
		var pending []int
		for i, ev := range events {
			if !in[i] && ev.Call <= stop.time {
				pending = append(pending, i)
			}
		}
		sort.Slice(pending, func(a, c int) bool { return events[pending[a]].Call < events[pending[c]].Call })
		for _, i := range pending {
			b = fmt.Appendf(b, "  %s\n", describe(i))
		}
	}
	return string(b)
}
