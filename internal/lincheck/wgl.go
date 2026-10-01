// Package lincheck decides whether a concurrent history is linearizable.
//
// The search is the algorithm of Wing and Gong as improved by Lowe ("Testing
// for linearizability", 2017): walk a doubly linked list of call and return
// events, tentatively linearize any operation whose call is reached, and
// backtrack when the return of an operation that has not been linearized is
// reached. A cache of (set of linearized operations, model state) pairs that
// have already been explored prunes the search; the set is hashed
// incrementally with Zobrist hashing so a step costs O(1) apart from the
// cache insertion.
//
// Operations whose outcome is unknown (a write that timed out) have their
// return placed at infinity: they may be linearized at any point after
// their call, or not at all. The search succeeds as soon as every operation
// with a known outcome has been linearized.
//
// The KV checker (kv.go) partitions a history by key and checks each key
// separately, which is sound because linearizability is local (Herlihy and
// Wing, 1990): a history is linearizable if and only if its restriction to
// every object is.
package lincheck

import (
	"fmt"
	"math"
	"math/bits"
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
	// Describe renders a state and an operation for explanations.
	DescribeState func(s S) string
	DescribeOp    func(op O) string
}

// Event is one operation of a history. Call and Return are positions in a
// total order of events (timestamps or sequence numbers). Operation a
// precedes b in real time if a.Return < b.Call.
type Event[O any] struct {
	Op      O
	Client  int
	Call    int64
	Return  int64
	Unknown bool // outcome not observed; Return is ignored
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
	// Steps is the number of search steps taken.
	Steps int
	// Explanation is set for a violation: the longest linearizable prefix
	// found and the operation that could not be placed after it.
	Explanation string
}

type entry struct {
	op     int
	call   bool
	time   int64
	match  *entry
	prev   *entry
	next   *entry
	unknwn bool
}

type bitset []uint64

func (b bitset) set(i int)   { b[i/64] |= 1 << (uint(i) % 64) }
func (b bitset) clear(i int) { b[i/64] &^= 1 << (uint(i) % 64) }
func (b bitset) count() int {
	n := 0
	for _, w := range b {
		n += bits.OnesCount64(w)
	}
	return n
}

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

type cached[S comparable] struct {
	bits  bitset
	state S
}

// Check searches for a linearization of events under model m, taking at
// most budget steps (no limit if budget <= 0).
func Check[S comparable, O any](m Model[S, O], events []Event[O], budget int) Result {
	n := len(events)
	if n == 0 {
		return Result{Verdict: Linearizable}
	}
	all := make([]*entry, 0, 2*n)
	for i, ev := range events {
		c := &entry{op: i, call: true, time: ev.Call, unknwn: ev.Unknown}
		rt := ev.Return
		if ev.Unknown {
			rt = math.MaxInt64
		}
		r := &entry{op: i, time: rt, unknwn: ev.Unknown}
		c.match, r.match = r, c
		all = append(all, c, r)
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

	lift := func(e *entry) {
		e.prev.next = e.next
		if e.next != nil {
			e.next.prev = e.prev
		}
		r := e.match
		r.prev.next = r.next
		if r.next != nil {
			r.next.prev = r.prev
		}
	}
	unlift := func(e *entry) {
		r := e.match
		r.prev.next = r
		if r.next != nil {
			r.next.prev = r
		}
		e.prev.next = e
		if e.next != nil {
			e.next.prev = e
		}
	}

	type frame struct {
		e     *entry
		state S
	}
	var (
		lin      = make(bitset, (n+63)/64)
		zob      uint64
		cache    = map[uint64][]cached[S]{}
		stack    []frame
		state    = m.Init()
		e        = head.next
		steps    int
		best     []int // deepest linearized prefix seen
		bestStop *entry
		bestSt   S
	)
	seen := func(h uint64, s S) bool {
		for _, c := range cache[h] {
			if c.state == s && c.bits.equal(lin) {
				return true
			}
		}
		cache[h] = append(cache[h], cached[S]{bits: append(bitset(nil), lin...), state: s})
		return false
	}

	for head.next != nil {
		steps++
		if budget > 0 && steps > budget {
			return Result{Verdict: Inconclusive, Steps: steps}
		}
		if e.call {
			ok, next := m.Step(state, events[e.op].Op, e.unknwn)
			if ok {
				lin.set(e.op)
				z := zob ^ zobrist(e.op)
				if !seen(z^m.Hash(next), next) {
					stack = append(stack, frame{e: e, state: state})
					state, zob = next, z
					lift(e)
					e = head.next
					continue
				}
				lin.clear(e.op)
			}
			e = e.next
			continue
		}
		// A return: the operation it ends must already be linearized,
		// and it is not.
		if e.unknwn {
			// Only returns at infinity remain: every operation with a
			// known outcome is linearized.
			return Result{Verdict: Linearizable, Steps: steps}
		}
		if len(stack) >= len(best) {
			best = best[:0]
			for _, f := range stack {
				best = append(best, f.e.op)
			}
			bestStop, bestSt = e, state
		}
		if len(stack) == 0 {
			return Result{Verdict: Violation, Steps: steps, Explanation: explain(m, events, best, bestStop, bestSt)}
		}
		f := stack[len(stack)-1]
		stack = stack[:len(stack)-1]
		state = f.state
		lin.clear(f.e.op)
		zob ^= zobrist(f.e.op)
		unlift(f.e)
		e = f.e.next
	}
	return Result{Verdict: Linearizable, Steps: steps}
}

func explain[S comparable, O any](m Model[S, O], events []Event[O], best []int, stop *entry, st S) string {
	describe := func(i int) string {
		ev := events[i]
		ret := fmt.Sprint(ev.Return)
		if ev.Unknown {
			ret = "?"
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
	b = fmt.Appendf(b, "longest linearizable prefix (%d of %d operations):\n", len(best), len(events))
	s := m.Init()
	for _, i := range best {
		_, s = m.Step(s, events[i].Op, events[i].Unknown)
		b = fmt.Appendf(b, "  %s  -> %s\n", describe(i), desc(s))
	}
	if stop != nil {
		b = fmt.Appendf(b, "no operation can be linearized next. The earliest pending return is\n  %s\n", describe(stop.op))
		b = fmt.Appendf(b, "which is impossible in state %s, and no other pending operation leads to a state where it is.\n", desc(st))
		b = fmt.Appendf(b, "operations that had been invoked but not linearized at that point:\n")
		in := map[int]bool{}
		for _, i := range best {
			in[i] = true
		}
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
