package lincheck

import (
	"fmt"
	"hash/fnv"
	"sort"
	"strings"

	"github.com/useless-husband/conclave/internal/kv"
)

// Op is one key-value operation as a client saw it.
type Op struct {
	Client int
	Cmd    kv.Command // Kind, Key, Value, Expect, ExpectAbsent
	Call   int64
	Return int64
	// Unknown marks an operation whose outcome the client never learned,
	// such as a write that timed out. It may or may not have taken effect.
	// If Return is not zero it bounds when the operation can have taken
	// effect: a client session gives that bound, because once a later
	// write of the same session has been applied, an earlier one never
	// will be.
	Unknown bool
	// Result is the observed outcome when Unknown is false: Code OK or
	// NotFound for get and delete, OK or CASFailed for cas, OK for put,
	// with Found and Value describing the key as the operation found it
	// (see kv.Result).
	Result kv.Result
}

func (o Op) String() string {
	out := "?"
	if !o.Unknown {
		out = o.Result.Code.String()
		if o.Cmd.Kind == kv.Get && o.Result.Code == kv.OK {
			out = fmt.Sprintf("%q", o.Result.Value)
		}
	}
	return fmt.Sprintf("%v -> %s", o.Cmd, out)
}

// register is the state of one key.
type register struct {
	value   string
	present bool
}

func (r register) String() string {
	if !r.present {
		return "<absent>"
	}
	return fmt.Sprintf("%q", r.value)
}

func stepKV(s register, op Op, unknown bool) (bool, register) {
	c, res := op.Cmd, op.Result
	// sees reports whether the result describes state s.
	sees := func() bool { return res.Found == s.present && (!s.present || res.Value == s.value) }
	switch c.Kind {
	case kv.Get:
		if unknown {
			return true, s
		}
		if s.present {
			return res.Code == kv.OK && sees(), s
		}
		return res.Code == kv.NotFound, s
	case kv.Put:
		return unknown || res.Code == kv.OK, register{value: c.Value, present: true}
	case kv.Delete:
		if unknown {
			return true, register{}
		}
		if s.present {
			return res.Code == kv.OK && sees(), register{}
		}
		return res.Code == kv.NotFound, s
	case kv.CAS:
		match := c.ExpectAbsent && !s.present || !c.ExpectAbsent && s.present && s.value == c.Expect
		if match {
			return unknown || res.Code == kv.OK, register{value: c.Value, present: true}
		}
		return unknown || res.Code == kv.CASFailed && sees(), s
	}
	return false, s
}

// KVModel is the sequential specification of one key.
var KVModel = Model[register, Op]{
	Init: func() register { return register{} },
	Step: stepKV,
	Hash: func(r register) uint64 {
		h := fnv.New64a()
		if r.present {
			h.Write([]byte{1})
		}
		h.Write([]byte(r.value))
		return h.Sum64()
	},
	EffectKey: func(o Op) string {
		c := o.Cmd
		switch c.Kind {
		case kv.Put:
			return "put\x00" + c.Value
		case kv.Delete:
			return "delete"
		case kv.CAS:
			if c.ExpectAbsent {
				return "cas-absent\x00" + c.Value
			}
			return "cas\x00" + c.Expect + "\x00" + c.Value
		}
		return ""
	},
	DescribeState: func(r register) string { return r.String() },
	DescribeOp:    func(o Op) string { return o.String() },
}

// KeyResult is the verdict for one key.
type KeyResult struct {
	Key string
	Ops int
	Result
}

// Report is the verdict for a whole history.
type Report struct {
	Verdict Verdict
	Keys    []KeyResult // one per key, sorted by key
	Ops     int
	// Dropped counts operations left out because they cannot affect the
	// verdict: reads with an unknown outcome, and writes with an unknown
	// outcome whose value no operation ever observed (see CheckKV).
	Dropped int
}

// Failed returns the per-key results that are violations.
func (r Report) Failed() []KeyResult {
	var out []KeyResult
	for _, k := range r.Keys {
		if k.Verdict == Violation {
			out = append(out, k)
		}
	}
	return out
}

func (r Report) String() string {
	var b strings.Builder
	fmt.Fprintf(&b, "%v: %d operations on %d keys", r.Verdict, r.Ops, len(r.Keys))
	for _, k := range r.Keys {
		if k.Verdict != Linearizable {
			fmt.Fprintf(&b, "\nkey %q (%d operations): %v after %d steps", k.Key, k.Ops, k.Verdict, k.Steps)
			if k.Explanation != "" {
				b.WriteString("\n")
				b.WriteString(k.Explanation)
			}
		}
	}
	return b.String()
}

// CheckKV partitions ops by key and checks every key, spending at most
// budget search steps per key (no limit if budget <= 0). Operations whose
// Return is before their Call are rejected by panicking: that is a bug in
// the recorder, not in the system under test.
//
// Before searching, CheckKV drops every write with an unknown outcome whose
// value is never mentioned by another operation of the same key: not
// returned by a get, a delete or a failed cas, and not expected by any cas.
// This is sound because every outcome in the model that depends on the
// key's state reveals that state. Given a linearization in which such a
// write w takes effect, the operations placed after w and before the next
// write either reveal w's value (impossible, it is never mentioned) or are
// unknown-outcome cas operations that fail in that state and can be left
// out; the next write does not depend on the state. Removing w and those
// no-ops leaves a valid linearization. Without this reduction every pending
// unknown write doubles the search space, and histories from runs with many
// timeouts exhaust any budget.
func CheckKV(ops []Op, budget int) Report {
	byKey := map[string][]Op{}
	rep := Report{}
	for _, o := range ops {
		if (!o.Unknown || o.Return != 0) && o.Return < o.Call {
			panic(fmt.Sprintf("lincheck: operation returns before it is called: %+v", o))
		}
		if o.Cmd.Kind == kv.Get && o.Unknown {
			rep.Dropped++
			continue
		}
		byKey[o.Cmd.Key] = append(byKey[o.Cmd.Key], o)
	}
	keys := make([]string, 0, len(byKey))
	for k := range byKey {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		kops := byKey[k]
		mentioned := map[string]bool{}
		for _, o := range kops {
			if o.Cmd.Kind == kv.CAS && !o.Cmd.ExpectAbsent {
				mentioned[o.Cmd.Expect] = true
			}
			if !o.Unknown && o.Result.Found {
				mentioned[o.Result.Value] = true
			}
		}
		events := make([]Event[Op], 0, len(kops))
		for _, o := range kops {
			if o.Unknown && (o.Cmd.Kind == kv.Put || o.Cmd.Kind == kv.CAS) && !mentioned[o.Cmd.Value] {
				rep.Dropped++
				continue
			}
			events = append(events, Event[Op]{Op: o, Client: o.Client, Call: o.Call, Return: o.Return, Unknown: o.Unknown})
		}
		rep.Ops += len(events)
		res := Check(KVModel, events, budget)
		rep.Keys = append(rep.Keys, KeyResult{Key: k, Ops: len(events), Result: res})
		switch {
		case res.Verdict == Violation:
			rep.Verdict = Violation
		case res.Verdict == Inconclusive && rep.Verdict == Linearizable:
			rep.Verdict = Inconclusive
		}
	}
	return rep
}
