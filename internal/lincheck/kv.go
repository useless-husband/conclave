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
	Unknown bool
	// Result is the observed outcome when Unknown is false: Code OK or
	// NotFound (with Value) for get, OK or NotFound for delete, OK or
	// CASFailed for cas, OK for put.
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
	switch c.Kind {
	case kv.Get:
		if unknown {
			return true, s
		}
		if s.present {
			return res.Code == kv.OK && res.Value == s.value, s
		}
		return res.Code == kv.NotFound, s
	case kv.Put:
		return unknown || res.Code == kv.OK, register{value: c.Value, present: true}
	case kv.Delete:
		if unknown {
			return true, register{}
		}
		if s.present {
			return res.Code == kv.OK, register{}
		}
		return res.Code == kv.NotFound, s
	case kv.CAS:
		match := c.ExpectAbsent && !s.present || !c.ExpectAbsent && s.present && s.value == c.Expect
		if match {
			return unknown || res.Code == kv.OK, register{value: c.Value, present: true}
		}
		return unknown || res.Code == kv.CASFailed, s
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
	Dropped int // reads with unknown outcome, which cannot affect the verdict
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
func CheckKV(ops []Op, budget int) Report {
	byKey := map[string][]Event[Op]{}
	rep := Report{}
	for _, o := range ops {
		if !o.Unknown && o.Return < o.Call {
			panic(fmt.Sprintf("lincheck: operation returns before it is called: %+v", o))
		}
		if o.Cmd.Kind == kv.Get && o.Unknown {
			rep.Dropped++
			continue
		}
		rep.Ops++
		byKey[o.Cmd.Key] = append(byKey[o.Cmd.Key], Event[Op]{Op: o, Client: o.Client, Call: o.Call, Return: o.Return, Unknown: o.Unknown})
	}
	keys := make([]string, 0, len(byKey))
	for k := range byKey {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		res := Check(KVModel, byKey[k], budget)
		rep.Keys = append(rep.Keys, KeyResult{Key: k, Ops: len(byKey[k]), Result: res})
		switch {
		case res.Verdict == Violation:
			rep.Verdict = Violation
		case res.Verdict == Inconclusive && rep.Verdict == Linearizable:
			rep.Verdict = Inconclusive
		}
	}
	return rep
}
