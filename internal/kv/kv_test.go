package kv

import (
	"fmt"
	"reflect"
	"testing"

	"github.com/useless-husband/conclave/internal/mutation"
	"github.com/useless-husband/conclave/internal/prng"
	"github.com/useless-husband/conclave/internal/raft"
)

type applier struct {
	t     *testing.T
	s     *Store
	index uint64
}

func (a *applier) do(c Command) Result {
	a.index++
	return a.s.Apply(a.index, c.Encode())
}

func TestCommandRoundTrip(t *testing.T) {
	for _, c := range []Command{
		{Kind: Get, Key: "k"},
		{Kind: Put, Session: 7, Seq: 3, Key: "k", Value: "v"},
		{Kind: Delete, Session: 1, Seq: 1, Key: ""},
		{Kind: CAS, Session: 9, Seq: 1 << 40, Key: "k", Expect: "a", Value: "b"},
		{Kind: CAS, Key: "k", ExpectAbsent: true, Value: "b"},
		{Kind: Register},
	} {
		b := c.Encode()
		got, err := DecodeCommand(b)
		if err != nil || got != c {
			t.Fatalf("%v: got %+v, %v", c, got, err)
		}
		for i := 0; i < len(b); i++ {
			if _, err := DecodeCommand(b[:i]); err == nil {
				t.Fatalf("%v: prefix of %d bytes decoded", c, i)
			}
		}
	}
	if _, err := DecodeCommand([]byte{99, 0, 0, 0, 0, 0, 0}); err == nil {
		t.Fatal("unknown kind decoded")
	}
}

func TestOperations(t *testing.T) {
	a := &applier{t: t, s: New(0, mutation.Set{})}
	steps := []struct {
		c    Command
		want Result
	}{
		{Command{Kind: Get, Key: "x"}, Result{Code: NotFound}},
		{Command{Kind: Delete, Key: "x"}, Result{Code: NotFound}},
		{Command{Kind: CAS, Key: "x", Expect: "", Value: "1"}, Result{Code: CASFailed}},
		{Command{Kind: CAS, Key: "x", ExpectAbsent: true, Value: "1"}, Result{Code: OK}},
		{Command{Kind: CAS, Key: "x", ExpectAbsent: true, Value: "2"}, Result{Code: CASFailed, Value: "1"}},
		{Command{Kind: CAS, Key: "x", Expect: "1", Value: "2"}, Result{Code: OK}},
		{Command{Kind: Get, Key: "x"}, Result{Code: OK, Value: "2"}},
		{Command{Kind: Put, Key: "x", Value: ""}, Result{Code: OK}},
		{Command{Kind: Get, Key: "x"}, Result{Code: OK, Value: ""}},
		{Command{Kind: CAS, Key: "x", Expect: "", Value: "3"}, Result{Code: OK}},
		{Command{Kind: Delete, Key: "x"}, Result{Code: OK}},
		{Command{Kind: Get, Key: "x"}, Result{Code: NotFound}},
	}
	for i, s := range steps {
		if got := a.do(s.c); got != s.want {
			t.Fatalf("step %d %v: got %+v, want %+v", i, s.c, got, s.want)
		}
	}
	if got := a.s.Apply(99, []byte{0xff}); got.Code != BadCommand {
		t.Fatalf("garbage entry: %+v", got)
	}
}

func TestSessionsApplyRetriesOnce(t *testing.T) {
	a := &applier{t: t, s: New(0, mutation.Set{})}
	sess := a.do(Command{Kind: Register}).Session
	if sess == 0 {
		t.Fatal("no session id")
	}
	put := Command{Kind: CAS, Session: sess, Seq: 1, Key: "k", ExpectAbsent: true, Value: "a"}
	if got := a.do(put); got.Code != OK {
		t.Fatalf("first attempt: %+v", got)
	}
	// The retry would fail if applied again; instead it returns the
	// first attempt's result.
	if got := a.do(put); got.Code != OK {
		t.Fatalf("retry: %+v", got)
	}
	if got := a.do(Command{Kind: Put, Session: sess, Seq: 2, Key: "k", Value: "b"}); got.Code != OK {
		t.Fatal(got)
	}
	// A write the client abandoned arrives late and must not apply.
	if got := a.do(Command{Kind: Put, Session: sess, Seq: 1, Key: "k", Value: "late"}); got.Code != Stale {
		t.Fatalf("late write: %+v", got)
	}
	if v, _ := a.s.Read("k"); v != "b" {
		t.Fatalf("value %q, want b", v)
	}
	if got := a.do(Command{Kind: Put, Session: 12345, Seq: 1, Key: "k", Value: "x"}); got.Code != SessionExpired {
		t.Fatalf("unknown session: %+v", got)
	}
}

func TestDuplicateApplyMutant(t *testing.T) {
	a := &applier{t: t, s: New(0, mutation.ForTest(t, mutation.DuplicateApply))}
	sess := a.do(Command{Kind: Register}).Session
	put := Command{Kind: CAS, Session: sess, Seq: 1, Key: "k", ExpectAbsent: true, Value: "a"}
	a.do(put)
	if got := a.do(put); got.Code != CASFailed {
		t.Fatalf("mutant retry: %+v, want the write applied a second time", got)
	}
}

func TestSessionEvictionIsLRUByLogPosition(t *testing.T) {
	a := &applier{t: t, s: New(2, mutation.Set{})}
	s1 := a.do(Command{Kind: Register}).Session
	s2 := a.do(Command{Kind: Register}).Session
	a.do(Command{Kind: Put, Session: s1, Seq: 1, Key: "k", Value: "v"}) // s1 is now the most recent
	s3 := a.do(Command{Kind: Register}).Session
	if a.s.Sessions() != 2 || a.s.Evictions != 1 {
		t.Fatalf("sessions=%d evictions=%d", a.s.Sessions(), a.s.Evictions)
	}
	if got := a.do(Command{Kind: Put, Session: s2, Seq: 1, Key: "k", Value: "v"}); got.Code != SessionExpired {
		t.Fatalf("evicted session: %+v", got)
	}
	for _, s := range []uint64{s1, s3} {
		if got := a.do(Command{Kind: Put, Session: s, Seq: 5, Key: "k", Value: "v"}); got.Code != OK {
			t.Fatalf("live session %d: %+v", s, got)
		}
	}
}

func TestSnapshotRoundTripAndDeterminism(t *testing.T) {
	rng := prng.New(1)
	a := &applier{t: t, s: New(8, mutation.Set{})}
	var sessions []uint64
	for i := 0; i < 2000; i++ {
		key := fmt.Sprintf("k%d", rng.Intn(50))
		var c Command
		switch rng.Intn(5) {
		case 0:
			res := a.do(Command{Kind: Register})
			sessions = append(sessions, res.Session)
			continue
		case 1:
			c = Command{Kind: Delete, Key: key}
		case 2:
			c = Command{Kind: CAS, Key: key, Expect: fmt.Sprint(rng.Intn(5)), Value: fmt.Sprint(rng.Intn(5))}
		default:
			c = Command{Kind: Put, Key: key, Value: fmt.Sprint(rng.Intn(5))}
		}
		if len(sessions) > 0 && rng.Chance(1, 2) {
			c.Session = sessions[rng.Intn(len(sessions))]
			c.Seq = uint64(i)
		}
		a.do(c)
	}
	snap := a.s.Snapshot()
	b := New(8, mutation.Set{})
	if err := b.Restore(snap); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(a.s.data, b.data) || !reflect.DeepEqual(a.s.sessions, b.sessions) {
		t.Fatal("restored store differs")
	}
	if a.s.Digest() != b.Digest() {
		t.Fatal("digest differs")
	}
	// Both continue identically.
	for i := 0; i < 100; i++ {
		c := Command{Kind: Put, Session: sessions[len(sessions)-1], Seq: uint64(10000 + i), Key: "z", Value: fmt.Sprint(i)}
		if x, y := a.s.Apply(uint64(5000+i), c.Encode()), b.Apply(uint64(5000+i), c.Encode()); x != y {
			t.Fatalf("diverged: %+v vs %+v", x, y)
		}
	}
	if err := b.Restore(snap[:len(snap)-1]); err == nil {
		t.Fatal("truncated snapshot accepted")
	}
	if err := b.Restore(nil); err != nil || b.Len() != 0 {
		t.Fatal("empty snapshot")
	}
}

func TestResultCodec(t *testing.T) {
	r := Result{Code: CASFailed, Value: "cur", Session: 3}
	var e raft.Encoder
	EncodeResult(&e, r)
	d := raft.NewDecoder(e.B)
	if got := DecodeResult(d); got != r || d.Err() != nil {
		t.Fatalf("%+v %v", got, d.Err())
	}
}
