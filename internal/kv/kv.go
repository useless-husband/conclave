// Package kv is the replicated state machine: a string key-value map with
// get, put, delete and compare-and-swap, plus the client sessions that make
// retried writes take effect exactly once.
//
// Every server applies the same committed commands in the same order, so
// Apply must be a pure function of the store and the command: no clock, no
// randomness, no map iteration order. Session eviction, which a real
// deployment needs to bound memory, is therefore driven by log position
// rather than time.
//
// # Sessions
//
// A client first commits a Register command; the log index at which it
// commits becomes its session ID. Each write then carries (session, seq)
// with seq increasing by one per write. The store remembers, per session,
// the highest seq applied and its result. A command whose seq equals that
// number is a retry of a write that already took effect: the cached result
// is returned and nothing is applied. A lower seq belongs to a write the
// client has given up on; it is refused. This is the scheme of the Raft
// dissertation (§6.3).
package kv

import (
	"errors"
	"fmt"
	"hash/fnv"
	"sort"

	"github.com/useless-husband/conclave/internal/mutation"
	"github.com/useless-husband/conclave/internal/raft"
)

// Kind is the kind of a command.
type Kind uint8

const (
	// Get reads a key. Reads are normally served through ReadIndex and
	// never enter the log, but a Get in the log is valid.
	Get Kind = iota + 1
	// Put sets a key.
	Put
	// Delete removes a key.
	Delete
	// CAS sets a key only if its current value is Expect (or, with
	// ExpectAbsent, only if it does not exist).
	CAS
	// Register opens a client session.
	Register

	numKinds
)

var kindNames = [...]string{Get: "get", Put: "put", Delete: "delete", CAS: "cas", Register: "register"}

func (k Kind) String() string {
	if k == 0 || k >= numKinds {
		return fmt.Sprintf("kind(%d)", uint8(k))
	}
	return kindNames[k]
}

// IsWrite reports whether the command changes the store.
func (k Kind) IsWrite() bool { return k == Put || k == Delete || k == CAS }

// Command is one request to the state machine.
type Command struct {
	Kind Kind
	// Session and Seq identify a write for de-duplication. Session 0
	// means no session: the command is applied every time it commits.
	Session uint64
	Seq     uint64

	Key          string
	Value        string
	Expect       string
	ExpectAbsent bool
}

func (c Command) String() string {
	switch c.Kind {
	case Get, Delete:
		return fmt.Sprintf("%v(%q)", c.Kind, c.Key)
	case Put:
		return fmt.Sprintf("put(%q, %q)", c.Key, c.Value)
	case CAS:
		if c.ExpectAbsent {
			return fmt.Sprintf("cas(%q, <absent>, %q)", c.Key, c.Value)
		}
		return fmt.Sprintf("cas(%q, %q, %q)", c.Key, c.Expect, c.Value)
	}
	return c.Kind.String()
}

// Encode returns the command's log representation.
func (c Command) Encode() []byte {
	var e raft.Encoder
	e.Byte(byte(c.Kind))
	e.Uvarint(c.Session)
	e.Uvarint(c.Seq)
	e.Bytes([]byte(c.Key))
	e.Bytes([]byte(c.Value))
	e.Bytes([]byte(c.Expect))
	e.Bool(c.ExpectAbsent)
	return e.B
}

// ErrBadCommand is returned for bytes that are not an encoded Command.
var ErrBadCommand = errors.New("kv: malformed command")

// DecodeCommand parses the output of Encode.
func DecodeCommand(b []byte) (Command, error) {
	d := raft.NewDecoder(b)
	c := Command{
		Kind:    Kind(d.Byte()),
		Session: d.Uvarint(),
		Seq:     d.Uvarint(),
	}
	c.Key = string(d.Bytes())
	c.Value = string(d.Bytes())
	c.Expect = string(d.Bytes())
	c.ExpectAbsent = d.Bool()
	if d.Err() != nil || d.Remaining() != 0 || c.Kind == 0 || c.Kind >= numKinds {
		return Command{}, ErrBadCommand
	}
	return c, nil
}

// Code is the outcome of a command.
type Code uint8

const (
	// OK: the command took effect (or, for Get, the key exists).
	OK Code = iota
	// NotFound: Get or Delete of a key that does not exist.
	NotFound
	// CASFailed: the key's current value did not match.
	CASFailed
	// SessionExpired: the session is unknown, normally because it was
	// evicted. Whether an earlier attempt of this write took effect can
	// no longer be determined.
	SessionExpired
	// Stale: the session has already applied a later write, so this one
	// was abandoned by its client and is refused.
	Stale
	// BadCommand: the entry did not decode.
	BadCommand
)

var codeNames = [...]string{"ok", "not-found", "cas-failed", "session-expired", "stale", "bad-command"}

func (c Code) String() string {
	if int(c) >= len(codeNames) {
		return fmt.Sprintf("code(%d)", uint8(c))
	}
	return codeNames[c]
}

// Result is what a command returned.
type Result struct {
	Code Code
	// Value is the value read by Get, or the current value when a CAS
	// fails (empty if the key is absent).
	Value string
	// Session is the new session's ID, for Register.
	Session uint64
}

func (r Result) String() string {
	switch {
	case r.Session != 0:
		return fmt.Sprintf("ok session=%d", r.Session)
	case r.Code == OK && r.Value != "":
		return fmt.Sprintf("ok %q", r.Value)
	}
	return r.Code.String()
}

// EncodeResult appends r.
func EncodeResult(e *raft.Encoder, r Result) {
	e.Byte(byte(r.Code))
	e.Bytes([]byte(r.Value))
	e.Uvarint(r.Session)
}

// DecodeResult reads a Result.
func DecodeResult(d *raft.Decoder) Result {
	return Result{Code: Code(d.Byte()), Value: string(d.Bytes()), Session: d.Uvarint()}
}

type session struct {
	lastSeq uint64
	last    Result
	// touched is the log index of the session's latest command, the
	// eviction order.
	touched uint64
}

// DefaultMaxSessions bounds the number of live sessions.
const DefaultMaxSessions = 4096

// Store is the state machine. It is not safe for concurrent use.
type Store struct {
	data        map[string]string
	sessions    map[uint64]*session
	maxSessions int
	mut         mutation.Set

	// Evictions counts sessions dropped to stay under the limit.
	Evictions int
}

// New returns an empty store that keeps at most maxSessions sessions
// (DefaultMaxSessions if maxSessions <= 0).
func New(maxSessions int, mut mutation.Set) *Store {
	if maxSessions <= 0 {
		maxSessions = DefaultMaxSessions
	}
	return &Store{
		data:        map[string]string{},
		sessions:    map[uint64]*session{},
		maxSessions: maxSessions,
		mut:         mut,
	}
}

// Read returns the current value of key.
func (s *Store) Read(key string) (string, bool) {
	v, ok := s.data[key]
	return v, ok
}

// Len returns the number of keys.
func (s *Store) Len() int { return len(s.data) }

// Sessions returns the number of live sessions.
func (s *Store) Sessions() int { return len(s.sessions) }

// Apply applies the command committed at log index index.
func (s *Store) Apply(index uint64, data []byte) Result {
	c, err := DecodeCommand(data)
	if err != nil {
		return Result{Code: BadCommand}
	}
	if c.Kind == Register {
		s.sessions[index] = &session{touched: index}
		s.evict(index)
		return Result{Code: OK, Session: index}
	}
	if c.Session == 0 || !c.Kind.IsWrite() {
		return s.exec(c)
	}
	sess := s.sessions[c.Session]
	if sess == nil {
		return Result{Code: SessionExpired}
	}
	switch {
	case c.Seq < sess.lastSeq:
		return Result{Code: Stale}
	case c.Seq == sess.lastSeq && !s.mut.Has(mutation.DuplicateApply):
		sess.touched = index
		return sess.last
	}
	res := s.exec(c)
	sess.lastSeq, sess.last, sess.touched = c.Seq, res, index
	return res
}

func (s *Store) exec(c Command) Result {
	cur, ok := s.data[c.Key]
	switch c.Kind {
	case Get:
		if !ok {
			return Result{Code: NotFound}
		}
		return Result{Code: OK, Value: cur}
	case Put:
		s.data[c.Key] = c.Value
		return Result{Code: OK}
	case Delete:
		if !ok {
			return Result{Code: NotFound}
		}
		delete(s.data, c.Key)
		return Result{Code: OK}
	case CAS:
		if c.ExpectAbsent && ok || !c.ExpectAbsent && (!ok || cur != c.Expect) {
			return Result{Code: CASFailed, Value: cur}
		}
		s.data[c.Key] = c.Value
		return Result{Code: OK}
	}
	return Result{Code: BadCommand}
}

// evict drops the least recently used sessions until the limit holds. The
// newest session (just registered at index) is never the victim.
func (s *Store) evict(index uint64) {
	for len(s.sessions) > s.maxSessions {
		var victim uint64
		var oldest uint64
		for id, sess := range s.sessions {
			if id == index {
				continue
			}
			if victim == 0 || sess.touched < oldest || sess.touched == oldest && id < victim {
				victim, oldest = id, sess.touched
			}
		}
		delete(s.sessions, victim)
		s.Evictions++
	}
}

const snapMagic = "ckv1"

// Snapshot serializes the store. The encoding is canonical: equal stores
// produce equal bytes.
func (s *Store) Snapshot() []byte {
	e := raft.Encoder{B: []byte(snapMagic)}
	keys := make([]string, 0, len(s.data))
	for k := range s.data {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	e.Uvarint(uint64(len(keys)))
	for _, k := range keys {
		e.Bytes([]byte(k))
		e.Bytes([]byte(s.data[k]))
	}
	ids := make([]uint64, 0, len(s.sessions))
	for id := range s.sessions {
		ids = append(ids, id)
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	e.Uvarint(uint64(len(ids)))
	for _, id := range ids {
		sess := s.sessions[id]
		e.Uvarint(id)
		e.Uvarint(sess.lastSeq)
		e.Uvarint(sess.touched)
		EncodeResult(&e, sess.last)
	}
	return e.B
}

// Restore replaces the store's contents with a snapshot. Empty data
// restores the empty store (the snapshot of a newly bootstrapped cluster).
func (s *Store) Restore(data []byte) error {
	s.data = map[string]string{}
	s.sessions = map[uint64]*session{}
	if len(data) == 0 {
		return nil
	}
	if len(data) < len(snapMagic) || string(data[:len(snapMagic)]) != snapMagic {
		return errors.New("kv: snapshot has a bad magic number")
	}
	d := raft.NewDecoder(data[len(snapMagic):])
	n := d.Uvarint()
	for i := uint64(0); i < n && d.Err() == nil; i++ {
		k := string(d.Bytes())
		s.data[k] = string(d.Bytes())
	}
	n = d.Uvarint()
	for i := uint64(0); i < n && d.Err() == nil; i++ {
		id := d.Uvarint()
		sess := &session{lastSeq: d.Uvarint(), touched: d.Uvarint()}
		sess.last = DecodeResult(d)
		s.sessions[id] = sess
	}
	if d.Err() != nil || d.Remaining() != 0 {
		s.data = map[string]string{}
		s.sessions = map[uint64]*session{}
		return errors.New("kv: snapshot does not decode")
	}
	return nil
}

// Digest returns a hash of the complete state, for comparing replicas.
func (s *Store) Digest() uint64 {
	h := fnv.New64a()
	h.Write(s.Snapshot())
	return h.Sum64()
}
