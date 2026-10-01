package sim

import (
	"fmt"

	"github.com/useless-husband/conclave/internal/kv"
	"github.com/useless-husband/conclave/internal/lincheck"
	"github.com/useless-husband/conclave/internal/node"
	"github.com/useless-husband/conclave/internal/prng"
	"github.com/useless-husband/conclave/internal/raft"
)

// client is a simulated client. It runs one operation at a time, retrying
// each on other servers until it gets an answer or its deadline passes,
// with the same session and sequence number for every attempt of a write;
// this is the protocol of the real client library (package client).
type client struct {
	idx     int
	rng     *prng.Rand
	session uint64
	seq     uint64
	leader  raft.NodeID
	opNum   uint64
	cur     *clientOp
	timer   uint64 // generation of the live timer
	written int
	seen    map[string]string // last value observed per key, for CAS
	// abandoned lists the history entries of this session's writes whose
	// outcome is unknown. Once a later write of the session is applied,
	// they never will be, which bounds them (see lincheck.Op.Unknown).
	abandoned []int
}

type clientOp struct {
	num      uint64
	cmd      kv.Command
	start    Time
	deadline Time
	attempts []uint64
	// open holds the attempts that have not been refused outright: each
	// of them may still take effect. An answer saying "not executed"
	// covers only the attempt it answers; a duplicated or late refusal of
	// an earlier attempt says nothing about the one in flight.
	open map[uint64]bool
	// unknown is set when a server answered that the write may or may
	// not take effect.
	unknown bool
}

// mayHaveExecuted reports whether the write could have taken effect.
func (op *clientOp) mayHaveExecuted() bool {
	return op.cmd.Kind.IsWrite() && (op.unknown || len(op.open) > 0)
}

func (op *clientOp) latest() uint64 {
	if len(op.attempts) == 0 {
		return 0
	}
	return op.attempts[len(op.attempts)-1]
}

func (s *Sim) addClient(i int) {
	c := &client{idx: i, rng: s.rng.Fork(300 + uint64(i)), seen: map[string]string{}}
	s.clients = append(s.clients, c)
	s.wakeClient(c, Time(c.rng.Intn(int(10*Millisecond)))+1, false)
}

// wakeClient arms the client's timer: either a resend after a refusal, or
// the timeout of the attempt in flight.
func (s *Sim) wakeClient(c *client, d Time, resend bool) {
	c.timer++
	r := 0
	if resend {
		r = 1
	}
	s.at(d, &event{kind: evClient, cl: c.idx, gen: c.timer, retry: r})
}

func (s *Sim) onClientTimer(e *event) {
	c := s.clients[e.cl]
	if e.gen != c.timer {
		return
	}
	switch {
	case c.cur == nil:
		s.startOp(c)
	case e.retry == 1:
		s.attempt(c)
	default:
		// The attempt timed out. It stays open: its request or answer
		// may be lost, or it may still be in a log somewhere.
		c.leader = raft.None
		s.attempt(c)
	}
}

func (s *Sim) startOp(c *client) {
	c.opNum++
	op := &clientOp{num: c.opNum, start: s.now, deadline: s.now + s.prof.OpDeadline, open: map[uint64]bool{}}
	if c.session == 0 {
		op.cmd = kv.Command{Kind: kv.Register}
	} else {
		key := fmt.Sprintf("k%d", c.rng.Intn(s.prof.Keys))
		c.written++
		val := fmt.Sprintf("c%d.%d", c.idx, c.written)
		switch r := c.rng.Intn(10); {
		case r < 4:
			op.cmd = kv.Command{Kind: kv.Get, Key: key}
		case r < 7:
			op.cmd = kv.Command{Kind: kv.Put, Key: key, Value: val}
		case r < 9:
			op.cmd = kv.Command{Kind: kv.CAS, Key: key, Value: val}
			if prev, ok := c.seen[key]; ok && c.rng.Chance(3, 4) {
				op.cmd.Expect = prev
			} else {
				op.cmd.ExpectAbsent = true
			}
		default:
			op.cmd = kv.Command{Kind: kv.Delete, Key: key}
		}
		if op.cmd.Kind.IsWrite() {
			c.seq++
			op.cmd.Session, op.cmd.Seq = c.session, c.seq
		}
	}
	c.cur = op
	s.attempt(c)
}

// attempt sends the current operation to the server the client believes
// leads, or to a random server.
func (s *Sim) attempt(c *client) {
	op := c.cur
	if s.now >= op.deadline {
		s.giveUp(c)
		return
	}
	target := c.leader
	if t := s.node(target); t == nil || t.retired {
		var live []raft.NodeID
		for _, n := range s.nodes {
			if !n.retired {
				live = append(live, n.id)
			}
		}
		target = live[c.rng.Intn(len(live))]
	}
	s.nextReq++
	id := s.nextReq
	op.attempts = append(op.attempts, id)
	op.open[id] = true
	s.routes[id] = route{client: c.idx, op: op.num}
	s.tracef("client %d sends %v to n%d (request %d)", c.idx, op.cmd, target, id)
	s.sendRequest(target, node.Request{ID: id, Op: node.OpCommand, Cmd: op.cmd})
	s.wakeClient(c, s.prof.ClientTimeout, false)
}

func (s *Sim) onResponse(e *event) {
	c := s.clients[e.cl]
	rt, ok := s.routes[e.resp.ID]
	if !ok || c.cur == nil || rt.op != c.cur.num {
		return // an answer to an operation already finished
	}
	op, resp := c.cur, e.resp
	if resp.Status != node.StatusOK {
		s.tracef("client %d request %d: %v (leader hint n%d)", c.idx, resp.ID, resp.Status, resp.Leader)
	}
	if resp.Status == node.StatusOK {
		// Any attempt's answer is the operation's answer: every attempt
		// of a write carries the same session and sequence number.
		s.complete(c, resp)
		return
	}
	switch resp.Status {
	case node.StatusNotLeader, node.StatusRetry:
		delete(op.open, resp.ID)
	case node.StatusUnknown:
		op.unknown = true
	default:
		s.violate("client", "client %d: request %v rejected: %+v", c.idx, op.cmd, resp)
		s.finishOp(c)
		return
	}
	if resp.ID != op.latest() {
		// A late answer to an attempt the client has already given up
		// waiting for; the attempt in flight decides what happens next.
		return
	}
	if l := s.node(resp.Leader); l != nil && !l.retired {
		c.leader = resp.Leader
	} else {
		c.leader = raft.None
	}
	if resp.Status == node.StatusNotLeader && resp.Leader != raft.None {
		s.attempt(c)
		return
	}
	s.wakeClient(c, Time(1+c.rng.Intn(20))*Millisecond, true)
}

func (s *Sim) complete(c *client, resp node.Response) {
	op := c.cur
	res := resp.Result
	switch {
	case op.cmd.Kind == kv.Register:
		c.session, c.seq = res.Session, 0
		s.tracef("client %d registered session %d", c.idx, res.Session)
		s.finishOp(c)
		return
	case res.Code == kv.SessionExpired:
		// Whether an earlier attempt took effect can no longer be known,
		// but the session is gone for good: nothing of it can take
		// effect from now on.
		s.tracef("client %d session %d expired during %v", c.idx, c.session, op.cmd)
		op.unknown = true
		s.giveUp(c)
		s.bound(c)
		c.session = 0
		return
	case res.Code == kv.Stale:
		s.violate("session", "client %d: the latest write of session %d, %v (seq %d), was refused as stale",
			c.idx, op.cmd.Session, op.cmd, op.cmd.Seq)
		s.finishOp(c)
		return
	}
	switch {
	case op.cmd.Kind == kv.Get && res.Code == kv.OK:
		c.seen[op.cmd.Key] = res.Value
	case op.cmd.Kind == kv.Get || op.cmd.Kind == kv.Delete:
		delete(c.seen, op.cmd.Key)
	case res.Code == kv.OK:
		c.seen[op.cmd.Key] = op.cmd.Value
	case res.Found:
		c.seen[op.cmd.Key] = res.Value
	}
	s.record(c, op, false, res)
	if op.cmd.Kind.IsWrite() {
		s.bound(c)
	}
	s.stats.Ops++
	if !s.chaos && op.start >= s.healed {
		s.healOps++
	}
	s.tracef("client %d %v -> %v (index %d)", c.idx, op.cmd, res, resp.Index)
	s.finishOp(c)
}

// giveUp ends the current operation without an answer. A write that may
// have reached a log enters the history with an unknown outcome; anything
// else had no effect and is left out.
func (s *Sim) giveUp(c *client) {
	op := c.cur
	if op.mayHaveExecuted() {
		c.abandoned = append(c.abandoned, len(s.hist))
		s.record(c, op, true, kv.Result{})
		s.stats.Unknown++
		s.tracef("client %d gives up on %v: outcome unknown", c.idx, op.cmd)
	} else {
		s.tracef("client %d gives up on %v: not executed", c.idx, op.cmd)
	}
	s.finishOp(c)
}

func (s *Sim) record(c *client, op *clientOp, unknown bool, res kv.Result) {
	h := lincheck.Op{Client: c.idx, Cmd: op.cmd, Call: int64(op.start), Unknown: unknown, Result: res}
	if !unknown {
		h.Return = int64(s.now)
	}
	// Session bookkeeping is not part of the observable behaviour.
	h.Cmd.Session, h.Cmd.Seq = 0, 0
	s.hist = append(s.hist, h)
}

// bound closes the abandoned writes of the client's session: a write of the
// session has just been applied (or the session has been evicted), so they
// took effect before now or never will.
func (s *Sim) bound(c *client) {
	for _, i := range c.abandoned {
		s.hist[i].Return = int64(s.now)
	}
	c.abandoned = c.abandoned[:0]
}

func (s *Sim) finishOp(c *client) {
	for _, id := range c.cur.attempts {
		delete(s.routes, id)
	}
	c.cur = nil
	// Think time is at least a microsecond so that one client's
	// operations never touch in time.
	s.wakeClient(c, 1+Time(c.rng.Intn(int(s.prof.ThinkMax))), false)
}

// abandon is called at the end of the run for the operation in flight.
func (c *client) abandon(s *Sim) {
	if c.cur == nil {
		return
	}
	s.giveUp(c)
	c.timer++
}
