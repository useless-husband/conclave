package sim

import (
	"github.com/useless-husband/conclave/internal/node"
	"github.com/useless-husband/conclave/internal/raft"
)

// delay draws one network delay: usually uniform in [DelayMin, DelayMax],
// occasionally a spike of up to SpikeMax, which is how messages overtake
// each other.
func (s *Sim) delay() Time {
	p := &s.prof
	if p.SpikePermille > 0 && s.netRng.Intn(1000) < p.SpikePermille {
		return p.DelayMin + Time(s.netRng.Intn(int(p.SpikeMax)))
	}
	return p.DelayMin + Time(s.netRng.Intn(int(p.DelayMax-p.DelayMin)+1))
}

// lossy decides whether one transmission is lost and how many copies of it
// arrive.
func (s *Sim) copies() int {
	if s.prof.DropPermille > 0 && s.netRng.Intn(1000) < s.prof.DropPermille {
		return 0
	}
	if s.prof.DupPermille > 0 && s.netRng.Intn(1000) < s.prof.DupPermille {
		return 2
	}
	return 1
}

// sendRaft carries a message between two servers. The message travels as
// bytes, exactly as over TCP: the receiver decodes its own copy.
func (s *Sim) sendRaft(from raft.NodeID, m raft.Message) {
	s.stats.Messages++
	if s.blocked[[2]raft.NodeID{from, m.To}] {
		s.stats.Dropped++
		return
	}
	b := raft.EncodeMessage(nil, m)
	n := s.copies()
	if n == 0 {
		s.stats.Dropped++
	}
	src := s.node(from)
	for i := 0; i < n; i++ {
		e := &event{kind: evDeliver, node: m.To, msg: b, sent: s.now}
		s.at(s.delay(), e)
		if src != nil {
			src.outbox = append(src.outbox, e)
		}
	}
}

// sendWindow is how long a message may sit in the sender's buffers: a
// crash loses the messages sent within the last LossWindow of it.
const sendWindow = 2 * Millisecond

// loseSendBuffer cancels the messages n sent within the last window before
// a crash, as a machine that loses power takes its socket buffers with it.
func (s *Sim) loseSendBuffer(n *simNode, window Time) int {
	lost := 0
	for _, e := range n.outbox {
		if e.sent >= s.now-window && !e.cancelled {
			e.cancelled = true
			lost++
		}
	}
	n.outbox = n.outbox[:0]
	return lost
}

// trimOutbox forgets messages old enough to have left the machine.
func (n *simNode) trimOutbox(now Time) {
	i := 0
	for i < len(n.outbox) && n.outbox[i].sent < now-sendWindow {
		i++
	}
	if i > 0 {
		n.outbox = append(n.outbox[:0], n.outbox[i:]...)
	}
}

// sendRequest carries a client request to a server. Clients are never
// partitioned away, but their messages can be lost, duplicated and
// delayed like any other.
func (s *Sim) sendRequest(to raft.NodeID, req node.Request) {
	for i := s.copies(); i > 0; i-- {
		s.at(s.delay(), &event{kind: evRequest, node: to, req: req})
	}
}

func (s *Sim) onRequest(e *event) {
	n := s.node(e.node)
	if n == nil || n.nd == nil {
		return
	}
	if s.now < n.pausedUntil {
		e.at = n.pausedUntil
		s.schedule(e)
		return
	}
	s.onNode(n, func(nd *node.Node) { nd.Submit(e.req) })
}

// routeResponse sends a server's answer back to whoever asked.
func (s *Sim) routeResponse(from *simNode, resp node.Response) {
	rt, ok := s.routes[resp.ID]
	if !ok {
		return
	}
	if rt.admin {
		s.onAdminResponse(from, rt, resp)
		return
	}
	for i := s.copies(); i > 0; i-- {
		s.at(s.delay(), &event{kind: evResponse, cl: rt.client, resp: resp})
	}
}

// partition kinds.
const (
	partIsolate = iota
	partSplit
	partOneWayOut
	partOneWayIn
	partBridge
	partRandom
	numPartKinds
)

var partNames = [...]string{"isolate", "split", "one-way-out", "one-way-in", "bridge", "random-links"}

// partition installs a new partition among the servers that are members or
// about to be.
func (s *Sim) partition() {
	s.blocked = map[[2]raft.NodeID]bool{}
	var ids []raft.NodeID
	for _, n := range s.nodes {
		if !n.retired {
			ids = append(ids, n.id)
		}
	}
	if len(ids) < 2 {
		return
	}
	r := s.nemRng
	// Shuffle deterministically.
	for i := len(ids) - 1; i > 0; i-- {
		j := r.Intn(i + 1)
		ids[i], ids[j] = ids[j], ids[i]
	}
	// Prefer the leader as the victim half of the time: that is where the
	// interesting failures are.
	if l := s.leader(); l != nil && r.Chance(1, 2) {
		for i, id := range ids {
			if id == l.id {
				ids[0], ids[i] = ids[i], ids[0]
			}
		}
	}
	cut := func(a, b raft.NodeID) { s.blocked[[2]raft.NodeID{a, b}] = true }
	kind := r.Intn(numPartKinds)
	switch kind {
	case partIsolate:
		for _, b := range ids[1:] {
			cut(ids[0], b)
			cut(b, ids[0])
		}
	case partSplit:
		k := 1 + r.Intn(len(ids)-1)
		for _, a := range ids[:k] {
			for _, b := range ids[k:] {
				cut(a, b)
				cut(b, a)
			}
		}
	case partOneWayOut:
		// ids[0] can receive but not send.
		for _, b := range ids[1:] {
			cut(ids[0], b)
		}
	case partOneWayIn:
		// ids[0] can send but not receive.
		for _, b := range ids[1:] {
			cut(b, ids[0])
		}
	case partBridge:
		// ids[0] sees everyone; the rest are split in two halves that only
		// see the bridge.
		rest := ids[1:]
		k := len(rest) / 2
		for _, a := range rest[:k] {
			for _, b := range rest[k:] {
				cut(a, b)
				cut(b, a)
			}
		}
	case partRandom:
		for _, a := range ids {
			for _, b := range ids {
				if a != b && r.Chance(1, 3) {
					cut(a, b)
				}
			}
		}
	}
	s.stats.Partitions++
	s.tracef("PARTITION %s %s", partNames[kind], s.describeBlocked())
}

func (s *Sim) describeBlocked() string {
	out := ""
	for _, a := range s.nodes {
		for _, b := range s.nodes {
			if s.blocked[[2]raft.NodeID{a.id, b.id}] {
				if out != "" {
					out += " "
				}
				out += "n" + itoa(uint64(a.id)) + "-x>n" + itoa(uint64(b.id))
			}
		}
	}
	if out == "" {
		return "(no links cut)"
	}
	return out
}

func itoa(v uint64) string {
	if v == 0 {
		return "0"
	}
	var b [20]byte
	i := len(b)
	for v > 0 {
		i--
		b[i] = byte('0' + v%10)
		v /= 10
	}
	return string(b[i:])
}

func (s *Sim) healPartition() {
	if len(s.blocked) == 0 {
		return
	}
	s.blocked = map[[2]raft.NodeID]bool{}
	s.tracef("HEAL network")
}
