package sim

import (
	"github.com/useless-husband/conclave/internal/node"
	"github.com/useless-husband/conclave/internal/raft"
)

type nemesis uint8

const (
	nemPartition nemesis = iota + 1
	nemCrash
	nemPause
	nemMembership
	nemTransfer
	nemReconfigure // one-off membership change, not rescheduled
)

func (s *Sim) every(k nemesis) Time {
	switch k {
	case nemPartition:
		return s.prof.PartitionEvery
	case nemCrash:
		return s.prof.CrashEvery
	case nemPause:
		return s.prof.PauseEvery
	case nemMembership:
		return s.prof.MembershipEvery
	case nemTransfer:
		return s.prof.TransferEvery
	}
	return 0
}

// next schedules the next action of kind k, a uniformly random time between
// zero and twice the profile's mean away.
func (s *Sim) next(k nemesis) {
	if mean := s.every(k); mean > 0 {
		s.at(1+Time(s.nemRng.Intn(int(2*mean))), &event{kind: evNemesis, nem: k})
	}
}

func (s *Sim) startNemesis() {
	for k := nemPartition; k <= nemTransfer; k++ {
		s.next(k)
	}
}

func (s *Sim) onNemesis(e *event) {
	if !s.chaos {
		return
	}
	switch e.nem {
	case nemPartition:
		if len(s.blocked) > 0 {
			s.healPartition()
		} else {
			s.partition()
		}
	case nemCrash:
		s.nemCrash()
	case nemPause:
		s.nemPause()
	case nemMembership:
		s.nemMembership()
	case nemTransfer:
		s.nemTransfer()
	case nemReconfigure:
		s.nemMembership()
		return
	}
	s.next(e.nem)
}

// victim picks a live server, the leader half of the time.
func (s *Sim) victim() *simNode {
	if l := s.leader(); l != nil && s.nemRng.Chance(1, 2) {
		return l
	}
	var live []*simNode
	for _, n := range s.nodes {
		if n.nd != nil {
			live = append(live, n)
		}
	}
	if len(live) == 0 {
		return nil
	}
	return live[s.nemRng.Intn(len(live))]
}

func (s *Sim) nemCrash() {
	n := s.victim()
	if n == nil {
		return
	}
	if s.nemRng.Intn(100) < s.prof.MidIOPercent {
		if !n.disk.Armed() {
			k := 1 + s.nemRng.Intn(6)
			n.disk.Arm(k)
			s.tracef("n%d will crash during its disk operation %d from now", n.id, k)
		}
		return
	}
	s.crash(n, "killed")
}

func (s *Sim) nemPause() {
	n := s.victim()
	if n == nil || s.now < n.pausedUntil {
		return
	}
	d := 20*Millisecond + Time(s.nemRng.Intn(int(800*Millisecond)))
	n.pausedUntil = s.now + d
	s.stats.Pauses++
	s.tracef("n%d PAUSE for %v", n.id, d)
}

// crashed is called by crash: schedule the restart.
func (s *Sim) scheduleRestart(n *simNode) {
	var d Time
	if n.bounce && s.chaos {
		d = 1*Millisecond + Time(s.nemRng.Intn(int(30*Millisecond)))
	} else if s.chaos {
		d = 10*Millisecond + Time(s.nemRng.Intn(int(2*Second)))
	} else {
		d = 10*Millisecond + Time(s.nemRng.Intn(int(100*Millisecond)))
	}
	n.bounce = false
	s.at(d, &event{kind: evRestart, node: n.id, gen: n.inc})
}

// committedConf returns the leader and its membership if the latest change
// in its log is committed.
func (s *Sim) committedConf() (*simNode, raft.Membership, bool) {
	l := s.leader()
	if l == nil {
		return nil, raft.Membership{}, false
	}
	st := l.nd.Raft().Status()
	if st.ConfIndex > st.Commit {
		return l, st.Conf, false
	}
	return l, st.Conf, true
}

func (s *Sim) admin(l *simNode, req node.Request) {
	s.nextReq++
	req.ID = s.nextReq
	s.routes[req.ID] = route{admin: true, adminOp: req.Op, member: req.Member}
	s.onNode(l, func(nd *node.Node) { nd.Submit(req) })
}

func (s *Sim) nemMembership() {
	l, conf, ok := s.committedConf()
	if !ok {
		return
	}
	s.retireRemoved(conf)
	voters := conf.IDs()
	add := len(voters) < 5 && (len(voters) <= 3 || s.nemRng.Chance(1, 2))
	if add {
		var cand *simNode
		for _, n := range s.nodes {
			if !n.joined && !n.retired && !n.broken && n.nd != nil {
				cand = n
				break
			}
		}
		if cand == nil {
			cand = s.addNode(false)
			s.tracef("n%d started empty, to be added", cand.id)
		}
		s.tracef("ADD n%d requested at leader n%d", cand.id, l.id)
		s.admin(l, node.Request{Op: node.OpAddMember, Member: cand.id})
		return
	}
	if len(voters) > 3 {
		x := voters[s.nemRng.Intn(len(voters))]
		s.tracef("REMOVE n%d requested at leader n%d", x, l.id)
		s.admin(l, node.Request{Op: node.OpRemoveMember, Member: x})
	}
}

// retireRemoved shuts down, for good, every server that was a member and no
// longer is according to a committed membership.
func (s *Sim) retireRemoved(conf raft.Membership) {
	for _, n := range s.nodes {
		if conf.Contains(n.id) {
			n.joined = true
			continue
		}
		if n.joined && !n.retired {
			n.retired = true
			n.nd = nil
			n.inc++
			s.tracef("n%d RETIRED (removed from the cluster)", n.id)
		}
	}
}

func (s *Sim) onAdminResponse(from *simNode, rt route, resp node.Response) {
	delete(s.routes, resp.ID)
	switch rt.adminOp {
	case node.OpAddMember:
		if resp.Status == node.StatusOK {
			s.stats.MembershipAdds++
		}
		s.tracef("ADD n%d at n%d: %v", rt.member, from.id, resp.Status)
	case node.OpRemoveMember:
		if resp.Status == node.StatusOK {
			s.stats.MembershipRems++
		}
		s.tracef("REMOVE n%d at n%d: %v", rt.member, from.id, resp.Status)
	case node.OpTransfer:
		s.tracef("TRANSFER to n%d at n%d: %v", rt.member, from.id, resp.Status)
	}
}

func (s *Sim) nemTransfer() {
	l := s.leader()
	if l == nil {
		return
	}
	voters := l.nd.Raft().Membership().IDs()
	if len(voters) < 2 {
		return
	}
	to := voters[s.nemRng.Intn(len(voters))]
	if to == l.id {
		return
	}
	s.stats.Transfers++
	s.admin(l, node.Request{Op: node.OpTransfer, Member: to})
}

// heal ends the faulty phase: the network is repaired, paused servers
// resume, crashed ones restart, and no new faults are injected.
func (s *Sim) heal() {
	s.chaos = false
	s.healed = s.now
	s.tracef("HEAL everything: no more faults from now on")
	s.healPartition()
	for _, n := range s.nodes {
		n.pausedUntil = 0
		n.disk.Disarm()
		if n.nd == nil && !n.retired && !n.broken {
			s.at(Time(1+s.nemRng.Intn(int(50*Millisecond))), &event{kind: evRestart, node: n.id, gen: n.inc})
		}
	}
}
