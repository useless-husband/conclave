package transport

import (
	"testing"
	"time"

	"github.com/useless-husband/conclave/internal/raft"
)

func recv(t *testing.T, tr *Transport) raft.Message {
	t.Helper()
	select {
	case m := <-tr.Inbox():
		return m
	case <-time.After(5 * time.Second):
		t.Fatal("no message")
	}
	return raft.Message{}
}

func TestExchangeAndLearnFromHello(t *testing.T) {
	a, err := Listen(1, "127.0.0.1:0", nil)
	if err != nil {
		t.Fatal(err)
	}
	defer a.Close()
	b, err := Listen(2, "127.0.0.1:0", nil)
	if err != nil {
		t.Fatal(err)
	}
	defer b.Close()
	// Only a knows where b is, like a leader contacting a server it has
	// just added.
	a.SetAddrs(map[raft.NodeID]string{2: b.Addr()})
	big := make([]byte, 3<<20)
	a.Send(raft.Message{Type: raft.MsgApp, From: 1, To: 2, Term: 3, Index: 7, Entries: []raft.Entry{{Index: 8, Term: 3, Data: big}}})
	m := recv(t, b)
	if m.Type != raft.MsgApp || m.Term != 3 || len(m.Entries[0].Data) != len(big) {
		t.Fatalf("got %+v", m.Type)
	}
	// b learned a's address from the hello frame and can answer.
	b.Send(raft.Message{Type: raft.MsgAppResp, From: 2, To: 1, Term: 3, Index: 8})
	if m := recv(t, a); m.Type != raft.MsgAppResp || m.Index != 8 {
		t.Fatalf("got %+v", m)
	}
}

func TestReconnectAfterPeerRestart(t *testing.T) {
	a, err := Listen(1, "127.0.0.1:0", nil)
	if err != nil {
		t.Fatal(err)
	}
	defer a.Close()
	b, err := Listen(2, "127.0.0.1:0", nil)
	if err != nil {
		t.Fatal(err)
	}
	addr := b.Addr()
	a.SetAddrs(map[raft.NodeID]string{2: addr})
	a.Send(raft.Message{Type: raft.MsgHeartbeat, From: 1, To: 2, Term: 1})
	recv(t, b)
	b.Close()
	b, err = Listen(2, addr, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer b.Close()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		a.Send(raft.Message{Type: raft.MsgHeartbeat, From: 1, To: 2, Term: 2})
		select {
		case m := <-b.Inbox():
			if m.Term == 2 {
				return
			}
		case <-time.After(50 * time.Millisecond):
		}
	}
	t.Fatal("no delivery after the peer restarted")
}

func TestRejectsStrangers(t *testing.T) {
	if _, _, err := decodeHello([]byte("GET / HTTP/1.1\r\n")); err == nil {
		t.Fatal("accepted a non-peer")
	}
	id, addr, err := decodeHello(encodeHello(7, "127.0.0.1:1"))
	if err != nil || id != 7 || addr != "127.0.0.1:1" {
		t.Fatalf("%v %q %v", id, addr, err)
	}
}
