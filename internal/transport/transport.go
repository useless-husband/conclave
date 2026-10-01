// Package transport carries Raft messages between servers over TCP.
//
// Each outbound connection starts with a hello frame naming the sender and
// its listen address, so that a server that has just been added, and does
// not yet know the membership, can still answer the leader. After that the
// connection carries messages in length-prefixed frames, encoded with the
// same codec the simulator uses.
//
// Send never blocks. Every peer has a bounded queue drained by its own
// goroutine; when the queue is full or the peer unreachable, messages are
// dropped, which Raft tolerates by design.
package transport

import (
	"bufio"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"sync"
	"time"

	"github.com/useless-husband/conclave/internal/raft"
)

const (
	// MaxFrame bounds a single message (snapshots included).
	MaxFrame  = 256 << 20
	queueSize = 4096
	helloTag  = "conclave-raft\x01"
)

// Transport is one server's endpoint.
type Transport struct {
	self   raft.NodeID
	ln     net.Listener
	inbox  chan raft.Message
	logger *log.Logger

	mu     sync.Mutex
	addrs  map[raft.NodeID]string // from the membership
	heard  map[raft.NodeID]string // from hello frames
	peers  map[raft.NodeID]*peer
	conns  map[net.Conn]bool
	closed bool
	done   chan struct{}
	wg     sync.WaitGroup
}

// Listen starts accepting peer connections on addr.
func Listen(self raft.NodeID, addr string, logger *log.Logger) (*Transport, error) {
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return nil, err
	}
	t := &Transport{
		self:   self,
		ln:     ln,
		inbox:  make(chan raft.Message, 1024),
		logger: logger,
		addrs:  map[raft.NodeID]string{},
		heard:  map[raft.NodeID]string{},
		peers:  map[raft.NodeID]*peer{},
		conns:  map[net.Conn]bool{},
		done:   make(chan struct{}),
	}
	t.wg.Add(1)
	go t.accept()
	return t, nil
}

// Addr returns the address peers should dial.
func (t *Transport) Addr() string { return t.ln.Addr().String() }

// Inbox delivers received messages.
func (t *Transport) Inbox() <-chan raft.Message { return t.inbox }

// SetAddrs replaces the addresses known from the membership.
func (t *Transport) SetAddrs(addrs map[raft.NodeID]string) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.addrs = addrs
	for id, p := range t.peers {
		// A member that left, or moved: stop its connection. If it is
		// still needed (a server not yet in the membership, known from its
		// hello), the next Send starts a new one.
		if a, ok := addrs[id]; !ok || a != p.addr {
			p.stop()
			delete(t.peers, id)
		}
	}
}

func (t *Transport) addrOf(id raft.NodeID) string {
	if a, ok := t.addrs[id]; ok {
		return a
	}
	return t.heard[id]
}

// Send queues m for delivery. It never blocks.
func (t *Transport) Send(m raft.Message) {
	t.mu.Lock()
	if t.closed {
		t.mu.Unlock()
		return
	}
	p := t.peers[m.To]
	if p == nil {
		addr := t.addrOf(m.To)
		if addr == "" {
			t.mu.Unlock()
			return
		}
		p = newPeer(t, m.To, addr)
		t.peers[m.To] = p
	}
	t.mu.Unlock()
	select {
	case p.queue <- raft.EncodeMessage(nil, m):
	default:
		// Queue full: drop. Raft retransmits what matters.
	}
}

// Close stops everything and waits for the goroutines to exit.
func (t *Transport) Close() error {
	t.mu.Lock()
	if t.closed {
		t.mu.Unlock()
		return nil
	}
	t.closed = true
	close(t.done)
	err := t.ln.Close()
	for _, p := range t.peers {
		p.stop()
	}
	for c := range t.conns {
		c.Close()
	}
	t.mu.Unlock()
	t.wg.Wait()
	return err
}

func (t *Transport) accept() {
	defer t.wg.Done()
	for {
		c, err := t.ln.Accept()
		if err != nil {
			return
		}
		t.mu.Lock()
		if t.closed {
			t.mu.Unlock()
			c.Close()
			return
		}
		t.conns[c] = true
		t.wg.Add(1)
		t.mu.Unlock()
		go t.serve(c)
	}
}

func readFrame(r *bufio.Reader, buf []byte) ([]byte, error) {
	var hdr [4]byte
	if _, err := io.ReadFull(r, hdr[:]); err != nil {
		return nil, err
	}
	n := binary.BigEndian.Uint32(hdr[:])
	if n > MaxFrame {
		return nil, fmt.Errorf("frame of %d bytes exceeds the limit", n)
	}
	if cap(buf) < int(n) {
		buf = make([]byte, n)
	}
	buf = buf[:n]
	_, err := io.ReadFull(r, buf)
	return buf, err
}

func writeFrame(w *bufio.Writer, b []byte) error {
	var hdr [4]byte
	binary.BigEndian.PutUint32(hdr[:], uint32(len(b)))
	if _, err := w.Write(hdr[:]); err != nil {
		return err
	}
	_, err := w.Write(b)
	return err
}

func encodeHello(id raft.NodeID, addr string) []byte {
	e := raft.Encoder{B: []byte(helloTag)}
	e.Uvarint(uint64(id))
	e.Bytes([]byte(addr))
	return e.B
}

func decodeHello(b []byte) (raft.NodeID, string, error) {
	if len(b) < len(helloTag) || string(b[:len(helloTag)]) != helloTag {
		return 0, "", errors.New("not a conclave peer")
	}
	d := raft.NewDecoder(b[len(helloTag):])
	id := raft.NodeID(d.Uvarint())
	addr := string(d.Bytes())
	if d.Err() != nil || d.Remaining() != 0 || id == raft.None {
		return 0, "", errors.New("malformed hello")
	}
	return id, addr, nil
}

func (t *Transport) serve(c net.Conn) {
	defer func() {
		c.Close()
		t.mu.Lock()
		delete(t.conns, c)
		t.mu.Unlock()
		t.wg.Done()
	}()
	r := bufio.NewReaderSize(c, 64<<10)
	b, err := readFrame(r, nil)
	if err != nil {
		return
	}
	from, addr, err := decodeHello(b)
	if err != nil {
		t.logf("rejecting connection from %v: %v", c.RemoteAddr(), err)
		return
	}
	t.mu.Lock()
	t.heard[from] = addr
	t.mu.Unlock()
	var buf []byte
	for {
		buf, err = readFrame(r, buf)
		if err != nil {
			return
		}
		m, err := raft.DecodeMessage(buf)
		if err != nil || m.From != from || m.To != t.self {
			t.logf("dropping connection from n%d: bad message (%v)", from, err)
			return
		}
		select {
		case t.inbox <- m:
		case <-t.done:
			return
		}
	}
}

func (t *Transport) logf(format string, args ...any) {
	if t.logger != nil {
		t.logger.Printf(format, args...)
	}
}

// peer owns the outbound connection to one server.
type peer struct {
	t     *Transport
	id    raft.NodeID
	addr  string
	queue chan []byte
	done  chan struct{}
	once  sync.Once
}

func newPeer(t *Transport, id raft.NodeID, addr string) *peer {
	p := &peer{t: t, id: id, addr: addr, queue: make(chan []byte, queueSize), done: make(chan struct{})}
	t.wg.Add(1)
	go p.run()
	return p
}

func (p *peer) stop() { p.once.Do(func() { close(p.done) }) }

func (p *peer) run() {
	defer p.t.wg.Done()
	backoff := 10 * time.Millisecond
	for {
		var first []byte
		select {
		case <-p.done:
			return
		case first = <-p.queue:
		}
		c, err := net.DialTimeout("tcp", p.addr, time.Second)
		if err != nil {
			// Drop what is queued: it is stale by the time the peer is
			// back, and Raft will send fresh messages.
			p.drain()
			select {
			case <-p.done:
				return
			case <-time.After(backoff):
			}
			backoff = min(2*backoff, time.Second)
			continue
		}
		backoff = 10 * time.Millisecond
		p.t.mu.Lock()
		p.t.conns[c] = true
		p.t.mu.Unlock()
		err = p.pump(c, first)
		c.Close()
		p.t.mu.Lock()
		delete(p.t.conns, c)
		p.t.mu.Unlock()
		if err == nil {
			return
		}
	}
}

func (p *peer) drain() {
	for {
		select {
		case <-p.queue:
		default:
			return
		}
	}
}

// pump writes frames until the peer is stopped (nil) or the connection
// fails (an error).
func (p *peer) pump(c net.Conn, first []byte) error {
	w := bufio.NewWriterSize(c, 64<<10)
	if err := writeFrame(w, encodeHello(p.t.self, p.t.Addr())); err != nil {
		return err
	}
	msg := first
	for {
		c.SetWriteDeadline(time.Now().Add(5 * time.Second))
		if err := writeFrame(w, msg); err != nil {
			return err
		}
		select {
		case msg = <-p.queue:
			continue
		default:
		}
		if err := w.Flush(); err != nil {
			return err
		}
		select {
		case <-p.done:
			return nil
		case msg = <-p.queue:
		}
	}
}
