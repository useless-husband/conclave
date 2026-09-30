package raft

import (
	"encoding/binary"
	"errors"
	"fmt"
)

// The wire and on-disk encoding is a plain sequence of unsigned varints and
// length-prefixed byte strings. It is shared by the TCP transport, the
// write-ahead log and the simulator's network, so that the simulator
// exercises the same bytes production does.

// ErrCorrupt is returned when decoding malformed input.
var ErrCorrupt = errors.New("raft: malformed encoding")

// maxDecodeLen bounds any single length field so that corrupt input cannot
// ask for an absurd allocation.
const maxDecodeLen = 1 << 30

// Encoder appends primitive values to a byte slice.
type Encoder struct{ B []byte }

// Uvarint appends v.
func (e *Encoder) Uvarint(v uint64) { e.B = binary.AppendUvarint(e.B, v) }

// Byte appends one byte.
func (e *Encoder) Byte(b byte) { e.B = append(e.B, b) }

// Bool appends a boolean.
func (e *Encoder) Bool(b bool) {
	if b {
		e.B = append(e.B, 1)
	} else {
		e.B = append(e.B, 0)
	}
}

// Bytes appends a length-prefixed byte string.
func (e *Encoder) Bytes(p []byte) {
	e.Uvarint(uint64(len(p)))
	e.B = append(e.B, p...)
}

// Decoder reads values written by Encoder. After the first error every
// read returns a zero value and Err reports the error.
type Decoder struct {
	b   []byte
	err error
}

// NewDecoder returns a Decoder over b. Byte strings returned by the decoder
// are copies and do not alias b.
func NewDecoder(b []byte) *Decoder { return &Decoder{b: b} }

// Err returns the first decoding error.
func (d *Decoder) Err() error { return d.err }

// Remaining returns the number of undecoded bytes.
func (d *Decoder) Remaining() int { return len(d.b) }

func (d *Decoder) fail() {
	if d.err == nil {
		d.err = ErrCorrupt
	}
	d.b = nil
}

// Uvarint reads one unsigned varint.
func (d *Decoder) Uvarint() uint64 {
	if d.err != nil {
		return 0
	}
	v, n := binary.Uvarint(d.b)
	if n <= 0 {
		d.fail()
		return 0
	}
	d.b = d.b[n:]
	return v
}

// Byte reads one byte.
func (d *Decoder) Byte() byte {
	if d.err != nil {
		return 0
	}
	if len(d.b) < 1 {
		d.fail()
		return 0
	}
	v := d.b[0]
	d.b = d.b[1:]
	return v
}

// Bool reads a boolean and rejects any byte other than 0 or 1.
func (d *Decoder) Bool() bool {
	b := d.Byte()
	if b > 1 {
		d.fail()
	}
	return b == 1
}

// Bytes reads a length-prefixed byte string. An empty string decodes as nil.
func (d *Decoder) Bytes() []byte {
	n := d.Uvarint()
	if d.err != nil {
		return nil
	}
	if n > uint64(len(d.b)) || n > maxDecodeLen {
		d.fail()
		return nil
	}
	if n == 0 {
		return nil
	}
	out := make([]byte, n)
	copy(out, d.b[:n])
	d.b = d.b[n:]
	return out
}

// count reads an element count and checks it against the bytes left, given
// that each element needs at least min bytes.
func (d *Decoder) count(min int) int {
	n := d.Uvarint()
	if d.err != nil {
		return 0
	}
	if n > uint64(len(d.b)/min) {
		d.fail()
		return 0
	}
	return int(n)
}

// EncodeEntry appends e.
func EncodeEntry(enc *Encoder, e Entry) {
	enc.Uvarint(e.Index)
	enc.Uvarint(e.Term)
	enc.Byte(byte(e.Type))
	enc.Bytes(e.Data)
}

// DecodeEntry reads one entry.
func DecodeEntry(d *Decoder) Entry {
	var e Entry
	e.Index = d.Uvarint()
	e.Term = d.Uvarint()
	e.Type = EntryType(d.Byte())
	e.Data = d.Bytes()
	if e.Type > EntryConfChange {
		d.fail()
	}
	return e
}

// EncodeEntries appends a counted list of entries.
func EncodeEntries(enc *Encoder, ents []Entry) {
	enc.Uvarint(uint64(len(ents)))
	for _, e := range ents {
		EncodeEntry(enc, e)
	}
}

// DecodeEntries reads a counted list of entries.
func DecodeEntries(d *Decoder) []Entry {
	n := d.count(4)
	if n == 0 {
		return nil
	}
	out := make([]Entry, 0, n)
	for i := 0; i < n && d.err == nil; i++ {
		out = append(out, DecodeEntry(d))
	}
	if d.err != nil {
		return nil
	}
	return out
}

// EncodeMembership appends m.
func EncodeMembership(enc *Encoder, m Membership) {
	enc.Uvarint(uint64(len(m.Members)))
	for _, mem := range m.Members {
		enc.Uvarint(uint64(mem.ID))
		enc.Bytes(mem.Meta)
	}
}

// DecodeMembership reads a membership and checks that it is sorted and free
// of duplicates and zero IDs.
func DecodeMembership(d *Decoder) Membership {
	n := d.count(2)
	var m Membership
	var prev NodeID
	for i := 0; i < n && d.err == nil; i++ {
		id := NodeID(d.Uvarint())
		meta := d.Bytes()
		if id == None || (i > 0 && id <= prev) {
			d.fail()
			break
		}
		prev = id
		m.Members = append(m.Members, Member{ID: id, Meta: meta})
	}
	if d.err != nil {
		return Membership{}
	}
	return m
}

// EncodeSnapshot appends s.
func EncodeSnapshot(enc *Encoder, s Snapshot) {
	enc.Uvarint(s.Index)
	enc.Uvarint(s.Term)
	EncodeMembership(enc, s.Conf)
	enc.Bytes(s.Data)
}

// DecodeSnapshot reads a snapshot.
func DecodeSnapshot(d *Decoder) Snapshot {
	var s Snapshot
	s.Index = d.Uvarint()
	s.Term = d.Uvarint()
	s.Conf = DecodeMembership(d)
	s.Data = d.Bytes()
	return s
}

// EncodeConf returns the encoding stored in an EntryConfChange: the complete
// membership that takes effect at that entry.
func EncodeConf(m Membership) []byte {
	var enc Encoder
	EncodeMembership(&enc, m)
	return enc.B
}

// DecodeConf parses the data of an EntryConfChange.
func DecodeConf(b []byte) (Membership, error) {
	d := NewDecoder(b)
	m := DecodeMembership(d)
	if d.err == nil && d.Remaining() != 0 {
		d.fail()
	}
	if d.err != nil {
		return Membership{}, fmt.Errorf("membership: %w", d.err)
	}
	return m, nil
}

const (
	flagReject   = 1 << 0
	flagTransfer = 1 << 1
	flagSnapshot = 1 << 2
)

// EncodeMessage appends m to buf and returns the extended slice.
func EncodeMessage(buf []byte, m Message) []byte {
	enc := Encoder{B: buf}
	enc.Byte(byte(m.Type))
	var flags byte
	if m.Reject {
		flags |= flagReject
	}
	if m.Transfer {
		flags |= flagTransfer
	}
	if m.Snapshot != nil {
		flags |= flagSnapshot
	}
	enc.Byte(flags)
	enc.Uvarint(uint64(m.From))
	enc.Uvarint(uint64(m.To))
	enc.Uvarint(m.Term)
	enc.Uvarint(m.Index)
	enc.Uvarint(m.LogTerm)
	enc.Uvarint(m.Commit)
	enc.Uvarint(m.ConflictIndex)
	enc.Uvarint(m.ConflictTerm)
	enc.Uvarint(m.Context)
	EncodeEntries(&enc, m.Entries)
	if m.Snapshot != nil {
		EncodeSnapshot(&enc, *m.Snapshot)
	}
	return enc.B
}

// DecodeMessage parses one message. It never panics on malformed input and
// requires that the whole buffer is consumed.
func DecodeMessage(b []byte) (Message, error) {
	d := NewDecoder(b)
	var m Message
	m.Type = MsgType(d.Byte())
	flags := d.Byte()
	m.Reject = flags&flagReject != 0
	m.Transfer = flags&flagTransfer != 0
	m.From = NodeID(d.Uvarint())
	m.To = NodeID(d.Uvarint())
	m.Term = d.Uvarint()
	m.Index = d.Uvarint()
	m.LogTerm = d.Uvarint()
	m.Commit = d.Uvarint()
	m.ConflictIndex = d.Uvarint()
	m.ConflictTerm = d.Uvarint()
	m.Context = d.Uvarint()
	m.Entries = DecodeEntries(d)
	if flags&flagSnapshot != 0 {
		s := DecodeSnapshot(d)
		m.Snapshot = &s
	}
	if d.err == nil {
		switch {
		case m.Type == 0 || m.Type >= numMsgTypes,
			flags&^(flagReject|flagTransfer|flagSnapshot) != 0,
			m.From == None,
			d.Remaining() != 0,
			(m.Type == MsgSnap) != (m.Snapshot != nil):
			d.fail()
		}
	}
	if d.err != nil {
		return Message{}, fmt.Errorf("message: %w", d.err)
	}
	for i := range m.Entries {
		// Entries in one message must be consecutive; checking here keeps
		// the core free of bounds surprises from a corrupt peer.
		if m.Entries[i].Index != m.Index+1+uint64(i) {
			return Message{}, fmt.Errorf("message: %w", ErrCorrupt)
		}
	}
	return m, nil
}
