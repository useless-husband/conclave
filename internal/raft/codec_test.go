package raft

import (
	"bytes"
	"reflect"
	"testing"
)

func sampleMessages() []Message {
	return []Message{
		{Type: MsgVote, From: 1, To: 2, Term: 7, Index: 12, LogTerm: 6, Transfer: true},
		{Type: MsgPreVoteResp, From: 2, To: 1, Term: 8, Reject: true},
		{Type: MsgApp, From: 1, To: 3, Term: 7, Index: 10, LogTerm: 6, Commit: 9, Entries: []Entry{
			{Index: 11, Term: 7, Type: EntryNormal, Data: []byte("hello")},
			{Index: 12, Term: 7, Type: EntryNoop},
			{Index: 13, Term: 7, Type: EntryConfChange, Data: EncodeConf(NewMembership(Member{ID: 1}, Member{ID: 4, Meta: []byte("m")}))},
		}},
		{Type: MsgAppResp, From: 3, To: 1, Term: 7, Index: 10, Reject: true, ConflictIndex: 4, ConflictTerm: 2},
		{Type: MsgHeartbeat, From: 1, To: 3, Term: 7, Commit: 9, Context: 1 << 40},
		{Type: MsgSnap, From: 1, To: 3, Term: 7, Snapshot: &Snapshot{
			Index: 100, Term: 6, Data: bytes.Repeat([]byte{0xab}, 300),
			Conf: NewMembership(Member{ID: 1, Meta: []byte("a")}, Member{ID: 2}, Member{ID: 3, Meta: []byte("ccc")}),
		}},
		{Type: MsgTimeoutNow, From: 1, To: 2, Term: 7},
	}
}

func TestMessageRoundTrip(t *testing.T) {
	for _, m := range sampleMessages() {
		b := EncodeMessage(nil, m)
		got, err := DecodeMessage(b)
		if err != nil {
			t.Fatalf("%v: %v", m.Type, err)
		}
		if !reflect.DeepEqual(got, m) {
			t.Fatalf("round trip\n got %+v\nwant %+v", got, m)
		}
		// Every strict prefix must be rejected, never accepted or panic.
		for i := 0; i < len(b); i++ {
			if _, err := DecodeMessage(b[:i]); err == nil {
				t.Fatalf("%v: %d-byte prefix of %d decoded without error", m.Type, i, len(b))
			}
		}
		if _, err := DecodeMessage(append(b, 0)); err == nil {
			t.Fatalf("%v: trailing byte accepted", m.Type)
		}
	}
}

func TestDecodeRejectsNonConsecutiveEntries(t *testing.T) {
	m := Message{Type: MsgApp, From: 1, To: 2, Term: 1, Index: 5, Entries: []Entry{{Index: 6, Term: 1}, {Index: 8, Term: 1}}}
	if _, err := DecodeMessage(EncodeMessage(nil, m)); err == nil {
		t.Fatal("append with a gap between entries decoded")
	}
}

func TestDecodeMembershipRejectsUnsorted(t *testing.T) {
	var enc Encoder
	enc.Uvarint(2)
	enc.Uvarint(5)
	enc.Bytes(nil)
	enc.Uvarint(5)
	enc.Bytes(nil)
	d := NewDecoder(enc.B)
	DecodeMembership(d)
	if d.Err() == nil {
		t.Fatal("duplicate member accepted")
	}
}

func TestConfRoundTrip(t *testing.T) {
	conf := NewMembership(Member{ID: 7, Meta: []byte("meta")}, Member{ID: 2})
	got, err := DecodeConf(EncodeConf(conf))
	if err != nil || !reflect.DeepEqual(got, conf) {
		t.Fatalf("got %+v, %v", got, err)
	}
	if _, err := DecodeConf(append(EncodeConf(conf), 0)); err == nil {
		t.Fatal("trailing byte accepted")
	}
}

func FuzzDecodeMessage(f *testing.F) {
	for _, m := range sampleMessages() {
		f.Add(EncodeMessage(nil, m))
	}
	f.Add([]byte{})
	f.Add([]byte{5, 0, 1, 2, 3, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0x7f})
	f.Fuzz(func(t *testing.T, b []byte) {
		m, err := DecodeMessage(b)
		if err != nil {
			return
		}
		// Anything that decodes must re-encode to something that decodes
		// to the same message.
		again, err := DecodeMessage(EncodeMessage(nil, m))
		if err != nil {
			t.Fatalf("re-encoded message does not decode: %v", err)
		}
		if !reflect.DeepEqual(m, again) {
			t.Fatalf("unstable round trip\n%+v\n%+v", m, again)
		}
	})
}
