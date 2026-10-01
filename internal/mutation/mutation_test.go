package mutation

import "testing"

func TestSet(t *testing.T) {
	var empty Set
	if !empty.Empty() || empty.String() != "none" {
		t.Fatal("zero Set is not empty")
	}
	s := ForTest(t, AckBeforeFsync, DuplicateApply)
	for _, m := range All() {
		want := m == AckBeforeFsync || m == DuplicateApply
		if s.Has(m) != want {
			t.Fatalf("%v: Has=%v", m, s.Has(m))
		}
	}
	if s.String() != "ack-before-fsync,duplicate-apply" {
		t.Fatal(s.String())
	}
	if len(All()) != 10 {
		t.Fatalf("%d mutations", len(All()))
	}
	for _, m := range All() {
		if m.String() == "" || m.String()[0] == 'm' && m.String()[:8] == "mutation" {
			t.Fatalf("mutation %d has no name", m)
		}
	}
}

func TestUnknownMutationPanics(t *testing.T) {
	defer func() {
		if recover() == nil {
			t.Fatal("no panic")
		}
	}()
	ForTest(t, Mutation(200))
}
