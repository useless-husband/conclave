package prng

import "testing"

// The generator's output is part of the simulator's contract: if these
// values change, every recorded seed means something else.
func TestGoldenSequence(t *testing.T) {
	r := New(42)
	// Reference values from an independent implementation of
	// splitmix64-seeded xoshiro256** for seed 42.
	want := []uint64{0x15780b2e0c2ec716, 0x6104d9866d113a7e, 0xae17533239e499a1, 0xecb8ad4703b360a1}
	for i, w := range want {
		if got := r.Uint64(); got != w {
			t.Fatalf("value %d = %#x, want %#x", i, got, w)
		}
	}
}

func TestIntnRangeAndSpread(t *testing.T) {
	r := New(7)
	var counts [10]int
	for i := 0; i < 100000; i++ {
		v := r.Intn(10)
		if v < 0 || v >= 10 {
			t.Fatalf("Intn(10) = %d", v)
		}
		counts[v]++
	}
	for i, c := range counts {
		if c < 9500 || c > 10500 {
			t.Fatalf("bucket %d has %d of 100000 draws", i, c)
		}
	}
	for i := 0; i < 1000; i++ {
		if v := r.Between(-3, 3); v < -3 || v > 3 {
			t.Fatalf("Between(-3,3) = %d", v)
		}
	}
	if r.Chance(0, 10) || !r.Chance(10, 10) {
		t.Fatal("Chance bounds")
	}
}

func TestForkIsIndependentAndStable(t *testing.T) {
	a, b := New(1), New(1)
	fa := a.Fork(3)
	a.Uint64() // consuming from the parent must not change the fork
	fb := b.Fork(3)
	for i := 0; i < 10; i++ {
		if x, y := fa.Uint64(), fb.Uint64(); x != y {
			t.Fatalf("fork diverged at %d: %#x vs %#x", i, x, y)
		}
	}
	if New(1).Fork(1).Uint64() == New(1).Fork(2).Uint64() {
		t.Fatal("different streams produced the same first value")
	}
}
