package main

import "testing"

func TestParseRange(t *testing.T) {
	lo, hi, err := parseRange("3-10")
	if err != nil || lo != 3 || hi != 10 {
		t.Fatal(lo, hi, err)
	}
	for _, bad := range []string{"", "5", "0-3", "9-2", "a-b", "1-"} {
		if _, _, err := parseRange(bad); err == nil {
			t.Fatalf("%q accepted", bad)
		}
	}
}
