// Package prng is a small, fully specified pseudo-random generator.
//
// The simulator's promise is that one seed produces one run, bit for bit,
// on any machine and any Go release. math/rand does not make that promise
// across Go versions for all of its methods, so the simulator uses this
// generator (xoshiro256**, seeded through splitmix64) for every random
// decision.
package prng

import "math/bits"

// Rand is a deterministic generator. It is not safe for concurrent use.
type Rand struct{ s [4]uint64 }

func splitmix(x *uint64) uint64 {
	*x += 0x9e3779b97f4a7c15
	z := *x
	z = (z ^ (z >> 30)) * 0xbf58476d1ce4e5b9
	z = (z ^ (z >> 27)) * 0x94d049bb133111eb
	return z ^ (z >> 31)
}

// New returns a generator for the given seed.
func New(seed uint64) *Rand {
	r := &Rand{}
	x := seed
	for i := range r.s {
		r.s[i] = splitmix(&x)
	}
	return r
}

// Fork returns an independent generator derived from this generator's seed
// material and a stream label, without consuming from the parent. Giving
// each component of the simulation its own stream keeps a change in how one
// component uses randomness from reshuffling all the others.
func (r *Rand) Fork(stream uint64) *Rand {
	x := r.s[0] ^ bits.RotateLeft64(r.s[1], 17) ^ (stream * 0xd6e8feb86659fd93)
	out := &Rand{}
	for i := range out.s {
		out.s[i] = splitmix(&x)
	}
	return out
}

// Uint64 returns the next 64 random bits.
func (r *Rand) Uint64() uint64 {
	s := &r.s
	result := bits.RotateLeft64(s[1]*5, 7) * 9
	t := s[1] << 17
	s[2] ^= s[0]
	s[3] ^= s[1]
	s[1] ^= s[2]
	s[0] ^= s[3]
	s[2] ^= t
	s[3] = bits.RotateLeft64(s[3], 45)
	return result
}

// Intn returns a uniform integer in [0, n). It panics if n <= 0.
func (r *Rand) Intn(n int) int {
	if n <= 0 {
		panic("prng: Intn with non-positive bound")
	}
	// Lemire's multiply-shift with rejection: unbiased and fast.
	bound := uint64(n)
	hi, lo := bits.Mul64(r.Uint64(), bound)
	if lo < bound {
		threshold := -bound % bound
		for lo < threshold {
			hi, lo = bits.Mul64(r.Uint64(), bound)
		}
	}
	return int(hi)
}

// Between returns a uniform integer in [lo, hi].
func (r *Rand) Between(lo, hi int64) int64 {
	if hi <= lo {
		return lo
	}
	return lo + int64(r.Intn(int(hi-lo+1)))
}

// Chance reports true with probability num/den.
func (r *Rand) Chance(num, den int) bool {
	if num <= 0 {
		return false
	}
	return r.Intn(den) < num
}
