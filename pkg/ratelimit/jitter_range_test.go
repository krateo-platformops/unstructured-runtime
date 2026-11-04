package ratelimit

import (
	"testing"
	"time"
)

// The jitter moved from math/rand to crypto/rand to satisfy gosec G404 genuinely rather than by
// suppression. The draw arithmetic changed with it (8 random bytes, mod d), so the range it
// produces is re-asserted here over enough draws to catch an off-by-one at either edge: a value
// below d/2 would bring deferred items back early, and early is the dangerous direction for a
// backoff.
func TestJitterStaysInHalfToThreeHalves(t *testing.T) {
	d := 2 * time.Second
	for i := 0; i < 5000; i++ {
		got := jitter(d)
		if got < d/2 || got >= d*3/2 {
			t.Fatalf("jitter(%v) = %v on draw %d, outside [%v, %v)", d, got, i, d/2, d*3/2)
		}
	}
	if jitter(0) != 0 {
		t.Error("jitter(0) must be 0, or a zero delay would become a positive one")
	}
}
