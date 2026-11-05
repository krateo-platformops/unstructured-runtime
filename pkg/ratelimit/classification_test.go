package ratelimit

import (
	"testing"
	"time"
)

// ONE TABLE FOR THE SHARED VOCABULARY. provider-runtime's Classify (provider-runtime#34,
// pkg/ratelimit/signal.go) decides the same Signal shapes, and the two runtimes must give the same
// answer for each, so this table's rows follow its order:
//
//	1 Throttled                             → hold, capacity to the floor
//	2 counters at or under a quarter        → advisory (halve)
//	3 RetryAfter > 0, whatever the counters → advisory (halve)
//	4 a future ResetAt with NO counters     → advisory (halve)
//	5 anything else, incl. healthy counters
//	  with a future ResetAt                 → nothing
//
// Row 3b is the one the two runtimes disagreed on before (#57 read it as a no-op).
func TestClassificationMatchesTheSharedVocabulary(t *testing.T) {
	const ceiling = 8
	cases := []struct {
		name string
		sig  func(now time.Time) Signal
		want int
	}{
		{"1 throttled", func(time.Time) Signal { return Signal{Throttled: true, RetryAfter: time.Minute} }, 1},
		{"2 low counters with a future reset", func(n time.Time) Signal {
			return Signal{HasQuota: true, Limit: 5000, Remaining: 1000, ResetAt: n.Add(time.Hour)}
		}, 4},
		{"3a bare RetryAfter", func(time.Time) Signal { return Signal{RetryAfter: 2 * time.Second} }, 4},
		{"3b healthy counters AND RetryAfter — the server asked for a delay", func(n time.Time) Signal {
			return Signal{HasQuota: true, Limit: 5000, Remaining: 4900, RetryAfter: 2 * time.Second, ResetAt: n.Add(time.Hour)}
		}, 4},
		{"4 bare future reset, no counters", func(n time.Time) Signal { return Signal{ResetAt: n.Add(time.Minute)} }, 4},
		{"5a healthy counters with a future reset (GitHub, every response)", func(n time.Time) Signal {
			return Signal{HasQuota: true, Limit: 5000, Remaining: 4900, ResetAt: n.Add(time.Hour)}
		}, ceiling},
		{"5b healthy counters, nothing else", func(time.Time) Signal {
			return Signal{HasQuota: true, Limit: 5000, Remaining: 4900}
		}, ceiling},
		{"5c a reset already in the past", func(n time.Time) Signal { return Signal{ResetAt: n.Add(-time.Minute)} }, ceiling},
	}
	for _, c := range cases {
		g, clk := testGovernor(ceiling, time.Hour)
		s := c.sig(clk.now())
		s.Key = "k"
		g.Observe(s)
		if got := g.Capacity("k"); got != c.want {
			t.Errorf("%s: capacity %d, want %d", c.name, got, c.want)
		}
	}
	t.Logf("%d rows", len(cases))
}
