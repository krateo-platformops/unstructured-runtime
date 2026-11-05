package ratelimit

import (
	"testing"
	"time"
)

// githubShaped is what a GitHub response carries on EVERY call: counters AND a reset instant in the
// future. The original healthy-quota control (TestHealthyQuotaDoesNotReduceCapacity) omitted
// ResetAt, so its input never spanned the distinction that mattered and it passed against a
// governor that halved on every GitHub response. Reported by the provider-runtime session.
func githubShaped(c *clock, remaining int) Signal {
	return Signal{Key: "api.github.example.com", HasQuota: true, Limit: 5000, Remaining: remaining,
		ResetAt: c.now().Add(time.Hour)}
}

func TestHealthyGitHubShapedQuotaDoesNotReduceCapacity(t *testing.T) {
	t.Run("healthy counters with a future reset — capacity stays at the ceiling", func(t *testing.T) {
		g, c := testGovernor(8, time.Minute)
		const reports = 10
		for range reports {
			g.Observe(githubShaped(c, 4900))
			c.add(time.Second)
		}
		if got := g.Capacity("api.github.example.com"); got != 8 {
			t.Errorf("capacity = %d after %d HEALTHY GitHub-shaped reports, want the ceiling 8. The "+
				"future X-RateLimit-Reset was read as a delay, so every successful call halved "+
				"capacity", got, reports)
		}
	})

	// CONTROL: the SAME shape, with a quarter of the quota or less, must still halve. Without it
	// the arm above passes against a governor that ignores every signal carrying counters.
	t.Run("control: low counters with a future reset — halved", func(t *testing.T) {
		g, c := testGovernor(8, time.Minute)
		g.Observe(githubShaped(c, 1000))
		if got := g.Capacity("api.github.example.com"); got != 4 {
			t.Errorf("capacity = %d after a GitHub-shaped report with 1000/5000 left, want 4", got)
		}
	})

	// CONTROL: Azure's shape — a delay and NO counters — is still advisory. The fix gives healthy
	// quota its own arm ahead of the delay arm; this proves it did not swallow the delay arm too.
	t.Run("control: a delay with no counters — halved", func(t *testing.T) {
		g, _ := testGovernor(8, time.Minute)
		g.Observe(Signal{Key: "dev.azure.example.com", RetryAfter: 2 * time.Second})
		if got := g.Capacity("dev.azure.example.com"); got != 4 {
			t.Errorf("capacity = %d after an Azure-shaped delay, want 4", got)
		}
	})
}

// A HEALTHY SIGNAL MUST NOT RESET THE RECOVERY CLOCK. A key that reports counters on every
// response is never quiet for a RecoveryInterval, so if every signal restarted the clock, capacity
// cut once would never come back.
func TestHealthySignalsDoNotBlockRecovery(t *testing.T) {
	run := func(t *testing.T, between func(*clock) Signal) int {
		g, c := testGovernor(4, 10*time.Second)
		g.Observe(Signal{Key: "api.github.example.com", HasQuota: true, Limit: 5000, Remaining: 100})
		if got := g.Capacity("api.github.example.com"); got != 2 {
			t.Fatalf("setup: capacity = %d after an advisory cut, want 2", got)
		}
		for range 12 {
			c.add(time.Second)
			g.Observe(between(c))
			if r, _ := g.TryAcquire("api.github.example.com"); r != nil {
				r()
			}
		}
		return g.Capacity("api.github.example.com")
	}

	t.Run("healthy reports every second — capacity recovers", func(t *testing.T) {
		if got := run(t, func(c *clock) Signal { return githubShaped(c, 4900) }); got != 3 {
			t.Errorf("capacity = %d after 12s of HEALTHY reports with a 10s interval, want 3. A "+
				"healthy signal restarted the recovery clock, so a busy key never recovers", got)
		}
	})

	// CONTROL: cutting reports every second must hold it down. Without this the arm above passes
	// against a governor that recovers regardless of what it is told.
	t.Run("control: low-quota reports every second — no recovery", func(t *testing.T) {
		if got := run(t, func(c *clock) Signal { return githubShaped(c, 100) }); got != 1 {
			t.Errorf("capacity = %d under continuous low-quota reports, want the floor 1", got)
		}
	})
}

// RECOVERY COUNTS FROM THE END OF A HOLD. After a 30s Retry-After the key must spend a clean
// RecoveryInterval at the floor before rising; rising the instant the hold lifts is a step up with
// no evidence the server recovered.
func TestRecoveryCountsFromTheEndOfAHold(t *testing.T) {
	g, c := testGovernor(4, 10*time.Second)
	g.Observe(Signal{Key: "h", Throttled: true, RetryAfter: 30 * time.Second})

	c.add(31 * time.Second) // hold over 1s ago; 31s since the cut
	r, d := g.TryAcquire("h")
	if d != nil {
		t.Fatalf("deferred (%v) after the hold lifted", d)
	}
	r()
	if got := g.Capacity("h"); got != 1 {
		t.Errorf("capacity = %d one second after a 30s hold lifted, want the floor 1. Recovery "+
			"counted from the cut, not from the end of the hold", got)
	}

	// CONTROL: a full interval after the hold, it DOES rise — or the assertion above passes
	// against a governor that never recovers.
	c.add(10 * time.Second)
	if r, _ := g.TryAcquire("h"); r != nil {
		r()
	}
	if got := g.Capacity("h"); got != 2 {
		t.Errorf("capacity = %d a full interval after the hold lifted, want 2", got)
	}
}
