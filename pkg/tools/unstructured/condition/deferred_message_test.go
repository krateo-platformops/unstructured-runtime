package condition

import (
	"strings"
	"testing"
	"time"
)

// The printed deadline is ROUNDED UP: a truncated one names an instant at which a retry is still
// refused. Rows: a sub-second deadline (must round up) and an exact one (the control — must not move).
func TestReconcileDeferredPrintsTheDeadlineRoundedUp(t *testing.T) {
	base := time.Date(2026, 11, 5, 12, 0, 0, 0, time.UTC)
	for _, c := range []struct {
		name  string
		until time.Time
		want  string
	}{
		{"400ms past the second — rounded up", base.Add(400 * time.Millisecond), "2026-11-05T12:00:01Z"},
		{"exactly on the second — unchanged (control)", base, "2026-11-05T12:00:00Z"},
	} {
		msg := ReconcileDeferred("api.github.example.com", c.until).Message
		if !strings.Contains(msg, c.want) {
			t.Errorf("%s: message %q does not name %s", c.name, msg, c.want)
		}
	}
}

// At capacity nothing refused us, so the message must not say "rate-limited"; with a deadline it
// must. The second arm is the control: without it the first passes against a message that never
// says "rate-limited" at all.
func TestReconcileDeferredWordsTheTwoCasesDifferently(t *testing.T) {
	atCap := ReconcileDeferred("h", time.Time{}).Message
	if strings.Contains(atCap, "rate-limited") {
		t.Errorf("at-capacity message %q says rate-limited; the runtime is pacing itself and no "+
			"server refused anything", atCap)
	}
	throttled := ReconcileDeferred("h", time.Now().Add(time.Hour)).Message
	if !strings.Contains(throttled, "rate-limited by the server") {
		t.Errorf("throttled message %q does not say the server rate-limited us", throttled)
	}
}
