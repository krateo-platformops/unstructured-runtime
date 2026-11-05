package conformance

import (
	"context"
	"fmt"
	"runtime"
	"testing"
	"time"

	"github.com/krateo-platformops/unstructured-runtime/pkg/ratelimit"
)

// fakeTB records whether an assertion failed. Fatalf ends the goroutine the way testing.T does, so
// each check runs in its own goroutine.
type fakeTB struct {
	testing.TB
	failed bool
	msgs   []string
}

func (f *fakeTB) Helper() {}
func (f *fakeTB) Errorf(format string, a ...any) {
	f.failed = true
	f.msgs = append(f.msgs, fmt.Sprintf(format, a...))
}
func (f *fakeTB) Fatalf(format string, a ...any) { f.Errorf(format, a...); runtime.Goexit() }

func check(assert func(testing.TB, func(context.Context) error), signals ...ratelimit.Signal) *fakeTB {
	f := &fakeTB{}
	done := make(chan struct{})
	go func() {
		defer close(done)
		assert(f, func(ctx context.Context) error {
			for _, s := range signals {
				ratelimit.Report(ctx, s)
			}
			return nil
		})
	}()
	<-done
	return f
}

// The healthy check passes for what a correct handler reports on a healthy response — including
// GitHub-shaped counters, which its first version (zero signals required) wrongly failed — and fails
// for anything that would slow a governor down. Rows span the signal SHAPES, not the code arms.
func TestAssertHealthyDoesNotSlowDown(t *testing.T) {
	reset := time.Now().Add(time.Hour)
	cases := []struct {
		name     string
		signals  []ratelimit.Signal
		wantFail bool
	}{
		{"no signal — passes", nil, false},
		{"GitHub-shaped healthy counters with a future reset — passes",
			[]ratelimit.Signal{{Key: "api.github.example.com", HasQuota: true, Limit: 5000, Remaining: 4900, ResetAt: reset}}, false},
		{"throttle on a healthy response — fails",
			[]ratelimit.Signal{{Key: "h", Throttled: true, RetryAfter: time.Minute}}, true},
		{"low quota on a healthy response — fails",
			[]ratelimit.Signal{{Key: "h", HasQuota: true, Limit: 5000, Remaining: 100, ResetAt: reset}}, true},
		{"bare delay on a healthy response — fails",
			[]ratelimit.Signal{{Key: "h", RetryAfter: 2 * time.Second}}, true},
	}
	for _, c := range cases {
		f := check(AssertHealthyDoesNotSlowDown, c.signals...)
		if f.failed != c.wantFail {
			t.Errorf("%s: failed=%v, want %v (%v)", c.name, f.failed, c.wantFail, f.msgs)
		}
	}
	t.Logf("%d rows", len(cases))
}

// The two positive checks keep their meaning under the testing.TB signature.
func TestAssertReportsThrottleAndAdvisory(t *testing.T) {
	if f := check(AssertReportsThrottle, ratelimit.Signal{Key: "h", Throttled: true, RetryAfter: time.Minute}); f.failed {
		t.Errorf("a well-formed throttle failed AssertReportsThrottle: %v", f.msgs)
	}
	// CONTROL: reporting nothing must fail it.
	if f := check(AssertReportsThrottle); !f.failed {
		t.Error("AssertReportsThrottle passed for a handler that reported nothing")
	}
	if f := check(AssertReportsAdvisory, ratelimit.Signal{Key: "h", HasQuota: true, Limit: 100, Remaining: 10}); f.failed {
		t.Errorf("a quota report failed AssertReportsAdvisory: %v", f.msgs)
	}
	if f := check(AssertReportsAdvisory); !f.failed {
		t.Error("AssertReportsAdvisory passed for a handler that reported nothing")
	}
}
