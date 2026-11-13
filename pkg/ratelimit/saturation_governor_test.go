package ratelimit

import (
	"context"
	"testing"
	"time"
)

// #66, layer 2: the local capacity inside the governor. Mirrors provider-runtime v1.9.1's governor
// tests for the CPU input.

type fakeReader struct {
	r     rawSample
	reads int
}

func (f *fakeReader) read() rawSample { f.reads++; return f.r }

type satClock struct{ t time.Time }

func (c *satClock) now() time.Time { return c.t }

func satGovernor(t *testing.T, ceiling int) (*aimd, *fakeReader, *satClock) {
	t.Helper()
	c := &satClock{t: time.Unix(1000, 0)}
	g, err := NewWithSaturation(Options{Ceiling: ceiling, RecoveryInterval: 30 * time.Second, Now: c.now,
		Jitter: func(d time.Duration) time.Duration { return d }}, SaturationOptions{Enabled: true, CgroupDir: fixture("healthy")}, nil)
	if err != nil {
		t.Fatal(err)
	}
	a := g.(*aimd)
	fr := &fakeReader{r: rawSample{cpu: reading{enabled: true}}}
	a.sat.reader = fr
	return a, fr, c
}

func TestTryAcquireDoesNoIO(t *testing.T) {
	g, fr, _ := satGovernor(t, 4)
	for i := 0; i < 10; i++ {
		if rel, _ := g.TryAcquire(""); rel != nil {
			rel()
		}
	}
	if fr.reads != 0 {
		t.Errorf("TryAcquire read the cgroup files %d times; only the sampler may", fr.reads)
	}
}

func TestStartStopsWithItsContextAndOffReturnsAtOnce(t *testing.T) {
	g, fr, _ := satGovernor(t, 4)
	g.sat.opts.SamplePeriod = 10 * time.Millisecond
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- g.Start(ctx) }()
	time.Sleep(50 * time.Millisecond)
	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Errorf("Start returned %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Start did not return after its context was cancelled")
	}
	if fr.reads == 0 {
		t.Errorf("Start did not sample (reads=%d)", fr.reads)
	}
	off := New(Options{Ceiling: 4}).(*aimd)
	if err := off.Start(context.Background()); err != nil {
		t.Errorf("Start without saturation must return nil at once, got %v", err)
	}
}

func TestNewWithSaturationRejectsOutOfRangeSettings(t *testing.T) {
	if _, err := NewWithSaturation(Options{Ceiling: 4}, SaturationOptions{Enabled: true, CPUPressureThreshold: 150}, nil); err == nil {
		t.Error("an out-of-range threshold must be an error, not a silent cap")
	}
	g, err := NewWithSaturation(Options{Ceiling: 4}, SaturationOptions{}, nil)
	if err != nil || g.(*aimd).sat != nil {
		t.Errorf("Enabled=false must be plain New (err %v)", err)
	}
}
