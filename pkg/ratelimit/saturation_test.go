package ratelimit

import (
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// #66. Layer 1: the CPU-pressure sampler and the local-capacity step, mirrored from provider-runtime
// v1.19.0 (pkg/ratelimit/saturation_test.go, the windowed CPU step of provider-runtime#95; #82 here).
// The governor integration is tested separately.

func fixture(name string) string { return filepath.Join("testdata", name) }

func TestCPUPressureFixturesParse(t *testing.T) {
	for _, tc := range []struct {
		dir     string
		enabled bool
		stall   time.Duration
		why     string
	}{
		// #82: the cumulative "some total" (microseconds), not avg10. Both fixtures carry
		// total=81234567.
		{"cpu-pressure", true, 81234567 * time.Microsecond, ""},
		{"healthy", true, 81234567 * time.Microsecond, ""},
		// No PSI on the kernel: DISABLED, never 0 (which would read as healthy).
		{"no-psi", false, 0, "cpu.pressure not found"},
		{"does-not-exist", false, 0, "no cgroup v2 directory"},
	} {
		t.Run(tc.dir, func(t *testing.T) {
			r := cgroupFiles{dir: fixture(tc.dir)}.read()
			if r.cpu.enabled != tc.enabled || r.stall != tc.stall {
				t.Fatalf("read = %+v, want enabled=%v stall=%v", r, tc.enabled, tc.stall)
			}
			if !tc.enabled && !strings.Contains(r.cpu.disabledBecause, tc.why) {
				t.Errorf("disabled reason %q does not say %q", r.cpu.disabledBecause, tc.why)
			}
		})
	}
}

// testSat is a saturation driven by samples handed to step directly.
func testSat(t *testing.T, o SaturationOptions) *saturation {
	t.Helper()
	o.Enabled = true
	o.CgroupDir = fixture("healthy")
	s, err := newSaturation(o, nil)
	if err != nil {
		t.Fatal(err)
	}
	return s
}

var (
	clean = cpuSample{reading: reading{enabled: true, percent: 2}, confirmed: true}
	off   = cpuSample{reading: disabled("no PSI")}
)

const testCeiling = 8

func TestLocalCapacitySlowStartsAndRampsPerStartupStep(t *testing.T) {
	s := testSat(t, SaturationOptions{})
	t0 := time.Unix(0, 0)
	st := localState{capacity: 1, ramping: true}
	st = s.step(t0, clean, st, testCeiling) // starts the clock only
	if st.capacity != 1 || !st.ramping {
		t.Fatalf("first step = %+v, want capacity 1, ramping", st)
	}
	st = s.step(t0.Add(time.Second), clean, st, testCeiling)
	if st.capacity != 1 {
		t.Fatalf("before StartupRampInterval capacity = %d, want 1", st.capacity)
	}
	for i := 1; i < testCeiling; i++ {
		st = s.step(t0.Add(time.Duration(i)*2*time.Second), clean, st, testCeiling)
	}
	if st.capacity != testCeiling || st.ramping {
		t.Fatalf("after the ramp = %+v, want the ceiling %d, no longer ramping", st, testCeiling)
	}
	st = s.step(t0.Add(time.Hour), clean, st, testCeiling)
	if st.capacity != testCeiling {
		t.Errorf("capacity passed the ceiling: %d", st.capacity)
	}
}

func TestStartupRampIntervalIsConfigurable(t *testing.T) {
	s := testSat(t, SaturationOptions{StartupRampInterval: 10 * time.Second})
	t0 := time.Unix(0, 0)
	st := s.step(t0, clean, localState{capacity: 1, ramping: true}, testCeiling)
	if st = s.step(t0.Add(5*time.Second), clean, st, testCeiling); st.capacity != 1 {
		t.Fatalf("5s into a 10s ramp interval capacity = %d, want 1", st.capacity)
	}
	if st = s.step(t0.Add(10*time.Second), clean, st, testCeiling); st.capacity != 2 {
		t.Fatalf("at 10s capacity = %d, want 2", st.capacity)
	}
}

// A missing input never saturates and never ramps the floor DOWN: it holds capacity where it is.
func TestMissingInputIsNeverSaturated(t *testing.T) {
	s := testSat(t, SaturationOptions{})
	t0 := time.Unix(0, 0)
	st := localState{capacity: testCeiling}
	for i := 0; i < 5; i++ {
		st = s.step(t0.Add(time.Duration(i)*time.Second), off, st, testCeiling)
	}
	if st.capacity != testCeiling || st.cause != "" {
		t.Errorf("a disabled input changed capacity or reported a cause: %+v", st)
	}
	if got := s.snapshot(); got.Enabled {
		t.Errorf("snapshot before any read reports the input enabled: %+v", got)
	}
}

func TestSnapshotReportsDisabledNotZero(t *testing.T) {
	s, err := newSaturation(SaturationOptions{Enabled: true, CgroupDir: fixture("no-psi")}, nil)
	if err != nil {
		t.Fatal(err)
	}
	s.read(time.Unix(0, 0))
	if got := s.snapshot(); got.Enabled || got.Percent != 0 || got.Input != InputCPUPressure {
		t.Errorf("a missing input must snapshot as disabled with no percent, got %+v", got)
	}
	var nilSat *saturation
	if got := nilSat.snapshot(); got.Enabled {
		t.Errorf("saturation off must snapshot as disabled, got %+v", got)
	}
	// The control: a present input snapshots enabled (its share is 0 on the first sample).
	s2, _ := newSaturation(SaturationOptions{Enabled: true, CgroupDir: fixture("cpu-pressure")}, nil)
	s2.read(time.Unix(0, 0))
	if got := s2.snapshot(); !got.Enabled {
		t.Errorf("the control: a present input must snapshot enabled, got %+v", got)
	}
}

// cpuFeed is a cgroup reader whose cumulative CPU stall the test advances, a second at a time, with
// the governor's clock (provider-runtime v1.19.0's helper, on this module's governor).
type cpuFeed struct {
	g     *aimd
	c     *satClock
	total time.Duration
}

func newCPUFeed(t *testing.T, ceiling int) *cpuFeed {
	t.Helper()
	g, _, c := satGovernor(t, ceiling)
	f := &cpuFeed{g: g, c: c}
	g.sat.reader = f
	g.sample()
	return f
}

func (f *cpuFeed) read() rawSample { return rawSample{cpu: reading{enabled: true}, stall: f.total} }

// second advances the clock by a second in which some task stalled for share of the time, samples,
// and returns the local capacity.
func (f *cpuFeed) second(share float64) int {
	f.c.t = f.c.t.Add(time.Second)
	f.total += time.Duration(share * float64(time.Second))
	f.g.sample()
	return f.g.LocalCapacity()
}

// ramp takes clean seconds until local capacity is at the ceiling.
func (f *cpuFeed) ramp(t *testing.T) {
	t.Helper()
	for range 2 * (f.g.opts.Ceiling - 1) {
		f.second(0)
	}
	if got := f.g.LocalCapacity(); got != f.g.opts.Ceiling {
		t.Fatalf("after the ramp local capacity = %d, want the ceiling %d", got, f.g.opts.Ceiling)
	}
}

func (f *cpuFeed) pressure() float64 { return f.g.LocalInput().Percent }

// #82, ported from provider-runtime v1.19.0: CPU pressure is the share of the last CPUPressureWindow
// that some task stalled, from the change of the cumulative counter: a stall shows in the next
// sample, and is gone a window after it ends.
func TestCPUPressureIsTheStallShareOverTheWindow(t *testing.T) {
	f := newCPUFeed(t, 4)
	for range 10 {
		f.second(0.2)
	}
	if got := f.pressure(); got < 19.99 || got > 20.01 {
		t.Fatalf("after 10s stalled a fifth of the time, CPU pressure = %v%%, want 20", got)
	}
	f.second(1)
	if got := f.pressure(); got < 27.99 || got > 28.01 {
		t.Fatalf("one fully stalled second after nine at a fifth: CPU pressure = %v%%, want 28", got)
	}
	for range 10 {
		f.second(0)
	}
	if got := f.pressure(); got != 0 {
		t.Errorf("a window after the stall ended, CPU pressure = %v%%, want 0", got)
	}
}

// A counter that goes backwards is another cgroup's: the window starts again instead of reading the
// difference as a stall.
func TestCPUStallCounterGoingBackwardsRestartsTheWindow(t *testing.T) {
	f := newCPUFeed(t, 8)
	f.ramp(t)
	for range 10 {
		f.second(0.3)
	}
	if got := f.pressure(); got < 29.99 || got > 30.01 {
		t.Fatalf("CPU pressure = %v%%, want 30", got)
	}
	f.total = 0
	for i := range 12 {
		if got := f.second(0); got != 8 {
			t.Fatalf("%ds after the counter restarted local capacity = %d, want 8", i+1, got)
		}
		if got := f.pressure(); got != 0 {
			t.Fatalf("%ds after the counter restarted CPU pressure = %v%%, want 0", i+1, got)
		}
	}
}

// A CPU cut needs a whole window of evidence: pressure over the threshold in the first seconds of a
// governor's life holds capacity, and doesn't cut it.
func TestACPUCutNeedsAWholeWindow(t *testing.T) {
	f := newCPUFeed(t, 8)
	f.g.local.Store(&localState{capacity: 8})
	for i := 1; i < 10; i++ {
		if got := f.second(1); got != 8 {
			t.Fatalf("%ds into the first window local capacity = %d, want 8: the stall isn't confirmed yet", i, got)
		}
	}
	if got := f.second(1); got != 4 {
		t.Errorf("after a whole stalled window local capacity = %d, want 8 halved to 4", got)
	}
}

// THE DEFECT #82 FIXES. CPU pressure cuts at most once per window, however long it stays over the
// threshold: the next cut needs a window measured after the last one. The floor of one is kept.
// Under the old per-sample avg10 rule this sequence went 16, 8, 4, 2, 1 in five seconds.
func TestCPUPressureCutsOncePerWindowAndFloorsAtOne(t *testing.T) {
	f := newCPUFeed(t, 16)
	f.ramp(t)
	// The window holds 10%, 20%, ... of stall: over 50% at the sixth second.
	for i := 1; i <= 5; i++ {
		if got := f.second(1); got != 16 {
			t.Fatalf("after %d stalled seconds (%.0f%%) local capacity = %d, want 16", i, f.pressure(), got)
		}
	}
	want := 16
	for _, cut := range []int{8, 4, 2, 1, 1} {
		if got := f.second(1); got != cut {
			t.Fatalf("local capacity = %d, want %d halved to %d", got, want, cut)
		}
		want = cut
		for i := 1; i < 10; i++ {
			if got := f.second(1); got != want {
				t.Fatalf("%ds after a cut, still stalled: local capacity = %d, want %d (one cut per window)", i, got, want)
			}
		}
	}
}

// After a CPU cut, capacity holds while the window still shows the stall, and then rises by one per
// StartupRampInterval, not per RecoveryInterval (which was 30s here before #82).
func TestAfterACPUCutCapacityRecoversPerRampInterval(t *testing.T) {
	f := newCPUFeed(t, 16)
	f.ramp(t)
	for range 6 {
		f.second(1)
	}
	if got := f.g.LocalCapacity(); got != 8 {
		t.Fatalf("after six stalled seconds local capacity = %d, want 8", got)
	}
	// The six stalled seconds stay in the window, over the threshold, for four more samples; the
	// fifth reads 50%, which is clean (over means strictly greater).
	for i := 1; i <= 5; i++ {
		if got := f.second(0); got != 8 {
			t.Fatalf("%ds after the stall ended (%.0f%%) local capacity = %d, want 8", i, f.pressure(), got)
		}
	}
	for want := 9; want <= 16; want++ {
		if got := f.second(0); got != want {
			t.Fatalf("local capacity = %d, want %d: +1 per 2s after a CPU cut", got, want)
		}
		if want < 16 {
			if got := f.second(0); got != want {
				t.Fatalf("local capacity = %d, want %d: +1 per 2s, not per sample", got, want)
			}
		}
	}
}

// Pressure over the threshold that doesn't cut still isn't clean: capacity doesn't rise during the
// startup ramp while it lasts, and the ramp ends (this module's DeferRamping classification).
func TestUnconfirmedCPUPressureHoldsTheStartupRamp(t *testing.T) {
	f := newCPUFeed(t, 8)
	for i := 1; i <= 6; i++ {
		if got := f.second(1); got != 1 {
			t.Fatalf("%ds of stall from the start: local capacity = %d, want 1", i, got)
		}
	}
	if st := f.g.local.Load(); st.ramping {
		t.Errorf("an over-threshold sample must end the startup ramp, even unconfirmed and at the floor: %+v", st)
	}
}

func TestSaturationOptionsValidate(t *testing.T) {
	for _, o := range []SaturationOptions{
		{SamplePeriod: -time.Second},
		{StartupRampInterval: -time.Second},
		{CPUPressureThreshold: -1},
		{CPUPressureThreshold: 101},
	} {
		if _, err := o.withDefaults(); err == nil {
			t.Errorf("%+v: want an error", o)
		}
	}
	got, err := SaturationOptions{}.withDefaults()
	if err != nil || got.SamplePeriod != DefaultSamplePeriod || got.StartupRampInterval != DefaultStartupRampInterval ||
		got.CPUPressureThreshold != DefaultCPUPressureThreshold {
		t.Errorf("defaults = %+v (err %v)", got, err)
	}
}

func TestOwnCgroupDirFollowsProcSelfCgroup(t *testing.T) {
	if got, want := ownCgroupDir(fixture("proc-self"), fixture("cgroup-mount")), filepath.Join(fixture("cgroup-mount"), "kubepods", "pod1", "ctr"); got != want {
		t.Errorf("ownCgroupDir = %q, want %q", got, want)
	}
	if got := ownCgroupDir(fixture("does-not-exist"), "/sys/fs/cgroup"); got != "/sys/fs/cgroup" {
		t.Errorf("without /proc/self/cgroup, ownCgroupDir = %q, want the mount", got)
	}
	// A path climbing out of the mount stays under it; naming no directory there, the mount is used.
	if got := ownCgroupDir(fixture("proc-escape"), fixture("cgroup-mount")); got != fixture("cgroup-mount") {
		t.Errorf("a climbing cgroup path gave %q, want the mount", got)
	}
}
