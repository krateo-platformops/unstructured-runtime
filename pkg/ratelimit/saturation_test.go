package ratelimit

import (
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// #66. Layer 1: the CPU-pressure sampler and the local-capacity step, mirrored from provider-runtime
// v1.9.1 (pkg/ratelimit/saturation_test.go). The governor integration is tested separately.

func fixture(name string) string { return filepath.Join("testdata", name) }

func TestCPUPressureFixturesParse(t *testing.T) {
	for _, tc := range []struct {
		dir     string
		enabled bool
		percent float64
		why     string
	}{
		{"cpu-pressure", true, 81.40, ""},
		{"healthy", true, 2.10, ""},
		// No PSI on the kernel: DISABLED, never 0 (which would read as healthy).
		{"no-psi", false, 0, "cpu.pressure not found"},
		{"does-not-exist", false, 0, "no cgroup v2 directory"},
	} {
		t.Run(tc.dir, func(t *testing.T) {
			r := cgroupFiles{dir: fixture(tc.dir)}.read()
			if r.enabled != tc.enabled || r.percent != tc.percent {
				t.Fatalf("read = %+v, want enabled=%v percent=%v", r, tc.enabled, tc.percent)
			}
			if !tc.enabled && !strings.Contains(r.disabledBecause, tc.why) {
				t.Errorf("disabled reason %q does not say %q", r.disabledBecause, tc.why)
			}
		})
	}
}

// testSat is a saturation driven by readings handed to step directly.
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
	clean = reading{enabled: true, percent: 2}
	hot   = reading{enabled: true, percent: 81}
	off   = disabled("no PSI")
)

const testCeiling, testRecovery = 8, 30 * time.Second

func TestLocalCapacitySlowStartsAndRampsPerStartupStep(t *testing.T) {
	s := testSat(t, SaturationOptions{})
	t0 := time.Unix(0, 0)
	st := localState{capacity: capacityFloor, ramping: true}
	st = s.step(t0, clean, st, testCeiling, testRecovery) // starts the clock only
	if st.capacity != 1 || !st.ramping {
		t.Fatalf("first sample must only start the clock, got %+v", st)
	}
	st = s.step(t0.Add(time.Second), clean, st, testCeiling, testRecovery)
	if st.capacity != 1 {
		t.Fatalf("a step before StartupRampInterval (2s) raised capacity: %+v", st)
	}
	for i := 2; i <= testCeiling; i++ {
		st = s.step(t0.Add(time.Duration(i)*2*time.Second), clean, st, testCeiling, testRecovery)
	}
	if st.capacity != testCeiling || st.ramping {
		t.Fatalf("after the ramp, want capacity %d and ramping false, got %+v", testCeiling, st)
	}
	st = s.step(t0.Add(time.Hour), clean, st, testCeiling, testRecovery)
	if st.capacity != testCeiling {
		t.Errorf("capacity rose above the ceiling: %+v", st)
	}
}

func TestStartupRampIntervalIsConfigurable(t *testing.T) {
	s := testSat(t, SaturationOptions{StartupRampInterval: 10 * time.Second})
	t0 := time.Unix(0, 0)
	st := s.step(t0, clean, localState{capacity: 1, ramping: true}, testCeiling, testRecovery)
	if st = s.step(t0.Add(5*time.Second), clean, st, testCeiling, testRecovery); st.capacity != 1 {
		t.Fatalf("raised before the configured 10s interval: %+v", st)
	}
	if st = s.step(t0.Add(10*time.Second), clean, st, testCeiling, testRecovery); st.capacity != 2 {
		t.Fatalf("did not raise at the configured interval: %+v", st)
	}
}

func TestSaturationHalvesOncePerSampleAndFloorsAtOne(t *testing.T) {
	s := testSat(t, SaturationOptions{})
	t0 := time.Unix(0, 0)
	st := localState{capacity: testCeiling}
	for i, want := range []int{4, 2, 1, 1} {
		st = s.step(t0.Add(time.Duration(i)*time.Second), hot, st, testCeiling, testRecovery)
		if st.capacity != want || !strings.Contains(st.cause, "CPU pressure 81%") {
			t.Fatalf("hot sample %d: want capacity %d with a CPU cause, got %+v", i, want, st)
		}
	}
}

// Provider-runtime's third trap: a saturated sample at the FLOOR still counts as a cut. It ends the
// ramp and restarts the clean interval, even though capacity cannot fall.
func TestASaturatedSampleAtTheFloorIsStillACut(t *testing.T) {
	s := testSat(t, SaturationOptions{})
	t0 := time.Unix(0, 0)
	st := s.step(t0, clean, localState{capacity: 1, ramping: true}, testCeiling, testRecovery)
	st = s.step(t0.Add(time.Second), hot, st, testCeiling, testRecovery)
	if st.capacity != 1 || st.ramping {
		t.Fatalf("a hot sample at the floor must end the ramp, got %+v", st)
	}
	// The clean interval restarted at the hot sample, and after a cut it is the RECOVERY interval.
	if st = s.step(t0.Add(time.Second+2*time.Second), clean, st, testCeiling, testRecovery); st.capacity != 1 {
		t.Fatalf("raised on the startup interval after a cut: %+v", st)
	}
	if st = s.step(t0.Add(time.Second+testRecovery), clean, st, testCeiling, testRecovery); st.capacity != 2 {
		t.Fatalf("did not raise one recovery interval after the cut: %+v", st)
	}
}

func TestAfterACutLocalRecoversPerCleanRecoveryInterval(t *testing.T) {
	s := testSat(t, SaturationOptions{})
	t0 := time.Unix(0, 0)
	st := s.step(t0, hot, localState{capacity: testCeiling}, testCeiling, testRecovery) // -> 4
	if st.capacity != 4 {
		t.Fatalf("setup: %+v", st)
	}
	for i, want := range []int{5, 6} {
		at := t0.Add(time.Duration(i+1) * testRecovery)
		if st = s.step(at.Add(-time.Second), clean, st, testCeiling, testRecovery); st.capacity != want-1 {
			t.Fatalf("raised before a full recovery interval: %+v", st)
		}
		if st = s.step(at, clean, st, testCeiling, testRecovery); st.capacity != want || st.cause != "" {
			t.Fatalf("want %d with the cause cleared after a clean recovery interval, got %+v", want, st)
		}
	}
}

// A missing input never saturates and never ramps the floor DOWN: it holds capacity where it is.
func TestMissingInputIsNeverSaturated(t *testing.T) {
	s := testSat(t, SaturationOptions{})
	t0 := time.Unix(0, 0)
	st := localState{capacity: testCeiling}
	for i := 0; i < 5; i++ {
		st = s.step(t0.Add(time.Duration(i)*time.Second), off, st, testCeiling, testRecovery)
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
	s.read()
	if got := s.snapshot(); got.Enabled || got.Percent != 0 || got.Input != InputCPUPressure {
		t.Errorf("a missing input must snapshot as disabled with no percent, got %+v", got)
	}
	var nilSat *saturation
	if got := nilSat.snapshot(); got.Enabled {
		t.Errorf("saturation off must snapshot as disabled, got %+v", got)
	}
	s2, _ := newSaturation(SaturationOptions{Enabled: true, CgroupDir: fixture("cpu-pressure")}, nil)
	s2.read()
	if got := s2.snapshot(); !got.Enabled || got.Percent != 81.40 {
		t.Errorf("the control: a present input must snapshot enabled with its value, got %+v", got)
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
