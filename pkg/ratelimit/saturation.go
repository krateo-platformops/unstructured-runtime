package ratelimit

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/krateo-platformops/unstructured-runtime/pkg/logging"
)

// Saturation: the controller's own CPU saturation as a second input to the governor, beside the
// rate-limit signals (#66). Mirrored from provider-runtime v1.19.0 (pkg/ratelimit/saturation.go, the
// CPU step of provider-runtime#95), CPU pressure only: provider-runtime's experimental memory
// working-set input is NOT ported, because its own re-measurement found it prevented OOMKills in 0
// of 19 runs.
//
// With saturation enabled, every gated pass needs a slot in a process-wide LOCAL capacity, whether
// or not the handler implements a RateLimitKeyer. Local capacity starts at 1 and rises by one per
// clean StartupRampInterval, up to the ceiling. CPU pressure over the threshold for a whole
// CPUPressureWindow halves it, minimum 1, at most once per window.
//
// #82: until v1.19.0's port this halved on every 1s sample of cpu.pressure "some avg10". avg10 is an
// exponential average with a 10s time constant, so one stall stayed over the threshold for several
// samples and cut several times (16 to 1), and recovery was +1 per 30s; provider-runtime measured
// the old rule settling at ~72% of a 250m limit with 31% less throughput.
const (
	// DefaultSamplePeriod is how often the sampler reads the cgroup files. CPU pressure is measured
	// over CPUPressureWindow, so a faster sample gains little and costs CPU in the container it
	// protects.
	DefaultSamplePeriod = time.Second
	// DefaultStartupRampInterval is how long local capacity waits between steps of +1, from the start
	// and after a CPU cut, so 16 workers are all admitted after about 30s.
	DefaultStartupRampInterval = 2 * time.Second
	// DefaultCPUPressureThreshold is the CPU pressure percentage above which the controller counts as
	// CPU-saturated: the share of the last CPUPressureWindow in which some task of the container
	// waited for CPU. Over means strictly greater. PROVISIONAL (#66).
	DefaultCPUPressureThreshold = 50.0
	// CPUPressureWindow is the time CPU pressure is measured over, from the change of cpu.pressure's
	// cumulative "some total" counter, and the least time between two CPU cuts. A cut needs a whole
	// window of evidence, and the next one a whole window measured after it, so no cut is made twice
	// on the same stall. The kernel's "avg10" is not used: it still holds most of a stall 10s after
	// it ended (#82).
	CPUPressureWindow = 10 * time.Second

	// InputCPUPressure names the one saturation input, as reported in snapshots.
	InputCPUPressure = "cpu_pressure"

	defaultCgroupMount = "/sys/fs/cgroup"
	procSelfDir        = "/proc/self"
)

// SaturationOptions configures the local input. The zero value is OFF.
type SaturationOptions struct {
	// Enabled turns the local input on.
	Enabled bool

	// CgroupDir is the cgroup v2 directory to read. Empty means the process's own cgroup:
	// /sys/fs/cgroup joined with the path in /proc/self/cgroup when that directory exists, else
	// /sys/fs/cgroup (which, with the private cgroup namespace Kubernetes gives containers on cgroup
	// v2, is the container's own).
	CgroupDir string

	// SamplePeriod is how often the files are read. Defaults to DefaultSamplePeriod.
	SamplePeriod time.Duration

	// StartupRampInterval is the time between steps of +1, from the start and after a CPU cut.
	// Defaults to DefaultStartupRampInterval. A step happens on the first sample after the interval
	// has passed, so a longer SamplePeriod sets the pace.
	StartupRampInterval time.Duration

	// CPUPressureThreshold is the CPU pressure percentage, in (0, 100], above which the controller is
	// saturated: the share of the last CPUPressureWindow in which some task waited for CPU, from
	// cpu.pressure's "some total". Defaults to DefaultCPUPressureThreshold.
	CPUPressureThreshold float64
}

func (o SaturationOptions) withDefaults() (SaturationOptions, error) {
	if o.SamplePeriod == 0 {
		o.SamplePeriod = DefaultSamplePeriod
	}
	if o.StartupRampInterval == 0 {
		o.StartupRampInterval = DefaultStartupRampInterval
	}
	if o.CPUPressureThreshold == 0 {
		o.CPUPressureThreshold = DefaultCPUPressureThreshold
	}
	switch {
	case o.SamplePeriod < 0:
		return o, fmt.Errorf("ratelimit: saturation SamplePeriod must be positive, got %s", o.SamplePeriod)
	case o.StartupRampInterval < 0:
		return o, fmt.Errorf("ratelimit: saturation StartupRampInterval must be positive, got %s", o.StartupRampInterval)
	case o.CPUPressureThreshold < 0 || o.CPUPressureThreshold > 100:
		return o, fmt.Errorf("ratelimit: saturation CPUPressureThreshold must be a percentage in (0, 100], got %v", o.CPUPressureThreshold)
	}
	return o, nil
}

// reading is the input's value in one sample.
type reading struct {
	enabled bool
	// percent is meaningful only when enabled.
	percent float64
	// disabledBecause says why the input is off, when it is. A disabled input is reported as
	// disabled, never as 0, which would read as healthy.
	disabledBecause string
}

func disabled(why string) reading { return reading{disabledBecause: why} }

// rawSample is one read of the cgroup files: whether CPU pressure is available, and its cumulative
// "some total" stall. saturation.read turns the stall into the window's share.
type rawSample struct {
	cpu   reading
	stall time.Duration
}

// cpuSample is one sample as the step sees it: the stall share over the window, and whether that
// share covers a whole CPUPressureWindow.
type cpuSample struct {
	reading
	confirmed bool
}

// stallPoint is cpu.pressure's cumulative stall at one sample.
type stallPoint struct {
	at    time.Time
	total time.Duration
}

// cgroupReader reads one sample. It is the only part that does I/O, and only the sampler goroutine
// calls it.
type cgroupReader interface {
	read() rawSample
}

// localState is the process-wide local capacity the sampler publishes. It is replaced, never
// mutated, so TryAcquire reads it with one atomic load.
type localState struct {
	capacity int
	// cause names what made the last sample saturated. A clean sample clears it.
	cause string
	// ramping is true from the start until the first sample over the threshold (whether or not it
	// cut), or until capacity reaches the ceiling, whichever comes first. A local deferral is a
	// DeferRamping only while it is true.
	ramping bool
}

type saturation struct {
	opts   SaturationOptions
	dir    string
	reader cgroupReader
	log    logging.Logger

	// last is the latest reading, for snapshots.
	last atomic.Pointer[reading]

	// Owned by the sampler goroutine.
	loggedDisabled bool
	// lastStep is when capacity last changed or a sample was last saturated; zero until the first
	// sample.
	lastStep time.Time
	// saturatedOnce is set by the first sample over the threshold, cut or not, and ends the startup
	// ramp.
	saturatedOnce bool
	// stall is the cumulative CPU stall at the samples of the last CPUPressureWindow, oldest first;
	// its first point is the window's start.
	stall []stallPoint
	// lastCPUCut is when CPU pressure last cut capacity; zero until it has.
	lastCPUCut time.Time
}

func newSaturation(o SaturationOptions, log logging.Logger) (*saturation, error) {
	o, err := o.withDefaults()
	if err != nil {
		return nil, err
	}
	if log == nil {
		log = logging.NewNopLogger()
	}
	dir := o.CgroupDir
	if dir == "" {
		dir = ownCgroupDir(procSelfDir, defaultCgroupMount)
	}
	return &saturation{opts: o, dir: dir, reader: cgroupFiles{dir: dir}, log: log}, nil
}

// read takes one sample at now, turns the cumulative CPU stall into its share of the window,
// publishes it, and logs the input the first time it is found disabled.
func (s *saturation) read(now time.Time) cpuSample {
	raw := s.reader.read()
	smp := cpuSample{reading: raw.cpu}
	if smp.enabled {
		smp.percent, smp.confirmed = s.stallShare(now, raw.stall)
	} else {
		s.stall = nil
	}
	r := smp.reading
	s.last.Store(&r)
	if !r.enabled && !s.loggedDisabled {
		s.loggedDisabled = true
		s.log.Info("Saturation input disabled; the governor's local capacity stays where it is",
			"input", InputCPUPressure, "reason", r.disabledBecause, "cgroup", s.dir)
	}
	return smp
}

// stallShare records the cumulative CPU stall read at now and returns the percentage of time stalled
// since the window's start, and whether that span is a whole CPUPressureWindow. The window's start
// is the latest sample at least a window old, or the first sample while there is none. A counter that
// went backwards belongs to another cgroup, and starts the window again.
func (s *saturation) stallShare(now time.Time, total time.Duration) (percent float64, confirmed bool) {
	if n := len(s.stall); n > 0 && total < s.stall[n-1].total {
		s.stall = nil
	}
	if n := len(s.stall); n == 0 || now.After(s.stall[n-1].at) {
		s.stall = append(s.stall, stallPoint{at: now, total: total})
	}
	for len(s.stall) > 1 && now.Sub(s.stall[1].at) >= CPUPressureWindow {
		s.stall = s.stall[1:]
	}
	start := s.stall[0]
	span := now.Sub(start.at)
	if span <= 0 {
		return 0, false
	}
	return min(100, 100*float64(total-start.total)/float64(span)), span >= CPUPressureWindow
}

// step returns the local state that follows cur after sample smp, taken at now. The first call only
// starts the clock.
//
//   - CPU pressure over the threshold: capacity halves, minimum 1, if the pressure covers a whole
//     CPUPressureWindow and the last CPU cut is at least a window old. Otherwise capacity holds: the
//     stall isn't confirmed yet, or it is the one the last cut already answered. Either way the
//     sample is not clean: it restarts the clean interval and ends the startup ramp, EVEN AT THE
//     FLOOR (provider-runtime's third trap).
//   - A clean sample: capacity +1, up to ceiling, once StartupRampInterval has passed since the last
//     step or over-threshold sample, from the start and after a CPU cut (provider-runtime v1.19.0:
//     recovery after a CPU cut is the ramp interval, not RecoveryInterval).
func (s *saturation) step(now time.Time, smp cpuSample, cur localState, ceiling int) localState {
	if s.lastStep.IsZero() {
		s.lastStep = now
	}
	next := cur
	if over, cause := s.evaluate(smp.reading); over {
		next.cause = cause
		s.saturatedOnce, s.lastStep = true, now
		if smp.confirmed && (s.lastCPUCut.IsZero() || now.Sub(s.lastCPUCut) >= CPUPressureWindow) {
			next.capacity = max(capacityFloor, cur.capacity/2)
			s.lastCPUCut = now
		}
	} else {
		next.cause = ""
		if cur.capacity < ceiling && now.Sub(s.lastStep) >= s.opts.StartupRampInterval {
			next.capacity = cur.capacity + 1
			s.lastStep = now
		}
	}
	next.ramping = !s.saturatedOnce && next.capacity < ceiling
	return next
}

func (s *saturation) evaluate(r reading) (saturated bool, cause string) {
	if r.enabled && r.percent > s.opts.CPUPressureThreshold {
		return true, "CPU pressure " + strconv.FormatFloat(r.percent, 'f', 0, 64) + "% (some, over " + CPUPressureWindow.String() + ")"
	}
	return false, ""
}

// snapshot returns the last reading as reported: Enabled false (and no Percent) when the input is
// missing or saturation is off.
func (s *saturation) snapshot() InputSnapshot {
	is := InputSnapshot{Input: InputCPUPressure}
	if s == nil {
		return is
	}
	if r := s.last.Load(); r != nil && r.enabled {
		is.Enabled, is.Percent = true, r.percent
	}
	return is
}

// InputSnapshot is the saturation input's last sample.
type InputSnapshot struct {
	Input string
	// Enabled is false when the input is missing (no cgroup v2, no PSI) or saturation is off. A
	// disabled input has no Percent: zero would read as healthy.
	Enabled bool
	// Percent is the share of the last CPUPressureWindow in which some task waited for CPU, from
	// cpu.pressure's "some total" (#82; it was "some avg10").
	Percent float64
}

type cgroupFiles struct{ dir string }

func (c cgroupFiles) read() rawSample {
	root, err := os.OpenRoot(c.dir)
	if err != nil {
		return rawSample{cpu: disabled("no cgroup v2 directory at " + c.dir + ": " + err.Error())}
	}
	defer func() { _ = root.Close() }()
	r, stall := readPressure(root, "cpu.pressure", "some")
	return rawSample{cpu: r, stall: stall}
}

// readPressure returns total, the cumulative stall in microseconds, of the given line ("some" or
// "full") of a PSI file such as cpu.pressure:
//
//	some avg10=1.53 avg60=0.87 avg300=0.24 total=1234567
//
// The reading is enabled with no percent: saturation.read derives it from the change in the total.
func readPressure(root *os.Root, file, line string) (reading, time.Duration) {
	b, err := root.ReadFile(file)
	if err != nil {
		return disabled(missingWhy(file, err)), 0
	}
	sc := bufio.NewScanner(bytes.NewReader(b))
	for sc.Scan() {
		fields := strings.Fields(sc.Text())
		if len(fields) == 0 || fields[0] != line {
			continue
		}
		for _, f := range fields[1:] {
			v, ok := strings.CutPrefix(f, "total=")
			if !ok {
				continue
			}
			us, err := strconv.ParseInt(v, 10, 64)
			if err != nil || us < 0 || us > int64(maxStall/time.Microsecond) {
				return disabled(file + ": cannot parse " + strconv.Quote(f)), 0
			}
			return reading{enabled: true}, time.Duration(us) * time.Microsecond
		}
	}
	return disabled(file + ": no " + strconv.Quote(line) + " total value"), 0
}

// maxStall is the largest cumulative stall a time.Duration holds, about 292 years.
const maxStall = time.Duration(1<<63 - 1)

func missingWhy(file string, err error) string {
	if errors.Is(err, fs.ErrNotExist) {
		return file + " not found (no cgroup v2 or no PSI on this kernel)"
	}
	return file + ": " + err.Error()
}

// ownCgroupDir is the process's own cgroup v2 directory: mount joined with the "0::" path from
// <proc>/cgroup, when that directory exists, else mount. Both are opened as roots, so a path in
// /proc/self/cgroup cannot escape the mount.
func ownCgroupDir(proc, mount string) string {
	procRoot, err := os.OpenRoot(proc)
	if err != nil {
		return mount
	}
	defer func() { _ = procRoot.Close() }()
	b, err := procRoot.ReadFile("cgroup")
	if err != nil {
		return mount
	}
	var rel string
	sc := bufio.NewScanner(bytes.NewReader(b))
	for sc.Scan() {
		if p, ok := strings.CutPrefix(sc.Text(), "0::"); ok {
			rel = strings.TrimPrefix(filepath.Clean("/"+p), "/")
			break
		}
	}
	if rel == "" {
		return mount
	}
	mountRoot, err := os.OpenRoot(mount)
	if err != nil {
		return mount
	}
	defer func() { _ = mountRoot.Close() }()
	if fi, err := mountRoot.Stat(rel); err != nil || !fi.IsDir() {
		return mount
	}
	return filepath.Join(mount, rel)
}
