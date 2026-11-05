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
// rate-limit signals (#66). Mirrored from provider-runtime v1.9.1 (pkg/ratelimit/saturation.go,
// 06f9ae1), CPU pressure only: provider-runtime's experimental memory working-set input is NOT
// ported, because its own re-measurement found it prevented OOMKills in 0 of 19 runs.
//
// With saturation enabled, every gated pass needs a slot in a process-wide LOCAL capacity, whether
// or not the handler implements a RateLimitKeyer. Local capacity starts at 1, rises by one per
// StartupRampInterval until its first cut, and after any cut rises by one per clean RecoveryInterval
// (the same rule as for API limits), up to the ceiling. A sample whose CPU pressure is over the
// threshold is a cut: capacity halves, minimum 1.
const (
	// DefaultSamplePeriod is how often the sampler reads the cgroup files. CPU pressure's "avg10" is
	// already a 10-second average, so a faster sample gains nothing and costs CPU in the container
	// it protects.
	DefaultSamplePeriod = time.Second
	// DefaultStartupRampInterval is how long local capacity waits between steps of +1 from the start
	// until its first cut.
	DefaultStartupRampInterval = 2 * time.Second
	// DefaultCPUPressureThreshold is the cpu.pressure "some avg10" percentage above which the
	// controller counts as CPU-saturated. PROVISIONAL: provider-runtime measured it on a synthetic
	// load at a 250m limit, and core-provider is measuring it on the cdc (#66).
	DefaultCPUPressureThreshold = 50.0

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

	// StartupRampInterval is the time between steps of +1 from the start until the first cut.
	// Defaults to DefaultStartupRampInterval. A step happens on the first sample after the interval
	// has passed, so a longer SamplePeriod sets the pace.
	StartupRampInterval time.Duration

	// CPUPressureThreshold is the cpu.pressure "some avg10" percentage, in (0, 100], above which the
	// controller is saturated. Defaults to DefaultCPUPressureThreshold.
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

// cgroupReader reads one sample. It is the only part that does I/O, and only the sampler goroutine
// calls it.
type cgroupReader interface {
	read() reading
}

// localState is the process-wide local capacity the sampler publishes. It is replaced, never
// mutated, so TryAcquire reads it with one atomic load.
type localState struct {
	capacity int
	// cause names what made the last sample saturated. A clean sample clears it.
	cause string
	// ramping is true from the start until the first cut, or until capacity reaches the ceiling,
	// whichever comes first. A local deferral is a DeferRamping only while it is true.
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
	// cut is set by the first saturated sample, and ends the startup ramp.
	cut bool
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

// read takes one sample, publishes it, and logs the input the first time it is found disabled.
func (s *saturation) read() reading {
	r := s.reader.read()
	s.last.Store(&r)
	if !r.enabled && !s.loggedDisabled {
		s.loggedDisabled = true
		s.log.Info("Saturation input disabled; the governor's local capacity stays where it is",
			"input", InputCPUPressure, "reason", r.disabledBecause, "cgroup", s.dir)
	}
	return r
}

// step returns the local state that follows cur after reading r, taken at now. The first call only
// starts the clock.
//
//   - CPU pressure over the threshold: a cut, capacity halves, minimum 1. step runs once per sample,
//     so this halves at most once per sample period.
//   - A clean sample: capacity +1, up to ceiling, once enough time has passed since the last step or
//     saturated sample. Until the first cut that time is StartupRampInterval, so a restarted
//     controller eases into its backlog. After any cut it is the recovery interval.
//
// A saturated sample counts as a cut EVEN AT THE FLOOR, where capacity cannot fall further: it ends
// the startup ramp and restarts the clean interval (provider-runtime's third trap).
func (s *saturation) step(now time.Time, r reading, cur localState, ceiling int, recovery time.Duration) localState {
	if s.lastStep.IsZero() {
		s.lastStep = now
	}
	next := cur
	if saturated, cause := s.evaluate(r); saturated {
		next.capacity = max(capacityFloor, cur.capacity/2)
		next.cause = cause
		s.cut, s.lastStep = true, now
	} else {
		next.cause = ""
		wait := s.opts.StartupRampInterval
		if s.cut {
			wait = recovery
		}
		if cur.capacity < ceiling && now.Sub(s.lastStep) >= wait {
			next.capacity = cur.capacity + 1
			s.lastStep = now
		}
	}
	next.ramping = !s.cut && next.capacity < ceiling
	return next
}

func (s *saturation) evaluate(r reading) (saturated bool, cause string) {
	if r.enabled && r.percent > s.opts.CPUPressureThreshold {
		return true, "CPU pressure " + strconv.FormatFloat(r.percent, 'f', 0, 64) + "% (some avg10)"
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
	// Percent is cpu.pressure "some avg10".
	Percent float64
}

type cgroupFiles struct{ dir string }

func (c cgroupFiles) read() reading {
	root, err := os.OpenRoot(c.dir)
	if err != nil {
		return disabled("no cgroup v2 directory at " + c.dir + ": " + err.Error())
	}
	defer func() { _ = root.Close() }()
	return readPressure(root, "cpu.pressure", "some")
}

func readPressure(root *os.Root, file, line string) reading {
	b, err := root.ReadFile(file)
	if err != nil {
		return disabled(missingWhy(file, err))
	}
	sc := bufio.NewScanner(bytes.NewReader(b))
	for sc.Scan() {
		fields := strings.Fields(sc.Text())
		if len(fields) == 0 || fields[0] != line {
			continue
		}
		for _, f := range fields[1:] {
			v, ok := strings.CutPrefix(f, "avg10=")
			if !ok {
				continue
			}
			p, err := strconv.ParseFloat(v, 64)
			if err != nil || p < 0 {
				return disabled(file + ": cannot parse " + strconv.Quote(f))
			}
			return reading{enabled: true, percent: p}
		}
	}
	return disabled(file + ": no " + strconv.Quote(line) + " avg10 value")
}

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
