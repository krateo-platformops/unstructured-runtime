package controller

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/krateo-platformops/plumbing/shortid"
	ctrlevent "github.com/krateo-platformops/unstructured-runtime/pkg/controller/event"
	"github.com/krateo-platformops/unstructured-runtime/pkg/controller/objectref"
	"github.com/krateo-platformops/unstructured-runtime/pkg/controller/priorityqueue"
	"github.com/krateo-platformops/unstructured-runtime/pkg/meta"
	"github.com/krateo-platformops/unstructured-runtime/pkg/ratelimit"
	unstructuredtools "github.com/krateo-platformops/unstructured-runtime/pkg/tools/unstructured"
	"github.com/krateo-platformops/unstructured-runtime/pkg/tools/unstructured/condition"
	"github.com/stretchr/testify/require"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	dynamicfake "k8s.io/client-go/dynamic/fake"
	k8stesting "k8s.io/client-go/testing"
)

const (
	cuRecord = "credential-use.test/refs"
	cuHold   = "test.example.org/hold" // another owner's finalizer: the fake client does not reap
)

// cuLog is the one ordered list every fake of a test appends to, so call order is asserted across the
// CredentialUse, the external client and the apiserver writes.
type cuLog struct {
	mu  sync.Mutex
	all []string
}

func (l *cuLog) add(s string) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.all = append(l.all, s)
}

func (l *cuLog) get() []string {
	l.mu.Lock()
	defer l.mu.Unlock()
	return slices.Clone(l.all)
}

// only returns the entries with one of the prefixes.
func (l *cuLog) only(prefixes ...string) []string {
	var out []string
	for _, s := range l.get() {
		for _, p := range prefixes {
			if strings.HasPrefix(s, p) {
				out = append(out, s)
				break
			}
		}
	}
	return out
}

type cuResolvedKey struct{}

// fakeCredentialUse records its calls and answers with what the test sets.
type fakeCredentialUse struct {
	log *cuLog

	acquire     func(context.Context, *unstructured.Unstructured) (CredentialUseAcquisition, error)
	before      func(context.Context, *unstructured.Unstructured, CredentialUseAcquisition) error
	own         func(o, n *unstructured.Unstructured) bool
	established func(*unstructured.Unstructured) bool
}

func (f *fakeCredentialUse) Acquire(ctx context.Context, obj *unstructured.Unstructured) (CredentialUseAcquisition, error) {
	f.log.add("cu:acquire")
	if f.acquire == nil {
		return CredentialUseAcquisition{Opaque: "granted"}, nil
	}
	return f.acquire(ctx, obj)
}

func (f *fakeCredentialUse) BeforeEstablish(ctx context.Context, obj *unstructured.Unstructured, acq CredentialUseAcquisition) error {
	f.log.add("cu:before")
	if f.before == nil {
		return nil
	}
	return f.before(ctx, obj, acq)
}

func (f *fakeCredentialUse) Forget(obj *unstructured.Unstructured) bool {
	ann := maps.Clone(obj.GetAnnotations())
	if _, ok := ann[cuRecord]; !ok {
		return false
	}
	f.log.add("cu:forget")
	delete(ann, cuRecord)
	obj.SetAnnotations(ann)
	return true
}

func (f *fakeCredentialUse) IsOwnRecordWrite(o, n *unstructured.Unstructured) bool {
	return f.own != nil && f.own(o, n)
}

func (f *fakeCredentialUse) IsEstablished(obj *unstructured.Unstructured) bool {
	if f.established != nil {
		return f.established(obj)
	}
	return slices.Contains(obj.GetFinalizers(), finalizerName)
}

func (f *fakeCredentialUse) WithResolved(ctx context.Context, acq CredentialUseAcquisition) context.Context {
	return context.WithValue(ctx, cuResolvedKey{}, acq.Opaque)
}

// cuClient is the external client. It implements ExistenceChecker; plainCUClient hides that.
type cuClient struct {
	log       *cuLog
	exists    bool
	upToDate  bool
	existsErr error
	actErr    error
	key       string

	mu       sync.Mutex
	resolved []any // what each call found under cuResolvedKey
	deadline bool  // a call's context carried a deadline
}

func (c *cuClient) call(ctx context.Context, name string) {
	c.log.add("ext:" + name)
	c.mu.Lock()
	defer c.mu.Unlock()
	c.resolved = append(c.resolved, ctx.Value(cuResolvedKey{}))
	if _, ok := ctx.Deadline(); ok {
		c.deadline = true
	}
}

func (c *cuClient) Exists(ctx context.Context, _ *unstructured.Unstructured) (bool, error) {
	c.call(ctx, "exists")
	return c.exists, c.existsErr
}

func (c *cuClient) Observe(ctx context.Context, _ *unstructured.Unstructured) (ExternalObservation, error) {
	c.call(ctx, "observe")
	return ExternalObservation{ResourceExists: c.exists, ResourceUpToDate: c.upToDate}, c.actErr
}

func (c *cuClient) Create(ctx context.Context, _ *unstructured.Unstructured) error {
	c.call(ctx, "create")
	return c.actErr
}

func (c *cuClient) Update(ctx context.Context, _ *unstructured.Unstructured) error {
	c.call(ctx, "update")
	return c.actErr
}

func (c *cuClient) Delete(ctx context.Context, _ *unstructured.Unstructured) error {
	c.call(ctx, "delete")
	return c.actErr
}

func (c *cuClient) RateLimitKey(*unstructured.Unstructured) (string, error) {
	c.log.add("ext:key")
	return c.key, nil
}

// plainCUClient is an ExternalClient and nothing else: the recovery falls back to Observe.
type plainCUClient struct{ ExternalClient }

// resultCUClient is a ResultCreator.
type resultCUClient struct{ *cuClient }

func (r resultCUClient) CreateWithResult(ctx context.Context, _ *unstructured.Unstructured) (CreateResult, error) {
	r.call(ctx, "createWithResult")
	return CreateResult{Requested: true}, nil
}

// cuWrite is one recorded Update of the object, as the apiserver received it.
type cuWrite struct{ finalizer, record bool }

type cuHarness struct {
	t      *testing.T
	ctrl   *Controller
	opts   Options
	log    *cuLog
	cu     *fakeCredentialUse
	ext    *cuClient
	rec    *mockEventRecorder
	ref    objectref.ObjectRef
	writes *[]cuWrite
}

type cuSetup struct {
	noCU     bool // nil CredentialUse
	ext      *cuClient
	wrap     func(*cuClient) ExternalClient
	governor ratelimit.Governor
	obj      func(*unstructured.Unstructured)
	deleting bool
}

// cuNew builds a controller over a fake apiserver holding one object, "holder". The object carries the
// record and another owner's finalizer, so it outlives the removal of ours.
func cuNew(t *testing.T, s cuSetup) *cuHarness {
	t.Helper()
	sid, err := shortid.New(1, shortid.DefaultABC, 2342)
	require.NoError(t, err)
	l := &cuLog{}
	h := &cuHarness{t: t, log: l, cu: &fakeCredentialUse{log: l}, rec: &mockEventRecorder{}, writes: &[]cuWrite{}}
	h.opts = createTestOptions()
	h.opts.MaxRetries = 5
	h.opts.RateLimitGovernor = s.governor
	h.opts.Recorder, h.opts.ThrottledRecorder = h.rec, h.rec
	if !s.noCU {
		h.opts.CredentialUse = h.cu
	}
	h.ctrl, err = New(sid, h.opts)
	require.NoError(t, err)
	t.Cleanup(h.ctrl.queue.ShutDown)

	h.ext = s.ext
	if h.ext == nil {
		h.ext = &cuClient{exists: true, upToDate: true}
	}
	h.ext.log = l
	h.ctrl.externalClient = h.ext
	if s.wrap != nil {
		h.ctrl.externalClient = s.wrap(h.ext)
	}

	obj := createTestUnstructured("holder", h.opts.Namespace)
	obj.SetFinalizers([]string{finalizerName, cuHold})
	obj.SetAnnotations(map[string]string{cuRecord: "v1"})
	if s.obj != nil {
		s.obj(obj)
	}
	res := h.opts.Client.Resource(h.opts.GVR).Namespace(h.opts.Namespace)
	created, err := res.Create(context.TODO(), obj, metav1.CreateOptions{})
	require.NoError(t, err)
	if s.deleting {
		now := metav1.Now()
		created.SetDeletionTimestamp(&now)
		created, err = res.Update(context.TODO(), created, metav1.UpdateOptions{})
		require.NoError(t, err)
	}
	require.Equal(t, s.deleting, meta.WasDeleted(created), "premise: deleting as asked")
	h.ref = objectref.ObjectRef{APIVersion: created.GetAPIVersion(), Kind: created.GetKind(), Name: created.GetName(), Namespace: created.GetNamespace()}

	// Recorded only from here: the pass's own writes.
	h.react("update", func(a k8stesting.Action) (bool, runtime.Object, error) {
		u := a.(k8stesting.UpdateAction).GetObject().(*unstructured.Unstructured)
		_, hasRecord := u.GetAnnotations()[cuRecord]
		w := cuWrite{finalizer: slices.Contains(u.GetFinalizers(), finalizerName), record: hasRecord}
		l.mu.Lock()
		defer l.mu.Unlock()
		*h.writes = append(*h.writes, w)
		l.all = append(l.all, "api:update")
		return false, nil, nil
	})
	h.react("patch", func(a k8stesting.Action) (bool, runtime.Object, error) {
		l.add("api:patch:" + a.GetSubresource())
		return false, nil, nil
	})
	return h
}

func (h *cuHarness) react(verb string, fn k8stesting.ReactionFunc) {
	h.opts.Client.(*dynamicfake.FakeDynamicClient).PrependReactor(verb, "tests", fn)
}

func (h *cuHarness) event(et ctrlevent.EventType) ctrlevent.Event {
	return ctrlevent.Event{EventType: et, ObjectRef: h.ref}
}

func (h *cuHarness) pass(et ctrlevent.EventType) error {
	return h.ctrl.processItem(context.TODO(), h.event(et))
}

func (h *cuHarness) get() *unstructured.Unstructured {
	h.t.Helper()
	got, err := h.opts.Client.Resource(h.opts.GVR).Namespace(h.ref.Namespace).Get(context.TODO(), h.ref.Name, metav1.GetOptions{})
	require.NoError(h.t, err)
	return got
}

func (h *cuHarness) cond(typ string) *metav1.Condition {
	h.t.Helper()
	for _, c := range unstructuredtools.GetConditions(h.get()) {
		if c.Type == typ {
			return &c
		}
	}
	return nil
}

func (h *cuHarness) hasRecord() bool {
	_, ok := h.get().GetAnnotations()[cuRecord]
	return ok
}

func cuStuck(err error) func(context.Context, *unstructured.Unstructured) (CredentialUseAcquisition, error) {
	return func(context.Context, *unstructured.Unstructured) (CredentialUseAcquisition, error) {
		return CredentialUseAcquisition{}, err
	}
}

func cuResult(a CredentialUseAcquisition) func(context.Context, *unstructured.Unstructured) (CredentialUseAcquisition, error) {
	return func(context.Context, *unstructured.Unstructured) (CredentialUseAcquisition, error) { return a, nil }
}

func pendingSince(age time.Duration) func(*unstructured.Unstructured) {
	return func(o *unstructured.Unstructured) { meta.SetExternalCreatePending(o, time.Now().Add(-age)) }
}

const (
	inGrace   = time.Second
	pastGrace = 24 * time.Hour
)

// cuPaths is every way a pass reaches the external client, plus those that never do.
var cuPaths = []struct {
	name  string
	setup cuSetup
	evt   ctrlevent.EventType
	// want is the order of CredentialUse and external calls.
	want []string
}{
	{"observe, established", cuSetup{}, ctrlevent.Observe,
		[]string{"cu:acquire", "ext:observe"}},
	{"observe, not established", cuSetup{obj: noFinalizer}, ctrlevent.Observe,
		[]string{"cu:acquire", "cu:before", "ext:observe"}},
	{"create, not established", cuSetup{obj: noFinalizer, ext: &cuClient{}}, ctrlevent.Create,
		[]string{"cu:acquire", "cu:before", "ext:create"}},
	{"create through a ResultCreator", cuSetup{wrap: func(c *cuClient) ExternalClient { return resultCUClient{c} }}, ctrlevent.Create,
		[]string{"cu:acquire", "ext:createWithResult"}},
	{"update", cuSetup{}, ctrlevent.Update,
		[]string{"cu:acquire", "ext:update"}},
	{"delete", cuSetup{deleting: true}, ctrlevent.Delete,
		[]string{"cu:acquire", "ext:delete", "cu:forget"}},
	{"delete that fails", cuSetup{deleting: true, ext: &cuClient{exists: true, actErr: errors.New("vendor refused")}}, ctrlevent.Delete,
		[]string{"cu:acquire", "ext:delete"}},
	{"observe of a deleting object", cuSetup{deleting: true}, ctrlevent.Observe,
		[]string{"cu:acquire", "ext:observe"}},
	{"update of a deleting object", cuSetup{deleting: true}, ctrlevent.Update,
		[]string{"cu:acquire", "ext:update"}},
	{"recovery, resource found", cuSetup{obj: pendingSince(pastGrace)}, ctrlevent.Observe,
		[]string{"cu:acquire", "ext:exists", "ext:observe"}},
	{"recovery before the finalizer was ever added", cuSetup{ext: &cuClient{}, obj: func(o *unstructured.Unstructured) { noFinalizer(o); pendingSince(pastGrace)(o) }}, ctrlevent.Create,
		[]string{"cu:acquire", "ext:exists", "cu:before", "ext:create"}},
	{"recovery, absent inside the grace", cuSetup{ext: &cuClient{}, obj: pendingSince(inGrace)}, ctrlevent.Observe,
		[]string{"cu:acquire", "ext:exists"}},
	{"recovery through Observe (no ExistenceChecker)", cuSetup{wrap: func(c *cuClient) ExternalClient { return plainCUClient{c} }, obj: pendingSince(pastGrace)}, ctrlevent.Observe,
		[]string{"cu:acquire", "ext:observe", "ext:observe"}},
	{"recovery of a deleting object", cuSetup{deleting: true, obj: pendingSince(pastGrace)}, ctrlevent.Delete,
		[]string{"cu:acquire", "ext:exists", "ext:delete", "cu:forget"}},
	{"recovery unanswerable, deleting", cuSetup{deleting: true, obj: pendingSince(pastGrace), ext: &cuClient{existsErr: ExistenceUnanswerable(errors.New("no findby"))}}, ctrlevent.Delete,
		[]string{"cu:acquire", "ext:exists", "cu:forget"}},
	{"the rate-limit key is read after the acquire", cuSetup{ext: &cuClient{exists: true, upToDate: true, key: "git.example.com"}, governor: ratelimit.New(ratelimit.Options{Ceiling: 4, RecoveryInterval: time.Hour})}, ctrlevent.Observe,
		[]string{"cu:acquire", "ext:key", "ext:observe"}},
	{"deferred by the governor", cuSetup{ext: &cuClient{key: "git.example.com"}, governor: throttled("git.example.com")}, ctrlevent.Observe,
		[]string{"cu:acquire", "ext:key"}},
	// No external call, so no Acquire.
	{"paused", cuSetup{obj: func(o *unstructured.Unstructured) {
		meta.AddAnnotations(o, map[string]string{meta.AnnotationKeyReconciliationPaused: "true"})
	}}, ctrlevent.Observe, nil},
	{"orphan release", cuSetup{deleting: true, obj: orphan}, ctrlevent.Delete,
		[]string{"cu:forget"}},
	// The one external call an Acquire does not precede: an object already finalized.
	{"already finalized, record gone", cuSetup{deleting: true, obj: func(o *unstructured.Unstructured) { noFinalizer(o); o.SetAnnotations(nil) }}, ctrlevent.Delete,
		[]string{"ext:delete"}},
	{"already finalized, record left", cuSetup{deleting: true, obj: noFinalizer}, ctrlevent.Delete,
		[]string{"cu:forget", "ext:delete"}},
}

func noFinalizer(o *unstructured.Unstructured) { o.SetFinalizers([]string{cuHold}) }

func orphan(o *unstructured.Unstructured) {
	meta.AddAnnotations(o, map[string]string{meta.AnnotationKeyDeletionPolicy: string(meta.DeletionPolicyOrphan)})
}

// The order on every path, and exactly ONE Acquire on every path that makes an external call
// (except for an object already finalized).
func TestCredentialUse_OneAcquireBeforeEveryExternalCall(t *testing.T) {
	var withCalls int
	for _, tc := range cuPaths {
		t.Run(tc.name, func(t *testing.T) {
			h := cuNew(t, tc.setup)
			_ = h.pass(tc.evt)
			require.Equal(t, tc.want, h.log.only("cu:", "ext:"))
			if ext := h.log.only("ext:"); len(ext) > 0 && !strings.HasPrefix(tc.name, "already finalized") {
				withCalls++
				require.Len(t, h.log.only("cu:acquire"), 1, "Acquires in one pass")
			}
		})
	}
	require.Greater(t, withCalls, 10, "denominator: paths that made an external call")
}

// Nil CredentialUse: the same external calls and the same API writes, in the same order, on every path.
// The differential is against a CredentialUse that grants everything and finds no record, so nothing
// but the calls themselves can differ.
func TestCredentialUse_NilChangesNothing(t *testing.T) {
	for _, tc := range cuPaths {
		t.Run(tc.name, func(t *testing.T) {
			run := func(noCU bool) ([]string, error, map[string]any) {
				s := tc.setup
				s.noCU = noCU
				if s.ext != nil {
					e := cuClient{exists: s.ext.exists, upToDate: s.ext.upToDate, existsErr: s.ext.existsErr, actErr: s.ext.actErr, key: s.ext.key}
					s.ext = &e
				}
				seed := s.obj
				s.obj = func(o *unstructured.Unstructured) {
					if seed != nil {
						seed(o)
					}
					meta.RemoveAnnotations(o, cuRecord)
				}
				h := cuNew(t, s)
				err := h.pass(tc.evt)
				got := h.get()
				return h.log.only("ext:", "api:"), err, map[string]any{"finalizers": got.GetFinalizers()}
			}
			onLog, onErr, onObj := run(false)
			offLog, offErr, offObj := run(true)
			require.Equal(t, offLog, onLog)
			require.Equal(t, fmt.Sprint(offErr != nil), fmt.Sprint(onErr != nil))
			require.Equal(t, offObj, onObj)
		})
	}
}

// What Acquire resolved reaches the existence check and the handler, with no deadline added to either.
func TestCredentialUse_HandlersGetTheResolvedContextAndNoDeadline(t *testing.T) {
	h := cuNew(t, cuSetup{obj: pendingSince(pastGrace)})
	require.NoError(t, h.pass(ctrlevent.Observe))
	require.Equal(t, []any{"granted", "granted"}, h.ext.resolved, "the existence check and Observe")
	require.False(t, h.ext.deadline, "the 30s bound is per credential-use call: a handler pass must not inherit it")

	off := cuNew(t, cuSetup{noCU: true, obj: pendingSince(pastGrace)})
	require.NoError(t, off.pass(ctrlevent.Observe))
	require.Equal(t, []any{nil, nil}, off.ext.resolved, "control: without a CredentialUse there is nothing to carry")
}

// FORGET AND THE FINALIZER IN ONE WRITE, at each of the three sites. Asserted on the recorded writes:
// exactly one Update, and it carries neither. A record removed first would end the protection early; a
// finalizer removed first would leave the record behind.
func TestCredentialUse_ForgetInTheFinalizersOwnWrite(t *testing.T) {
	sites := []struct {
		name  string
		setup cuSetup
	}{
		{"orphan path", cuSetup{deleting: true, obj: orphan}},
		{"handleDelete", cuSetup{deleting: true}},
		{"releaseUnanswerableOnDelete", cuSetup{deleting: true, obj: pendingSince(pastGrace), ext: &cuClient{existsErr: ExistenceUnanswerable(errors.New("no findby"))}}},
	}
	for _, tc := range sites {
		t.Run(tc.name, func(t *testing.T) {
			h := cuNew(t, tc.setup)
			require.True(t, h.hasRecord(), "premise")
			require.NoError(t, h.pass(ctrlevent.Delete))
			require.Equal(t, []cuWrite{{finalizer: false, record: false}}, *h.writes, "one Update, with both gone")
			require.Equal(t, []string{cuHold}, h.get().GetFinalizers(), "only ours is removed")
			require.LessOrEqual(t, len(h.log.only("api:patch:")), 1, "no patch besides handleDelete's status")
		})
		t.Run("control, no CredentialUse: "+tc.name, func(t *testing.T) {
			s := tc.setup
			s.noCU = true
			if s.ext != nil {
				s.ext = &cuClient{existsErr: s.ext.existsErr}
			}
			h := cuNew(t, s)
			require.NoError(t, h.pass(ctrlevent.Delete))
			require.Equal(t, []cuWrite{{finalizer: false, record: true}}, *h.writes, "the recorder sees a record that stays")
		})
	}
}

// THE FINALIZER IS GONE AND THE RECORD IS NOT: the record goes alone, finalizers untouched.
func TestCredentialUse_StaleRecordIsDroppedAlone(t *testing.T) {
	t.Run("dropped, in a write of its own, before the dispatch", func(t *testing.T) {
		h := cuNew(t, cuSetup{deleting: true, obj: func(o *unstructured.Unstructured) { noFinalizer(o); orphan(o) }})
		require.NoError(t, h.pass(ctrlevent.Delete))
		require.Equal(t, []cuWrite{{finalizer: false, record: false}}, *h.writes)
		require.Equal(t, []string{cuHold}, h.get().GetFinalizers())
		require.Empty(t, h.log.only("cu:acquire"), "an already finalized object is not acquired for")
	})
	t.Run("control: no record, no write", func(t *testing.T) {
		h := cuNew(t, cuSetup{deleting: true, obj: func(o *unstructured.Unstructured) {
			noFinalizer(o)
			o.SetAnnotations(nil)
			orphan(o)
		}})
		require.NoError(t, h.pass(ctrlevent.Delete))
		require.Empty(t, *h.writes)
	})
	t.Run("control: not deleting, the record stays", func(t *testing.T) {
		h := cuNew(t, cuSetup{obj: noFinalizer})
		require.NoError(t, h.pass(ctrlevent.Observe))
		require.True(t, h.hasRecord())
		require.Equal(t, []cuWrite{{finalizer: true, record: true}}, *h.writes, "only the finalizer's add")
	})
	t.Run("the implementation still counts it established: kept, and acquired", func(t *testing.T) {
		h := cuNew(t, cuSetup{deleting: true, obj: noFinalizer, ext: &cuClient{actErr: errors.New("vendor refused")}})
		h.cu.established = func(*unstructured.Unstructured) bool { return true }
		require.Error(t, h.pass(ctrlevent.Delete))
		require.True(t, h.hasRecord())
		require.Equal(t, []string{"cu:acquire", "ext:delete"}, h.log.only("cu:", "ext:"))
	})
	for name, tc := range map[string]struct {
		err  error
		want func(*testing.T, error)
	}{
		"conflict: quiet, nothing dispatched": {
			apierrors.NewConflict(createTestOptions().GVR.GroupResource(), "holder", errors.New("stale")),
			func(t *testing.T, err error) { require.ErrorIs(t, err, ErrCredentialUseConflict) },
		},
		"gone: the pass ends": {
			apierrors.NewNotFound(createTestOptions().GVR.GroupResource(), "holder"),
			func(t *testing.T, err error) { require.NoError(t, err) },
		},
		"anything else: returned": {
			errors.New("apiserver down"),
			func(t *testing.T, err error) {
				require.ErrorContains(t, err, "apiserver down")
				require.NotErrorIs(t, err, ErrCredentialUseConflict)
			},
		},
	} {
		t.Run(name, func(t *testing.T) {
			h := cuNew(t, cuSetup{deleting: true, obj: noFinalizer})
			h.react("update", func(k8stesting.Action) (bool, runtime.Object, error) { return true, nil, tc.err })
			tc.want(t, h.pass(ctrlevent.Delete))
			require.Empty(t, h.log.only("ext:"))
			require.True(t, h.hasRecord())
		})
	}
}

var cuStops = []struct {
	name    string
	acquire func(context.Context, *unstructured.Unstructured) (CredentialUseAcquisition, error)
	// synced is the condition written, "" for none.
	synced string
}{
	{"MustNotConnect", cuResult(CredentialUseAcquisition{MustNotConnect: true, Err: errors.New("the Secret is being deleted")}), "the Secret is being deleted"},
	{"Pending", cuResult(CredentialUseAcquisition{Pending: true, RetryAfter: 5 * time.Second}), ""},
	{"holder conflict", cuStuck(fmt.Errorf("a1: %w", ErrCredentialUseConflict)), ""},
	{"a plain error", cuStuck(errors.New("informers not synced")), "informers not synced"},
}

// AN OBJECT WITH A CREATE IN DOUBT. On each outcome that stops the pass the existence check is not
// made, and a check that was not made must never read as "does not exist": no external call, nothing
// queued, and the create annotations and the finalizers exactly as they were. The pending marker is
// past its grace, so a wrong "absent" would create at once.
func TestCredentialUse_StopNeverDecidesACreateInDoubt(t *testing.T) {
	for _, deleting := range []bool{false, true} {
		evts := []ctrlevent.EventType{ctrlevent.Create, ctrlevent.Observe, ctrlevent.Update}
		if deleting {
			evts = []ctrlevent.EventType{ctrlevent.Delete, ctrlevent.Observe}
		}
		for _, evt := range evts {
			for _, established := range []bool{true, false} {
				if deleting && !established {
					continue // already finalized: not acquired for
				}
				seed := func(o *unstructured.Unstructured) {
					pendingSince(pastGrace)(o)
					meta.SetExternalCreateFailed(o, time.Now().Add(-2*pastGrace))
					if !established {
						noFinalizer(o)
					}
				}
				arm := fmt.Sprintf("deleting=%t established=%t %s", deleting, established, evt)
				for _, stop := range cuStops {
					t.Run(arm+": "+stop.name, func(t *testing.T) {
						h := cuNew(t, cuSetup{deleting: deleting, obj: seed, ext: &cuClient{exists: false}})
						h.cu.acquire = stop.acquire
						before := h.get()
						require.True(t, meta.ExternalCreateIncomplete(before), "premise: a create in doubt")

						_ = h.pass(evt)

						require.Equal(t, []string{"cu:acquire"}, h.log.only("cu:", "ext:"), "one Acquire, and not one external call")
						require.Zero(t, h.ctrl.queue.Len(), "nothing may be queued: a follow-up Create least of all")
						after := h.get()
						require.Equal(t, before.GetAnnotations(), after.GetAnnotations())
						require.Equal(t, before.GetFinalizers(), after.GetFinalizers())
						require.Empty(t, *h.writes, "no Update of the object")
					})
				}
				t.Run(arm+": control, granted", func(t *testing.T) {
					h := cuNew(t, cuSetup{deleting: deleting, obj: seed, ext: &cuClient{exists: false}})
					require.NoError(t, h.pass(evt))
					require.Contains(t, h.log.only("ext:"), "ext:exists", "the check runs")
					require.False(t, meta.ExternalCreateIncomplete(h.get()), "and the recovery proceeds on its answer")
				})
			}
		}
	}
}

// A DELETION PASS THAT STOPS RELEASES NOTHING: finalizer and record kept, no Delete, at every event
// type. "Could not determine" included.
func TestCredentialUse_StopOnADeletionPassKeepsTheFinalizer(t *testing.T) {
	for _, stop := range cuStops {
		for _, evt := range []ctrlevent.EventType{ctrlevent.Delete, ctrlevent.Observe, ctrlevent.Update} {
			t.Run(fmt.Sprintf("%s on %s", stop.name, evt), func(t *testing.T) {
				h := cuNew(t, cuSetup{deleting: true})
				h.cu.acquire = stop.acquire
				require.Error(t, h.pass(evt), "the pass comes back")
				require.Contains(t, h.get().GetFinalizers(), finalizerName)
				require.True(t, h.hasRecord())
				require.Empty(t, h.log.only("ext:", "cu:forget"))
				require.Empty(t, *h.writes)
			})
		}
	}
	t.Run("control: granted, released", func(t *testing.T) {
		h := cuNew(t, cuSetup{deleting: true})
		require.NoError(t, h.pass(ctrlevent.Delete))
		require.NotContains(t, h.get().GetFinalizers(), finalizerName)
		require.False(t, h.hasRecord())
	})
	t.Run("control: degraded, released", func(t *testing.T) {
		h := cuNew(t, cuSetup{deleting: true})
		h.cu.acquire = cuResult(CredentialUseAcquisition{Degraded: true, Err: errors.New("informers not synced")})
		require.NoError(t, h.pass(ctrlevent.Delete))
		require.NotContains(t, h.get().GetFinalizers(), finalizerName)
	})
}

// What each stop writes: Synced=False/ReconcileError with the cause, or nothing at all. Ready is
// untouched in every case, and no new Ready reason appears.
func TestCredentialUse_StopConditions(t *testing.T) {
	ready := func(o *unstructured.Unstructured) {
		require.NoError(t, unstructuredtools.SetConditions(o, condition.Available()))
	}
	for _, established := range []bool{true, false} {
		for _, stop := range cuStops {
			t.Run(fmt.Sprintf("%s, established=%t", stop.name, established), func(t *testing.T) {
				h := cuNew(t, cuSetup{obj: func(o *unstructured.Unstructured) {
					ready(o)
					if !established {
						noFinalizer(o)
					}
				}})
				h.cu.acquire = stop.acquire
				was := h.cond(condition.TypeReady)
				require.NotNil(t, was, "premise: a Ready to leave alone")
				require.Error(t, h.pass(ctrlevent.Observe))
				require.Equal(t, was, h.cond(condition.TypeReady))
				require.Empty(t, h.log.only("ext:", "cu:before"))
				require.Empty(t, h.rec.events)
				synced := h.cond(condition.TypeSynced)
				if stop.synced == "" {
					require.Nil(t, synced)
					require.Empty(t, h.log.only("api:"), "no write of any kind")
					return
				}
				require.NotNil(t, synced)
				require.Equal(t, metav1.ConditionFalse, synced.Status)
				require.Equal(t, condition.ReasonReconcileError, synced.Reason)
				require.Contains(t, synced.Message, stop.synced)
				require.Equal(t, []string{"api:patch:status"}, h.log.only("api:"))
			})
		}
	}
	t.Run("control: granted writes Synced=True and leaves Ready", func(t *testing.T) {
		h := cuNew(t, cuSetup{obj: ready})
		require.NoError(t, h.pass(ctrlevent.Observe))
		require.Equal(t, condition.ReasonReconcileSuccess, h.cond(condition.TypeSynced).Reason)
	})
	t.Run("MustNotConnect without an error says why", func(t *testing.T) {
		h := cuNew(t, cuSetup{})
		h.cu.acquire = cuResult(CredentialUseAcquisition{MustNotConnect: true})
		require.ErrorContains(t, h.pass(ctrlevent.Observe), errCredentialUseUnheld)
		require.Equal(t, errCredentialUseUnheld, h.cond(condition.TypeSynced).Message)
	})
	t.Run("Pending wins over MustNotConnect", func(t *testing.T) {
		h := cuNew(t, cuSetup{})
		h.cu.acquire = cuResult(CredentialUseAcquisition{Pending: true, MustNotConnect: true, RetryAfter: time.Minute})
		require.Equal(t, &requeueAfter{after: time.Minute, keepPriority: true}, h.pass(ctrlevent.Observe))
		require.Empty(t, h.log.only("api:"))
	})
}

// Degraded proceeds, and says so once.
func TestCredentialUse_DegradedProceedsWithOneEvent(t *testing.T) {
	degraded := func(h *cuHarness) int {
		n := 0
		for _, e := range h.rec.events {
			if e.Reason == reasonCredentialUseDegraded {
				n++
			}
		}
		return n
	}
	h := cuNew(t, cuSetup{})
	h.cu.acquire = cuResult(CredentialUseAcquisition{Degraded: true, Err: errors.New("the index is unreadable")})
	require.NoError(t, h.pass(ctrlevent.Observe))
	require.Equal(t, []string{"cu:acquire", "ext:observe"}, h.log.only("cu:", "ext:"))
	require.Equal(t, 1, degraded(h))
	require.Equal(t, condition.ReasonReconcileSuccess, h.cond(condition.TypeSynced).Reason)

	ok := cuNew(t, cuSetup{})
	require.NoError(t, ok.pass(ctrlevent.Observe))
	require.Zero(t, degraded(ok), "control: a granted pass has no such event")
}

// Reread: the object is read again, and the pass goes on with that copy.
func TestCredentialUse_Reread(t *testing.T) {
	for _, reread := range []bool{true, false} {
		t.Run(fmt.Sprintf("reread=%t", reread), func(t *testing.T) {
			h := cuNew(t, cuSetup{obj: noFinalizer})
			h.cu.acquire = func(ctx context.Context, _ *unstructured.Unstructured) (CredentialUseAcquisition, error) {
				// A webhook defaulted the object during the record write.
				u := h.get()
				u.SetLabels(map[string]string{"defaulted": "yes"})
				_, err := h.opts.Client.Resource(h.opts.GVR).Namespace(h.ref.Namespace).Update(ctx, u, metav1.UpdateOptions{})
				require.NoError(t, err)
				return CredentialUseAcquisition{Reread: reread}, nil
			}
			var seen string
			h.cu.before = func(_ context.Context, o *unstructured.Unstructured, _ CredentialUseAcquisition) error {
				seen = o.GetLabels()["defaulted"]
				return nil
			}
			require.NoError(t, h.pass(ctrlevent.Observe))
			if reread {
				require.Equal(t, "yes", seen)
				require.Equal(t, "yes", h.get().GetLabels()["defaulted"], "the finalizer's write must not revert the default")
			} else {
				require.Empty(t, seen, "control: the pass's first read does not show it")
			}
		})
	}

	// A change that would have ended the pass earlier ends it now.
	for name, mutate := range map[string]func(*unstructured.Unstructured){
		"paused meanwhile": func(u *unstructured.Unstructured) {
			meta.AddAnnotations(u, map[string]string{meta.AnnotationKeyReconciliationPaused: "true"})
		},
		"deleted meanwhile": func(u *unstructured.Unstructured) {
			now := metav1.Now()
			u.SetDeletionTimestamp(&now)
		},
	} {
		t.Run(name, func(t *testing.T) {
			h := cuNew(t, cuSetup{})
			h.cu.acquire = func(ctx context.Context, _ *unstructured.Unstructured) (CredentialUseAcquisition, error) {
				u := h.get()
				mutate(u)
				_, err := h.opts.Client.Resource(h.opts.GVR).Namespace(h.ref.Namespace).Update(ctx, u, metav1.UpdateOptions{})
				require.NoError(t, err)
				return CredentialUseAcquisition{Reread: true}, nil
			}
			require.ErrorIs(t, h.pass(ctrlevent.Observe), ErrCredentialUseConflict)
			require.Empty(t, h.log.only("ext:"))
		})
	}
	t.Run("orphaned meanwhile, deleting: no external delete", func(t *testing.T) {
		h := cuNew(t, cuSetup{deleting: true})
		h.cu.acquire = func(ctx context.Context, _ *unstructured.Unstructured) (CredentialUseAcquisition, error) {
			u := h.get()
			orphan(u)
			_, err := h.opts.Client.Resource(h.opts.GVR).Namespace(h.ref.Namespace).Update(ctx, u, metav1.UpdateOptions{})
			require.NoError(t, err)
			return CredentialUseAcquisition{Reread: true}, nil
		}
		require.ErrorIs(t, h.pass(ctrlevent.Delete), ErrCredentialUseConflict)
		require.Empty(t, h.log.only("ext:"))
	})
	t.Run("gone meanwhile: the pass ends", func(t *testing.T) {
		h := cuNew(t, cuSetup{})
		h.cu.acquire = func(ctx context.Context, _ *unstructured.Unstructured) (CredentialUseAcquisition, error) {
			require.NoError(t, h.opts.Client.Resource(h.opts.GVR).Namespace(h.ref.Namespace).Delete(ctx, h.ref.Name, metav1.DeleteOptions{}))
			return CredentialUseAcquisition{Reread: true}, nil
		}
		require.NoError(t, h.pass(ctrlevent.Observe))
		require.Empty(t, h.log.only("ext:"))
	})
	t.Run("the read fails: nothing is dispatched on the old copy", func(t *testing.T) {
		h := cuNew(t, cuSetup{})
		h.cu.acquire = func(context.Context, *unstructured.Unstructured) (CredentialUseAcquisition, error) {
			h.react("get", func(k8stesting.Action) (bool, runtime.Object, error) {
				return true, nil, errors.New("apiserver down")
			})
			return CredentialUseAcquisition{Reread: true}, nil
		}
		require.ErrorContains(t, h.pass(ctrlevent.Observe), "apiserver down")
		require.Empty(t, h.log.only("ext:"))
	})
}

// BeforeEstablish runs immediately before the finalizer's write, gets this pass's Acquire result, and
// an error means no finalizer and no dispatch.
func TestCredentialUse_BeforeEstablish(t *testing.T) {
	t.Run("granted: between the acquire and the write that adds the finalizer", func(t *testing.T) {
		h := cuNew(t, cuSetup{obj: noFinalizer})
		var got any
		h.cu.before = func(_ context.Context, o *unstructured.Unstructured, acq CredentialUseAcquisition) error {
			got = acq.Opaque
			require.NotContains(t, o.GetFinalizers(), finalizerName)
			return nil
		}
		require.NoError(t, h.pass(ctrlevent.Observe))
		require.Equal(t, []string{"cu:acquire", "cu:before", "api:update", "ext:observe"}, h.log.get()[:4])
		require.Equal(t, "granted", got)
		require.Contains(t, h.get().GetFinalizers(), finalizerName)
	})
	t.Run("control: established already, not called", func(t *testing.T) {
		h := cuNew(t, cuSetup{})
		require.NoError(t, h.pass(ctrlevent.Observe))
		require.Empty(t, h.log.only("cu:before"))
	})
	t.Run("error: no finalizer, no dispatch, recorded", func(t *testing.T) {
		h := cuNew(t, cuSetup{obj: noFinalizer})
		h.cu.before = func(context.Context, *unstructured.Unstructured, CredentialUseAcquisition) error {
			return errors.New("the Secret appeared")
		}
		require.ErrorContains(t, h.pass(ctrlevent.Observe), "the Secret appeared")
		require.Equal(t, []string{cuHold}, h.get().GetFinalizers())
		require.Empty(t, *h.writes)
		require.Empty(t, h.log.only("ext:"))
		require.Equal(t, condition.ReasonReconcileError, h.cond(condition.TypeSynced).Reason)
		require.Nil(t, h.cond(condition.TypeReady))
	})
	t.Run("conflict: quiet", func(t *testing.T) {
		h := cuNew(t, cuSetup{obj: noFinalizer})
		h.cu.before = func(context.Context, *unstructured.Unstructured, CredentialUseAcquisition) error {
			return ErrCredentialUseConflict
		}
		require.ErrorIs(t, h.pass(ctrlevent.Observe), ErrCredentialUseConflict)
		require.Empty(t, h.log.only("ext:", "api:"))
	})
}

// Each call runs under its own bounded context; the pass does not.
func TestCredentialUse_CallTimeout(t *testing.T) {
	require.Equal(t, 30*time.Second, CredentialUseCallTimeout)

	t.Run("the default deadline", func(t *testing.T) {
		h := cuNew(t, cuSetup{obj: noFinalizer})
		check := func(ctx context.Context) {
			d, ok := ctx.Deadline()
			require.True(t, ok)
			require.InDelta(t, 30, time.Until(d).Seconds(), 5)
		}
		var calls int
		h.cu.acquire = func(ctx context.Context, _ *unstructured.Unstructured) (CredentialUseAcquisition, error) {
			calls++
			check(ctx)
			return CredentialUseAcquisition{}, nil
		}
		h.cu.before = func(ctx context.Context, _ *unstructured.Unstructured, _ CredentialUseAcquisition) error {
			calls++
			check(ctx)
			return nil
		}
		require.NoError(t, h.pass(ctrlevent.Observe))
		require.Equal(t, 2, calls)
	})

	hang := func(ctx context.Context) error {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(10 * time.Second):
			return errors.New("the call's context was never cancelled")
		}
	}
	t.Run("a hung Acquire ends, stops the pass, and the pass can still write", func(t *testing.T) {
		h := cuNew(t, cuSetup{})
		h.ctrl.credentialUseTimeout = 20 * time.Millisecond
		h.cu.acquire = func(ctx context.Context, _ *unstructured.Unstructured) (CredentialUseAcquisition, error) {
			return CredentialUseAcquisition{}, hang(ctx)
		}
		require.ErrorIs(t, h.pass(ctrlevent.Observe), context.DeadlineExceeded)
		require.Empty(t, h.log.only("ext:"))
		require.Equal(t, condition.ReasonReconcileError, h.cond(condition.TypeSynced).Reason)
	})
	t.Run("a hung BeforeEstablish ends, and adds no finalizer", func(t *testing.T) {
		h := cuNew(t, cuSetup{obj: noFinalizer})
		h.ctrl.credentialUseTimeout = 20 * time.Millisecond
		h.cu.before = func(ctx context.Context, _ *unstructured.Unstructured, _ CredentialUseAcquisition) error {
			return hang(ctx)
		}
		require.ErrorIs(t, h.pass(ctrlevent.Observe), context.DeadlineExceeded)
		require.NotContains(t, h.get().GetFinalizers(), finalizerName)
	})
	t.Run("control: the same short bound, a call that answers", func(t *testing.T) {
		h := cuNew(t, cuSetup{obj: noFinalizer})
		h.ctrl.credentialUseTimeout = 20 * time.Millisecond
		require.NoError(t, h.pass(ctrlevent.Observe))
		require.Contains(t, h.get().GetFinalizers(), finalizerName)
	})
}

// cuQueue records how each item was added.
type cuQueue struct {
	priorityqueue.PriorityQueue[any]
	mu   sync.Mutex
	adds []cuAdd
}

type cuAdd struct {
	ev   ctrlevent.Event
	opts priorityqueue.AddOpts
}

func (q *cuQueue) AddWithOpts(o priorityqueue.AddOpts, items ...any) {
	q.mu.Lock()
	for _, it := range items {
		if ev, ok := it.(ctrlevent.Event); ok {
			q.adds = append(q.adds, cuAdd{ev, o})
		}
	}
	q.mu.Unlock()
	q.PriorityQueue.AddWithOpts(o, items...)
}

// work runs ev through the worker, at priority, and returns what was queued after it.
func (h *cuHarness) work(ev ctrlevent.Event, priority int) []cuAdd {
	h.t.Helper()
	q := &cuQueue{PriorityQueue: h.ctrl.queue}
	h.ctrl.queue = q
	h.ctrl.runCtx = context.Background()
	h.ctrl.items.Store(ctrlevent.DigestForEvent(ev), struct{}{})
	q.PriorityQueue.AddWithOpts(priorityqueue.AddOpts{Priority: priority}, ev)
	require.True(h.t, h.ctrl.processNext(context.Background(), "test"))
	q.mu.Lock()
	defer q.mu.Unlock()
	return slices.Clone(q.adds)
}

// The requeue each outcome gets from the worker.
func TestCredentialUse_Requeues(t *testing.T) {
	const prio = 37
	exhaust := func(h *cuHarness, ev ctrlevent.Event) {
		for range h.opts.MaxRetries {
			h.opts.GlobalRateLimiter.When(ev)
		}
	}
	t.Run("conflict: rate-limited, its own event, never dropped", func(t *testing.T) {
		h := cuNew(t, cuSetup{})
		h.cu.acquire = cuStuck(ErrCredentialUseConflict)
		ev := h.event(ctrlevent.Update)
		exhaust(h, ev)
		got := h.work(ev, prio)
		require.Len(t, got, 1)
		require.Equal(t, ctrlevent.Update, got[0].ev.EventType)
		require.Equal(t, priorityqueue.AddOpts{RateLimited: true, Priority: prio}, got[0].opts)
	})
	t.Run("control: an ordinary error at the same budget is dropped", func(t *testing.T) {
		h := cuNew(t, cuSetup{})
		h.cu.acquire = cuStuck(errors.New("not a conflict"))
		ev := h.event(ctrlevent.Update)
		exhaust(h, ev)
		require.Empty(t, h.work(ev, prio))
	})
	t.Run("MustNotConnect and a plain error: rate-limited", func(t *testing.T) {
		for _, i := range []int{0, 3} {
			h := cuNew(t, cuSetup{})
			h.cu.acquire = cuStops[i].acquire
			ev := h.event(ctrlevent.Create)
			got := h.work(ev, prio)
			require.Len(t, got, 1)
			require.Equal(t, priorityqueue.AddOpts{RateLimited: true, Priority: prio}, got[0].opts)
			require.Equal(t, 1, h.opts.GlobalRateLimiter.NumRequeues(ev), "it counts as a failure")
		}
	})
	t.Run("Pending: after RetryAfter, its own event and priority, no backoff, never dropped", func(t *testing.T) {
		h := cuNew(t, cuSetup{})
		h.cu.acquire = cuStops[1].acquire
		ev := h.event(ctrlevent.Create)
		exhaust(h, ev)
		got := h.work(ev, prio)
		require.Len(t, got, 1)
		require.Equal(t, ev, got[0].ev)
		require.Equal(t, priorityqueue.AddOpts{After: 5 * time.Second, Priority: prio}, got[0].opts)
		require.Equal(t, h.opts.MaxRetries, h.opts.GlobalRateLimiter.NumRequeues(ev), "the failure history is left as it was")
	})
	t.Run("Pending without a RetryAfter is not a hot loop", func(t *testing.T) {
		h := cuNew(t, cuSetup{})
		h.cu.acquire = cuResult(CredentialUseAcquisition{Pending: true})
		got := h.work(h.event(ctrlevent.Observe), prio)
		require.Len(t, got, 1)
		require.Equal(t, credentialUseMinRetry, got[0].opts.After)
	})

	retry := func(h *cuHarness) {
		h.cu.acquire = cuResult(CredentialUseAcquisition{RetryAfter: 7 * time.Second})
	}
	for _, et := range []ctrlevent.EventType{ctrlevent.Create, ctrlevent.Update, ctrlevent.Observe} {
		t.Run(fmt.Sprintf("RetryAfter: a %s that succeeded runs again as an Observe", et), func(t *testing.T) {
			h := cuNew(t, cuSetup{})
			retry(h)
			ev := h.event(et)
			h.opts.GlobalRateLimiter.When(ev)
			got := h.work(ev, prio)
			require.Len(t, got, 1)
			require.Equal(t, ctrlevent.Observe, got[0].ev.EventType, "re-running the act would repeat it")
			require.Equal(t, priorityqueue.AddOpts{After: 7 * time.Second, Priority: NormalPriority}, got[0].opts)
			require.Len(t, h.log.only("ext:"), 1, "it proceeded")
			require.Zero(t, h.opts.GlobalRateLimiter.NumRequeues(ev), "the pass succeeded: its failure history is forgotten")
		})
	}
	t.Run("control: no RetryAfter, nothing queued", func(t *testing.T) {
		h := cuNew(t, cuSetup{})
		require.Empty(t, h.work(h.event(ctrlevent.Observe), prio))
	})
	t.Run("RetryAfter: a Delete that released its finalizer is not requeued", func(t *testing.T) {
		h := cuNew(t, cuSetup{deleting: true})
		retry(h)
		require.Empty(t, h.work(h.event(ctrlevent.Delete), prio))
		require.NotContains(t, h.get().GetFinalizers(), finalizerName)
	})
	t.Run("RetryAfter: a failure keeps its own backoff", func(t *testing.T) {
		h := cuNew(t, cuSetup{ext: &cuClient{actErr: errors.New("vendor refused")}})
		retry(h)
		got := h.work(h.event(ctrlevent.Update), prio)
		require.Len(t, got, 1)
		require.Equal(t, ctrlevent.Update, got[0].ev.EventType)
		require.True(t, got[0].opts.RateLimited)
	})
	t.Run("RetryAfter: a deferral keeps the governor's delay", func(t *testing.T) {
		h := cuNew(t, cuSetup{ext: &cuClient{key: "git.example.com"}, governor: throttled("git.example.com")})
		retry(h)
		got := h.work(h.event(ctrlevent.Observe), prio)
		require.Len(t, got, 1)
		require.Greater(t, got[0].opts.After, time.Minute)
	})
	for _, tc := range []struct{ retry, want time.Duration }{
		{7 * time.Second, 7 * time.Second},
		{time.Hour, externalCreateRecoveryReobserveInterval},
	} {
		t.Run(fmt.Sprintf("RetryAfter %s: a pass that already asked to run again takes the sooner", tc.retry), func(t *testing.T) {
			h := cuNew(t, cuSetup{ext: &cuClient{}, obj: pendingSince(inGrace)})
			h.cu.acquire = cuResult(CredentialUseAcquisition{RetryAfter: tc.retry})
			got := h.work(h.event(ctrlevent.Create), prio)
			require.Len(t, got, 1)
			require.Equal(t, ctrlevent.Create, got[0].ev.EventType, "its own event")
			require.Equal(t, priorityqueue.AddOpts{After: tc.want, Priority: LowPriority}, got[0].opts)
		})
	}

	// How the pass is counted.
	require.Equal(t, outcomeStatusConflict, classifyOutcome(fmt.Errorf("x: %w", ErrCredentialUseConflict)))
	require.Equal(t, outcomeDeferred, classifyOutcome(&requeueAfter{after: 1}))
	require.Equal(t, outcomeSuccess, classifyOutcome(&requeueAfter{after: 1, next: &ctrlevent.Event{}}))
	require.Equal(t, outcomeFailure, classifyOutcome(errors.New("x")))
}

// The informer drops an update event that only wrote the record. The fake apiserver keeps
// resourceVersion fixed, so the edit changes the spec, which always queues an Update when not dropped.
func TestCredentialUse_InformerDropsOwnRecordWrites(t *testing.T) {
	for _, own := range []bool{true, false} {
		t.Run(fmt.Sprintf("own=%t", own), func(t *testing.T) {
			h := cuNew(t, cuSetup{})
			asked := make(chan struct{}, 16)
			h.cu.own = func(o, n *unstructured.Unstructured) bool {
				asked <- struct{}{}
				return own
			}
			stop := make(chan struct{})
			defer close(stop)
			go h.ctrl.informer.Run(stop)
			require.Eventually(t, h.ctrl.informer.HasSynced, 5*time.Second, 10*time.Millisecond)
			require.Eventually(t, func() bool { return h.ctrl.queue.Len() == 1 }, 5*time.Second, 10*time.Millisecond, "the add event")

			u := h.get()
			require.NoError(t, unstructured.SetNestedField(u.Object, "edited", "spec", "field1"))
			_, err := h.opts.Client.Resource(h.opts.GVR).Namespace(h.ref.Namespace).Update(context.TODO(), u, metav1.UpdateOptions{})
			require.NoError(t, err)
			select {
			case <-asked:
			case <-time.After(5 * time.Second):
				t.Fatal("the informer never asked: the zero below would be vacuous")
			}
			if own {
				require.Never(t, func() bool { return h.ctrl.queue.Len() > 1 }, 300*time.Millisecond, 10*time.Millisecond)
			} else {
				require.Eventually(t, func() bool { return h.ctrl.queue.Len() == 2 }, 5*time.Second, 10*time.Millisecond)
			}
		})
	}
}

// Enqueue queues exactly the event the informer would, so the one-pass-per-object gate and the digest
// dedup both see one object.
func TestCredentialUse_Enqueue(t *testing.T) {
	for _, deleting := range []bool{false, true} {
		t.Run(fmt.Sprintf("deleting=%t", deleting), func(t *testing.T) {
			h := cuNew(t, cuSetup{deleting: deleting})
			key := types.NamespacedName{Namespace: h.ref.Namespace, Name: h.ref.Name}
			require.False(t, h.ctrl.Enqueue(key), "control: the informer has not delivered it")
			require.Zero(t, h.ctrl.queue.Len())

			require.NoError(t, h.ctrl.store.Add(h.get()))
			require.True(t, h.ctrl.Enqueue(key))
			require.True(t, h.ctrl.Enqueue(key), "still served")
			require.Equal(t, 1, h.ctrl.queue.Len(), "and queued once")
			require.False(t, h.ctrl.Enqueue(types.NamespacedName{Namespace: "elsewhere", Name: h.ref.Name}))

			it, _, _ := h.ctrl.queue.GetWithPriority()
			ev := it.(ctrlevent.Event)
			require.Equal(t, h.ref, ev.ObjectRef)
			want := ctrlevent.Observe
			if deleting {
				want = ctrlevent.Delete
			}
			require.Equal(t, want, ev.EventType)
		})
	}
	t.Run("the store is the informer's", func(t *testing.T) {
		h := cuNew(t, cuSetup{})
		stop := make(chan struct{})
		defer close(stop)
		go h.ctrl.informer.Run(stop)
		require.Eventually(t, func() bool {
			return h.ctrl.Enqueue(types.NamespacedName{Namespace: h.ref.Namespace, Name: h.ref.Name})
		}, 5*time.Second, 10*time.Millisecond)
	})
}
