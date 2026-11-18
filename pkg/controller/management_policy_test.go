package controller

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/krateo-platformops/plumbing/shortid"
	ctrlevent "github.com/krateo-platformops/unstructured-runtime/pkg/controller/event"
	"github.com/krateo-platformops/unstructured-runtime/pkg/controller/objectref"
	"github.com/krateo-platformops/unstructured-runtime/pkg/meta"
	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
)

// observingClient answers Observe with a fixed observation and counts every verb.
type observingClient struct {
	recordingExternalClient
	obs ExternalObservation
}

func (o *observingClient) Observe(ctx context.Context, mg *unstructured.Unstructured) (ExternalObservation, error) {
	o.observed++
	return o.obs, nil
}

// #110 Phase 0. krateo.io/management-policy was documented as restricting which phases act, and
// before this only delete honoured it. Each preset × observed state × event source, asserting the
// verbs the HANDLER was called with and the follow-up the observe pass queued:
//
//   - the event source matters because a Create or Update reaches the dispatch from handleObserve's
//     follow-up, from the informer (CRUpdated -> Update by default), and from the watched
//     annotations; a gate in only one of them is the defect this replaces;
//   - `default` is the CONTROL in every row: the same object and event with no restriction must
//     call the verb, or a zero below measures a harness that never dispatches.
func TestManagementPolicy_GatesCreateAndUpdate(t *testing.T) {
	absent := ExternalObservation{ResourceExists: false}
	drifted := ExternalObservation{ResourceExists: true, ResourceUpToDate: false}

	for _, tc := range []struct {
		name   string
		policy string
		obs    ExternalObservation
		event  ctrlevent.EventType
		// wantCalled: the handler's Create (absent) or Update (drifted) was called by this pass.
		wantCalled bool
		// wantFollowUp: the observe pass queued the Create/Update follow-up.
		wantFollowUp bool
		// wantErr: the pass returns an error containing this text.
		wantErr string
	}{
		// absent, Observe event: does the observe pass queue a Create?
		{"default/absent/observe", meta.ManagementPolicyDefault, absent, ctrlevent.Observe, false, true, ""},
		{"observe-create-update/absent/observe", meta.ManagementPolicyObserveCreateUpdate, absent, ctrlevent.Observe, false, true, ""},
		{"observe-delete/absent/observe", meta.ManagementPolicyObserveDelete, absent, ctrlevent.Observe, false, false, ""},
		{"observe/absent/observe", meta.ManagementPolicyObserve, absent, ctrlevent.Observe, false, false, "external resource does not exist"},
		// drifted, Observe event: does the observe pass queue an Update?
		{"default/drifted/observe", meta.ManagementPolicyDefault, drifted, ctrlevent.Observe, false, true, ""},
		{"observe-create-update/drifted/observe", meta.ManagementPolicyObserveCreateUpdate, drifted, ctrlevent.Observe, false, true, ""},
		{"observe-delete/drifted/observe", meta.ManagementPolicyObserveDelete, drifted, ctrlevent.Observe, false, false, ""},
		{"observe/drifted/observe", meta.ManagementPolicyObserve, drifted, ctrlevent.Observe, false, false, ""},
		// a Create event dispatched directly (watched annotations, a follow-up queued before the
		// policy changed): is the handler's Create called?
		{"default/absent/create", meta.ManagementPolicyDefault, absent, ctrlevent.Create, true, false, ""},
		{"observe-delete/absent/create", meta.ManagementPolicyObserveDelete, absent, ctrlevent.Create, false, false, ""},
		{"observe/absent/create", meta.ManagementPolicyObserve, absent, ctrlevent.Create, false, false, "external resource does not exist"},
		// an Update event from the informer (CRUpdated -> Update): is the handler's Update called?
		{"default/drifted/update", meta.ManagementPolicyDefault, drifted, ctrlevent.Update, true, false, ""},
		{"observe-create-update/drifted/update", meta.ManagementPolicyObserveCreateUpdate, drifted, ctrlevent.Update, true, false, ""},
		{"observe-delete/drifted/update", meta.ManagementPolicyObserveDelete, drifted, ctrlevent.Update, false, false, ""},
		{"observe/drifted/update", meta.ManagementPolicyObserve, drifted, ctrlevent.Update, false, false, ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			sid, err := shortid.New(1, shortid.DefaultABC, 2342)
			require.NoError(t, err)
			opts := createTestOptions()
			cli := &observingClient{obs: tc.obs}
			ctrl, err := New(sid, opts)
			require.NoError(t, err)
			ctrl.externalClient = cli

			name := strings.ReplaceAll(tc.name, "/", "-")
			obj := createTestUnstructured(name, opts.Namespace)
			ann := obj.GetAnnotations()
			if ann == nil {
				ann = map[string]string{}
			}
			ann[meta.AnnotationKeyManagementPolicy] = tc.policy
			obj.SetAnnotations(ann)
			obj.SetFinalizers([]string{finalizerName})
			created, err := opts.Client.Resource(opts.GVR).Namespace(opts.Namespace).Create(context.TODO(), obj, metav1.CreateOptions{})
			require.NoError(t, err)
			// PREMISE: the annotation is read back as the runtime reads it, so a misspelt key cannot
			// turn a restricted row into an unrestricted one that passes for the wrong reason.
			require.Equal(t, tc.policy == meta.ManagementPolicyDefault || tc.policy == meta.ManagementPolicyObserveCreateUpdate, meta.ShouldCreate(created))

			ref := objectref.ObjectRef{APIVersion: created.GetAPIVersion(), Kind: created.GetKind(), Name: created.GetName(), Namespace: created.GetNamespace()}
			err = ctrl.processItem(context.TODO(), ctrlevent.Event{EventType: tc.event, ObjectRef: ref})
			if tc.wantErr != "" {
				require.Error(t, err)
				require.Contains(t, err.Error(), tc.wantErr)
			} else {
				require.NoError(t, err)
			}

			called := cli.created + cli.updated
			require.Equal(t, tc.wantCalled, called > 0,
				"handler Create/Update calls: created=%d updated=%d observed=%d", cli.created, cli.updated, cli.observed)
			if !tc.wantCalled {
				require.Zero(t, cli.deleted)
			}

			followUp := ctrlevent.Create
			if tc.obs.ResourceExists {
				followUp = ctrlevent.Update
			}
			_, queued := ctrl.items.Load(ctrlevent.DigestForEvent(ctrlevent.Event{EventType: followUp, ObjectRef: ref}))
			require.Equal(t, tc.wantFollowUp, queued, "follow-up %s queued", followUp)
		})
	}
}

// erroringObserveClient fails Observe, as a transport error or a handler's "created but no
// identifier recorded" does.
type erroringObserveClient struct{ recordingExternalClient }

func (e *erroringObserveClient) Observe(ctx context.Context, mg *unstructured.Unstructured) (ExternalObservation, error) {
	e.observed++
	return ExternalObservation{}, fmt.Errorf("the create reported success but no identifier was recorded")
}

// #110, oasgen-provider's finding on v1.28.0: an absent resource under `observe` is a declaration
// the operator made, and must not share CannotObserveExternalResource, the generic observe-ERROR
// reason that also covers "the external resource MAY EXIST unreferenced". CONTROL: a failing Observe
// still emits CannotObserveExternalResource, so the first arm is not a reason that changed everywhere.
func TestManagementPolicy_AbsentUnderObserveHasItsOwnReason(t *testing.T) {
	reasons := func(t *testing.T, cli ExternalClient, policy string) []string {
		t.Helper()
		sid, err := shortid.New(1, shortid.DefaultABC, 2342)
		require.NoError(t, err)
		opts := createTestOptions()
		rec, throttled := &mockEventRecorder{}, &mockEventRecorder{}
		opts.Recorder, opts.ThrottledRecorder = rec, throttled
		ctrl, err := New(sid, opts)
		require.NoError(t, err)
		ctrl.externalClient = cli
		obj := createTestUnstructured("reason-"+policy, opts.Namespace)
		obj.SetAnnotations(map[string]string{meta.AnnotationKeyManagementPolicy: policy})
		obj.SetFinalizers([]string{finalizerName})
		created, err := opts.Client.Resource(opts.GVR).Namespace(opts.Namespace).Create(context.TODO(), obj, metav1.CreateOptions{})
		require.NoError(t, err)
		ref := objectref.ObjectRef{APIVersion: created.GetAPIVersion(), Kind: created.GetKind(), Name: created.GetName(), Namespace: created.GetNamespace()}
		_ = ctrl.processItem(context.TODO(), ctrlevent.Event{EventType: ctrlevent.Observe, ObjectRef: ref})
		var out []string
		for _, r := range []*mockEventRecorder{rec, throttled} {
			r.mu.Lock()
			for _, e := range r.events {
				out = append(out, string(e.Reason))
			}
			r.mu.Unlock()
		}
		return out
	}
	got := reasons(t, &observingClient{obs: ExternalObservation{ResourceExists: false}}, meta.ManagementPolicyObserve)
	require.Contains(t, got, "ExternalResourceAbsentUnderPolicy")
	require.NotContains(t, got, "CannotObserveExternalResource")

	ctrl := reasons(t, &erroringObserveClient{}, meta.ManagementPolicyDefault)
	require.Contains(t, ctrl, "CannotObserveExternalResource", "control: a failing Observe keeps the generic reason")
}
