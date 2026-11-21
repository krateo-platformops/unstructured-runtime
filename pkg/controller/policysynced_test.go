package controller

import (
	"context"
	"errors"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/krateo-platformops/plumbing/managementpolicy"
	"github.com/krateo-platformops/plumbing/shortid"
	ctrlevent "github.com/krateo-platformops/unstructured-runtime/pkg/controller/event"
	"github.com/krateo-platformops/unstructured-runtime/pkg/controller/objectref"
	"github.com/krateo-platformops/unstructured-runtime/pkg/meta"
	unstructuredtools "github.com/krateo-platformops/unstructured-runtime/pkg/tools/unstructured"
	"github.com/krateo-platformops/unstructured-runtime/pkg/tools/unstructured/condition"
	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
)

// failingObserveClient's Observe always errors: the pass outcome is ReconcileError.
type failingObserveClient struct{ recordingExternalClient }

func (f *failingObserveClient) Observe(ctx context.Context, mg *unstructured.Unstructured) (ExternalObservation, error) {
	f.observed++
	return ExternalObservation{}, errors.New("observe boom")
}

// syncedAfterPass runs one real pass (processItem) on a fresh object with ann, and returns its Synced
// condition and the pass error.
func syncedAfterPass(t *testing.T, cli ExternalClient, name string, ann map[string]string) (metav1.Condition, error) {
	t.Helper()
	sid, err := shortid.New(1, shortid.DefaultABC, 2342)
	require.NoError(t, err)
	opts := createTestOptions()
	opts.Recorder, opts.ThrottledRecorder = &mockEventRecorder{}, &mockEventRecorder{}
	ctrl, err := New(sid, opts)
	require.NoError(t, err)
	ctrl.externalClient = cli
	obj := createTestUnstructured(name, opts.Namespace)
	obj.SetAnnotations(ann)
	res := opts.Client.Resource(opts.GVR).Namespace(opts.Namespace)
	created, err := res.Create(context.TODO(), obj, metav1.CreateOptions{})
	require.NoError(t, err)
	perr := ctrl.processItem(context.TODO(), ctrlevent.Event{EventType: ctrlevent.Observe, ObjectRef: objectref.ObjectRef{
		APIVersion: created.GetAPIVersion(), Kind: created.GetKind(), Name: created.GetName(), Namespace: created.GetNamespace()}})
	cur, err := res.Get(context.TODO(), name, metav1.GetOptions{})
	require.NoError(t, err)
	for _, c := range unstructuredtools.GetConditions(cur) {
		if c.Type == condition.TypeSynced {
			return c, perr
		}
	}
	t.Fatalf("%s: no Synced condition after the pass", name)
	return metav1.Condition{}, perr
}

// Diego, 2026-11-21 (#110): ManagementPolicyInvalid wins over every pass outcome on Synced, then
// DeletionPolicyInvalid; paused keeps its reason. Each arm runs a real pass. CONTROLS: the same paths
// with valid annotations keep ReconcileSuccess / ReconcileError / ReconcilePaused.
func TestPolicyInvalidOwnsSynced(t *testing.T) {
	const badMP, badDP = "observe-creat", "orphaned"
	for _, tc := range []struct {
		name       string
		cli        ExternalClient
		ann        map[string]string
		wantReason string
		wantErr    bool
		mustSay    []string
	}{
		{"observe success, invalid policy", &recordingExternalClient{}, map[string]string{meta.AnnotationKeyManagementPolicy: badMP}, managementpolicy.ReasonInvalid, false, []string{meta.AnnotationKeyManagementPolicy, `"observe-creat"`}},
		{"observe ERROR, invalid policy: the policy still wins, and the error is still returned", &failingObserveClient{}, map[string]string{meta.AnnotationKeyManagementPolicy: badMP}, managementpolicy.ReasonInvalid, true, []string{`"observe-creat"`}},
		{"observe success, invalid deletion-policy (reported though nothing is deleting)", &recordingExternalClient{}, map[string]string{meta.AnnotationKeyDeletionPolicy: badDP}, managementpolicy.ReasonDeletionPolicyInvalid, false, []string{`"orphaned"`}},
		{"observe ERROR, invalid deletion-policy", &failingObserveClient{}, map[string]string{meta.AnnotationKeyDeletionPolicy: badDP}, managementpolicy.ReasonDeletionPolicyInvalid, true, []string{`"orphaned"`}},
		{"both invalid: management-policy wins, and names the deletion-policy too", &recordingExternalClient{}, map[string]string{meta.AnnotationKeyManagementPolicy: badMP, meta.AnnotationKeyDeletionPolicy: badDP}, managementpolicy.ReasonInvalid, false, []string{`"observe-creat"`, `"orphaned"`}},
		{"paused with an invalid policy keeps ReconcilePaused: no pass ran", &recordingExternalClient{}, map[string]string{meta.AnnotationKeyManagementPolicy: badMP, meta.AnnotationKeyReconciliationPaused: "true"}, condition.ReasonReconcilePaused, false, nil},
		{"control: valid policy, success", &recordingExternalClient{}, map[string]string{meta.AnnotationKeyManagementPolicy: "observe", meta.AnnotationKeyDeletionPolicy: "orphan"}, condition.ReasonReconcileSuccess, false, nil},
		{"control: valid policy, error", &failingObserveClient{}, nil, condition.ReasonReconcileError, true, []string{"observe boom"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			name := "ps-" + strings.ToLower(strings.NewReplacer(" ", "-", ",", "", ":", "", "(", "", ")", "").Replace(tc.name))
			if len(name) > 60 {
				name = name[:60]
			}
			name = strings.TrimRight(name, "-")
			synced, perr := syncedAfterPass(t, tc.cli, name, tc.ann)
			require.Equal(t, tc.wantReason, synced.Reason, "Synced: %+v", synced)
			require.Equal(t, tc.wantErr, perr != nil, "pass error: %v (the policy report must not swallow or invent a pass error)", perr)
			for _, s := range tc.mustSay {
				require.Contains(t, synced.Message, s)
			}
		})
	}
}

// ReconcileDeferred keeps its reason too (no pass ran). policySynced is the funnel's only decision.
func TestPolicySynced_DeferredAndOtherTypesPassThrough(t *testing.T) {
	el := createTestUnstructured("ps-deferred", "default")
	el.SetAnnotations(map[string]string{meta.AnnotationKeyManagementPolicy: "observe-creat"})
	d := condition.ReconcileDeferred("api.example.org", time.Time{})
	require.Equal(t, condition.ReasonReconcileDeferred, policySynced(el, d).Reason)
	ready := metav1.Condition{Type: condition.TypeReady, Status: metav1.ConditionTrue, Reason: "Available"}
	require.Equal(t, ready, policySynced(el, ready), "a non-Synced condition passes through unchanged")
	// CONTROL: the same object's ReconcileSuccess IS rewritten, so the pass-throughs above are not an
	// instrument that rewrites nothing.
	require.Equal(t, managementpolicy.ReasonInvalid, policySynced(el, condition.ReconcileSuccess()).Reason)
}

// Every condition write in this package goes through setConditions, so no path can report a Synced
// outcome the policy should have overridden. The behavioural test above drives the observe paths and
// the policy table the create/update/absent paths; this pins the rest by construction: a direct
// unstructuredtools.SetConditions call anywhere but the funnel fails here, naming the site.
// CONTROL: the funnel's own call is found, so a parser that saw nothing cannot pass.
func TestNoConditionWriteBypassesTheFunnel(t *testing.T) {
	files, err := filepath.Glob("*.go")
	require.NoError(t, err)
	var funnel, bypass []string
	scanned := 0
	for _, f := range files {
		if strings.HasSuffix(f, "_test.go") {
			continue
		}
		src, err := os.ReadFile(f)
		require.NoError(t, err)
		fs := token.NewFileSet()
		file, err := parser.ParseFile(fs, f, src, 0)
		require.NoError(t, err)
		scanned++
		ast.Inspect(file, func(n ast.Node) bool {
			call, ok := n.(*ast.CallExpr)
			if !ok {
				return true
			}
			sel, ok := call.Fun.(*ast.SelectorExpr)
			if !ok || sel.Sel.Name != "SetConditions" {
				return true
			}
			if x, ok := sel.X.(*ast.Ident); ok && x.Name == "unstructuredtools" {
				site := fs.Position(call.Pos()).String()
				if f == "policysynced.go" {
					funnel = append(funnel, site)
				} else {
					bypass = append(bypass, site)
				}
			}
			return true
		})
	}
	require.Greater(t, scanned, 3, "premise: the package's files were scanned")
	require.Len(t, funnel, 1, "control: the funnel's own unstructuredtools.SetConditions call must be found")
	require.Empty(t, bypass, "condition writes that bypass setConditions (and so the #110 policy report)")
}
