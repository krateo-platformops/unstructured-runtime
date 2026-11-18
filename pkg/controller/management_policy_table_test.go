package controller

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"github.com/krateo-platformops/plumbing/shortid"
	ctrlevent "github.com/krateo-platformops/unstructured-runtime/pkg/controller/event"
	"github.com/krateo-platformops/unstructured-runtime/pkg/controller/objectref"
	"github.com/krateo-platformops/unstructured-runtime/pkg/meta"
	unstructuredtools "github.com/krateo-platformops/unstructured-runtime/pkg/tools/unstructured"
	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

var updatePolicyTable = flag.Bool("update-policy-table", false, "rewrite testdata/management-policy-table.tsv")

// #110 Phase 1.1, unstructured-runtime half: the MEASURED outcome of every management policy ×
// deletion policy × observed state, as a TSV pinned in testdata/management-policy-table.tsv. The
// row and column order match provider-runtime's table of the same name, so the two diff line by line;
// a difference between them is a difference between the runtimes, which D9 removes.
//
// One row is one reconcile AS THE INFORMER DRIVES IT: an Observe event (the resync) for a live
// object, a Delete event (controller.go UpdateFunc) for one being deleted, and then every follow-up
// that pass queued (Create / Update), so a create the observe pass schedules counts as this row's
// create, as it does in provider-runtime's single Reconcile.
//
// The table describes what the code DOES; it asserts nothing about what it should do. That is D4's
// decision, taken against it. Regenerate with -update-policy-table and review the diff.
func TestManagementPolicyTable(t *testing.T) {
	policies := []string{"", meta.ManagementPolicyDefault, meta.ManagementPolicyObserveCreateUpdate, meta.ManagementPolicyObserveDelete, meta.ManagementPolicyObserve, "observe-create"}
	deletions := []string{"", meta.DeletionPolicyDelete, meta.DeletionPolicyOrphan}
	states := []struct {
		name     string
		obs      ExternalObservation
		deleting bool
	}{
		{"absent", ExternalObservation{ResourceExists: false}, false},
		{"up-to-date", ExternalObservation{ResourceExists: true, ResourceUpToDate: true}, false},
		{"drifted", ExternalObservation{ResourceExists: true, ResourceUpToDate: false}, false},
		{"deleting-present", ExternalObservation{ResourceExists: true, ResourceUpToDate: true}, true},
		{"deleting-absent", ExternalObservation{ResourceExists: false}, true},
	}
	label := func(s string) string {
		if s == "" {
			return "unset"
		}
		return s
	}

	var b strings.Builder
	b.WriteString(strings.Join([]string{
		"management-policy", "deletion-policy", "state",
		"observe", "create", "update", "delete", "finalizer-removed",
		"ready-status", "ready-reason", "ready-message",
		"synced-status", "synced-reason", "synced-message",
		"events", "error", "requeue",
	}, "\t") + "\n")

	for _, mp := range policies {
		for _, dp := range deletions {
			for _, st := range states {
				b.WriteString(strings.Join(policyRow(t, mp, dp, st.name, st.obs, st.deleting, label), "\t") + "\n")
			}
		}
	}

	golden := filepath.Join("testdata", "management-policy-table.tsv")
	if *updatePolicyTable {
		require.NoError(t, os.MkdirAll("testdata", 0o755))
		require.NoError(t, os.WriteFile(golden, []byte(b.String()), 0o644))
		return
	}
	want, err := os.ReadFile(golden)
	require.NoError(t, err, "no golden table: run with -update-policy-table and review it")
	require.Equal(t, string(want), b.String(), "the measured table changed; if intended, re-run with -update-policy-table and review the diff")
	// 6 × 3 × 5 rows plus the header: a table that silently lost rows would also pass a diff
	// against an equally short golden file.
	require.Equal(t, 1+6*3*5, strings.Count(b.String(), "\n"))
}

// tsvCell keeps one cell on one line and tab-free.
func tsvCell(s string) string {
	s = strings.ReplaceAll(s, "\t", " ")
	s = strings.ReplaceAll(s, "\n", " ")
	if s == "" {
		return "-"
	}
	return s
}

func policyRow(t *testing.T, mp, dp, state string, obs ExternalObservation, deleting bool, label func(string) string) []string {
	t.Helper()
	sid, err := shortid.New(1, shortid.DefaultABC, 2342)
	require.NoError(t, err)
	opts := createTestOptions()
	rec := &mockEventRecorder{}
	throttled := &mockEventRecorder{}
	opts.Recorder, opts.ThrottledRecorder = rec, throttled
	cli := &observingClient{obs: obs}
	ctrl, err := New(sid, opts)
	require.NoError(t, err)
	ctrl.externalClient = cli

	name := fmt.Sprintf("p-%s-%s-%s", label(mp), label(dp), state)
	obj := createTestUnstructured(name, opts.Namespace)
	ann := map[string]string{}
	if mp != "" {
		ann[meta.AnnotationKeyManagementPolicy] = mp
	}
	if dp != "" {
		ann[meta.AnnotationKeyDeletionPolicy] = dp
	}
	obj.SetAnnotations(ann)
	// A second finalizer keeps a deleting object present after ours is removed (the fake client
	// does not reap), so "finalizer-removed" is readable on every row.
	obj.SetFinalizers([]string{finalizerName, "test.example.org/hold"})
	res := opts.Client.Resource(opts.GVR).Namespace(opts.Namespace)
	created, err := res.Create(context.TODO(), obj, metav1.CreateOptions{})
	require.NoError(t, err)
	evType := ctrlevent.Observe
	if deleting {
		now := metav1.Now()
		created.SetDeletionTimestamp(&now)
		created, err = res.Update(context.TODO(), created, metav1.UpdateOptions{})
		require.NoError(t, err)
		require.True(t, meta.WasDeleted(created), "premise: the deleting rows carry a deletionTimestamp")
		evType = ctrlevent.Delete
	}
	ref := objectref.ObjectRef{APIVersion: created.GetAPIVersion(), Kind: created.GetKind(), Name: created.GetName(), Namespace: created.GetNamespace()}

	// The pass, then its follow-ups (bounded: a follow-up that queues another is a loop to report,
	// not to chase).
	var errs []string
	// "resync": no requeue; the informer resync is the next pass (provider-runtime writes after=<poll>).
	requeue := "resync"
	pending := []ctrlevent.EventType{evType}
	for hop := 0; len(pending) > 0 && hop < 3; hop++ {
		ev := ctrlevent.Event{EventType: pending[0], ObjectRef: ref}
		pending = pending[1:]
		if perr := ctrl.processItem(context.TODO(), ev); perr != nil {
			var ra *requeueAfter
			if errors.As(perr, &ra) {
				requeue = "after=" + ra.after.String()
			} else {
				errs = append(errs, perr.Error())
				requeue = "rate-limited"
			}
		}
		for _, next := range []ctrlevent.EventType{ctrlevent.Create, ctrlevent.Update, ctrlevent.Delete, ctrlevent.Observe} {
			d := ctrlevent.DigestForEvent(ctrlevent.Event{EventType: next, ObjectRef: ref})
			if _, ok := ctrl.items.LoadAndDelete(d); ok {
				pending = append(pending, next)
			}
		}
	}
	if len(pending) > 0 {
		errs = append(errs, fmt.Sprintf("follow-ups still queued after 3 hops: %v", pending))
	}

	cur, err := res.Get(context.TODO(), name, metav1.GetOptions{})
	require.NoError(t, err)
	var ready, synced metav1.Condition
	for _, c := range unstructuredtools.GetConditions(cur) {
		switch c.Type {
		case "Ready":
			ready = c
		case "Synced":
			synced = c
		}
	}
	var events []string
	for _, r := range []*mockEventRecorder{rec, throttled} {
		r.mu.Lock()
		for _, e := range r.events {
			events = append(events, string(e.Reason))
		}
		r.mu.Unlock()
	}
	slices.Sort(events)

	return []string{
		label(mp), label(dp), state,
		fmt.Sprint(cli.observed), fmt.Sprint(cli.created), fmt.Sprint(cli.updated), fmt.Sprint(cli.deleted),
		fmt.Sprint(!slices.Contains(cur.GetFinalizers(), finalizerName)),
		tsvCell(string(ready.Status)), tsvCell(ready.Reason), tsvCell(ready.Message),
		tsvCell(string(synced.Status)), tsvCell(synced.Reason), tsvCell(synced.Message),
		tsvCell(strings.Join(events, ",")), tsvCell(strings.Join(errs, " | ")), requeue,
	}
}
