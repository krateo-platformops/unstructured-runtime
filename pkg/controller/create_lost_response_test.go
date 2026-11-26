package controller

import (
	"context"
	"errors"
	"testing"

	"github.com/krateo-platformops/plumbing/shortid"
	ctrlevent "github.com/krateo-platformops/unstructured-runtime/pkg/controller/event"
	"github.com/krateo-platformops/unstructured-runtime/pkg/controller/objectref"
	"github.com/krateo-platformops/unstructured-runtime/pkg/meta"
	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
)

// vendorClient is an external system that can COMMIT a create and still fail the call: the response
// is lost after the vendor stored the request (acmp's fake Zendesk: lose_post_response: 500).
type vendorClient struct {
	fakeExternalClient
	commits   bool  // the create reaches the vendor and is stored
	createErr error // ...and the call still returns this
	creates   int
	stored    bool
}

func (v *vendorClient) Observe(ctx context.Context, mg *unstructured.Unstructured) (ExternalObservation, error) {
	return ExternalObservation{ResourceExists: v.stored, ResourceUpToDate: v.stored}, nil
}

func (v *vendorClient) Create(ctx context.Context, mg *unstructured.Unstructured) error {
	v.creates++
	if v.commits {
		v.stored = true
	}
	return v.createErr
}

func lostResponseRun(t *testing.T, client ExternalClient, passes int) *unstructured.Unstructured {
	t.Helper()
	sid, err := shortid.New(1, shortid.DefaultABC, 2342)
	require.NoError(t, err)
	opts := createTestOptions()
	ctrl, err := New(sid, opts)
	require.NoError(t, err)
	ctrl.externalClient = client
	obj := createTestUnstructured("lost-response", opts.Namespace)
	created, err := opts.Client.Resource(opts.GVR).Namespace(opts.Namespace).Create(context.TODO(), obj, metav1.CreateOptions{})
	require.NoError(t, err)
	evt := ctrlevent.Event{EventType: ctrlevent.Create, ObjectRef: objectref.ObjectRef{
		APIVersion: created.GetAPIVersion(), Kind: created.GetKind(), Name: created.GetName(), Namespace: created.GetNamespace()}}
	for i := 0; i < passes; i++ {
		// The worker re-delivers the SAME Create event only while a pass returns an ERROR (handleErr
		// requeues it); a pass that returns nil ends it. That redelivery is the path under test, so
		// it is driven directly here, without the queue's backoff.
		if err := ctrl.processItem(context.TODO(), evt); err == nil {
			break
		}
	}
	got, err := opts.Client.Resource(opts.GVR).Namespace(opts.Namespace).Get(context.TODO(), created.GetName(), metav1.GetOptions{})
	require.NoError(t, err)
	return got
}

// acmp's acceptance test: lose the create's response after the vendor committed, then assert exactly
// ONE create on the wire. On v1.18.0 and v1.21.1 the re-delivered Create created again every pass
// (6 tickets measured), because the failure was stamped external-create-failed and the
// incomplete-create recovery was never asked.
func TestALostCreateResponseCreatesOnce(t *testing.T) {
	client := &vendorClient{commits: true, createErr: errors.New("unexpected status: 500")}
	got := lostResponseRun(t, client, 6)
	require.Equal(t, 1, client.creates, "a create whose response was lost was repeated: the vendor now holds %d copies", client.creates)
	require.False(t, meta.GetExternalCreateSucceeded(got).IsZero(), "the confirmed create was not recorded as succeeded")
}

// The other side of the classification. A create the handler says was NOT committed retries
// directly, as before: confirming a negative it already knows would only delay it.
func TestANotCommittedCreateRetries(t *testing.T) {
	client := &vendorClient{commits: false, createErr: CreateNotCommitted(errors.New("unexpected status: 422"))}
	got := lostResponseRun(t, client, 3)
	require.Equal(t, 3, client.creates, "a create classified as not committed must retry on each pass")
	require.False(t, meta.GetExternalCreateFailed(got).IsZero(), "a not-committed create must be recorded as failed")
}

// An unclassified failure the vendor did NOT commit is not re-created inside the recovery grace
// period: absent is re-observed first, never re-created on the spot.
func TestAnUnclassifiedFailureIsNotRecreatedWithinTheGrace(t *testing.T) {
	client := &vendorClient{commits: false, createErr: errors.New("connection reset by peer")}
	got := lostResponseRun(t, client, 3)
	require.Equal(t, 1, client.creates, "an outcome-unknown create was re-created inside the recovery grace period")
	require.True(t, meta.ExternalCreateIncomplete(got), "the create must stay incomplete (pending newest) until its outcome is confirmed")
}

// The wrapper keeps the cause reachable and marks the classification.
func TestCreateNotCommittedWraps(t *testing.T) {
	cause := errors.New("unexpected status: 422")
	err := CreateNotCommitted(cause)
	require.True(t, errors.Is(err, ErrCreateNotCommitted))
	require.True(t, errors.Is(err, cause))
	require.NoError(t, CreateNotCommitted(nil))
	require.False(t, createCommittedNot(cause), "an unwrapped error must not read as not-committed")
}

// The unclassified arm reports the failure exactly as the classified one does: Ready and Synced carry
// the error. Only the create MARKER differs: pending stays newest, so the next pass confirms first.
func TestAnUnclassifiedFailureCarriesTheDiagnosticToo(t *testing.T) {
	client := &vendorClient{commits: false, createErr: errors.New("dial tcp 10.0.0.9:443: connection reset by peer")}
	got := lostResponseRun(t, client, 1)
	require.True(t, meta.ExternalCreateIncomplete(got), "premise: the unclassified arm leaves the create pending")
	require.NotContains(t, got.GetAnnotations(), meta.AnnotationKeyExternalCreateFailed, "an outcome-unknown create was recorded as failed")
	ready, synced := readyAndSynced(t, got)
	require.Contains(t, synced.Message, "connection reset by peer", "CONTROL: Synced must carry the error")
	require.Contains(t, ready.Message, "connection reset by peer", "Ready must name what failed on the unclassified arm too")
}
