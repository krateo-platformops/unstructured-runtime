//go:build envtest

package controller

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/krateo-platformops/plumbing/shortid"
	ctrlevent "github.com/krateo-platformops/unstructured-runtime/pkg/controller/event"
	"github.com/krateo-platformops/unstructured-runtime/pkg/meta"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	apiextensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/kubernetes"
	"sigs.k8s.io/controller-runtime/pkg/envtest"
)

// #110 D6, against a REAL apiserver: the fake dynamic client keeps resourceVersion fixed, so UpdateFunc
// reads every edit there as a periodic resync and cannot measure this. With the policy annotations
// watched (the builder's default since this change), an annotation-only edit of
// krateo.io/management-policy queues an Observe at once. CONTROL, and the measured BEFORE: with the
// previous default watch set (paused, external-create-pending), the same edit queues nothing; it
// waits for the resync, which is set to an hour here.
func TestEnvtest_PolicyAnnotationEditReconcilesAtOnce(t *testing.T) {
	if os.Getenv("KUBEBUILDER_ASSETS") == "" {
		t.Fatal("KUBEBUILDER_ASSETS is not set: this test exists to run against a real apiserver, and a skip would read as a pass")
	}
	preserve := true
	env := &envtest.Environment{CRDs: []*apiextensionsv1.CustomResourceDefinition{{
		ObjectMeta: metav1.ObjectMeta{Name: "tests.test.example.org"},
		Spec: apiextensionsv1.CustomResourceDefinitionSpec{
			Group: "test.example.org",
			Names: apiextensionsv1.CustomResourceDefinitionNames{Plural: "tests", Singular: "test", Kind: "Test", ListKind: "TestList"},
			Scope: apiextensionsv1.NamespaceScoped,
			Versions: []apiextensionsv1.CustomResourceDefinitionVersion{{
				Name: "v1", Served: true, Storage: true,
				Subresources: &apiextensionsv1.CustomResourceSubresources{Status: &apiextensionsv1.CustomResourceSubresourceStatus{}},
				Schema: &apiextensionsv1.CustomResourceValidation{OpenAPIV3Schema: &apiextensionsv1.JSONSchemaProps{
					Type: "object", XPreserveUnknownFields: &preserve,
				}},
			}},
		},
	}}}
	cfg, err := env.Start()
	require.NoError(t, err)
	t.Cleanup(func() { _ = env.Stop() })
	dyn, err := dynamic.NewForConfig(cfg)
	require.NoError(t, err)
	kc, err := kubernetes.NewForConfig(cfg)
	require.NoError(t, err)

	before := ctrlevent.NewAnnotationEvents(
		ctrlevent.AnnotationEvent{EventType: ctrlevent.Observe, Annotation: meta.AnnotationKeyReconciliationPaused, OnAction: ctrlevent.OnAny},
		ctrlevent.AnnotationEvent{EventType: ctrlevent.Create, Annotation: meta.AnnotationKeyExternalCreatePending, OnAction: ctrlevent.OnDelete},
	)
	after := append(ctrlevent.NewAnnotationEvents(before...),
		ctrlevent.AnnotationEvent{EventType: ctrlevent.Observe, Annotation: meta.AnnotationKeyManagementPolicy, OnAction: ctrlevent.OnAny},
		ctrlevent.AnnotationEvent{EventType: ctrlevent.Observe, Annotation: meta.AnnotationKeyDeletionPolicy, OnAction: ctrlevent.OnAny},
	)

	for i, tc := range []struct {
		name       string
		watch      ctrlevent.AnnotationEvents
		annotation string
		wantQueued bool
	}{
		{"watched/management-policy", after, meta.AnnotationKeyManagementPolicy, true},
		{"watched/deletion-policy", after, meta.AnnotationKeyDeletionPolicy, true},
		{"before/management-policy", before, meta.AnnotationKeyManagementPolicy, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ns := "policy-" + string(rune('a'+i))
			_, err := kc.CoreV1().Namespaces().Create(context.TODO(), &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: ns}}, metav1.CreateOptions{})
			require.NoError(t, err)
			sid, err := shortid.New(1, shortid.DefaultABC, 2342)
			require.NoError(t, err)
			opts := createTestOptions()
			opts.Client = dyn
			opts.Namespace = ns
			opts.ResyncInterval = time.Hour
			opts.WatchAnnotations = tc.watch
			c, err := New(sid, opts)
			require.NoError(t, err)
			stop := make(chan struct{})
			defer close(stop)
			go c.informer.Run(stop)

			res := dyn.Resource(opts.GVR).Namespace(ns)
			obj := createTestUnstructured("edited", ns)
			obj.SetResourceVersion("")
			created, err := res.Create(context.TODO(), obj, metav1.CreateOptions{})
			require.NoError(t, err)
			drainUntil(c, time.Now().Add(time.Second)) // the add event

			created.SetAnnotations(map[string]string{tc.annotation: "observe"})
			edited := time.Now()
			updated, err := res.Update(context.TODO(), created, metav1.UpdateOptions{})
			require.NoError(t, err)
			require.NotEqual(t, created.GetResourceVersion(), updated.GetResourceVersion(), "premise: a real apiserver bumps resourceVersion on the edit")

			got, at := waitForEvent(c, ctrlevent.Observe, edited.Add(2*time.Second))
			require.Equal(t, tc.wantQueued, got, "Observe queued within 2s of editing %s", tc.annotation)
			if got {
				t.Logf("%s: Observe queued %s after the edit", tc.name, at.Sub(edited))
				require.Less(t, at.Sub(edited), time.Second)
			}
		})
	}
}

// drainUntil empties the queue until deadline.
func drainUntil(c *Controller, deadline time.Time) {
	for time.Now().Before(deadline) {
		if c.queue.Len() == 0 {
			time.Sleep(20 * time.Millisecond)
			continue
		}
		i, _, _ := c.queue.GetWithPriority()
		if e, ok := i.(ctrlevent.Event); ok {
			c.items.Delete(ctrlevent.DigestForEvent(e))
		}
		c.queue.Done(i)
	}
}

// waitForEvent reports whether an event of type et is queued before deadline, and when.
func waitForEvent(c *Controller, et ctrlevent.EventType, deadline time.Time) (bool, time.Time) {
	for time.Now().Before(deadline) {
		if c.queue.Len() == 0 {
			time.Sleep(10 * time.Millisecond)
			continue
		}
		i, _, _ := c.queue.GetWithPriority()
		c.queue.Done(i)
		if e, ok := i.(ctrlevent.Event); ok && e.EventType == et {
			return true, time.Now()
		}
	}
	return false, time.Time{}
}
