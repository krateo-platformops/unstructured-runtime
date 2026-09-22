package controller

import (
	"context"
	"errors"
	"testing"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
)

// existenceCheckerClient implements both ExternalClient and ExistenceChecker. Observe always fails,
// standing in for a client whose Observe does convergence work that cannot succeed — the cdc's
// helm reconcile against a composition missing a permission.
type existenceCheckerClient struct {
	fakeExternalClient

	exists     bool
	existsErr  error
	existsCall int
}

func (c *existenceCheckerClient) Exists(ctx context.Context, mg *unstructured.Unstructured) (bool, error) {
	c.existsCall++
	return c.exists, c.existsErr
}

// The regression for the wedge behind core-provider#110/#130.
//
// A client whose Observe fails for convergence reasons must still be able to resolve an incomplete
// create, provided it can answer the narrow existence question. Before this, Observe's error was
// read as "cannot determine the result" and the resource refused forever — so the reconcile that
// would repair the convergence failure never ran again.
func TestExistenceCheckerResolvesWhenObserveCannot(t *testing.T) {
	cli := &existenceCheckerClient{
		fakeExternalClient: fakeExternalClient{
			ObserveErr: errors.New("reconcile: kube update (self-heal apply): cronjobs.batch is forbidden"),
		},
		exists: true,
	}
	c := &Controller{externalClient: cli}

	exists, err := c.externalResourceExists(context.Background(), &unstructured.Unstructured{})
	if err != nil {
		t.Fatalf("Observe's convergence failure must not block resolution, got: %v", err)
	}
	if !exists {
		t.Error("expected the checker's answer (exists=true) to be used")
	}
	if cli.existsCall != 1 {
		t.Errorf("expected Exists to be consulted exactly once, got %d calls", cli.existsCall)
	}
}

// Absence is a real answer too: the create never landed, so recovery may clear the marker and retry.
func TestExistenceCheckerReportsAbsence(t *testing.T) {
	cli := &existenceCheckerClient{
		fakeExternalClient: fakeExternalClient{ObserveErr: errors.New("observe blew up")},
		exists:             false,
	}
	c := &Controller{externalClient: cli}

	exists, err := c.externalResourceExists(context.Background(), &unstructured.Unstructured{})
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	if exists {
		t.Error("expected exists=false")
	}
}

// A checker that genuinely cannot tell must still produce the conservative refusal — we must never
// clear the pending marker on a guess, or we risk leaking a duplicate external resource.
func TestExistenceCheckerErrorStillRefuses(t *testing.T) {
	cli := &existenceCheckerClient{existsErr: errors.New("registry unreachable")}
	c := &Controller{externalClient: cli}

	if _, err := c.externalResourceExists(context.Background(), &unstructured.Unstructured{}); err == nil {
		t.Fatal("a checker error must propagate so recovery keeps refusing")
	}
}

// Purely additive: a client that does not implement ExistenceChecker behaves exactly as before.
func TestFallsBackToObserveWhenNoChecker(t *testing.T) {
	t.Run("observe succeeds", func(t *testing.T) {
		c := &Controller{externalClient: &fakeExternalClient{ObserveExists: true}}

		exists, err := c.externalResourceExists(context.Background(), &unstructured.Unstructured{})
		if err != nil {
			t.Fatalf("expected no error, got %v", err)
		}
		if !exists {
			t.Error("expected Observe's ResourceExists to be used")
		}
	})

	t.Run("observe fails", func(t *testing.T) {
		boom := errors.New("observe failed")
		c := &Controller{externalClient: &fakeExternalClient{ObserveErr: boom}}

		if _, err := c.externalResourceExists(context.Background(), &unstructured.Unstructured{}); !errors.Is(err, boom) {
			t.Fatalf("expected Observe's error to propagate unchanged, got %v", err)
		}
	})
}

// No client at all is not an answer either.
func TestNoExternalClientIsAnError(t *testing.T) {
	c := &Controller{}

	if _, err := c.externalResourceExists(context.Background(), &unstructured.Unstructured{}); err == nil {
		t.Fatal("expected an error when no external client is registered")
	}
}
