package controller

import (
	"context"
	"errors"
	"testing"

	"github.com/krateo-platformops/unstructured-runtime/pkg/meta"
	"github.com/stretchr/testify/require"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
)

// appearsLaterClient refuses the create with a CLASSIFIED error -- the vendor really did not commit
// it -- and then the resource appears ANYWAY, by a route this controller did not take.
//
// That is not hypothetical. A consumer's Keycloak policy create was refused 404 "Realm not found";
// the realm was then created, Keycloak created its own default policy along with it, and the retry
// 1.7 s later made a second one. 24 of 24 CRs duplicated and then sat unrecoverable at "findby
// matched 2 or more items on the declared identifiers".
type appearsLaterClient struct {
	fakeExternalClient
	creates     int
	appearsFrom int   // the create number after which the resource exists
	existsErr   error // when set, existence cannot be determined
	existsCalls int
}

func (c *appearsLaterClient) Observe(ctx context.Context, mg *unstructured.Unstructured) (ExternalObservation, error) {
	ex := c.creates >= c.appearsFrom
	return ExternalObservation{ResourceExists: ex, ResourceUpToDate: ex}, nil
}

func (c *appearsLaterClient) Exists(ctx context.Context, mg *unstructured.Unstructured) (bool, error) {
	c.existsCalls++
	if c.existsErr != nil {
		return false, c.existsErr
	}
	return c.creates >= c.appearsFrom, nil
}

func (c *appearsLaterClient) Create(ctx context.Context, mg *unstructured.Unstructured) error {
	c.creates++
	return CreateNotCommitted(errors.New("unexpected status: 404"))
}

// THE DEFECT. "Safe to retry blindly" is true about the create and not about the world: the refusal
// was correctly classified, and by the time the retry ran the resource existed.
func TestARefusedCreateThatAppearsMeanwhileIsAdoptedNotDuplicated(t *testing.T) {
	client := &appearsLaterClient{appearsFrom: 1} // exists once one create has been attempted
	got := lostResponseRun(t, client, 4)

	require.Equal(t, 1, client.creates,
		"the refused create was repeated although the resource existed before the retry: the vendor now "+
			"holds %d copies, which is the measured 24-of-24 duplication", client.creates)
	require.False(t, meta.GetExternalCreateSucceeded(got).IsZero(),
		"the existing resource was not adopted, so nothing records that this CR owns it")
	// ORDERING, NOT PRESENCE. The refusal's own failed stamp is history and stays on the object; what
	// decides every later pass is which marker is NEWEST. Asserting failed was absent was asserting on
	// a channel that does not carry the distinction -- it failed here while the behaviour was right.
	require.False(t, meta.ExternalCreateRefusedUnconfirmed(got),
		"the adoption did not supersede the refusal, so the next pass would confirm all over again")
	require.False(t, meta.ExternalCreateIncomplete(got),
		"an adopted resource must not read as an incomplete create")
	require.Positive(t, client.existsCalls, "the negative was never confirmed")
}

// THE INTENT THAT MUST SURVIVE. ErrCreateNotCommitted exists so a classified failure keeps retrying
// directly instead of "waiting out the grace, or stopping where nothing can be observed". When the
// existence check CANNOT answer, this must behave exactly as before: retry, never stop.
//
// Without that arm the fix would trade a duplication for a strand -- a kind with no read verb, or a
// findby that cannot match, would wait forever on a confirmation it can never get.
func TestARefusedCreateWhoseAbsenceCannotBeConfirmedStillRetries(t *testing.T) {
	client := &appearsLaterClient{appearsFrom: 99, existsErr: errors.New("no read verb declared")}
	got := lostResponseRun(t, client, 3)

	require.Equal(t, 3, client.creates,
		"a classified refusal whose absence cannot be confirmed must keep retrying directly, as it did "+
			"before this change; stopping here would strand it")
	require.False(t, meta.GetExternalCreateFailed(got).IsZero(),
		"it must still be recorded as failed, or the next pass reads it as incomplete and waits out the grace")
}

// The ordinary case, unchanged: refused, and the resource genuinely is not there.
func TestARefusedCreateThatIsStillAbsentRetriesAsBefore(t *testing.T) {
	client := &appearsLaterClient{appearsFrom: 99}
	got := lostResponseRun(t, client, 3)

	require.Equal(t, 3, client.creates, "a refused create of a resource that does not exist must retry each pass")
	require.False(t, meta.GetExternalCreateFailed(got).IsZero(), "it must be recorded as failed")
	require.True(t, meta.GetExternalCreateSucceeded(got).IsZero(), "nothing was created, so nothing may be adopted")
}
