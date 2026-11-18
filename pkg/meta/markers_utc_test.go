package meta

import (
	"strings"
	"testing"
	"time"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
)

// Every external-create marker is written in UTC whatever zone the time arrives in (a process with
// TZ set passes local time.Now()), so the stored strings carry no offset. The values still parse
// back to the same instant, and ExternalCreateIncomplete orders them by parsed time.
//
// CONTROL: the input really is in a non-UTC zone (its Format carries "+05:30"), so a pass here is not
// a UTC input passing through unchanged.
func TestExternalCreateMarkersAreWrittenInUTC(t *testing.T) {
	zone := time.FixedZone("IST", 5*3600+1800)
	local := time.Date(2026, 11, 18, 19, 50, 7, 0, zone)
	if !strings.HasSuffix(local.Format(time.RFC3339), "+05:30") {
		t.Fatalf("control: the input is not in a non-UTC zone: %s", local.Format(time.RFC3339))
	}
	for _, m := range []struct {
		key string
		set func(*unstructured.Unstructured, time.Time)
		get func(*unstructured.Unstructured) time.Time
	}{
		{AnnotationKeyExternalCreatePending, func(o *unstructured.Unstructured, t time.Time) { SetExternalCreatePending(o, t) }, func(o *unstructured.Unstructured) time.Time { return GetExternalCreatePending(o) }},
		{AnnotationKeyExternalCreateSucceeded, func(o *unstructured.Unstructured, t time.Time) { SetExternalCreateSucceeded(o, t) }, func(o *unstructured.Unstructured) time.Time { return GetExternalCreateSucceeded(o) }},
		{AnnotationKeyExternalCreateFailed, func(o *unstructured.Unstructured, t time.Time) { SetExternalCreateFailed(o, t) }, func(o *unstructured.Unstructured) time.Time { return GetExternalCreateFailed(o) }},
		{AnnotationKeyExternalCreateNotRequested, func(o *unstructured.Unstructured, t time.Time) { SetExternalCreateNotRequested(o, t) }, func(o *unstructured.Unstructured) time.Time { return GetExternalCreateNotRequested(o) }},
	} {
		o := &unstructured.Unstructured{Object: map[string]any{}}
		m.set(o, local)
		if got, want := o.GetAnnotations()[m.key], "2026-11-18T14:20:07Z"; got != want {
			t.Errorf("%s = %q, want %q (UTC, second precision)", m.key, got, want)
		}
		if got := m.get(o); !got.Equal(local) {
			t.Errorf("%s parses back to %v, want the same instant as %v", m.key, got, local)
		}
	}

	// A marker an earlier runtime wrote with an offset still orders by instant: pending at 14:20:07Z
	// written as +05:30 is OLDER than succeeded at 14:20:08Z, though its string sorts after it.
	o := &unstructured.Unstructured{Object: map[string]any{}}
	o.SetAnnotations(map[string]string{
		AnnotationKeyExternalCreatePending:   "2026-11-18T19:50:07+05:30",
		AnnotationKeyExternalCreateSucceeded: "2026-11-18T14:20:08Z",
	})
	if ExternalCreateIncomplete(o) {
		t.Error("an offset-carrying pending older than succeeded reads as incomplete: comparison is by string, not instant")
	}
}
