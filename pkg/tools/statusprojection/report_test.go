package statusprojection

import (
	"context"
	"reflect"
	"testing"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
)

// A partial RESTAction: the `urlStage` stage succeeded, the `bundle` stage failed, so its key is
// absent from .api and only snowplow's errorKey is there. Every field starts with a value an
// earlier pass wrote ("OLD"), which is what a reader sees unless this pass replaces it.
func partialAPI() map[string]any {
	return map[string]any{"api": map[string]any{
		"urlStage": map[string]any{"url": "https://fresh.example"},
		"error":    map[string]any{"code": float64(404)},
	}}
}

func crWithStatus(status map[string]any) *unstructured.Unstructured {
	cr := newCR()
	cr.Object["status"] = status
	return cr
}

// Unrefreshed lists exactly the fields holding a value this pass did not write, per mapping
// outcome, whatever the expression style: frontend's Access card keys on it (a stale URL kept by
// `// empty` must be flagged, a URL written fresh must not).
func TestUnrefreshedListsTheFieldsAStaleValueSurvives(t *testing.T) {
	cases := []struct {
		name    string
		status  map[string]any
		maps    []Mapping
		want    []string
		outcome []Outcome
	}{
		{"a field written fresh is not listed",
			map[string]any{"url": "OLD"},
			[]Mapping{{ForPath: "url", Expression: "${ .api.urlStage.url }"}},
			[]string{}, []Outcome{Written}},
		{"a plain path over a missing key writes null: written, so not listed",
			map[string]any{"synced": "OLD"},
			[]Mapping{{ForPath: "synced", Expression: "${ .api.bundle.synced }"}},
			[]string{}, []Outcome{Written}},
		{"`// empty` over a missing key keeps the old value: listed",
			map[string]any{"synced": "OLD"},
			[]Mapping{{ForPath: "synced", Expression: "${ .api.bundle.synced // empty }"}},
			[]string{"synced"}, []Outcome{NoOutput}},
		{"an iterator over a missing key fails and keeps the old value: listed",
			map[string]any{"items": "OLD"},
			[]Mapping{{ForPath: "items", Expression: "${ .api.bundle.items[] }"}},
			[]string{"items"}, []Outcome{Failed}},
		{"no output and no prior value: nothing stale, not listed",
			map[string]any{},
			[]Mapping{{ForPath: "synced", Expression: "${ .api.bundle.synced // empty }"}},
			[]string{}, []Outcome{NoOutput}},
		{"a nested path keeps its old value: listed by its declared path",
			map[string]any{"network": map[string]any{"host": "OLD"}},
			[]Mapping{{ForPath: "network.host", Expression: "${ .api.bundle.host // empty }"}},
			[]string{"network.host"}, []Outcome{NoOutput}},
		{"two mappings for one path, one writes: refreshed, not listed",
			map[string]any{"url": "OLD"},
			[]Mapping{{ForPath: "url", Expression: "${ .api.bundle.url // empty }"}, {ForPath: "url", Expression: "${ .api.urlStage.url }"}},
			[]string{}, []Outcome{NoOutput, Written}},
		{"several stale paths come back sorted and once each",
			map[string]any{"b": "OLD", "a": "OLD"},
			[]Mapping{{ForPath: "b", Expression: "${ .api.x // empty }"}, {ForPath: "a", Expression: "${ .api.x // empty }"}, {ForPath: "b", Expression: "${ .api.y // empty }"}},
			[]string{"a", "b"}, []Outcome{NoOutput, NoOutput, NoOutput}},
		{"a field holding null is not a value: nothing stale, not listed",
			map[string]any{"synced": nil},
			[]Mapping{{ForPath: "synced", Expression: "${ .api.bundle.synced // empty }"}},
			[]string{}, []Outcome{NoOutput}},
		{"a mapping with no forPath is a failure with no path to list",
			map[string]any{"url": "OLD"},
			[]Mapping{{ForPath: "", Expression: "${ 1 }"}},
			[]string{}, []Outcome{Failed}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			cr := crWithStatus(c.status)
			report, _ := ProjectReport(context.Background(), cr, partialAPI(), c.maps)
			var got []Outcome
			for _, r := range report.Results {
				got = append(got, r.Outcome)
				if (r.Outcome == Failed) != (r.Err != nil) {
					t.Errorf("%s: Outcome %s with Err %v", r.ForPath, r.Outcome, r.Err)
				}
			}
			if !reflect.DeepEqual(got, c.outcome) {
				t.Errorf("outcomes = %v, want %v", got, c.outcome)
			}
			if u := report.Unrefreshed(cr); !reflect.DeepEqual(u, c.want) {
				t.Errorf("Unrefreshed = %#v, want %#v", u, c.want)
			}
		})
	}
}

// The control the cases above rest on: the "OLD" values really do survive a NoOutput or Failed
// mapping, so a listed path names a value that is in fact stale (and a fresh write replaces it).
func TestTheListedValueIsTheOldOne(t *testing.T) {
	cr := crWithStatus(map[string]any{"url": "OLD", "synced": "OLD"})
	_, _ = ProjectReport(context.Background(), cr, partialAPI(), []Mapping{
		{ForPath: "url", Expression: "${ .api.urlStage.url }"},
		{ForPath: "synced", Expression: "${ .api.bundle.synced // empty }"},
	})
	if got := statusOf(t, cr, "url"); got != "https://fresh.example" {
		t.Errorf("url = %v, want the fresh value", got)
	}
	if got := statusOf(t, cr, "synced"); got != "OLD" {
		t.Errorf("synced = %v, want the value an earlier pass left", got)
	}
}

// Project is ProjectReport without the report: same writes, same joined error.
func TestProjectIsProjectReportWithoutTheReport(t *testing.T) {
	maps := []Mapping{{ForPath: "good", Expression: "${ .self.spec.service.host }"}, {ForPath: "bad", Expression: "${ .foo | (( }"}}
	a, b := newCR(), newCR()
	errA := Project(context.Background(), a, nil, maps)
	_, errB := ProjectReport(context.Background(), b, nil, maps)
	if (errA == nil) || (errB == nil) || errA.Error() != errB.Error() {
		t.Fatalf("errors differ: %v vs %v", errA, errB)
	}
	if !reflect.DeepEqual(a.Object, b.Object) {
		t.Fatalf("writes differ:\n%v\n%v", a.Object, b.Object)
	}
}

func TestProjectReportNilCR(t *testing.T) {
	if _, err := ProjectReport(context.Background(), nil, nil, nil); err == nil {
		t.Fatal("nil cr must be an error")
	}
	if u := (Report{Results: []MappingResult{{ForPath: "x", Outcome: NoOutput}}}).Unrefreshed(nil); len(u) != 0 {
		t.Fatalf("Unrefreshed(nil) = %v", u)
	}
}
