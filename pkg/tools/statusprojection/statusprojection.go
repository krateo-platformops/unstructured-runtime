// Package statusprojection projects values onto an unstructured CR's .status by evaluating
// declarative ${ jq } expressions over a combined "source root", reusing plumbing/jqutil
// (the same engine snowplow uses for widgetDataTemplate).
//
// It is the shared runtime half of Krateo's "status projection" design: a provider
// (composition-dynamic-controller, rest-dynamic-controller) supplies the CR, the resolved
// source documents, and a list of mappings; this package evaluates and writes status. It is
// pure — it performs no client calls; any I/O-bound source (e.g. RESTAction results) is
// resolved by the caller and passed in via `resolved`.
package statusprojection

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"

	"github.com/krateo-platformops/plumbing/jqutil"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
)

// Mapping mirrors snowplow's widgetDataTemplate item (decoupled from any provider CRD):
// write the (typed) result of Expression to .status at ForPath. The json tags are the wire
// format used to ship mappings between components (e.g. core-provider → the CDC ConfigMap).
type Mapping struct {
	// ForPath is the dotted path under .status to write, e.g. "endpoint" or "network.host".
	ForPath string `json:"forPath"`
	// Expression is a ${ jq } program evaluated over the source root. A bare literal (no
	// ${ } wrapper) is used verbatim. A bare path like ".self.spec.host" is the trivial copy.
	Expression string `json:"expression"`
}

// Project evaluates each mapping's Expression over the combined source root and writes the
// typed, normalized result into cr's .status at ForPath.
//
// The root is `resolved` plus the built-ins derived from cr: "self" (the whole object),
// "spec" and "status" (sugar for self.spec / self.status). Callers typically pass
// resolved={"helm": ..., "api": ...}. Numbers/objects/arrays are normalized to
// DeepCopyJSONValue-safe types before writing (jqutil.InferType can return int32, which
// unstructured.SetNestedField panics on).
//
// Errors are per-mapping and aggregated: a failing mapping degrades that one field; the
// others still apply. Returns a joined error (nil if all succeeded). ProjectReport is the same
// pass, also saying what each mapping did.
func Project(ctx context.Context, cr *unstructured.Unstructured, resolved map[string]any, mappings []Mapping) error {
	_, err := ProjectReport(ctx, cr, resolved, mappings)
	return err
}

// Outcome is what one mapping did to its field in one projection pass.
type Outcome string

const (
	// Written: the expression produced a value and it was written. A null counts: it was
	// written, and the apiserver drops a non-nullable null, so the field reads as absent.
	Written Outcome = "Written"
	// NoOutput: the expression produced no output (`// empty`, an empty iterator), so the field
	// was left exactly as it was, including any value an earlier pass wrote.
	NoOutput Outcome = "NoOutput"
	// Failed: evaluating or writing failed, so the field was left exactly as it was.
	Failed Outcome = "Failed"
)

// MappingResult is one mapping's outcome. Err is set when Outcome is Failed.
type MappingResult struct {
	ForPath string
	Outcome Outcome
	Err     error
}

// Report is what a projection pass did, one result per mapping in mapping order.
type Report struct {
	Results []MappingResult
}

// Unrefreshed returns the status paths (ForPath, as declared) whose field HOLDS a value in cr
// that this pass did not write: no mapping for the path was Written, and status.<path> is
// present and not null. Such a value is whatever an earlier pass left there, which a reader
// cannot tell from a fresh one. A path with no value is not listed: there is nothing stale to
// mistake for fresh. Sorted and de-duplicated, so the same pass gives the same list.
//
// Call it on the cr the pass wrote to, after ProjectReport.
func (r Report) Unrefreshed(cr *unstructured.Unstructured) []string {
	written := map[string]bool{}
	for _, res := range r.Results {
		if res.Outcome == Written {
			written[res.ForPath] = true
		}
	}
	seen := map[string]bool{}
	out := []string{}
	for _, res := range r.Results {
		p := res.ForPath
		if p == "" || written[p] || seen[p] {
			continue
		}
		seen[p] = true
		if cr == nil {
			continue
		}
		v, found, err := unstructured.NestedFieldNoCopy(cr.Object, append([]string{"status"}, strings.Split(p, ".")...)...)
		if err != nil || !found || v == nil {
			continue
		}
		out = append(out, p)
	}
	sort.Strings(out)
	return out
}

// ProjectReport is Project, also reporting each mapping's Outcome. The error is the same joined
// per-mapping error Project returns.
func ProjectReport(ctx context.Context, cr *unstructured.Unstructured, resolved map[string]any, mappings []Mapping) (Report, error) {
	if cr == nil {
		return Report{}, fmt.Errorf("nil cr")
	}
	root := buildRoot(cr, resolved)

	report := Report{Results: make([]MappingResult, 0, len(mappings))}
	var errs []error
	fail := func(path string, err error) {
		errs = append(errs, err)
		report.Results = append(report.Results, MappingResult{ForPath: path, Outcome: Failed, Err: err})
	}
	for _, m := range mappings {
		if m.ForPath == "" {
			fail("", fmt.Errorf("mapping with empty forPath"))
			continue
		}
		// gojq normalizes numbers in its input map IN PLACE, so never hand it the live
		// data: evaluate against a fresh deep copy each time (also isolates mappings from
		// one another and keeps cr / resolved untouched apart from the status writes).
		// safeDeepCopy guards against caller-supplied `resolved` sources that contain
		// non-JSON-safe values (runtime.DeepCopyJSONValue panics on those); a bad source
		// degrades to a per-mapping error rather than crashing the worker goroutine.
		dataCopy, err := safeDeepCopy(root)
		if err != nil {
			fail(m.ForPath, fmt.Errorf("forPath %q: %w", m.ForPath, err))
			continue
		}
		val, write, err := evalOne(ctx, m.Expression, dataCopy)
		if err != nil {
			fail(m.ForPath, fmt.Errorf("forPath %q: %w", m.ForPath, err))
			continue
		}
		if !write {
			// The expression produced no output (e.g. iterating an empty array); leave the
			// field unset rather than writing it.
			report.Results = append(report.Results, MappingResult{ForPath: m.ForPath, Outcome: NoOutput})
			continue
		}
		if err := setStatusField(cr, m.ForPath, val); err != nil {
			fail(m.ForPath, fmt.Errorf("forPath %q: %w", m.ForPath, err))
			continue
		}
		report.Results = append(report.Results, MappingResult{ForPath: m.ForPath, Outcome: Written})
	}
	return report, errors.Join(errs...)
}

// safeDeepCopy wraps runtime.DeepCopyJSONValue, which PANICS if v contains any value that is
// not a JSON-native type (e.g. a plain int/int32, a typed struct, map[string]string, or a
// channel). Caller-supplied `resolved` sources are not guaranteed to be JSON-safe, so a bad
// value is recovered and returned as an error rather than allowed to crash the goroutine.
func safeDeepCopy(v any) (out any, err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("non-JSON-safe source data: %v", r)
		}
	}()
	return runtime.DeepCopyJSONValue(v), nil
}

// SetObservedGeneration writes status.observedGeneration = metadata.generation.
func SetObservedGeneration(cr *unstructured.Unstructured) error {
	if cr == nil {
		return fmt.Errorf("nil cr")
	}
	return unstructured.SetNestedField(cr.Object, cr.GetGeneration(), "status", "observedGeneration")
}

// buildRoot assembles the jq evaluation root: the caller's resolved sources, plus the
// built-in self/spec/status views of the CR.
func buildRoot(cr *unstructured.Unstructured, resolved map[string]any) map[string]any {
	root := make(map[string]any, len(resolved)+3)
	for k, v := range resolved {
		root[k] = v
	}
	root["self"] = cr.Object
	if spec, ok := cr.Object["spec"]; ok {
		root["spec"] = spec
	}
	if status, ok := cr.Object["status"]; ok {
		root["status"] = status
	}
	return root
}

// evalOne resolves a single expression. A ${ jq } expression is evaluated over data; a bare
// literal is taken verbatim. The result string is typed via jqutil.InferType and normalized
// to DeepCopyJSONValue-safe types. The bool reports whether a value should be written: a jq
// program that yields NO output (e.g. iterating an empty array — jqutil.Eval returns the
// empty string, distinct from a "" string result which encodes as `""`) returns false, so
// the engine leaves the field unset rather than coercing it to "" (which would violate a
// non-string schema type and fail the whole status update).
func evalOne(ctx context.Context, expression string, data any) (any, bool, error) {
	if q, ok := jqutil.MaybeQuery(expression); ok {
		out, err := jqutil.Eval(ctx, jqutil.EvalOptions{Query: q, Data: data})
		if err != nil {
			return nil, false, err
		}
		if out == "" {
			return nil, false, nil
		}
		return normalize(jqutil.InferType(out)), true, nil
	}
	return normalize(jqutil.InferType(expression)), true, nil
}

// setStatusField writes val at status.<dotted forPath>, building intermediate objects.
func setStatusField(cr *unstructured.Unstructured, forPath string, val any) error {
	fields := append([]string{"status"}, strings.Split(forPath, ".")...)
	return unstructured.SetNestedField(cr.Object, val, fields...)
}

// normalize converts a value to types accepted by runtime.DeepCopyJSONValue (the strict
// type switch behind unstructured.SetNestedField): map[string]any, []any, string, int64,
// float64, bool, nil. In particular jqutil.InferType returns int32 for small integers and
// may leave json.Number nested inside maps/slices — both are coerced here.
func normalize(v any) any {
	switch x := v.(type) {
	case map[string]any:
		m := make(map[string]any, len(x))
		for k, e := range x {
			m[k] = normalize(e)
		}
		return m
	case []any:
		s := make([]any, len(x))
		for i, e := range x {
			s[i] = normalize(e)
		}
		return s
	case int:
		return int64(x)
	case int32:
		return int64(x)
	case int64, float64, string, bool, nil:
		return x
	case float32:
		return float64(x)
	default:
		// json.Number and any other stringer-ish numeric: prefer int64 then float64.
		if n, ok := v.(interface{ Int64() (int64, error) }); ok {
			if i, err := n.Int64(); err == nil {
				return i
			}
		}
		if n, ok := v.(interface{ Float64() (float64, error) }); ok {
			if f, err := n.Float64(); err == nil {
				return f
			}
		}
		return fmt.Sprintf("%v", x)
	}
}
