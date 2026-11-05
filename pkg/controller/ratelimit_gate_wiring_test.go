package controller

import (
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"
)

// WIRING. The design puts ONE gate in processItem rather than one per handler, and that is only
// sound if three structural facts hold. Each is asserted, because each can be broken by an edit
// that every behavioural test above still passes:
//
//  1. the external client is CALLED only from the five handler entry points — a sixth caller
//     would reach the endpoint ungated;
//  2. those entry points are called only from processItem — a second caller would bypass the gate;
//  3. in processItem the gate is a single TOP-LEVEL statement that precedes every entry call, so
//     no path reaches a handler without passing it. (Textual order plus top-level is the claim; it
//     holds for Go without goto or a closure declared above the gate and invoked below it.)
//
// Asserted over the AST of every non-test file in the package, because a grep cannot tell a call
// from a comment or a type assertion (acquireRateLimit asserts c.externalClient's type; worker.go's
// comments name externalClient.Delete).
var handlerEntries = map[string]bool{
	"externalResourceExists": true,
	"handleObserve":          true,
	"handleCreate":           true,
	"handleUpdate":           true,
	"handleDelete":           true,
}

// recvCall reports the method name when call is c.<name>(...) on the Controller receiver.
func recvCall(call *ast.CallExpr) (string, bool) {
	sel, ok := call.Fun.(*ast.SelectorExpr)
	if !ok {
		return "", false
	}
	if id, ok := sel.X.(*ast.Ident); ok && id.Name == "c" {
		return sel.Sel.Name, true
	}
	return "", false
}

// externalClientCall reports whether call is c.externalClient.<M>(...).
func externalClientCall(call *ast.CallExpr) bool {
	sel, ok := call.Fun.(*ast.SelectorExpr)
	if !ok {
		return false
	}
	inner, ok := sel.X.(*ast.SelectorExpr)
	if !ok || inner.Sel.Name != "externalClient" {
		return false
	}
	id, ok := inner.X.(*ast.Ident)
	return ok && id.Name == "c"
}

func TestRateLimitGateIsTheOnlyWayToTheExternalClient(t *testing.T) {
	files, err := filepath.Glob("*.go")
	if err != nil {
		t.Fatal(err)
	}
	fset := token.NewFileSet()
	var decls []*ast.FuncDecl
	for _, name := range files {
		if strings.HasSuffix(name, "_test.go") {
			continue
		}
		src, err := os.ReadFile(name)
		if err != nil {
			t.Fatal(err)
		}
		f, err := parser.ParseFile(fset, name, src, 0)
		if err != nil {
			t.Fatalf("parsing %s: %v", name, err)
		}
		for _, d := range f.Decls {
			if fd, ok := d.(*ast.FuncDecl); ok && fd.Body != nil {
				decls = append(decls, fd)
			}
		}
	}

	var processItem *ast.FuncDecl
	clientCallers := map[string]int{}
	entryCallers := map[string]int{}
	for _, fd := range decls {
		if fd.Name.Name == "processItem" && fd.Recv != nil {
			processItem = fd
		}
		ast.Inspect(fd.Body, func(n ast.Node) bool {
			call, ok := n.(*ast.CallExpr)
			if !ok {
				return true
			}
			if externalClientCall(call) {
				clientCallers[fd.Name.Name]++
			}
			if m, ok := recvCall(call); ok && handlerEntries[m] {
				entryCallers[fd.Name.Name]++
			}
			return true
		})
	}
	if processItem == nil {
		t.Fatal("processItem not found — this test has lost its subject")
	}

	// 1. Denominator first: zero external calls would make the loop below vacuous.
	total := 0
	for fn, n := range clientCallers {
		total += n
		if !handlerEntries[fn] {
			t.Errorf("%s calls the external client directly (%d call(s)). Only the handler entry "+
				"points may, because only they sit behind processItem's rate-limit gate", fn, n)
		}
	}
	if total == 0 {
		t.Fatal("found 0 external-client calls — the matcher is broken, not the code clean")
	}

	// 2.
	for fn, n := range entryCallers {
		if fn != "processItem" {
			t.Errorf("%s calls a handler entry point (%d call(s)), bypassing the gate in processItem", fn, n)
		}
	}

	// 3. The gate is ONE top-level statement, and every entry call comes after it.
	var gates []token.Pos
	for _, stmt := range processItem.Body.List {
		ast.Inspect(stmt, func(n ast.Node) bool {
			if call, ok := n.(*ast.CallExpr); ok {
				if m, ok := recvCall(call); ok && m == "acquireRateLimit" {
					gates = append(gates, stmt.Pos())
				}
			}
			return true
		})
	}
	var nested int
	ast.Inspect(processItem.Body, func(n ast.Node) bool {
		if call, ok := n.(*ast.CallExpr); ok {
			if m, ok := recvCall(call); ok && m == "acquireRateLimit" {
				nested++
			}
		}
		return true
	})
	if len(gates) != 1 || nested != 1 {
		t.Fatalf("processItem calls acquireRateLimit %d time(s), %d as top-level statement(s); want "+
			"exactly 1, top-level. A gate inside a branch leaves the other branch ungated", nested, len(gates))
	}
	gate := gates[0]

	var entries []string
	ast.Inspect(processItem.Body, func(n ast.Node) bool {
		call, ok := n.(*ast.CallExpr)
		if !ok {
			return true
		}
		m, ok := recvCall(call)
		if !ok || !handlerEntries[m] {
			return true
		}
		entries = append(entries, m)
		if call.Pos() < gate {
			t.Errorf("processItem calls %s at %s, BEFORE the rate-limit gate at %s: that call reaches "+
				"a throttled endpoint", m, fset.Position(call.Pos()), fset.Position(gate))
		}
		return true
	})
	sort.Strings(entries)
	t.Logf("%d external-client call(s); %d entry call(s) in processItem, all after the gate: %v",
		total, len(entries), entries)
	// A SET, not a count: a duplicated call would otherwise hide a missing one.
	seen := map[string]bool{}
	for _, e := range entries {
		seen[e] = true
	}
	for m := range handlerEntries {
		if !seen[m] {
			t.Errorf("processItem never calls %s. Either it moved — and its new caller is outside "+
				"this test's view — or the entry set above is stale", m)
		}
	}
}
