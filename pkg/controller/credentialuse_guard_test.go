package controller

import (
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

// controllerDecls parses this package's non-test functions.
func controllerDecls(t *testing.T) (*token.FileSet, []*ast.FuncDecl) {
	t.Helper()
	files, err := filepath.Glob("*.go")
	require.NoError(t, err)
	fset := token.NewFileSet()
	var decls []*ast.FuncDecl
	for _, name := range files {
		if strings.HasSuffix(name, "_test.go") {
			continue
		}
		src, err := os.ReadFile(name)
		require.NoError(t, err)
		f, err := parser.ParseFile(fset, name, src, 0)
		require.NoError(t, err)
		for _, d := range f.Decls {
			if fd, ok := d.(*ast.FuncDecl); ok && fd.Body != nil {
				decls = append(decls, fd)
			}
		}
	}
	require.NotEmpty(t, decls)
	return fset, decls
}

// finalizer writes: a fourth removal of our finalizer must not appear without Forget. Whether each
// site's one write carries both is measured by TestCredentialUse_ForgetInTheFinalizersOwnWrite; this
// keeps the list of sites that test covers complete.
func TestCredentialUse_FinalizerRemovalSites(t *testing.T) {
	_, decls := controllerDecls(t)
	release, remove, set := map[string]int{}, map[string]int{}, map[string]int{}
	for _, fd := range decls {
		ast.Inspect(fd.Body, func(n ast.Node) bool {
			call, ok := n.(*ast.CallExpr)
			if !ok {
				return true
			}
			sel, ok := call.Fun.(*ast.SelectorExpr)
			if !ok {
				return true
			}
			switch sel.Sel.Name {
			case "releaseOwnFinalizer":
				release[fd.Name.Name]++
			case "RemoveFinalizer":
				remove[fd.Name.Name]++
			case "SetFinalizers":
				set[fd.Name.Name]++
			}
			return true
		})
	}
	require.Equal(t, map[string]int{"processItem": 1, "handleDelete": 1, "releaseUnanswerableOnDelete": 1}, release,
		"a new site needs its own arm in TestCredentialUse_ForgetInTheFinalizersOwnWrite")
	require.Equal(t, map[string]int{"releaseOwnFinalizer": 1}, remove, "a finalizer removed outside releaseOwnFinalizer keeps the record")
	require.Equal(t, map[string]int{"processItem": 1}, set, "the one SetFinalizers is the add")
}

// The one Acquire is a top-level statement of processItem, followed by its stop, ahead of the
// rate-limit gate (which asks the handler for a key). TestRateLimitGateIsTheOnlyWayToTheExternalClient
// proves every other external call sits behind that gate and nowhere else, so together they put the
// Acquire ahead of every external call in the package, the informer handlers included.
func TestCredentialUse_AcquireIsAheadOfTheGate(t *testing.T) {
	fset, decls := controllerDecls(t)
	callers := map[string]int{}
	var processItem *ast.FuncDecl
	for _, fd := range decls {
		if fd.Name.Name == "processItem" {
			processItem = fd
		}
		ast.Inspect(fd.Body, func(n ast.Node) bool {
			if call, ok := n.(*ast.CallExpr); ok {
				if m, ok := recvCall(call); ok && m == "acquireCredentialUse" {
					callers[fd.Name.Name]++
				}
			}
			return true
		})
	}
	require.NotNil(t, processItem)
	require.Equal(t, map[string]int{"processItem": 1}, callers)

	stmtCalls := func(s ast.Stmt, name string) bool {
		a, ok := s.(*ast.AssignStmt)
		if !ok || len(a.Rhs) != 1 {
			return false
		}
		call, ok := a.Rhs[0].(*ast.CallExpr)
		if !ok {
			return false
		}
		m, ok := recvCall(call)
		return ok && m == name
	}
	acquireAt, gateAt := -1, -1
	for i, s := range processItem.Body.List {
		if stmtCalls(s, "acquireCredentialUse") {
			acquireAt = i
		}
		if stmtCalls(s, "acquireRateLimit") {
			gateAt = i
		}
	}
	require.GreaterOrEqual(t, acquireAt, 0, "not a top-level statement")
	require.Greater(t, gateAt, acquireAt)

	// The stop: `if hold.stop { return err }`, with nothing between.
	stop, ok := processItem.Body.List[acquireAt+1].(*ast.IfStmt)
	require.True(t, ok, "no stop after the Acquire at %s", fset.Position(processItem.Body.List[acquireAt].Pos()))
	cond, ok := stop.Cond.(*ast.SelectorExpr)
	require.True(t, ok)
	require.Equal(t, "stop", cond.Sel.Name)
	require.Len(t, stop.Body.List, 1)
	_, ok = stop.Body.List[0].(*ast.ReturnStmt)
	require.True(t, ok)
}
