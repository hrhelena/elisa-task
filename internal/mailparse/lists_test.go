package mailparse

import (
	"go/ast"
	"go/parser"
	"go/token"
	"maps"
	"os"
	"slices"
	"strconv"
	"strings"
	"testing"
)

// declaredConsts returns the string values of every constant of the named
// type declared in this package's non-test source files. It parses them, so
// a new constant is caught without updating any table.
func declaredConsts(t *testing.T, typeName string) []string {
	t.Helper()
	entries, err := os.ReadDir(".")
	if err != nil {
		t.Fatal(err)
	}
	fset := token.NewFileSet()
	var values []string
	for _, e := range entries {
		name := e.Name()
		if e.IsDir() || !strings.HasSuffix(name, ".go") || strings.HasSuffix(name, "_test.go") {
			continue
		}
		f, err := parser.ParseFile(fset, name, nil, 0)
		if err != nil {
			t.Fatal(err)
		}
		ast.Inspect(f, func(n ast.Node) bool {
			vs, ok := n.(*ast.ValueSpec)
			if !ok {
				return true
			}
			if id, ok := vs.Type.(*ast.Ident); !ok || id.Name != typeName {
				return false
			}
			for _, v := range vs.Values {
				lit, ok := v.(*ast.BasicLit)
				if !ok {
					t.Fatalf("%s constant with a non-literal value at %s", typeName, fset.Position(v.Pos()))
				}
				s, err := strconv.Unquote(lit.Value)
				if err != nil {
					t.Fatal(err)
				}
				values = append(values, s)
			}
			return false
		})
	}
	if len(values) == 0 {
		t.Fatalf("no %s constants found; the scan is broken", typeName)
	}
	return values
}

// checkList fails unless listed holds every declared value exactly once.
func checkList(t *testing.T, name string, declared, listed []string) {
	t.Helper()
	set := map[string]bool{}
	for _, v := range listed {
		if set[v] {
			t.Errorf("%s lists %q twice", name, v)
		}
		set[v] = true
	}
	slices.Sort(declared)
	if got := slices.Sorted(maps.Keys(set)); !slices.Equal(got, declared) {
		t.Errorf("%s = %v, want every declared constant %v", name, got, declared)
	}
}

// AllIssues and AllSkipReasons list every declared constant exactly once,
// so the /status key lists built from them cannot drift (#8).
func TestAllListsAreComplete(t *testing.T) {
	t.Parallel()
	var issues, reasons []string
	for _, v := range AllIssues() {
		issues = append(issues, string(v))
	}
	for _, v := range AllSkipReasons() {
		reasons = append(reasons, string(v))
	}
	checkList(t, "AllIssues", declaredConsts(t, "Issue"), issues)
	checkList(t, "AllSkipReasons", declaredConsts(t, "SkipReason"), reasons)
}
