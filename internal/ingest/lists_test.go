package ingest

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

	"github.com/hrhelena/elisa-task/internal/mailparse"
)

// engineSkipConsts returns the values of the engine's own skip reasons: every
// string constant named Skip* in this package's non-test source files. It
// parses them, so a new constant is caught without updating any table.
func engineSkipConsts(t *testing.T) []string {
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
		for _, decl := range f.Decls {
			gd, ok := decl.(*ast.GenDecl)
			if !ok || gd.Tok != token.CONST {
				continue
			}
			for _, spec := range gd.Specs {
				vs, ok := spec.(*ast.ValueSpec)
				if !ok {
					continue
				}
				for i, id := range vs.Names {
					if !strings.HasPrefix(id.Name, "Skip") {
						continue
					}
					lit, ok := vs.Values[i].(*ast.BasicLit)
					if !ok || lit.Kind != token.STRING {
						t.Fatalf("skip reason %s is not a string literal", id.Name)
					}
					s, err := strconv.Unquote(lit.Value)
					if err != nil {
						t.Fatal(err)
					}
					values = append(values, s)
				}
			}
		}
	}
	if len(values) == 0 {
		t.Fatal("no Skip* constants found; the scan is broken")
	}
	return values
}

// AllSkipReasons lists mailparse's skip reasons and every engine Skip*
// constant exactly once, so the /status key list built from it cannot
// drift (#8).
func TestAllSkipReasonsIsComplete(t *testing.T) {
	t.Parallel()
	want := engineSkipConsts(t)
	for _, r := range mailparse.AllSkipReasons() {
		want = append(want, string(r))
	}
	slices.Sort(want)

	set := map[string]bool{}
	for _, v := range AllSkipReasons() {
		if set[v] {
			t.Errorf("AllSkipReasons lists %q twice", v)
		}
		set[v] = true
	}
	if got := slices.Sorted(maps.Keys(set)); !slices.Equal(got, want) {
		t.Errorf("AllSkipReasons = %v, want %v", got, want)
	}
}
