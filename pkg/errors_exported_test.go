// Copyright 2026 Jeremy Hahn
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package quicraft

import (
	"go/ast"
	"go/parser"
	"go/token"
	"io/fs"
	"path/filepath"
	"sort"
	"strings"
	"testing"
)

// unreferencedExportedErrors lists exported Err* values in errors.go that
// nothing in pkg returns, each with a one-line reason. An entry is dead API
// to wire or remove before v1.0.0. The test fails when an entry becomes
// live or is no longer declared, so the list cannot go stale.
var unreferencedExportedErrors = map[string]string{
	"ErrShardNotStarted": "listed in temporaryErrors, but every registered-but-not-started path returns ErrShardNotReady; returning this from one of them would split one state over two errors",
}

// exportedErrorDecl is one exported Err* value declared in errors.go.
type exportedErrorDecl struct {
	name   string
	pos    token.Position
	hasDoc bool
	// alias is the identifier the value is initialised from when it
	// re-exports another sentinel (`ErrX = fsutil.ErrX` or `ErrX = ErrY`),
	// else empty. aliasExternal is set when alias names another package's
	// value.
	alias         string
	aliasExternal bool
}

// exportedErrorDecls returns every exported Err* value declared at the top
// level of f, with whether it carries a doc comment, and the identifiers
// inside f's top-level var declarations, which do not count as uses.
func exportedErrorDecls(fset *token.FileSet, f *ast.File) ([]exportedErrorDecl, map[*ast.Ident]bool) {
	var decls []exportedErrorDecl
	inVarDecl := make(map[*ast.Ident]bool)
	for _, d := range f.Decls {
		gd, ok := d.(*ast.GenDecl)
		if !ok || gd.Tok != token.VAR {
			continue
		}
		ast.Inspect(gd, func(n ast.Node) bool {
			if id, ok := n.(*ast.Ident); ok {
				inVarDecl[id] = true
			}
			return true
		})
		for _, s := range gd.Specs {
			vs := s.(*ast.ValueSpec)
			doc := vs.Doc
			if doc == nil && !gd.Lparen.IsValid() {
				doc = gd.Doc
			}
			for i, n := range vs.Names {
				if !n.IsExported() || !strings.HasPrefix(n.Name, "Err") {
					continue
				}
				d := exportedErrorDecl{
					name:   n.Name,
					pos:    fset.Position(n.Pos()),
					hasDoc: doc != nil && strings.TrimSpace(doc.Text()) != "",
				}
				if i < len(vs.Values) {
					switch v := vs.Values[i].(type) {
					case *ast.Ident:
						d.alias = v.Name
					case *ast.SelectorExpr:
						if _, ok := v.X.(*ast.Ident); ok {
							d.alias, d.aliasExternal = v.Sel.Name, true
						}
					}
				}
				decls = append(decls, d)
			}
		}
	}
	return decls, inVarDecl
}

// usedIdentifiers returns the names of the identifiers used in the non-test
// .go files of the pkg tree rooted at the working directory (tree) and in
// package quicraft itself (local). Declared names (ValueSpec names) and the
// identifiers inside errors.go's var declarations are not uses. It also
// returns the exported Err* declarations of errors.go.
func usedIdentifiers(t *testing.T) (tree, local map[string]bool, decls []exportedErrorDecl) {
	t.Helper()
	tree = make(map[string]bool)
	local = make(map[string]bool)
	fset := token.NewFileSet()
	err := filepath.WalkDir(".", func(p string, de fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if de.IsDir() {
			if name := de.Name(); p != "." && (name == "testdata" || strings.HasPrefix(name, ".")) {
				return filepath.SkipDir
			}
			return nil
		}
		if !strings.HasSuffix(p, ".go") || strings.HasSuffix(p, "_test.go") {
			return nil
		}
		f, err := parser.ParseFile(fset, p, nil, parser.ParseComments|parser.SkipObjectResolution)
		if err != nil {
			return err
		}
		isLocal := filepath.Dir(p) == "." && f.Name.Name == "quicraft"
		skip := make(map[*ast.Ident]bool)
		if isLocal && p == "errors.go" {
			decls, skip = exportedErrorDecls(fset, f)
		}
		ast.Inspect(f, func(n ast.Node) bool {
			switch n := n.(type) {
			case *ast.ValueSpec:
				for _, id := range n.Names {
					skip[id] = true
				}
			case *ast.Ident:
				if !skip[n] {
					tree[n.Name] = true
					if isLocal {
						local[n.Name] = true
					}
				}
			}
			return true
		})
		return nil
	})
	if err != nil {
		t.Fatalf("walk: %v", err)
	}
	return tree, local, decls
}

// TestExportedErrors_DocumentedAndReferenced lists every exported Err* value
// in errors.go by parsing the file (no hand-written list to drift) and fails
// unless each one has a doc comment and is live, or is allowlisted in
// unreferencedExportedErrors with a reason. A value is live when a non-test
// file of package quicraft references it outside the var declarations of
// errors.go (returned, wrapped, or matched by a typed error's Is method), or
// when it re-exports a sentinel that a non-test file of the pkg tree
// references outside its declaration: callers match such a value with
// errors.Is against what the lower package returns. The re-export check is
// by name, so it is an approximation where two packages share a name.
func TestExportedErrors_DocumentedAndReferenced(t *testing.T) {
	tree, local, decls := usedIdentifiers(t)
	if len(decls) == 0 {
		t.Fatal("no exported Err* values found in errors.go")
	}
	byName := make(map[string]exportedErrorDecl, len(decls))
	for _, d := range decls {
		byName[d.name] = d
	}
	var live func(name string, depth int) bool
	live = func(name string, depth int) bool {
		if local[name] {
			return true
		}
		d, ok := byName[name]
		if !ok || d.alias == "" || depth > len(decls) {
			return false
		}
		if d.aliasExternal {
			return tree[d.alias]
		}
		return live(d.alias, depth+1)
	}

	var unreferenced, stale []string
	for _, d := range decls {
		if !d.hasDoc {
			t.Errorf("%s: exported error %s has no doc comment", d.pos, d.name)
		}
		_, allowed := unreferencedExportedErrors[d.name]
		isLive := live(d.name, 0)
		switch {
		case !isLive && !allowed:
			unreferenced = append(unreferenced, d.name)
		case isLive && allowed:
			stale = append(stale, d.name+" is now live")
		}
	}
	for name, reason := range unreferencedExportedErrors {
		if _, ok := byName[name]; !ok {
			stale = append(stale, name+" is not declared in errors.go")
		}
		if strings.TrimSpace(reason) == "" {
			t.Errorf("unreferencedExportedErrors[%s] has no reason", name)
		}
	}
	sort.Strings(unreferenced)
	sort.Strings(stale)
	for _, name := range unreferenced {
		t.Errorf("exported error %s is returned by nothing in pkg; return it or allowlist it with a reason", name)
	}
	for _, s := range stale {
		t.Errorf("stale unreferencedExportedErrors entry: %s", s)
	}
}
