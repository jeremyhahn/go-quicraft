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

package buildgates

import (
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"io/fs"
	"path/filepath"
	"sort"
	"strings"
	"testing"
)

// docCommentRoots are the public trees whose exported API must be documented.
// contrib/dragonboat is a separate module; the gate parses its files directly,
// so it needs neither that module's requirements nor the network.
var docCommentRoots = []struct {
	dir      string
	minFiles int
}{
	{dir: filepath.Join(repoRoot, "pkg"), minFiles: 50},
	{dir: filepath.Join(repoRoot, "contrib", "dragonboat"), minFiles: 10},
}

// docCommentAllowlist names package directories (repository-relative) whose
// missing doc comments the gate does not yet enforce. An entry that no longer
// has anything missing fails the gate, so the list only shrinks.
var docCommentAllowlist = map[string]bool{}

// docGap is one exported identifier or package with no doc comment.
type docGap struct {
	pkgDir string
	file   string
	line   int
	what   string
}

// TestExportedIdentifiersHaveDocComments fails for each exported type,
// function, method on an exported type, constant and variable in a public
// package under pkg/ or contrib/dragonboat/ that carries no doc comment, and
// for each such package with no package comment.
//
// Public means not under an internal/ directory and not a _test.go file. A
// grouped const, var or type block's doc comment counts for its members.
// Build tags are not evaluated: every file is parsed, so a declaration behind
// a tag is held to the same rule.
func TestExportedIdentifiersHaveDocComments(t *testing.T) {
	var gaps []docGap
	scanned := 0
	// pkgDoc records, per package directory, whether any file carries a
	// package comment; pkgFirst keeps a position to report against.
	pkgDoc := map[string]bool{}
	pkgFirst := map[string]docGap{}

	for _, root := range docCommentRoots {
		rootScanned := 0
		err := filepath.WalkDir(root.dir, func(path string, d fs.DirEntry, err error) error {
			if err != nil {
				return err
			}
			if d.IsDir() {
				switch d.Name() {
				case "internal", "testdata", "vendor":
					return filepath.SkipDir
				}
				return nil
			}
			if !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
				return nil
			}
			rel, rerr := repoRelative(path)
			if rerr != nil {
				return rerr
			}
			fset := token.NewFileSet()
			// ParseComments is required: without it every Doc field is nil
			// and the gate reports every identifier.
			file, perr := parser.ParseFile(fset, path, nil, parser.ParseComments)
			if perr != nil {
				return perr
			}
			rootScanned++

			dir := filepath.ToSlash(filepath.Dir(rel))
			if _, seen := pkgDoc[dir]; !seen {
				pkgDoc[dir] = false
				pkgFirst[dir] = docGap{file: rel, line: fset.Position(file.Package).Line}
			}
			if file.Doc != nil {
				pkgDoc[dir] = true
			}
			for _, g := range undocumentedDecls(fset, rel, file) {
				g.pkgDir = dir
				gaps = append(gaps, g)
			}
			return nil
		})
		if err != nil {
			t.Fatalf("walking %s: %v", root.dir, err)
		}
		// A root that suddenly holds almost nothing means the tree moved and
		// the gate is passing over an empty set.
		if rootScanned < root.minFiles {
			t.Fatalf("scanned only %d files under %s, expected at least %d; "+
				"the tree moved and this gate is checking nothing",
				rootScanned, root.dir, root.minFiles)
		}
		scanned += rootScanned
	}

	for dir, has := range pkgDoc {
		if !has {
			first := pkgFirst[dir]
			gaps = append(gaps, docGap{pkgDir: dir, file: first.file, line: first.line, what: "package " + dir})
		}
	}

	var failing []docGap
	allowedHit := map[string]int{}
	for _, g := range gaps {
		if docCommentAllowlist[g.pkgDir] {
			allowedHit[g.pkgDir]++
			continue
		}
		failing = append(failing, g)
	}

	var stale []string
	for dir := range docCommentAllowlist {
		if allowedHit[dir] == 0 {
			stale = append(stale, dir)
		}
	}
	sort.Strings(stale)
	for _, dir := range stale {
		t.Errorf("docCommentAllowlist entry %s has nothing undocumented; remove it", dir)
	}

	if len(failing) > 0 {
		sort.Slice(failing, func(i, j int) bool {
			if failing[i].file != failing[j].file {
				return failing[i].file < failing[j].file
			}
			return failing[i].line < failing[j].line
		})
		var b strings.Builder
		b.WriteString("exported identifiers and packages without a doc comment:\n")
		for _, g := range failing {
			fmt.Fprintf(&b, "\t%s:%d: %s\n", g.file, g.line, g.what)
		}
		b.WriteString("\nAdd a comment directly above each, starting with its name, " +
			"saying what it does.")
		t.Errorf("%d missing doc comments\n%s", len(failing), b.String())
	}

	skipped := 0
	for _, n := range allowedHit {
		skipped += n
	}
	t.Logf("checked %d files; %d gaps in %d allowlisted packages not enforced",
		scanned, skipped, len(allowedHit))
}

// undocumentedDecls returns the exported top-level declarations in file
// (repository-relative path rel) that carry no doc comment.
func undocumentedDecls(fset *token.FileSet, rel string, file *ast.File) []docGap {
	var gaps []docGap
	add := func(pos token.Pos, what string) {
		gaps = append(gaps, docGap{file: rel, line: fset.Position(pos).Line, what: what})
	}
	for _, decl := range file.Decls {
		switch d := decl.(type) {
		case *ast.FuncDecl:
			if !d.Name.IsExported() || d.Doc != nil {
				continue
			}
			if d.Recv == nil {
				add(d.Pos(), "func "+d.Name.Name)
				continue
			}
			recv := receiverTypeName(d.Recv)
			if recv == "" || !ast.IsExported(recv) {
				continue
			}
			add(d.Pos(), "method "+recv+"."+d.Name.Name)
		case *ast.GenDecl:
			if d.Tok == token.IMPORT || d.Doc != nil {
				continue
			}
			for _, spec := range d.Specs {
				switch s := spec.(type) {
				case *ast.TypeSpec:
					if s.Name.IsExported() && s.Doc == nil {
						add(s.Pos(), "type "+s.Name.Name)
					}
				case *ast.ValueSpec:
					if s.Doc != nil {
						continue
					}
					for _, n := range s.Names {
						if n.IsExported() {
							add(n.Pos(), d.Tok.String()+" "+n.Name)
						}
					}
				}
			}
		}
	}
	return gaps
}

// TestUndocumentedDeclsGroupedTypes proves a doc comment on a grouped type
// block does not document the types inside it, while a doc comment on a
// const or var group, or on a standalone type, still covers its names.
func TestUndocumentedDeclsGroupedTypes(t *testing.T) {
	const src = `// Package fixture is a doc comment gate fixture.
package fixture

// Public types.
type (
	Foo struct{}
	Bar struct{}

	// Baz is documented.
	Baz struct{}
)

// Single is a standalone type.
type Single struct{}

// Limits of the fixture.
const (
	MaxA = 1
	MaxB = 2
)

// Defaults of the fixture.
var (
	DefA = 1
	DefB = 2
)
`
	fset := token.NewFileSet()
	file, err := parser.ParseFile(fset, "fixture.go", src, parser.ParseComments)
	if err != nil {
		t.Fatalf("parsing fixture: %v", err)
	}
	var got []string
	for _, g := range undocumentedDecls(fset, "fixture.go", file) {
		got = append(got, g.what)
	}
	want := []string{"type Foo", "type Bar"}
	if strings.Join(got, ",") != strings.Join(want, ",") {
		t.Fatalf("undocumentedDecls reported %q, want %q", got, want)
	}
}

// receiverTypeName returns the base type name of a method receiver, looking
// through pointers and type parameters.
func receiverTypeName(recv *ast.FieldList) string {
	if recv == nil || len(recv.List) == 0 {
		return ""
	}
	expr := recv.List[0].Type
	for {
		switch e := expr.(type) {
		case *ast.StarExpr:
			expr = e.X
		case *ast.IndexExpr:
			expr = e.X
		case *ast.IndexListExpr:
			expr = e.X
		case *ast.ParenExpr:
			expr = e.X
		case *ast.Ident:
			return e.Name
		default:
			return ""
		}
	}
}
