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

package dragonboat_perf

import (
	"go/ast"
	"go/parser"
	"go/token"
	"strings"
	"testing"
)

// TestEveryTolerantLoopIsFloored gates every tolerant loop on the floor.
//
// A benchmark loop that tolerates errors -- `errored++` or `errCount.Add(1)`
// followed by `continue` -- MUST hand its counts to requireFloor, or a run in
// which every operation failed reports zero samples and passes clean. The
// unit tests over proposeFloorVerdict prove the floor's arithmetic; nothing
// proved that each loop actually calls it, and the easy miss is a loop that is
// counted, logged, and never checked.
//
// The gate parses this file rather than running it, so it costs nothing and
// needs no cluster. For every function that increments an error counter, it
// requires both a requireFloor call and a tolerable classification in the
// same function body -- the floor bounds HOW MANY errors, tolerable bounds
// WHICH, and a loop with only one of them is wrong in the other direction.
func TestEveryTolerantLoopIsFloored(t *testing.T) {
	fset := token.NewFileSet()
	file, err := parser.ParseFile(fset, "dragonboat_bench.go", nil, parser.SkipObjectResolution)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}

	var checked int
	for _, decl := range file.Decls {
		fn, ok := decl.(*ast.FuncDecl)
		if !ok || fn.Body == nil {
			continue
		}
		var counts, floors, stops bool
		ast.Inspect(fn.Body, func(n ast.Node) bool {
			switch x := n.(type) {
			case *ast.IncDecStmt:
				if id, ok := x.X.(*ast.Ident); ok && id.Name == "errored" && x.Tok == token.INC {
					counts = true
				}
			case *ast.CallExpr:
				if sel, ok := x.Fun.(*ast.SelectorExpr); ok {
					if id, ok := sel.X.(*ast.Ident); ok && id.Name == "errCount" && sel.Sel.Name == "Add" {
						counts = true
					}
				}
				if id, ok := x.Fun.(*ast.Ident); ok {
					switch id.Name {
					case "requireFloor":
						floors = true
					case "tolerable":
						stops = true
					}
				}
			}
			return true
		})
		if !counts {
			continue
		}
		checked++
		if !floors {
			t.Errorf("%s counts tolerated errors but never calls requireFloor: a run "+
				"in which every operation failed would report no samples and pass "+
				"clean", fn.Name.Name)
		}
		if !stops {
			t.Errorf("%s counts tolerated errors without classifying them through "+
				"tolerable: a PERMANENT error would be counted as weather and the run "+
				"published at up to 20%% failures", fn.Name.Name)
		}
	}
	// The gate must be looking at something, or a rename of the counters
	// would make it pass by checking an empty set.
	if checked < 7 {
		t.Fatalf("found only %d tolerant loop(s); there are seven, so the scan is "+
			"no longer recognising them and this gate is verifying nothing", checked)
	}
	if !strings.Contains(fset.Position(file.Pos()).Filename, "dragonboat_bench.go") {
		t.Fatal("parsed the wrong file")
	}
}
