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

package dragonboat

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

// dragonboatV4NodeHostMethods is the exported method set of Dragonboat v4's
// NodeHost (v4.0.1). The README table's "Dragonboat" column is checked
// against it.
var dragonboatV4NodeHostMethods = []string{
	"Close", "GetLeaderID", "GetLogReader", "GetNoOPSession",
	"GetNodeHostInfo", "GetNodeHostRegistry", "GetNodeUser", "HasNodeInfo",
	"ID", "NAReadLocalNode", "NodeHostConfig", "Propose", "ProposeSession",
	"QueryRaftLog", "RaftAddress", "ReadIndex", "ReadLocalNode", "RemoveData",
	"RequestAddNonVoting", "RequestAddReplica", "RequestAddWitness",
	"RequestCompaction", "RequestDeleteReplica", "RequestLeaderTransfer",
	"RequestSnapshot", "StaleRead", "StartConcurrentReplica",
	"StartOnDiskReplica", "StartReplica", "StopReplica", "StopShard",
	"SyncCloseSession", "SyncGetSession", "SyncGetShardMembership",
	"SyncPropose", "SyncRead", "SyncRemoveData", "SyncRequestAddNonVoting",
	"SyncRequestAddReplica", "SyncRequestAddWitness",
	"SyncRequestDeleteReplica", "SyncRequestSnapshot",
}

// exportedNodeHostMethods parses the package's non-test Go files and returns
// the exported methods declared on NodeHost or *NodeHost.
func exportedNodeHostMethods(t *testing.T) map[string]bool {
	t.Helper()
	files, err := filepath.Glob("*.go")
	if err != nil {
		t.Fatalf("glob: %v", err)
	}
	fset := token.NewFileSet()
	methods := make(map[string]bool)
	for _, name := range files {
		if strings.HasSuffix(name, "_test.go") {
			continue
		}
		f, err := parser.ParseFile(fset, name, nil, parser.SkipObjectResolution)
		if err != nil {
			t.Fatalf("parse %s: %v", name, err)
		}
		for _, decl := range f.Decls {
			fn, ok := decl.(*ast.FuncDecl)
			if !ok || fn.Recv == nil || len(fn.Recv.List) != 1 || !fn.Name.IsExported() {
				continue
			}
			typ := fn.Recv.List[0].Type
			if star, ok := typ.(*ast.StarExpr); ok {
				typ = star.X
			}
			if id, ok := typ.(*ast.Ident); ok && id.Name == "NodeHost" {
				methods[fn.Name.Name] = true
			}
		}
	}
	return methods
}

// readmeMethodTable parses the "## NodeHost methods" table of README.md and
// returns each row's method name mapped to its "Dragonboat" cell.
func readmeMethodTable(t *testing.T) map[string]string {
	t.Helper()
	data, err := os.ReadFile("README.md")
	if err != nil {
		t.Fatalf("read README.md: %v", err)
	}
	rows := make(map[string]string)
	inSection := false
	for _, line := range strings.Split(string(data), "\n") {
		if strings.HasPrefix(line, "## ") {
			inSection = line == "## NodeHost methods"
			continue
		}
		if !inSection || !strings.HasPrefix(line, "| `") {
			continue
		}
		cells := strings.Split(line, "|")
		if len(cells) < 5 {
			t.Fatalf("README table row has too few cells: %q", line)
		}
		name := strings.Trim(strings.TrimSpace(cells[1]), "`")
		if _, dup := rows[name]; dup {
			t.Errorf("README table lists %s twice", name)
		}
		rows[name] = strings.TrimSpace(cells[2])
	}
	if len(rows) == 0 {
		t.Fatal(`README.md has no rows under "## NodeHost methods"`)
	}
	return rows
}

func sortedKeys(m map[string]bool) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}

// TestREADMEMethodTableMatchesNodeHost fails when the README's NodeHost method
// table drifts from the code: an exported method without a row, a row for a
// method the package does not export, or a "Dragonboat" cell that disagrees
// with Dragonboat v4's method set.
func TestREADMEMethodTableMatchesNodeHost(t *testing.T) {
	methods := exportedNodeHostMethods(t)
	rows := readmeMethodTable(t)
	upstream := make(map[string]bool, len(dragonboatV4NodeHostMethods))
	for _, m := range dragonboatV4NodeHostMethods {
		upstream[m] = true
	}

	for _, m := range sortedKeys(methods) {
		cell, ok := rows[m]
		if !ok {
			t.Errorf("NodeHost.%s is exported but missing from the README method table", m)
			continue
		}
		want := "no"
		if upstream[m] {
			want = "yes"
		}
		if cell != want {
			t.Errorf("README row %s: Dragonboat column is %q, want %q", m, cell, want)
		}
	}
	for name := range rows {
		if !methods[name] {
			t.Errorf("README method table lists %s, which NodeHost does not export", name)
		}
	}
	// The README states the shim omits no Dragonboat v4 method; keep it true.
	for _, m := range dragonboatV4NodeHostMethods {
		if !methods[m] {
			t.Errorf("Dragonboat v4 NodeHost.%s is not provided; list it as omitted in the README", m)
		}
	}
}
