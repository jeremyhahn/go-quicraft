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
	"go/ast"
	"go/parser"
	"go/token"
	"io/fs"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"testing"
)

// The port-0 reservation rule: a test node's listen port comes from a held reservation, never a released probe.

// listenFuncs are the calls that bind a local socket from an address argument.
// The value is the index of that argument.
var listenFuncs = map[string]int{
	"Listen":       1,
	"ListenPacket": 1,
	"ListenUDP":    1,
	"ListenTCP":    1,
}

// portZeroSite is one call that asks the kernel to choose a port.
type portZeroSite struct {
	// file is the repository-relative, slash-separated path, so a site keys
	// the same way from either integration root.
	file string
	call string
	addr string
	pos  string
}

// directPortMarker opts one call site out of
// TestNoPortZeroReservationsInIntegrationTrees. See "The marker" above.
const directPortMarker = "//quicraft:direct-port"

// portZeroRoots are the trees this gate walks: the two integration trees and test/performance.
var portZeroRoots = append(append([]integrationRoot{}, integrationRoots...),
	integrationRoot{dir: filepath.Join("..", "performance"), minFiles: 10},
)

// TestNoPortZeroReservationsInIntegrationTrees fails when a cluster address is
// taken from the kernel's ephemeral range instead of reserved through
// test/netport.
func TestNoPortZeroReservationsInIntegrationTrees(t *testing.T) {
	var (
		sites   []portZeroSite
		bare    []string
		scanned int
	)

	for _, root := range portZeroRoots {
		rootScanned := 0
		err := filepath.WalkDir(root.dir, func(path string, d fs.DirEntry, err error) error {
			if err != nil {
				return err
			}
			if d.IsDir() || !strings.HasSuffix(path, ".go") {
				return nil
			}

			fset := token.NewFileSet()
			// Build tags are not evaluated, so every file is scanned.
			file, perr := parser.ParseFile(fset, path, nil, parser.ParseComments)
			if perr != nil {
				return perr
			}
			rootScanned++

			rel, rerr := repoRelative(path)
			if rerr != nil {
				return rerr
			}

			excused, fileBare := excusedLines(fset, file, directPortMarker)
			bare = append(bare, fileBare...)

			ast.Inspect(file, func(n ast.Node) bool {
				call, ok := n.(*ast.CallExpr)
				if !ok {
					return true
				}
				sel, ok := call.Fun.(*ast.SelectorExpr)
				if !ok {
					return true
				}
				argIdx, gated := listenFuncs[sel.Sel.Name]
				if !gated || argIdx >= len(call.Args) {
					return true
				}
				addr, ok := stringLiteral(call.Args[argIdx])
				if !ok || !isPortZero(addr) {
					return true
				}
				if excused[fset.Position(call.Pos()).Line] {
					return true
				}
				sites = append(sites, portZeroSite{
					file: rel,
					call: exprText(sel),
					addr: addr,
					pos:  fset.Position(call.Pos()).String(),
				})
				return true
			})
			return nil
		})
		if err != nil {
			t.Fatalf("walking %s: %v", root.dir, err)
		}
		// A root that suddenly holds almost nothing means the tree moved and
		// the gate is passing over an empty set, which is the failure mode a
		// source-scanning gate has to rule out explicitly.
		if rootScanned < root.minFiles {
			t.Fatalf("scanned only %d files under %s, expected at least %d; "+
				"the tree moved and this gate is checking nothing",
				rootScanned, root.dir, root.minFiles)
		}
		scanned += rootScanned
	}

	reportBareMarkers(t, directPortMarker, bare)

	if len(sites) > 0 {
		sort.Slice(sites, func(i, j int) bool { return sites[i].pos < sites[j].pos })
		var b strings.Builder
		b.WriteString("cluster listen addresses must be reserved through " +
			"test/netport, not taken from the kernel's ephemeral range:\n")
		for _, s := range sites {
			b.WriteString("\t" + s.pos + ": " + s.call + "(..., " + strconv.Quote(s.addr) + ")\n")
		}
		b.WriteString("\nA port-0 bind draws from /proc/sys/net/ipv4/ip_local_port_range, " +
			"the same pool the kernel assigns to the next port-0 bind and to the local " +
			"end of every outbound connection. Closing the socket and returning the " +
			"address leaves nothing holding the port, so another caller can be given it " +
			"before the host binds. Use netport.MustReserveUDP(t.Fatalf) — or " +
			"netport.MustReserveTCP for a TCP harness — which allocates outside that " +
			"range.\n\nIf the socket is KEPT for the listener's lifetime rather than " +
			"closed and handed on, the class cannot occur and netport is not " +
			"applicable: say so with a " + directPortMarker + " comment carrying that " +
			"reason.")
		t.Fatal(b.String())
	}

	t.Logf("checked %d files across %d trees for port-0 reservations",
		scanned, len(portZeroRoots))
}

// stringLiteral reports the value of a basic string literal expression.
func stringLiteral(e ast.Expr) (string, bool) {
	lit, ok := e.(*ast.BasicLit)
	if !ok || lit.Kind != token.STRING {
		return "", false
	}
	v, err := strconv.Unquote(lit.Value)
	if err != nil {
		return "", false
	}
	return v, true
}

// isPortZero reports whether addr names port 0, in any of the spellings a
// listen address can take. A literal naming a concrete port is not matched.
func isPortZero(addr string) bool {
	i := strings.LastIndex(addr, ":")
	if i < 0 {
		return false
	}
	port := addr[i+1:]
	// Reject a bare IPv6 host with no port ("::1"), where the last colon is
	// part of the address rather than a separator.
	if strings.Contains(addr[:i], ":") && !strings.HasPrefix(addr, "[") {
		return false
	}
	n, err := strconv.Atoi(port)
	return err == nil && n == 0
}

// exprText renders a selector as it is written, for the failure message.
func exprText(sel *ast.SelectorExpr) string {
	if ident, ok := sel.X.(*ast.Ident); ok {
		return ident.Name + "." + sel.Sel.Name
	}
	return sel.Sel.Name
}
