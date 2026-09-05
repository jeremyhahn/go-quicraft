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

// The port-0 reservation rule.
//
// A cluster node in these suites has to be told its listen address before the
// host that binds it exists, so the address has to be chosen by the test. The
// obvious way to choose one is to ask the kernel:
//
//	pc, _ := net.ListenPacket("udp", "127.0.0.1:0")
//	addr := pc.LocalAddr().String()
//	pc.Close()
//
// and it is wrong every time. The port is free when it is read and nothing
// holds it afterwards, so the kernel is free to give the same number to the
// next caller — the next node of the same cluster, or the local end of any
// outbound connection — before the host binds. That is #131, and it is one
// instance of this repository's recurring class: a value that is not pinned,
// treated as if it were.
//
// It had four copies of the helper and a fifth open-coded loop when #131 was
// filed, which is why the remedy is a gate and not five edits. Reservation now
// goes through test/netport, which allocates from a window OUTSIDE
// ip_local_port_range, and this gate is what keeps a sixth copy from appearing.
//
// # The rule
//
// No file in the scanned trees may pass a port-0 address literal to
// net.Listen, net.ListenPacket, net.ListenUDP or ListenConfig.Listen/
// ListenPacket. Reserve through test/netport instead.
//
// A "port-0 address" is any string literal whose port component is 0 — ":0",
// "127.0.0.1:0", "[::1]:0", "localhost:0". A literal naming a CONCRETE port is
// not matched: that is what netport itself does when it probes a candidate,
// and what a test does when it deliberately rebinds an address it already
// holds.
//
// # What is scanned
//
// integrationRoots, shared with the leader-resolution gates in
// leader_resolution_test.go: the parent module's integration suites and
// contrib/dragonboat's. The gate parses rather than compiles, so the module
// boundary is invisible to it and one implementation covers both trees. See
// the integrationRoots comment for the full reasoning.
//
// # What is NOT scanned, and why
//
//   - pkg/. A transport unit test binds "127.0.0.1:0" through Config.
//     ListenAddress and the transport keeps that socket for its lifetime;
//     nothing is closed and handed on, so the class cannot occur.
//   - test/performance/. A separate Go module that cannot import test/netport
//     without a require and a replace directive, so the rule cannot be
//     satisfied there yet. It has its own reservation window already
//     (test/performance/benchutil, the TCP twin of test/netport) and its
//     remaining port-0 call site holds the socket open until the caller binds
//     rather than closing it. Folding that module onto test/netport is #163;
//     until then a gate over it would be a rule with a hole in it, which is
//     worse than a rule with a stated boundary.
//
// # No marker
//
// The leader-resolution gates take a //quicraft:direct-propose style marker
// because a direct proposal is sometimes exactly what a test means. A port-0
// reservation in these trees never is: netport.MustReserveUDP is strictly
// better at the same call site, so there is nothing for a marker to excuse.

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

// TestNoPortZeroReservationsInIntegrationTrees fails when a cluster address is
// taken from the kernel's ephemeral range instead of reserved through
// test/netport.
func TestNoPortZeroReservationsInIntegrationTrees(t *testing.T) {
	var (
		sites   []portZeroSite
		scanned int
	)

	for _, root := range integrationRoots {
		rootScanned := 0
		err := filepath.WalkDir(root.dir, func(path string, d fs.DirEntry, err error) error {
			if err != nil {
				return err
			}
			if d.IsDir() || !strings.HasSuffix(path, ".go") {
				return nil
			}

			fset := token.NewFileSet()
			// The integration files sit behind build tags, which
			// parser.ParseFile does not evaluate, so every file is scanned
			// regardless of which tags CI is running.
			file, perr := parser.ParseFile(fset, path, nil, parser.SkipObjectResolution)
			if perr != nil {
				return perr
			}
			rootScanned++

			rel, rerr := repoRelative(path)
			if rerr != nil {
				return rerr
			}

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
			"before the host binds (#131). Use netport.MustReserveUDP(t.Fatalf), which " +
			"allocates outside that range.")
		t.Fatal(b.String())
	}

	t.Logf("checked %d files across %d integration trees for port-0 reservations",
		scanned, len(integrationRoots))
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
