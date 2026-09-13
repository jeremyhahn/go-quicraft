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
// portZeroRoots: integrationRoots — the parent module's integration suites and
// contrib/dragonboat's, shared with the leader-resolution gates in
// leader_resolution_test.go — plus test/performance. The gate parses rather
// than compiles, so a module boundary is invisible to it and one
// implementation covers every tree. See the integrationRoots comment for the
// full reasoning.
//
// test/performance came under the gate with #163, when benchutil stopped
// carrying a second, TCP-shaped copy of the reservation window and started
// delegating to test/netport. Until then the rule could not be satisfied there
// and a gate over it would have been a rule with a hole in it.
//
// # What is NOT scanned, and why
//
//   - pkg/. A transport unit test binds "127.0.0.1:0" through Config.
//     ListenAddress and the transport keeps that socket for its lifetime;
//     nothing is closed and handed on, so the class cannot occur.
//
// # The marker
//
// This gate used to argue it needed none: netport.MustReserveUDP is strictly
// better at the same call site, so there was nothing to excuse. That held while
// only the integration trees were scanned. It does not hold in
// test/performance, which has a harness that binds port 0 and KEEPS the
// listener for the transport's lifetime — the pkg/ shape, in a scanned tree.
// netport is not better there; it is not applicable, because nothing is closed
// and handed on.
//
// So the gate takes //quicraft:direct-port, on the same terms as the
// leader-resolution markers: it sits on the line that needs it or at the top of
// the comment block above it, it carries its own reason, and a bare marker is
// reported rather than honoured.

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

// portZeroRoots are the trees this gate walks: the two integration trees the
// leader-resolution gates share, plus test/performance, which joined them in
// #163.
//
// It is a separate list rather than an addition to integrationRoots because
// test/performance holds benchmarks, not integration tests: the
// leader-resolution gates have nothing to say about it, and folding it into
// their root list would put every benchmark under three rules it was never
// written against.
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
			// The integration files sit behind build tags, which
			// parser.ParseFile does not evaluate, so every file is scanned
			// regardless of which tags CI is running.
			//
			// ParseComments is REQUIRED, not an optimisation to drop: without
			// it file.Comments is empty, excusedLines finds no markers, and
			// every //quicraft:direct-port comment is silently ignored while
			// the gate keeps reporting the site it excuses.
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
			"before the host binds (#131). Use netport.MustReserveUDP(t.Fatalf) — or " +
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
