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
	"strings"
	"testing"
)

// directProposeMarker opts one call site out of TestIntegrationProposalsResolveTheLeader.
//
// It is a marker rather than an allowlist in this file on purpose. A list of
// file:line pairs rots on the first edit above it, and a list of test names
// puts the justification in a different file from the code it excuses. The
// marker sits on the line that needs it and carries its own reason.
const directProposeMarker = "//quicraft:direct-propose"

// proposalCalls are the methods that submit an entry to Raft. Both must reach
// the shard's CURRENT leader, and neither forwards when the addressed replica
// has lost leadership without yet learning its successor.
var proposalCalls = map[string]bool{
	"Propose":     true,
	"SyncPropose": true,
}

// TestIntegrationProposalsResolveTheLeader fails when an integration test
// proposes through a Host it addressed directly, instead of through the
// suite's proposeToLeader / tryProposeToLeader helpers.
//
// # Why this gate exists
//
// Five times now a test has read the leader on one line and used it several
// lines later as though the observation pinned it (#84, #88, #90, #91, #103).
// It does not. Raft is free to hold an election in the gap, and the proposal is
// then refused CORRECTLY while the test reports a failure the cluster never
// had. Twice the class came back AFTER being fixed, because each fix was a
// round of hand-edits and the next test to be written reintroduced the pattern.
//
// The reason it kept escaping review is that it has two shapes. The direct one
//
//	cluster.hosts[leaderIdx].Propose(ctx, shardID, cmd)
//
// is greppable. The indirect one is not:
//
//	leaderHost := cluster.hosts[leaderIdx]   // the binding hides it
//	...
//	leaderHost.SyncPropose(ctx, shardID, cmd)
//
// #91 fixed sixteen sites of the first shape and missed every site of the
// second, one of which failed CI four hours later. So this gate does not try to
// recognise the caching at all — it is a rule about the CALL, not about how the
// receiver was obtained, and no binding can hide from it.
//
// # What a failure means
//
// Route the proposal through proposeToLeader (or tryProposeToLeader off the
// test goroutine), which resolves the leader at the point of use and retries
// the not-leader rejection.
//
// If the call is deliberate — the test is ABOUT the replica it addresses, such
// as a deposed incumbent that must NOT commit, or a follower whose forwarding
// is the assertion — put directProposeMarker and a reason on the line above.
// That is a real category and roughly a sixth of the propose sites in this
// suite are in it; the marker is how the judgement gets recorded next to the
// code instead of in a commit message nobody will find.
func TestIntegrationProposalsResolveTheLeader(t *testing.T) {
	roots := []string{
		filepath.Join("..", "integration"),
	}

	type finding struct {
		pos  string
		call string
	}
	var findings []finding
	var bare []string
	scanned := 0

	for _, root := range roots {
		err := filepath.WalkDir(root, func(path string, d fs.DirEntry, err error) error {
			if err != nil {
				return err
			}
			if d.IsDir() || !strings.HasSuffix(path, "_test.go") {
				return nil
			}

			fset := token.NewFileSet()
			// ParseComments so the marker is visible; the integration files are
			// behind build tags, which parser.ParseFile does not evaluate, so
			// every file is scanned regardless of which tags CI is running.
			file, perr := parser.ParseFile(fset, path, nil, parser.ParseComments)
			if perr != nil {
				return perr
			}
			scanned++

			// Every line occupied by a comment, so a marker can be found at
			// the top of a multi-line comment block and still excuse the
			// statement the block documents.
			commentLine := make(map[int]bool)
			for _, group := range file.Comments {
				for _, c := range group.List {
					start := fset.Position(c.Pos()).Line
					end := fset.Position(c.End()).Line
					for l := start; l <= end; l++ {
						commentLine[l] = true
					}
				}
			}

			// A marker excuses its own line (trailing comment) and the first
			// line below it that is not itself a comment — the statement the
			// block is written above.
			excused := make(map[int]bool)
			for _, group := range file.Comments {
				for _, c := range group.List {
					if !strings.HasPrefix(c.Text, directProposeMarker) {
						continue
					}
					line := fset.Position(c.Pos()).Line
					// A bare marker is not an exemption. The whole point is to
					// record WHY this call addresses one specific replica; a
					// marker with nothing after it silences the gate while
					// leaving the next reader exactly as stuck as before.
					if strings.TrimSpace(strings.TrimPrefix(c.Text, directProposeMarker)) == "" {
						bare = append(bare, fset.Position(c.Pos()).String())
						continue
					}
					// The marker's own line, for a trailing comment.
					excused[line] = true
					// And the first line below the comment block that is not
					// itself a comment: the statement the block documents.
					l := line + 1
					for commentLine[l] {
						l++
					}
					excused[l] = true
				}
			}

			ast.Inspect(file, func(n ast.Node) bool {
				call, ok := n.(*ast.CallExpr)
				if !ok {
					return true
				}
				sel, ok := call.Fun.(*ast.SelectorExpr)
				if !ok || !proposalCalls[sel.Sel.Name] {
					return true
				}
				// A method on the suite's own cluster helper is the fixed
				// version, not a violation: cluster.proposeToLeader style
				// wrappers never appear as a bare Propose selector, so
				// anything reaching here is a call on a *quicraft.Host.
				// A call spanning several lines carries its marker above the
				// statement, so check the statement's first line as well as
				// the selector's.
				line := fset.Position(call.Pos()).Line
				if excused[line] || excused[fset.Position(sel.Pos()).Line] {
					return true
				}
				findings = append(findings, finding{
					pos:  fset.Position(call.Pos()).String(),
					call: sel.Sel.Name,
				})
				return true
			})
			return nil
		})
		if err != nil {
			t.Fatalf("walking %s: %v", root, err)
		}
	}

	// A gate over an empty set passes for the wrong reason. If the integration
	// tree moves and this stops finding files, that must fail rather than go
	// quietly green.
	const minScanned = 20
	if scanned < minScanned {
		t.Fatalf("scanned only %d integration test files, expected at least %d: "+
			"the tree moved and this gate is now checking almost nothing",
			scanned, minScanned)
	}

	if len(bare) > 0 {
		t.Errorf("%d %s marker(s) carry no reason:\n  %s\n\n"+
			"The marker records WHY a call addresses one specific replica. "+
			"Without that it only silences this gate.",
			len(bare), directProposeMarker, strings.Join(bare, "\n  "))
	}

	if len(findings) == 0 {
		t.Logf("scanned %d integration test files: every proposal resolves the leader", scanned)
		return
	}

	sort.Slice(findings, func(i, j int) bool { return findings[i].pos < findings[j].pos })
	var b strings.Builder
	for _, f := range findings {
		b.WriteString("\n  " + f.pos + ": " + f.call)
	}
	t.Fatalf("%d integration proposal(s) address a Host directly instead of "+
		"resolving the leader at the point of use:%s\n\n"+
		"Observing a leader does not pin it. Route these through "+
		"proposeToLeader (or tryProposeToLeader off the test goroutine), which "+
		"resolves the leader when the call is made and retries the not-leader "+
		"rejection.\n\n"+
		"If a call is deliberately aimed at one specific replica — a deposed "+
		"incumbent that must NOT commit, a follower whose forwarding is the "+
		"assertion — put %q and the reason on the line above it.",
		len(findings), b.String(), directProposeMarker)
}
