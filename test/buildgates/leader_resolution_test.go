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

// directProposeMarker opts one call site out of TestIntegrationProposalsResolveTheLeader; it sits on the line it excuses and carries its reason.
const directProposeMarker = "//quicraft:direct-propose"

// directConfigChangeMarker opts one call site out of
// TestIntegrationConfigChangesAreIdempotent. Same shape and same rule as
// directProposeMarker: it sits on the line that needs it, it carries its own
// reason, and a bare marker is rejected rather than honoured.
const directConfigChangeMarker = "//quicraft:direct-config-change"

// directReadMarker opts one call site out of
// TestIntegrationReadsResolveTheLeader. Same shape and same rule as the two
// above: it sits on the line that needs it, it carries its own reason, and a
// bare marker is rejected rather than honoured.
const directReadMarker = "//quicraft:direct-read"

// integrationRoot is one test tree these gates walk, with the smallest number
// of files they must find there.
type integrationRoot struct {
	dir      string
	minFiles int
}

// integrationRoots are the multi-replica integration trees every gate here scans, contrib/dragonboat included, since go/parser reads files across module boundaries; single-voter unit tests are excluded.
var integrationRoots = []integrationRoot{
	// The parent module's integration suites (e2e, host, linearizability, raft).
	{dir: filepath.Join("..", "integration"), minFiles: 20},
	// The dragonboat adapter's integration suite. Its helpers are the module's
	// own — see the "Leader-resolving proposals" section of
	// contrib/dragonboat/test/integration/helpers_test.go for why the parent's
	// test/proposer package cannot be reused across that API boundary.
	{dir: filepath.Join("..", "..", "contrib", "dragonboat", "test", "integration"), minFiles: 20},
}

// proposalCalls are the methods that submit an entry to Raft and must reach the current leader.
var proposalCalls = map[string]bool{
	"Propose":     true,
	"SyncPropose": true,
}

// configChangeCalls are the sync and async membership-change methods in both modules; a lost response leaves commit unknown, so they have their own gate.
var configChangeCalls = map[string]bool{
	// quicraft.Host (pkg/host.go).
	"RequestAddNode":                     true,
	"RequestAddNodeWithIdentity":         true,
	"SyncRequestAddNode":                 true,
	"SyncRequestAddNodeWithIdentity":     true,
	"RequestRemoveNode":                  true,
	"SyncRequestRemoveNode":              true,
	"RequestAddObserver":                 true,
	"RequestAddObserverWithIdentity":     true,
	"SyncRequestAddObserver":             true,
	"SyncRequestAddObserverWithIdentity": true,
	"RequestAddWitness":                  true,
	"RequestAddWitnessWithIdentity":      true,
	"SyncRequestAddWitness":              true,
	// dragonboat.NodeHost (contrib/dragonboat/nodehost.go). RequestAddWitness
	// and SyncRequestAddWitness are spelled the same in both modules.
	"RequestAddReplica":                   true,
	"RequestAddReplicaWithIdentity":       true,
	"SyncRequestAddReplica":               true,
	"SyncRequestAddReplicaWithIdentity":   true,
	"RequestAddNonVoting":                 true,
	"RequestAddNonVotingWithIdentity":     true,
	"SyncRequestAddNonVoting":             true,
	"SyncRequestAddNonVotingWithIdentity": true,
	"SyncRequestAddWitnessWithIdentity":   true,
	"RequestDeleteReplica":                true,
	"SyncRequestDeleteReplica":            true,
}

// linearizableReadCalls are the leader-bound read methods in both modules; StaleRead, StaleReadBuf and FollowerRead are served locally and excluded. Add SyncReadBuf when an integration test first calls it.
var linearizableReadCalls = map[string]bool{
	"SyncRead":  true,
	"ReadIndex": true,
}

// directCall is one call to a gated method that no marker excuses.
type directCall struct {
	// file is the repository-relative, slash-separated path, so a site keys
	// the same way from either integration root.
	file string
	// fn is the enclosing top-level function (a method by its bare name), or
	// "" for a call in a package-level initializer.
	fn   string
	call string
	pos  string
}

// directCallScan is what one walk of integrationRoots found for one rule.
type directCallScan struct {
	direct []directCall
	// bare holds the positions of markers that carry no reason. A bare marker
	// is not an exemption: the whole point is to record WHY a call is
	// deliberate, and a marker with nothing after it silences the gate while
	// leaving the next reader exactly as stuck as before.
	bare []string
}

// scanDirectCalls parses every _test.go under integrationRoots and reports each call in calls not excused by marker, plus every bare marker; a marker excuses its own line or the first code line below its comment block.
func scanDirectCalls(t *testing.T, marker string, calls map[string]bool) directCallScan {
	t.Helper()

	var scan directCallScan
	for _, root := range integrationRoots {
		scanned := 0
		err := filepath.WalkDir(root.dir, func(path string, d fs.DirEntry, err error) error {
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

			rel, rerr := repoRelative(path)
			if rerr != nil {
				return rerr
			}

			excused, bare := excusedLines(fset, file, marker)
			scan.bare = append(scan.bare, bare...)
			pkgs := importedPackageNames(file)

			for _, decl := range file.Decls {
				fn := ""
				if fd, ok := decl.(*ast.FuncDecl); ok {
					fn = fd.Name.Name
				}
				ast.Inspect(decl, func(n ast.Node) bool {
					call, ok := n.(*ast.CallExpr)
					if !ok {
						return true
					}
					sel, ok := call.Fun.(*ast.SelectorExpr)
					if !ok || !calls[sel.Sel.Name] {
						return true
					}
					// Calls on the suite's own helpers and package-qualified calls (by imported package name) are not the class.
					if ident, ok := sel.X.(*ast.Ident); ok && pkgs[ident.Name] {
						return true
					}
					// A call spanning several lines carries its marker above
					// the statement, so check the statement's first line as
					// well as the selector's.
					line := fset.Position(call.Pos()).Line
					if excused[line] || excused[fset.Position(sel.Pos()).Line] {
						return true
					}
					scan.direct = append(scan.direct, directCall{
						file: rel,
						fn:   fn,
						call: sel.Sel.Name,
						pos:  fset.Position(call.Pos()).String(),
					})
					return true
				})
			}
			return nil
		})
		if err != nil {
			t.Fatalf("walking %s: %v", root.dir, err)
		}

		// A gate over an empty set passes for the wrong reason. If a tree
		// moves and this stops finding files, that must fail rather than go
		// quietly green. Counted PER ROOT: a total would let the parent
		// module's 34 files cover for a contrib tree that had vanished.
		if scanned < root.minFiles {
			t.Fatalf("scanned only %d test files under %s, expected at least %d: "+
				"the tree moved and this gate is now checking almost nothing",
				scanned, root.dir, root.minFiles)
		}
		t.Logf("scanned %d test files under %s", scanned, root.dir)
	}

	sort.Slice(scan.direct, func(i, j int) bool { return scan.direct[i].pos < scan.direct[j].pos })
	return scan
}

// repoRelative returns path relative to the repository root, slash-separated.
// Both inputs are relative to this package's directory and the target sits
// above it, so both are made absolute first: filepath.Rel cannot walk up
// through a ".." it was handed as text.
func repoRelative(path string) (string, error) {
	absRoot, err := filepath.Abs(repoRoot)
	if err != nil {
		return "", err
	}
	absPath, err := filepath.Abs(path)
	if err != nil {
		return "", err
	}
	rel, err := filepath.Rel(absRoot, absPath)
	if err != nil {
		return "", err
	}
	return filepath.ToSlash(rel), nil
}

// importedPackageNames returns the names file uses for its imports: the alias, else the path's last element, erring toward false positives.
func importedPackageNames(file *ast.File) map[string]bool {
	names := make(map[string]bool, len(file.Imports))
	for _, imp := range file.Imports {
		if imp.Name != nil {
			// A blank or dot import qualifies nothing.
			if imp.Name.Name != "_" && imp.Name.Name != "." {
				names[imp.Name.Name] = true
			}
			continue
		}
		path := strings.Trim(imp.Path.Value, `"`)
		if base := path[strings.LastIndex(path, "/")+1:]; base != "" {
			names[base] = true
		}
	}
	return names
}

// excusedLines resolves every marker in file to the lines it excuses, and
// reports the markers that carry no reason.
func excusedLines(fset *token.FileSet, file *ast.File, marker string) (excused map[int]bool, bare []string) {
	// Every line occupied by a comment, so a marker can be found at the top
	// of a multi-line comment block and still excuse the statement the block
	// documents.
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

	excused = make(map[int]bool)
	for _, group := range file.Comments {
		for _, c := range group.List {
			if !strings.HasPrefix(c.Text, marker) {
				continue
			}
			line := fset.Position(c.Pos()).Line
			if strings.TrimSpace(strings.TrimPrefix(c.Text, marker)) == "" {
				bare = append(bare, fset.Position(c.Pos()).String())
				continue
			}
			// The marker's own line, for a trailing comment.
			excused[line] = true
			// And the first line below the comment block that is not itself
			// a comment: the statement the block documents.
			l := line + 1
			for commentLine[l] {
				l++
			}
			excused[l] = true
		}
	}
	return excused, bare
}

// reportBareMarkers fails the test (without stopping it) for every marker that
// carries no reason, so the findings below it are still reported.
func reportBareMarkers(t *testing.T, marker string, bare []string) {
	t.Helper()
	if len(bare) == 0 {
		return
	}
	t.Errorf("%d %s marker(s) carry no reason:\n  %s\n\n"+
		"The marker records WHY a call is deliberate. "+
		"Without that it only silences this gate.",
		len(bare), marker, strings.Join(bare, "\n  "))
}

// formatDirectCalls renders findings one per line for a failure message.
func formatDirectCalls(calls []directCall) string {
	var b strings.Builder
	for _, c := range calls {
		b.WriteString("\n  " + c.pos + ": " + c.call)
	}
	return b.String()
}

// TestIntegrationProposalsResolveTheLeader fails when an integration test proposes through a directly addressed Host; use proposeToLeader or tryProposeToLeader, or put directProposeMarker with a reason above a deliberate call.
func TestIntegrationProposalsResolveTheLeader(t *testing.T) {
	scan := scanDirectCalls(t, directProposeMarker, proposalCalls)
	reportBareMarkers(t, directProposeMarker, scan.bare)

	if len(scan.direct) == 0 {
		t.Log("every integration proposal resolves the leader")
		return
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
		len(scan.direct), formatDirectCalls(scan.direct), directProposeMarker)
}

// pendingSite is a direct call deferred to a tracking issue, keyed by file, function, method and exact count; an entry that stops matching fails the gate.
type pendingSite struct {
	file  string
	fn    string
	call  string
	count int
	issue int
}

// pendingConfigChangeSites is TestIntegrationConfigChangesAreIdempotent's
// deferred table. It is expected to shrink to nothing; see pendingSite for why
// each entry exists and how it leaves.
var pendingConfigChangeSites = []pendingSite{
	// Empty: every site is either routed through its suite's idempotent
	// helper or marked with its reason. The type and the ratchet stay so the
	// next deferred
	// site, if there is one, has somewhere to go that fails when it is fixed.
}

// pendingReadSites is TestIntegrationReadsResolveTheLeader's deferred table, empty; waivers are markers, not entries.
var pendingReadSites = []pendingSite{}

// TestIntegrationConfigChangesAreIdempotent fails when an integration test issues a membership change directly; use test/membership (AddNode, DeleteNode, TryAddNode, TryDeleteNode) or put directConfigChangeMarker with a reason above it. pendingConfigChangeSites is its ratchet.
func TestIntegrationConfigChangesAreIdempotent(t *testing.T) {
	scan := scanDirectCalls(t, directConfigChangeMarker, configChangeCalls)
	reportBareMarkers(t, directConfigChangeMarker, scan.bare)

	violations := matchPending(t, "config change", "pendingConfigChangeSites",
		scan.direct, pendingConfigChangeSites)

	if len(violations) == 0 {
		t.Log("every untracked integration config change goes through the idempotent helper")
		return
	}

	t.Fatalf("%d integration config change(s) are issued directly instead of "+
		"through the suite's idempotent membership helper:%s\n\n"+
		"A config change is refused by a non-leader on the same terms as a "+
		"proposal, and it has a second unpinned value a proposal does not: "+
		"whether it COMMITTED. A retry that reads \"no answer\" as \"did not "+
		"happen\" turns a slow success into \"already exists in membership\" or "+
		"\"target node not found in membership\". Route these "+
		"through test/membership (AddNode / DeleteNode, or TryAddNode / "+
		"TryDeleteNode, over the suite's leader resolver), which resolves the "+
		"leader at the point of use and accepts a duplicate refusal only after an "+
		"attempt was genuinely submitted AND a linearizable membership read "+
		"confirms the requested state.\n\n"+
		"If the direct call IS the assertion — a stale ConfigChangeID that must "+
		"be refused, an address that must be rejected, an overlapping change "+
		"whose rejection is the test — put %q and the reason on the line above it.",
		len(violations), formatDirectCalls(violations), directConfigChangeMarker)
}

// matchPending splits direct into failing findings and those the deferred table covers, logs the deferred ones and fails for stale entries; kind and table name the class and variable in messages.
func matchPending(t *testing.T, kind, tableName string, direct []directCall, table []pendingSite) []directCall {
	t.Helper()

	seen := make([]int, len(table))
	var violations []directCall
	var deferred []string
	for _, c := range direct {
		i := pendingSiteFor(c, table, seen)
		if i < 0 {
			violations = append(violations, c)
			continue
		}
		seen[i]++
		deferred = append(deferred, fmt.Sprintf("%s: %s (#%d)", c.pos, c.call, table[i].issue))
	}

	// An entry that no longer matches must leave the table.
	var rotted []string
	for i, p := range table {
		if seen[i] != p.count {
			rotted = append(rotted, fmt.Sprintf("%s %s.%s: expected %d direct call(s), found %d (#%d)",
				p.file, p.fn, p.call, p.count, seen[i], p.issue))
		}
	}
	if len(rotted) > 0 {
		t.Errorf("%d deferred %s entr(y/ies) no longer match the tree:\n  %s\n\n"+
			"A site that was converted or moved must take its %s entry with it. "+
			"Delete the entry (or correct its count) in the same change.",
			len(rotted), kind, strings.Join(rotted, "\n  "), tableName)
	}

	if len(deferred) > 0 {
		t.Logf("%d direct %s(s) deferred to their tracking issue:\n  %s",
			len(deferred), kind, strings.Join(deferred, "\n  "))
	}
	return violations
}

// pendingSiteFor returns the index of the deferred entry in table that c still
// counts against, or -1 when no entry matches or the matching entry is already
// full. An over-full entry is deliberately a miss: the (count+1)th direct call
// under a tracked key is new, and new is a violation.
func pendingSiteFor(c directCall, table []pendingSite, seen []int) int {
	for i, p := range table {
		if p.file == c.file && p.fn == c.fn && p.call == c.call && seen[i] < p.count {
			return i
		}
	}
	return -1
}

// TestIntegrationReadsResolveTheLeader fails when an integration test issues SyncRead or ReadIndex through a directly addressed Host; use readFromLeader, readIndexFromLeader or tryReadFromLeader, or put directReadMarker with a reason above it.
func TestIntegrationReadsResolveTheLeader(t *testing.T) {
	scan := scanDirectCalls(t, directReadMarker, linearizableReadCalls)
	reportBareMarkers(t, directReadMarker, scan.bare)

	violations := matchPending(t, "read", "pendingReadSites", scan.direct, pendingReadSites)

	if len(violations) == 0 {
		t.Log("every untracked integration linearizable read resolves the leader")
		return
	}

	t.Fatalf("%d integration linearizable read(s) address a Host directly "+
		"instead of resolving the leader at the point of use:%s\n\n"+
		"A linearizable read is bound to the leader exactly as a proposal is: "+
		"SyncRead is a ReadIndex plus a local lookup, and a replica that has "+
		"lost the shard without yet learning its successor refuses both on the "+
		"same terms. Observing a leader does not pin it. Route these "+
		"through the suite's readFromLeader / readIndexFromLeader (or "+
		"tryReadFromLeader off the test goroutine), which resolves the leader "+
		"when the call is made and retries the not-leader rejection.\n\n"+
		"If a read is deliberately aimed at one specific replica — a deposed "+
		"incumbent that must refuse it, a follower whose forwarding is the "+
		"assertion — put %q and the reason on the line above it. A StaleRead or "+
		"a FollowerRead needs no marker: neither is in this gate's call set, "+
		"because neither needs a leader.",
		len(violations), formatDirectCalls(violations), directReadMarker)
}
