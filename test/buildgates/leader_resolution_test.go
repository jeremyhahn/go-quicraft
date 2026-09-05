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

// directProposeMarker opts one call site out of TestIntegrationProposalsResolveTheLeader.
//
// It is a marker rather than an allowlist in this file on purpose. A list of
// file:line pairs rots on the first edit above it, and a list of test names
// puts the justification in a different file from the code it excuses. The
// marker sits on the line that needs it and carries its own reason.
const directProposeMarker = "//quicraft:direct-propose"

// directConfigChangeMarker opts one call site out of
// TestIntegrationConfigChangesAreIdempotent. Same shape and same rule as
// directProposeMarker: it sits on the line that needs it, it carries its own
// reason, and a bare marker is rejected rather than honoured.
const directConfigChangeMarker = "//quicraft:direct-config-change"

// integrationRoot is one test tree these gates walk, with the smallest number
// of files they must find there.
type integrationRoot struct {
	dir      string
	minFiles int
}

// integrationRoots are the trees scanned by every gate in this file.
//
// # Why contrib/dragonboat is scanned from here, and not by its own gate
//
// contrib/dragonboat is a separate Go module with its own CI job, so the
// obvious move is a second copy of this gate inside it. That would be ~180
// lines of AST walking duplicated into a second module, and the duplication is
// not incidental: the marker convention, the bare-marker rule, the
// comment-block resolution and the empty-set floor all have to agree exactly
// between the copies or the rule quietly differs between the two trees, which
// is how a class like this survives a fix.
//
// It does not need to be duplicated, because the gate PARSES rather than
// compiles. go/parser reads files by path; it never resolves an import, loads a
// package or evaluates a build tag, so a module boundary is invisible to it —
// contrib/dragonboat's files are ordinary files in the same repository at a
// known relative path. Both trees are therefore checked by the one
// implementation, and a change to the rule lands in both at once.
//
// The cost is that a violation in contrib/dragonboat fails the PARENT module's
// job rather than the contrib job. That is the right way round: a violation
// fails CI either way, and one gate means there is exactly one place to fix the
// rule and no way for the two trees to drift apart.
//
// # What is NOT scanned, and why
//
// Only the multi-replica integration trees. Neither module's root-package unit
// tests are here, and neither should be: every shard in
// contrib/dragonboat/nodehost_test.go and nodehost_defects_test.go is a
// single-voter configuration (map[uint64]Target{replicaID: nh.RaftAddress()}),
// where the addressed host is the only replica that can lead and there is no
// successor for a proposal to be redirected to. The class this gate exists to
// catch cannot occur there, and marking 41 such calls would be noise that
// devalues the marker everywhere it does matter.
var integrationRoots = []integrationRoot{
	// The parent module's integration suites (e2e, host, linearizability, raft).
	{dir: filepath.Join("..", "integration"), minFiles: 20},
	// The dragonboat adapter's integration suite. Its helpers are the module's
	// own — see the "Leader-resolving proposals" section of
	// contrib/dragonboat/test/integration/helpers_test.go for why the parent's
	// test/proposer package cannot be reused across that API boundary.
	{dir: filepath.Join("..", "..", "contrib", "dragonboat", "test", "integration"), minFiles: 20},
}

// proposalCalls are the methods that submit an entry to Raft. Both must reach
// the shard's CURRENT leader, and neither forwards when the addressed replica
// has lost leadership without yet learning its successor.
var proposalCalls = map[string]bool{
	"Propose":     true,
	"SyncPropose": true,
}

// configChangeCalls are the methods that submit a membership change to Raft,
// in both modules' spellings. A config change reaches the leader and is
// refused by a non-leader on exactly the same terms as a proposal, and it
// carries a second unpinned value a proposal does not — whether it COMMITTED —
// which is why it has its own gate rather than a place in proposalCalls.
//
// The async forms are listed alongside the Sync ones because the class is
// about the CALL: an async RequestState whose ResultC is read after a
// leadership move has exactly the same ambiguity, and a private retry loop
// wrapped around either form hides nothing from this gate.
var configChangeCalls = map[string]bool{
	// quicraft.Host (pkg/host.go).
	"RequestAddNode":                 true,
	"RequestAddNodeWithIdentity":     true,
	"SyncRequestAddNode":             true,
	"SyncRequestAddNodeWithIdentity": true,
	"RequestRemoveNode":              true,
	"SyncRequestDeleteNode":          true,
	"RequestAddObserver":             true,
	"RequestAddObserverWithIdentity": true,
	"SyncRequestAddObserver":         true,
	"RequestAddWitness":              true,
	"RequestAddWitnessWithIdentity":  true,
	"SyncRequestAddWitness":          true,
	// dragonboat.NodeHost (contrib/dragonboat/nodehost.go). RequestAddWitness
	// and SyncRequestAddWitness are spelled the same in both modules.
	"RequestAddReplica":        true,
	"SyncRequestAddReplica":    true,
	"RequestAddNonVoting":      true,
	"SyncRequestAddNonVoting":  true,
	"RequestDeleteReplica":     true,
	"SyncRequestDeleteReplica": true,
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

// scanDirectCalls parses every _test.go under integrationRoots and reports each
// call to one of calls that is not excused by marker, plus every bare marker.
//
// It is the one implementation behind both gates in this file. The marker
// resolution — a marker excuses its own line (a trailing comment) and the first
// non-comment line below its comment block (the statement the block documents)
// — and the per-root file floor live here so the two rules cannot drift.
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
					// A method on the suite's own cluster helper is the fixed
					// version, not a violation: cluster.proposeToLeader style
					// wrappers never appear as a bare Propose selector, so
					// anything reaching here is a call on a *quicraft.Host or
					// a *dragonboat.NodeHost.
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

// pendingConfigChangeSite is one direct config-change call that
// TestIntegrationConfigChangesAreIdempotent knows about and has deferred to a
// tracking issue, because the file it sits in was owned by an in-flight pull
// request when the gate was installed and could not be edited from #132.
//
// # Why this is a ratchet and not an allowlist
//
// The marker convention above rejects lists for two reasons: a file:line list
// rots on the first edit above it, and a list of names puts the justification
// away from the code. This table is shaped to survive both objections while it
// exists, and to stop existing:
//
//   - it keys on (file, enclosing function, method, count), none of which move
//     when lines above the call are edited;
//   - every entry names the issue that deletes it, so the justification is a
//     link to the work rather than a sentence that can go stale;
//   - the gate FAILS when an entry stops matching — fixing a site without
//     removing its entry is red, so the table cannot outlive the sites; and
//   - a second direct call under an existing key is a fresh violation, not a
//     free ride, because count is exact.
//
// The alternative was to leave this gate red on main until #123 and #135 land,
// which is the same information with CI switched off for everyone in between.
type pendingConfigChangeSite struct {
	file  string
	fn    string
	call  string
	count int
	issue int
}

// pendingConfigChangeSites is the deferred table. It is expected to shrink to
// nothing; see pendingConfigChangeSite for why each entry exists and how it
// leaves.
var pendingConfigChangeSites = []pendingConfigChangeSite{

	// #123: the dragonboat adapter's suite, owned by PR #124 while in flight.
	// #123 already carries the cached-leader config change this gate is
	// about (OrderedMembershipChange's first add) and the site-by-site
	// disposition — which convert, which get the marker because the direct
	// call IS the assertion (a deadline-less context, an invalid address, a
	// stale ConfigChangeID, an overlapping change whose rejection is the
	// test, an async RequestState contract).
	{file: "contrib/dragonboat/test/integration/error_test.go", fn: "TestIntegration_DB_InvalidContextDeadline_AllSyncMethods", call: "SyncRequestAddReplica", count: 1, issue: 123},
	{file: "contrib/dragonboat/test/integration/error_test.go", fn: "TestIntegration_DB_InvalidContextDeadline_AllSyncMethods", call: "SyncRequestDeleteReplica", count: 1, issue: 123},
	{file: "contrib/dragonboat/test/integration/error_test.go", fn: "TestIntegration_DB_InvalidContextDeadline_AllSyncMethods", call: "SyncRequestAddNonVoting", count: 1, issue: 123},
	{file: "contrib/dragonboat/test/integration/error_test.go", fn: "TestIntegration_DB_InvalidAddressIsRejected", call: "SyncRequestAddReplica", count: 1, issue: 123},
	{file: "contrib/dragonboat/test/integration/membership_advanced_test.go", fn: "TestIntegration_DB_OrderedMembershipChange", call: "SyncRequestAddReplica", count: 3, issue: 123},
	{file: "contrib/dragonboat/test/integration/membership_advanced_test.go", fn: "TestIntegration_DB_SyncRequestDeleteReplica", call: "SyncRequestDeleteReplica", count: 1, issue: 123},
	{file: "contrib/dragonboat/test/integration/membership_advanced_test.go", fn: "TestIntegration_DB_SyncRequestAddReplica", call: "SyncRequestAddReplica", count: 1, issue: 123},
	{file: "contrib/dragonboat/test/integration/membership_advanced_test.go", fn: "TestIntegration_DB_SyncRequestAddNonVoting", call: "SyncRequestAddNonVoting", count: 1, issue: 123},
	{file: "contrib/dragonboat/test/integration/membership_advanced_test.go", fn: "TestIntegration_DB_ConcurrentConfigChangeRejected", call: "RequestAddReplica", count: 2, issue: 123},
	{file: "contrib/dragonboat/test/integration/membership_advanced_test.go", fn: "TestIntegration_DB_ConcurrentConfigChangeRejected", call: "SyncRequestAddReplica", count: 1, issue: 123},
	{file: "contrib/dragonboat/test/integration/membership_test.go", fn: "TestIntegration_DB_AsyncRequestAddReplica", call: "RequestAddReplica", count: 1, issue: 123},
	{file: "contrib/dragonboat/test/integration/snapshot_advanced_test.go", fn: "TestIntegration_DB_SnapshotTransferToNewNode", call: "SyncRequestAddReplica", count: 1, issue: 123},
	{file: "contrib/dragonboat/test/integration/witness_test.go", fn: "TestIntegration_DB_SyncRequestAddWitness", call: "SyncRequestAddWitness", count: 1, issue: 123},
	{file: "contrib/dragonboat/test/integration/witness_test.go", fn: "TestIntegration_DB_RequestAddWitness_Async", call: "RequestAddWitness", count: 1, issue: 123},
}

// TestIntegrationConfigChangesAreIdempotent fails when an integration test
// issues a membership change directly — SyncRequestAddNode,
// SyncRequestDeleteNode, RequestAddReplica and the rest of configChangeCalls —
// instead of through the suite's idempotent, leader-resolving helper.
//
// # Why this gate exists
//
// A config change has everything a proposal has: it is submitted through the
// leader, and a replica that has just lost leadership refuses it correctly
// while the test reports a failure the cluster never had. #123 found exactly
// that on OrderedMembershipChange's first add, and noted that proposalCalls did
// not see it.
//
// It also has a second unpinned value that a proposal does not. When the
// response is lost — a leadership move fails every pending request with a
// *MaybeAppliedError, and a deadline can only fire after delivery — the change
// is IN THE LOG and can still commit. A retry loop that reads "no answer" as
// "did not happen" then re-issues it, the engine refuses the duplicate
// correctly, and the test fails with "node already exists in membership" or
// "target node not found in membership" (#112, and #132 on the delete path).
// PR #122 fixed that in syncAddNode / syncDeleteNode: the refusal is accepted
// ONLY after an attempt in the same loop was genuinely submitted AND a
// linearizable SyncGetShardMembership confirms the exact requested state —
// replica present as a full voter at the requested address, or absent in every
// role. A blanket "not found is fine" is the wrong fix, and
// TestMembershipRetry_AlreadyExistsAtDifferentAddressStillFails is where that
// line is pinned.
//
// #122 alone did not end the class, for the same reason #91 did not end the
// propose class: it fixed the sites that happened to use its helper. Any test
// can still call SyncRequestDeleteNode itself, with its own retry, and get none
// of it. So this is a rule about the CALL, like the propose gate above, and no
// binding or private retry loop can hide from it.
//
// # What a failure means
//
// Route the change through syncAddNode / syncDeleteNode (the decision core is
// driveMembershipChange in test/integration/e2e/membership_idempotence_test.go;
// a suite without a copy gets one that keeps BOTH halves of the gate).
//
// If the direct call IS the assertion — a deadline-less context that must be
// refused, an invalid address, a stale ConfigChangeID, an overlapping change
// whose rejection is the test, an async RequestState whose ResultC contract is
// under test — put directConfigChangeMarker and a reason on the line above.
//
// # The deferred table
//
// pendingConfigChangeSites lists the sites this gate found when installed that
// sat in files owned by in-flight pull requests. They are reported, not fatal,
// and the table is a ratchet: an entry that stops matching fails the gate, so
// the fix for a tracked site includes deleting its entry.
func TestIntegrationConfigChangesAreIdempotent(t *testing.T) {
	scan := scanDirectCalls(t, directConfigChangeMarker, configChangeCalls)
	reportBareMarkers(t, directConfigChangeMarker, scan.bare)

	seen := make([]int, len(pendingConfigChangeSites))
	var violations []directCall
	var deferred []string
	for _, c := range scan.direct {
		i := pendingSiteFor(c, seen)
		if i < 0 {
			violations = append(violations, c)
			continue
		}
		seen[i]++
		deferred = append(deferred, fmt.Sprintf("%s: %s (#%d)", c.pos, c.call, pendingConfigChangeSites[i].issue))
	}

	// An entry that no longer matches has been fixed or moved. Either way it
	// must leave the table now, or the table starts to be the allowlist it
	// was designed not to be.
	var rotted []string
	for i, p := range pendingConfigChangeSites {
		if seen[i] != p.count {
			rotted = append(rotted, fmt.Sprintf("%s %s.%s: expected %d direct call(s), found %d (#%d)",
				p.file, p.fn, p.call, p.count, seen[i], p.issue))
		}
	}
	if len(rotted) > 0 {
		t.Errorf("%d deferred config-change entr(y/ies) no longer match the tree:\n  %s\n\n"+
			"A site that was converted or moved must take its pendingConfigChangeSites "+
			"entry with it. Delete the entry (or correct its count) in the same change.",
			len(rotted), strings.Join(rotted, "\n  "))
	}

	if len(deferred) > 0 {
		t.Logf("%d direct config change(s) deferred to their tracking issue:\n  %s",
			len(deferred), strings.Join(deferred, "\n  "))
	}

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
		"\"target node not found in membership\" (#112, #132). Route these "+
		"through syncAddNode / syncDeleteNode (driveMembershipChange in "+
		"test/integration/e2e/membership_idempotence_test.go), which resolve the "+
		"leader at the point of use and accept a duplicate refusal only after an "+
		"attempt was genuinely submitted AND a linearizable membership read "+
		"confirms the requested state.\n\n"+
		"If the direct call IS the assertion — a stale ConfigChangeID that must "+
		"be refused, an address that must be rejected, an overlapping change "+
		"whose rejection is the test — put %q and the reason on the line above it.",
		len(violations), formatDirectCalls(violations), directConfigChangeMarker)
}

// pendingSiteFor returns the index of the deferred entry c still counts
// against, or -1 when no entry matches or the matching entry is already full.
// An over-full entry is deliberately a miss: the (count+1)th direct call under
// a tracked key is new, and new is a violation.
func pendingSiteFor(c directCall, seen []int) int {
	for i, p := range pendingConfigChangeSites {
		if p.file == c.file && p.fn == c.fn && p.call == c.call && seen[i] < p.count {
			return i
		}
	}
	return -1
}
