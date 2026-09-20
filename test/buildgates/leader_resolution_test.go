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

// linearizableReadCalls are the methods that serve a read THROUGH the leader,
// in both modules' spellings — quicraft.Host and dragonboat.NodeHost spell
// both of them the same.
//
// A linearizable read is bound to the leader exactly as a proposal is.
// SyncRead is a ReadIndex plus a local lookup at the confirmed index;
// handleFollowerReadIndex forwards when the addressed follower knows the
// leader and refuses with a not-leader carrying LeaderID 0 when it does not;
// and a read pending on a replica that loses the shard is failed with the same
// MaybeApplied-wrapped not-leader a pending proposal is. Same class, same
// remedy, so it gets a gate on the same terms.
//
// # What is deliberately NOT in this set
//
// StaleRead, StaleReadBuf and FollowerRead are served from the addressed
// replica's own applied state and need no leader at all. A test that calls one
// has already chosen a specific replica — that IS the subject — so requiring a
// marker on every such site would be noise that devalues the marker where it
// matters. There are nine of them across the two trees and not one is the
// class.
//
// SyncReadBuf is leader-bound and belongs here by the same reasoning as
// SyncRead, but #154 fixed this call set at SyncRead and ReadIndex and it is
// not this change's call to widen it. There is currently no SyncReadBuf call
// in either integration tree (verified by the same AST walk), so the set is
// complete against the trees as they stand; see the PR for the follow-up.
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
					// A method on the suite's own cluster helper is the fixed
					// version, not a violation: cluster.proposeToLeader style
					// wrappers never appear as a bare Propose selector.
					//
					// A PACKAGE-qualified call is not a method call at all, so
					// it is not the class either. proposalCalls and
					// configChangeCalls never needed this — test/proposer and
					// test/membership spell their entry points ToLeader, Try,
					// AddNode and so on, none of which collide. The read set
					// does: the fixed form of a gated ReadIndex is
					// proposer.ReadIndex, whose selector is the identical
					// bare ReadIndex. Skipping the receiver on the strength of
					// its NAME would be a hole; skipping it because the file
					// imports a package by that name is a fact about the file,
					// and it is what makes the read gate share this scanner
					// instead of forking it.
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

// importedPackageNames returns the identifiers by which file refers to the
// packages it imports: the explicit alias where there is one, otherwise the
// last element of the import path.
//
// Using the path's last element is an approximation — a package whose name
// differs from its directory would be missed — and it is deliberately the
// conservative direction. A miss leaves a package-qualified call in the
// findings, which is a false POSITIVE that a human reads and fixes; the
// opposite error would silently excuse a real one.
//
// The residual hole is a local variable that shadows an imported package name
// AND is a Host: `proposer := cluster.hosts[i]; proposer.SyncRead(...)` would
// slip through. Nothing in either tree does that, go vet's shadow analysis
// would not love it, and the alternative — full type resolution — means
// loading both modules' packages, which is what this gate exists to avoid (see
// integrationRoots on why it parses rather than compiles).
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

// pendingSite is one direct call that a gate knows about and has deferred to a
// tracking issue, because the file it sits in was owned by an in-flight pull
// request when the gate was installed and could not be edited.
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
// The alternative was to leave a gate red on main until every follow-up lands,
// which is the same information with CI switched off for everyone in between.
//
// The type and the machinery below it are shared by every gate in this file
// for the same reason the scanner is: a second copy of a ratchet is a second
// place for the rule to drift.
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
	// Empty since #123 retired the dragonboat adapter's fourteen rows: every
	// contrib site is now either routed through that suite's idempotent
	// helper or marked with its reason. #135 retired the six root-module
	// rows before it. The type and the ratchet stay so the next deferred
	// site, if there is one, has somewhere to go that fails when it is fixed.
}

// pendingReadSites is TestIntegrationReadsResolveTheLeader's deferred table,
// and it is EMPTY.
//
// A waiver never belongs here — a waiver is a marker in the file that needs it,
// and recording one as "deferred" would file a site that is CORRECT as a known
// violation. What belongs here is a conversion that cannot be made yet, and the
// entry is the ratchet that fails when it lands.
//
// It stays, with its type and its matching machinery, because the next
// deferred conversion needs somewhere to go that fails when it lands. An empty
// ratchet is the goal state, not a dead one.
//
// #161 held the adapter suite's 21 rows until #131 released
// contrib/dragonboat/test/integration/helpers_test.go. They are gone: 20 sites
// now route through the adapter's own readFromLeader / tryReadFromLeader, and
// the twenty-first turned out not to be a conversion at all and took a waiver
// (linearizable_test.go, the failover-without-warmup loop — it retries
// IsTempError on a fixed slice, which is wider and differently budgeted than
// any leader-resolving helper).
//
// #162 held the last row: syncReadExact's pinned read, deferred not because a
// file could not be edited but because the disposition was not a gate
// installation's to pick. It was settled by growing proposer.TryReadWith and
// routing the site there, so the pinned-retry loop has one home rather than a
// waiver blessing a third copy of it.
var pendingReadSites = []pendingSite{}

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
// PR #122 fixed that in the e2e suite's syncAddNode / syncDeleteNode, and
// #135 moved the decision into test/membership so every suite shares one copy
// (#145 retired the e2e original): the refusal is accepted ONLY after an
// attempt in the same loop was genuinely submitted AND a linearizable
// SyncGetShardMembership confirms the exact requested state — replica present
// as a full voter at the requested address, or absent in every role. A blanket
// "not found is fine" is the wrong fix, and
// TestRetry_AlreadyExistsAtDifferentAddressStillFails in test/membership is
// where that line is pinned.
//
// #122 alone did not end the class, for the same reason #91 did not end the
// propose class: it fixed the sites that happened to use its helper. Any test
// can still call SyncRequestDeleteNode itself, with its own retry, and get none
// of it. So this is a rule about the CALL, like the propose gate above, and no
// binding or private retry loop can hide from it.
//
// # What a failure means
//
// Route the change through test/membership — AddNode / DeleteNode, or the
// error-returning TryAddNode / TryDeleteNode — with the suite's leader
// resolver; the decision core is membership.Drive, and every suite binds the
// same one rather than keeping a copy.
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
		"\"target node not found in membership\" (#112, #132). Route these "+
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

// matchPending splits direct into the findings a gate must fail on and the
// ones its deferred table still accounts for, logs the deferred sites with
// their tracking issue, and fails (without stopping the test) for any entry
// that no longer matches the tree.
//
// kind names the class for the messages ("config change", "read"); table names
// the variable a reader has to edit.
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

	// An entry that no longer matches has been fixed or moved. Either way it
	// must leave the table now, or the table starts to be the allowlist it
	// was designed not to be.
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

// TestIntegrationReadsResolveTheLeader fails when an integration test issues a
// LINEARIZABLE read — SyncRead or ReadIndex — through a Host it addressed
// directly, instead of through the suite's leader-resolving read helper.
//
// # Why this gate exists
//
// This is the READ half of the class the two gates above cover. #147 found
// fifteen sites in the host suite where a leader was observed once
//
//	leaderIdx := cluster.waitForClusterLeader(shardID, timeout)
//	leaderHost := cluster.hosts[leaderIdx]
//	...
//	got, err := leaderHost.SyncRead(ctx, shardID, key)
//
// and then read from across a loop. One of them
// (TestHostAPI_MembershipChange_AddNodeWhileWriting) failed CI under the
// runners' own load with "not leader for shard 1, leader unknown" — refused
// CORRECTLY by an engine that had held an election in the gap, reporting a
// defect the cluster never had.
//
// #155 converted those fifteen and marked five. Nothing stopped the next test
// from reintroducing the pattern, which is precisely how the propose class came
// back twice after being "fixed" (#91, #103). So, like the two gates above,
// this is a rule about the CALL rather than about how the receiver was
// obtained, and no binding can hide from it.
//
// # What a failure means
//
// Route the read through the suite's helper — readFromLeader /
// readIndexFromLeader, or the error-returning tryReadFromLeader off the test
// goroutine — which resolves the leader at the point of use and retries ONLY
// the not-leader rejection. Re-issuing a refused read is always safe: a read
// that was refused was never answered.
//
// If the addressed replica IS the subject — a deposed incumbent whose refusal
// is the assertion, a follower whose forwarding is what is under test, the
// target of a leadership transfer whose own lease is the point, a shard that
// does not exist — keep the direct call and put directReadMarker and a reason
// on the line above. That is a real category: twenty of the sites across the
// two trees are in it.
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
		"same terms (#147). Observing a leader does not pin it. Route these "+
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
