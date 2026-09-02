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
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
	"time"

	"gopkg.in/yaml.v3"
)

// workflowsDir holds the workflows this repo runs on both planes.
var workflowsDir = filepath.Join(repoRoot, ".github", "workflows")

// TestEveryActionIsPinnedToACommitSHA pins the supply chain.
//
// A tag is a ref, and whoever controls the action can move it. Release and
// security jobs here hold write-scoped tokens and upload SARIF, so a moved tag
// runs attacker code with those credentials. `aquasecurity/trivy-action@master`
// was the sharpest case in this repo — not merely a mutable tag but a moving
// BRANCH, re-resolved on every run.
//
// A 40-hex ref is the only form that cannot be repointed. The trailing
// `# v<x>` comment is what keeps the pin readable and bumpable; it is not
// checked here because a comment is documentation, not a control.
//
// NOTE for whoever bumps one: `git ls-remote --tags --refs` returns the TAG
// OBJECT's sha for an ANNOTATED tag, which does not resolve as a pin and fails
// at checkout. Take the `^{}` peeled line instead. That is not hypothetical —
// `github/codeql-action@v3` is annotated and every other action here is not.
// isWorkflowFile reports whether name is a file GitHub Actions loads.
//
// BOTH extensions, deliberately. Actions loads `.github/workflows/*.yaml` as
// readily as `*.yml`, so a scan that accepts only `.yml` fails OPEN: a
// `release.yaml` carrying `uses: actions/checkout@v6` would be a live workflow
// with a mutable ref that this gate ignores entirely — and the gate would still
// pass, because its non-empty check is already satisfied by the `.yml` files.
func isWorkflowFile(name string) bool {
	return strings.HasSuffix(name, ".yml") || strings.HasSuffix(name, ".yaml")
}

// collectUses walks a parsed YAML document and appends the value of every
// `uses` key it finds, at any depth.
//
// PARSED, not regexed. A line-oriented pattern only recognises the conventional
// `- uses: <action>@<ref>` form, and YAML has others that Actions accepts
// perfectly well — a flow mapping `- { uses: actions/checkout@v6 }`, or a
// quoted key `- "uses": actions/checkout@v6`. A regex gate misses those
// entirely, and because the file's ordinary entries keep the "found something"
// floor satisfied, the gate still PASSES while the unpinned action runs. That
// is the same fail-open shape as skipping `.yaml`, one level down: the control
// reports success over syntax it cannot see.
func collectUses(n *yaml.Node, out *[]string, depth int) {
	// ALIASES ARE FOLLOWED. `uses: *checkout` produces an AliasNode, not a
	// scalar, so a collector that only reads scalars records nothing and the
	// gate passes over a live mutable ref. Anchors are ordinary Actions YAML:
	//
	//     x-ref: &checkout actions/checkout@v6
	//     ...
	//       - uses: *checkout
	//
	// Merge keys (`<<: *defaults`) are the same mechanism pointing at a mapping,
	// and following the alias covers them for free.
	//
	// depth bounds the walk. A self-referential alias is not expressible in a
	// document yaml.v3 will parse, but a bound costs nothing and a stack
	// overflow in a security gate is a bad way to find out otherwise.
	if n == nil || depth > 100 {
		return
	}
	if n.Kind == yaml.AliasNode {
		collectUses(n.Alias, out, depth+1)
		return
	}
	if n.Kind == yaml.MappingNode {
		for i := 0; i+1 < len(n.Content); i += 2 {
			k, v := n.Content[i], n.Content[i+1]
			if k.Value == "uses" {
				// Resolve through any chain of aliases to the scalar underneath.
				t, d := v, 0
				for t != nil && t.Kind == yaml.AliasNode && d <= 100 {
					t, d = t.Alias, d+1
				}
				if t != nil && t.Kind == yaml.ScalarNode {
					*out = append(*out, t.Value)
				}
			}
			collectUses(v, out, depth+1)
		}
		return
	}
	for _, c := range n.Content {
		collectUses(c, out, depth+1)
	}
}

// unpinnedRefs returns "<file>: <action>@<ref>" for every remote action in dir
// that is not pinned to a 40-hex commit, plus the total number of refs seen.
// The count is returned so a caller can refuse to pass over an empty scan.
//
// Local composite actions (`uses: ./...`) carry no ref and are skipped; docker
// refs (`docker://...`) are not GitHub actions and are out of scope for this
// gate, which is about a ref its owner can move.
func unpinnedRefs(t *testing.T, dir string) (bad []string, seen int) {
	t.Helper()
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatalf("read %s: %v", dir, err)
	}
	sha := regexp.MustCompile(`^[0-9a-f]{40}$`)
	for _, e := range entries {
		if e.IsDir() || !isWorkflowFile(e.Name()) {
			continue
		}
		raw, err := os.ReadFile(filepath.Join(dir, e.Name()))
		if err != nil {
			t.Fatalf("read %s: %v", e.Name(), err)
		}
		var doc yaml.Node
		if err := yaml.Unmarshal(raw, &doc); err != nil {
			t.Fatalf("%s is not parseable YAML: %v", e.Name(), err)
		}
		var uses []string
		collectUses(&doc, &uses, 0)
		for _, u := range uses {
			if strings.HasPrefix(u, "./") || strings.HasPrefix(u, "docker://") {
				continue
			}
			action, ref, ok := strings.Cut(u, "@")
			if !ok {
				bad = append(bad, e.Name()+": "+u+" (no @ref at all)")
				seen++
				continue
			}
			seen++
			if !sha.MatchString(ref) {
				bad = append(bad, e.Name()+": "+action+"@"+ref)
			}
		}
	}
	return bad, seen
}

func TestEveryActionIsPinnedToACommitSHA(t *testing.T) {
	bad, seen := unpinnedRefs(t, workflowsDir)
	if seen == 0 {
		t.Fatalf("found no `uses: <action>@<ref>` lines under %s. Discovery has "+
			"broken and this gate is verifying nothing.", workflowsDir)
	}
	for _, b := range bad {
		t.Errorf("%s is not pinned to a commit SHA.\n\n"+
			"A tag or branch is a ref its owner can move, and the jobs in these "+
			"workflows hold write-scoped tokens. Pin the 40-hex commit and keep the "+
			"version in a trailing comment. For an ANNOTATED tag take the `^{}` peeled "+
			"line from `git ls-remote --tags` — the unpeeled sha is the tag object and "+
			"does not resolve at checkout.", b)
	}
}

// TestThePinGateSeesYamlWorkflowsToo proves the scan is not fail-open on the
// other extension.
//
// Without it the gate's non-empty check passes on the strength of the `.yml`
// files while an entire `.yaml` workflow — equally live to GitHub Actions —
// goes unread. A supply-chain control that silently ignores a file class is
// worse than none, because the tree looks audited.
func TestThePinGateSeesYamlWorkflowsToo(t *testing.T) {
	dir := t.TempDir()
	// A pinned .yml alongside an UNPINNED .yaml: the exact shape that made the
	// first version of this gate pass while a mutable ref was live.
	if err := os.WriteFile(filepath.Join(dir, "pinned.yml"),
		[]byte("jobs:\n  a:\n    steps:\n      - uses: actions/checkout@"+
			"d23441a48e516b6c34aea4fa41551a30e30af803 # v6\n"), 0o644); err != nil {
		t.Fatalf("write: %v", err)
	}
	if err := os.WriteFile(filepath.Join(dir, "sneaky.yaml"),
		[]byte("jobs:\n  a:\n    steps:\n      - uses: actions/checkout@v6\n"), 0o644); err != nil {
		t.Fatalf("write: %v", err)
	}

	bad, seen := unpinnedRefs(t, dir)
	if seen != 2 {
		t.Fatalf("scanned %d refs, want 2 — the .yaml file was not read at all", seen)
	}
	if len(bad) != 1 || !strings.HasPrefix(bad[0], "sneaky.yaml:") {
		t.Fatalf("the unpinned ref in the .yaml workflow was not reported; got %v.\n"+
			"GitHub Actions loads .yaml as readily as .yml, so skipping it leaves a "+
			"live workflow with a mutable action ref unaudited.", bad)
	}
}

// TestThePinGateSeesEveryYAMLSpelling covers the other half of the same
// fail-open: syntax the gate cannot see.
//
// Actions accepts several spellings of the same key, and a line-oriented regex
// recognises only the conventional one. The others are not exotic — a flow
// mapping and a quoted key are both ordinary YAML — and because a file's normal
// entries keep the "found something" floor satisfied, a gate that misses them
// still PASSES while the unpinned action runs.
//
// Each case below carries exactly one mutable ref, and each must be reported.
func TestThePinGateSeesEveryYAMLSpelling(t *testing.T) {
	// wantSeen is 1 everywhere except the merge-key case, where it is 2 — and
	// that is correct rather than a bug: `x-step: &s {uses: ...}` is itself a
	// real `uses` in the document, and the merge is a second reference to it.
	// Both are live and both should be audited; double-reporting one mutable ref
	// is noise, while missing it is a hole.
	for _, tc := range []struct {
		name, body string
		wantSeen   int
	}{
		{"conventional", "jobs:\n  a:\n    steps:\n      - uses: actions/checkout@v6\n", 1},
		{"flow mapping", "jobs:\n  a:\n    steps:\n      - { uses: actions/checkout@v6 }\n", 1},
		{"double-quoted key", "jobs:\n  a:\n    steps:\n      - \"uses\": actions/checkout@v6\n", 1},
		{"single-quoted key", "jobs:\n  a:\n    steps:\n      - 'uses': actions/checkout@v6\n", 1},
		{"quoted value", "jobs:\n  a:\n    steps:\n      - uses: \"actions/checkout@v6\"\n", 1},
		{"nested under a reusable workflow", "jobs:\n  a:\n    steps:\n      - name: x\n        uses: actions/checkout@v6\n", 1},
		{"anchor + alias", "x-ref: &co actions/checkout@v6\njobs:\n  a:\n    steps:\n      - uses: *co\n", 1},
		{"merge key pulling in a uses", "x-step: &s\n  uses: actions/checkout@v6\njobs:\n  a:\n    steps:\n      - <<: *s\n        name: x\n", 2},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			if err := os.WriteFile(filepath.Join(dir, "w.yml"), []byte(tc.body), 0o644); err != nil {
				t.Fatalf("write: %v", err)
			}
			bad, seen := unpinnedRefs(t, dir)
			if seen != tc.wantSeen {
				t.Fatalf("scanned %d refs, want %d — this spelling is invisible to the gate, "+
					"so a mutable ref written this way would pass unaudited:\n%s",
					seen, tc.wantSeen, tc.body)
			}
			if len(bad) != tc.wantSeen {
				t.Fatalf("the mutable ref was not reported; got %v", bad)
			}
		})
	}

	// A local composite action has no ref to pin and must not be reported.
	t.Run("local action is not a finding", func(t *testing.T) {
		dir := t.TempDir()
		if err := os.WriteFile(filepath.Join(dir, "w.yml"),
			[]byte("jobs:\n  a:\n    steps:\n      - uses: ./.github/actions/thing\n"+
				"      - uses: actions/checkout@d23441a48e516b6c34aea4fa41551a30e30af803\n"), 0o644); err != nil {
			t.Fatalf("write: %v", err)
		}
		bad, seen := unpinnedRefs(t, dir)
		if seen != 1 {
			t.Fatalf("scanned %d refs, want 1 (the local action carries no ref)", seen)
		}
		if len(bad) != 0 {
			t.Fatalf("reported %v; a pinned remote action and a local one are both fine", bad)
		}
	})
}

// TestPublishRefusesATagThatMovedAfterValidation pins the release TOCTOU guard.
//
// `validate` and `release` are separate jobs on separate runners, each checking
// the tag out independently. A tag force-moved between them passes every
// self-consistency check either makes — tag and checkout agree, on the NEW
// commit — so every gate validated a tree that is not the one published.
//
// The guard must also refuse an ABSENT approved commit. An empty guard is not a
// passing guard, and empty is exactly what a renamed output produces.
func TestPublishRefusesATagThatMovedAfterValidation(t *testing.T) {
	path := filepath.Join(workflowsDir, "release.yml")
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read %s: %v", path, err)
	}
	body := string(raw)

	for _, want := range []struct{ needle, why string }{
		{"approved_sha:", "`validate` must EXPORT the commit it approved; without it the publish job has nothing to compare against"},
		{"needs.validate.outputs.approved_sha", "the publish job must READ the approved commit"},
		{`if [ -z "${APPROVED:-}" ]`, "an ABSENT approved commit must fail; an empty guard is not a passing guard"},
		{`if [ "$REMOTE_SHA" != "$APPROVED" ]`, "the publish job must compare what the tag points at ON THE REMOTE against the approved commit — comparing its own checkout cannot fail, since actions/checkout pins both jobs to the event SHA"},
		{`if [ -z "$REMOTE_SHA" ]`, "a tag deleted or renamed mid-release must fail, not publish"},
	} {
		if !strings.Contains(body, want.needle) {
			t.Errorf("release.yml is missing %q.\n\n%s.", want.needle, want.why)
		}
	}

	// Fail, do not fall back. Re-checking out the approved commit would publish
	// the right tree and hide that something moved the tag mid-release.
	if regexp.MustCompile(`(?m)^\s*git checkout "\$APPROVED"`).MatchString(body) {
		t.Error("release.yml checks out the approved commit after a mismatch. It must FAIL: " +
			"a mid-release tag move is an immutability breach a human needs to see, and " +
			"silently publishing the right tree hides it.")
	}

	// THE GUARD MUST ASK THE REMOTE. Comparing this job's `git rev-parse HEAD`
	// against the approved commit cannot detect anything: on a tag-push event
	// actions/checkout checks out the SHA captured in the triggering event, so
	// both jobs check out the same commit however the tag moves afterwards, and
	// the comparison passes by construction. Only resolving what the tag points
	// at NOW answers the question.
	if !strings.Contains(body, "git ls-remote --tags origin") {
		t.Error("the tag-immutability guard never queries the remote. Comparing two " +
			"checkouts of the same event SHA is a guard that cannot fail: actions/checkout " +
			"pins both jobs to the commit in the triggering event, while the publish step " +
			"resolves `tag_name` by NAME at publish time.")
	}
	if !strings.Contains(body, `refs/tags/${TAG}^{}`) {
		t.Error("the guard does not peel the tag ref. An ANNOTATED tag's refs/tags/<t> is " +
			"the tag OBJECT, not the commit, so an unpeeled comparison against a commit sha " +
			"never matches and the guard would refuse every annotated release.")
	}
}

// TestTagGuardIsAdjacentToPublication pins the guard's POSITION, which is as
// load-bearing as its logic.
//
// A time-of-check/time-of-use guard only covers the window it precedes. The
// first version of this check sat at the top of the publish job, leaving the
// build, the FIPS build, vet, the full race suite (900s timeout), coverage and
// release-note generation between it and `softprops/action-gh-release` —
// minutes in which the tag could move, after which the action still resolves
// `tag_name` by NAME and publishes the moved commit. Narrowing a TOCTOU window
// is not closing it.
//
// So: no executable step between the guard and the publish. A comment or a
// blank line is fine; a `run:` or a `uses:` is not.
func TestTagGuardIsAdjacentToPublication(t *testing.T) {
	path := filepath.Join(workflowsDir, "release.yml")
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read %s: %v", path, err)
	}
	body := string(raw)

	guard := strings.Index(body, "- name: Refuse a tag that moved after validation")
	publish := strings.Index(body, "- name: Create GitHub Release")
	if guard < 0 || publish < 0 {
		t.Fatalf("could not locate the guard (%d) and the publish step (%d) in release.yml; "+
			"if either was renamed, re-point this test rather than deleting it", guard, publish)
	}
	if guard > publish {
		t.Fatal("the tag guard runs AFTER the publish step — it protects nothing")
	}

	// Every step header strictly between the two.
	between := body[guard+1 : publish]
	var intervening []string
	for _, m := range regexp.MustCompile(`(?m)^      - (?:name|uses):\s*(.*)$`).FindAllStringSubmatch(between, -1) {
		intervening = append(intervening, strings.TrimSpace(m[1]))
	}
	if len(intervening) > 0 {
		t.Errorf("%d step(s) run between the tag-immutability guard and publication: %v\n\n"+
			"The guard covers only the window it precedes. Anything executable in "+
			"between is time in which the tag can move, after which the publish action "+
			"still resolves tag_name by NAME and publishes the moved commit. Move the "+
			"guard to sit immediately before `Create GitHub Release`.", len(intervening), intervening)
	}
}

// TestTagMovementGuardLogic exercises the guard's shell logic directly, against
// a real repository, rather than asserting that certain lines exist.
//
// The previous version of this test only searched release.yml for the
// comparison — and that comparison was inert, so the test passed over a guard
// that could never fire. Running the logic is the difference between checking
// that a control is present and checking that it works.
func TestTagMovementGuardLogic(t *testing.T) {
	dir := t.TempDir()
	remote := filepath.Join(t.TempDir(), "remote.git")

	run := func(name string, args ...string) string {
		t.Helper()
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		cmd := exec.CommandContext(ctx, name, args...)
		cmd.Dir = dir
		out, err := cmd.CombinedOutput()
		if err != nil {
			t.Fatalf("%s %v: %v\n%s", name, args, err, out)
		}
		return strings.TrimSpace(string(out))
	}

	initCtx, initCancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer initCancel()
	bare := exec.CommandContext(initCtx, "git", "init", "-q", "--bare", remote)
	bare.Dir = filepath.Dir(remote)
	if out, err := bare.CombinedOutput(); err != nil {
		t.Fatalf("git init --bare: %v\n%s", err, out)
	}
	if err := os.WriteFile(filepath.Join(dir, "f"), []byte("a"), 0o644); err != nil {
		t.Fatalf("write: %v", err)
	}
	run("git", "init", "-q", "-b", "main", ".")
	run("git", "-c", "commit.gpgsign=false", "-c", "user.name=t", "-c", "user.email=t@t", "add", ".")
	run("git", "-c", "commit.gpgsign=false", "-c", "user.name=t", "-c", "user.email=t@t", "commit", "-qm", "one")
	approved := run("git", "rev-parse", "HEAD")
	run("git", "remote", "add", "origin", remote)
	run("git", "-c", "tag.gpgsign=false", "-c", "user.name=t", "-c", "user.email=t@t", "tag", "-a", "v1.0.0-alpha", "-m", "v1.0.0-alpha")
	run("git", "push", "-q", "origin", "main", "refs/tags/v1.0.0-alpha")

	// The guard's resolution step, verbatim in shape.
	resolve := func() string {
		t.Helper()
		refs := run("git", "ls-remote", "--tags", "origin", "refs/tags/v1.0.0-alpha", "refs/tags/v1.0.0-alpha^{}")
		var peeled, plain string
		for _, line := range strings.Split(refs, "\n") {
			f := strings.Fields(line)
			if len(f) != 2 {
				continue
			}
			switch f[1] {
			case "refs/tags/v1.0.0-alpha^{}":
				peeled = f[0]
			case "refs/tags/v1.0.0-alpha":
				plain = f[0]
			}
		}
		if peeled != "" {
			return peeled
		}
		return plain
	}

	// An ANNOTATED tag must peel to the commit, not to the tag object.
	if got := resolve(); got != approved {
		tagObj := run("git", "rev-parse", "refs/tags/v1.0.0-alpha")
		t.Fatalf("resolution gave %s, want the commit %s (the tag OBJECT is %s).\n"+
			"An unpeeled comparison would refuse every annotated release.", got, approved, tagObj)
	}

	// Now MOVE the tag, exactly as the TOCTOU scenario does, and require the
	// resolution to change. This is what a HEAD-vs-APPROVED comparison could
	// never see: the checkout is unaffected.
	if err := os.WriteFile(filepath.Join(dir, "f"), []byte("b"), 0o644); err != nil {
		t.Fatalf("write: %v", err)
	}
	run("git", "-c", "commit.gpgsign=false", "-c", "user.name=t", "-c", "user.email=t@t", "commit", "-aqm", "two")
	moved := run("git", "rev-parse", "HEAD")
	run("git", "-c", "tag.gpgsign=false", "-c", "user.name=t", "-c", "user.email=t@t", "tag", "-f", "-a", "v1.0.0-alpha", "-m", "moved")
	run("git", "push", "-q", "--force", "origin", "refs/tags/v1.0.0-alpha")

	got := resolve()
	if got == approved {
		t.Fatal("the tag moved on the remote and resolution still reported the approved " +
			"commit — the guard cannot see a moved tag")
	}
	if got != moved {
		t.Fatalf("resolution gave %s after the move, want %s", got, moved)
	}

	// And a deleted tag must resolve to nothing, which the guard treats as fatal.
	run("git", "push", "-q", "origin", ":refs/tags/v1.0.0-alpha")
	if got := resolve(); got != "" {
		t.Fatalf("a deleted tag resolved to %q; the guard must fail on a missing tag "+
			"rather than publish", got)
	}
}
