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
	"strconv"
	"strings"
	"testing"
	"time"
)

// makefilePath (fips_gate_test.go) and repoRoot (dockerfile_integration_test.go)
// are declared elsewhere in this package and reused here.

// injectableInputs are every variable a caller could put a make function into
// and have it reach a parse-time or recipe-time expansion.
//
// The first four are the release-alpha inputs: attacker-influenced in the only
// sense that matters here, since a version string or a remote name gets copied
// out of an issue, a chat message or another repo's release notes and nothing
// about it looks like code.
//
// The last three are DERIVED build metadata, and they are on this list because
// renaming VERSION to BUILD_VERSION moved the hole rather than closing it: a
// command-line variable beats an ordinary makefile assignment, so
// `make build BUILD_VERSION='$(shell ...)'` reproduced the original defect
// under the new name, as did GIT_COMMIT and BUILD_DATE, which LDFLAGS expands
// on the same line. They are `override` in the Makefile, which discards a
// command-line value outright.
//
// NOT on this list, deliberately: GO, BUILD_DIR and the other documented knobs.
// Naming the program make should run is choosing a command on purpose. The
// property under test is narrower and is the one that matters — a value that
// looks like DATA must never be evaluated as make source.
var injectableInputs = []string{
	"VERSION", "RELEASE_REMOTE", "REPO_NAME", "AGENT_GIT",
	"BUILD_VERSION", "GIT_COMMIT", "BUILD_DATE",
}

// fixtureRemote is the name the release fixtures give their throwaway bare
// repository. Deliberately not "origin" or "dev": a test that borrows the
// repo's real remote name breaks the day that name changes, which is exactly
// what happened when the Gitea remote was renamed (#63).
const fixtureRemote = "fixture-remote"

// makeTargets are the three places the payload was observed to fire before the
// fix: the release target itself, an unrelated target, and a target that does
// not exist at all. The last two matter most — they prove the expansion was
// happening at PARSE time, before target selection, where no recipe guard could
// ever have reached it.
var makeTargets = []string{"release-alpha", "release", "build", "no-such-target-exists"}

// runMake runs `make -n <target> <vars...>` in the repository root and returns
// its combined output. -n is essential: the recipe is printed, never executed,
// so this test can never reach a git write no matter what the guards do.
func runMake(t *testing.T, target string, vars ...string) string {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	args := append([]string{"-n", "-C", repoRoot, target}, vars...)
	out, _ := exec.CommandContext(ctx, "make", args...).CombinedOutput()
	if ctx.Err() != nil {
		t.Fatalf("make timed out: %v", ctx.Err())
	}
	// A non-zero exit is expected and fine — most of these invocations are
	// supposed to be refused. Only the OUTPUT is under test.
	return string(out)
}

// TestSettableReleaseInputsAreNeverExpandedByMake is the regression test for a
// command-injection class that was live on main.
//
// A settable Make variable read anywhere in an IMMEDIATE (`:=`) assignment is
// expanded when the Makefile is PARSED — before a target is chosen, and before
// any guard in any recipe exists to reject it. While the build-metadata
// variable was itself named VERSION and expanded into LDFLAGS, this executed:
//
//	make build VERSION='$(shell echo pwned >&2)'
//
// on `build`, on `release-alpha`, and on a target that does not exist. The
// recipe's own guards were unreachable, being 1,500 lines further down.
//
// Two things keep it closed and this test fails if either is undone: the build
// metadata reads BUILD_VERSION so a command-line VERSION reaches nothing at
// parse time, and all four inputs cross into the recipe through $(value ...),
// which yields a variable's raw text instead of expanding it.
func TestSettableReleaseInputsAreNeverExpandedByMake(t *testing.T) {
	const canary = "MAKE_FUNCTION_EXECUTED"

	for _, input := range injectableInputs {
		for _, target := range makeTargets {
			t.Run(input+"/"+target, func(t *testing.T) {
				// A make function, not a shell construct. The distinction is the
				// whole point: shell metacharacters are contained by quoting,
				// whereas a make function embedded in a variable is evaluated by
				// make itself and quoting is no boundary at all.
				payload := input + `=$(shell echo ` + canary + ` >&2)v1.2.3-alpha`

				// A valid VERSION so the run gets past the first guard even when
				// the payload is on one of the other three inputs.
				out := runMake(t, target, "VERSION=v1.2.3-alpha", payload)

				if strings.Contains(out, canary) {
					t.Fatalf("make EXECUTED a function embedded in %s while running %q.\n"+
						"A release input is data, not source. Check that the build "+
						"metadata still reads BUILD_VERSION (never $(VERSION)) and that "+
						"release-alpha's export of %s still uses $(value ...).\n\noutput:\n%s",
						input, target, input, out)
				}
			})
		}
	}
}

// TestNoImmediateAssignmentReadsVERSION pins the structural invariant behind
// the behavioural test above.
//
// $(VERSION) in a `:=` assignment is the exact shape that made a release input
// executable at parse time. The behavioural test would catch a regression, but
// only for the payload it happens to try; this one rejects the shape itself,
// which is what a future edit would reintroduce by accident — renaming
// BUILD_VERSION back to VERSION "for consistency" looks entirely harmless.
//
// It cannot pass vacuously: it requires the LDFLAGS assignment to exist and to
// read BUILD_VERSION before it judges anything, so deleting or moving the line
// fails here rather than silently passing with nothing to check.
func TestNoImmediateAssignmentReadsVERSION(t *testing.T) {
	raw, err := os.ReadFile(makefilePath)
	if err != nil {
		t.Fatalf("read %s: %v", makefilePath, err)
	}

	var ldflags string
	immediate := regexp.MustCompile(`^\s*([A-Za-z_][A-Za-z0-9_]*)\s*:=(.*)$`)
	for i, line := range strings.Split(string(raw), "\n") {
		if strings.HasPrefix(strings.TrimSpace(line), "#") {
			continue // prose explaining the defect necessarily quotes it
		}
		m := immediate.FindStringSubmatch(line)
		if m == nil {
			continue
		}
		if m[1] == "LDFLAGS" {
			ldflags = m[2]
		}
		if strings.Contains(m[2], "$(VERSION)") {
			t.Errorf("Makefile:%d assigns with := and reads $(VERSION):\n  %s\n\n"+
				"VERSION is the release-alpha ARGUMENT. An immediate assignment "+
				"reading it is expanded at PARSE time, so `make <anything> "+
				"VERSION='$(shell ...)'` executes that function before any target "+
				"is selected. Read BUILD_VERSION for build metadata.",
				i+1, strings.TrimSpace(line))
		}
	}

	if ldflags == "" {
		t.Fatal("no `LDFLAGS :=` assignment found in the Makefile. This test " +
			"exists to prove that line reads BUILD_VERSION and not $(VERSION); " +
			"if version injection moved, re-point the test rather than deleting it.")
	}
	if !strings.Contains(ldflags, "$(BUILD_VERSION)") {
		t.Errorf("LDFLAGS no longer reads $(BUILD_VERSION):\n  LDFLAGS :=%s\n\n"+
			"The separation between the build-metadata variable and the release "+
			"argument is what keeps a settable VERSION out of parse-time expansion.",
			ldflags)
	}

	// Renaming alone was not enough: a command-line variable BEATS an ordinary
	// makefile assignment, so every variable LDFLAGS expands at parse time must
	// be `override` or the caller's value wins and gets evaluated as make source.
	body := string(raw)
	for _, name := range regexp.MustCompile(`\$\(([A-Z][A-Z0-9_]*)\)`).FindAllStringSubmatch(ldflags, -1) {
		v := name[1]
		if !regexp.MustCompile(`(?m)^override\s+` + v + `\s*:=`).MatchString(body) {
			t.Errorf("LDFLAGS expands $(%s) at parse time, but %s is not declared with `override`.\n\n"+
				"A command-line variable beats an ordinary makefile assignment, so "+
				"`make build %s='$(shell ...)'` would execute that function before any "+
				"target is selected. Derived metadata must be `override`; a value a "+
				"caller supplies must never be expanded at parse time.", v, v, v)
		}
	}
}

// TestReleaseAlphaRefusesBeforeMutatingAnything pins the guard chain.
//
// Every guard in release-alpha is read-only, and the first thing that changes
// state is `printf ... > VERSION`. A guard that stopped refusing would
// therefore not merely permit a bad release — it would write VERSION and commit
// before anything downstream noticed, and a published tag cannot be retracted.
//
// The refusals run against a DISPOSABLE COPY of the Makefile in t.TempDir(),
// never the real checkout, and no case supplies a resolvable remote. So the
// happy path is unreachable here by construction: this test cannot cut a
// release however wrong the recipe becomes.
func TestReleaseAlphaRefusesBeforeMutatingAnything(t *testing.T) {
	dir := t.TempDir()
	for _, name := range []string{"Makefile", "VERSION", "CHANGELOG.md", ".release-allowed-signers"} {
		raw, err := os.ReadFile(filepath.Join(repoRoot, name))
		if err != nil {
			t.Fatalf("read %s: %v", name, err)
		}
		if err := os.WriteFile(filepath.Join(dir, name), raw, 0o644); err != nil {
			t.Fatalf("write %s: %v", name, err)
		}
	}

	// A real repo on `main`, so the guards AFTER the branch check are reachable.
	// It has no remote, which is what keeps the happy path unreachable: the
	// recipe refuses at the remote check, several guards before the first write.
	for _, args := range [][]string{
		{"init", "-q", "-b", "main", "."},
		{"-c", "commit.gpgsign=false", "-c", "user.name=t", "-c", "user.email=t@t", "add", "."},
		{"-c", "commit.gpgsign=false", "-c", "user.name=t", "-c", "user.email=t@t", "commit", "-qm", "fixture"},
	} {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		cmd := exec.CommandContext(ctx, "git", args...)
		cmd.Dir = dir
		if out, err := cmd.CombinedOutput(); err != nil {
			cancel()
			t.Fatalf("git %v: %v\n%s", args, err, out)
		}
		cancel()
	}
	versionBefore, err := os.ReadFile(filepath.Join(dir, "VERSION"))
	if err != nil {
		t.Fatalf("read VERSION: %v", err)
	}

	// A disposable executable stands in for agent-git.sh so the guard chain is
	// reachable on ANY machine. The default AGENT_GIT is an absolute path into
	// the company checkout, which does not exist on a CI runner or on another
	// developer's box; depending on it would make these cases pass only here.
	// No case below reaches an invocation of it.
	stubAgentGit := filepath.Join(dir, "stub-agent-git.sh")
	if err := os.WriteFile(stubAgentGit, []byte("#!/usr/bin/env bash\nexit 0\n"), 0o755); err != nil {
		t.Fatalf("write stub: %v", err)
	}

	// Both release targets run ONE recipe (define release-body) and differ only
	// in the version form they accept, so every guard is exercised on each —
	// the stable `release` (R13) as much as `release-alpha`.
	for _, target := range []struct {
		name  string
		good  string // a well-formed version, which reaches the remote guard
		wrong []string
	}{
		{"release-alpha", "v0.9.1-alpha", []string{"VERSION=0.9.1-alpha", "VERSION=v0.9.1", "VERSION=v01.9.1-alpha"}},
		{"release", "v0.9.1", []string{"VERSION=0.9.1", "VERSION=v0.9.1-alpha", "VERSION=v01.9.1"}},
	} {
		t.Run(target.name, func(t *testing.T) {
			run := func(vars ...string) string {
				ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
				defer cancel()
				args := append([]string{"-C", dir, target.name, "AGENT_GIT=" + stubAgentGit}, vars...)
				cmd := exec.CommandContext(ctx, "make", args...)
				// GIT_TERMINAL_PROMPT=0 so an unreachable remote fails instead
				// of blocking on a credential prompt and hanging the suite.
				cmd.Env = append(os.Environ(), "GIT_TERMINAL_PROMPT=0")
				out, err := cmd.CombinedOutput()
				if ctx.Err() != nil {
					t.Fatalf("make timed out: %v", ctx.Err())
				}
				if err == nil {
					t.Fatalf("%s SUCCEEDED with %v — every case here must be "+
						"refused.\noutput:\n%s", target.name, vars, out)
				}
				return string(out)
			}

			if out := run(); !strings.Contains(out, "state the version explicitly") {
				t.Errorf("no version: refusal did not ask for one:\n%s", out)
			}
			for _, v := range target.wrong {
				if out := run(v); !strings.Contains(out, "not of the form") {
					t.Errorf("%s: the malformed version was not refused by the form guard:\n%s", v, out)
				}
			}
			if out := run("VERSION=" + target.good); !strings.Contains(out, "no git remote named") {
				t.Errorf("a well-formed %s did not reach the remote guard:\n%s", target.good, out)
			}

			// The accepted form is `override`: a command-line REL_PATTERN (or
			// REL_FORM) must not widen it, or `release-alpha` could cut a
			// stable tag and `release` a pre-release.
			wrongForm := target.wrong[1]
			if out := run(wrongForm, "REL_PATTERN=.", "REL_FORM=anything"); !strings.Contains(out, "not of the form") {
				t.Errorf("a command-line REL_PATTERN widened the accepted form:\n%s", out)
			}

			// An AMBIENT VERSION must not drive a release. `$(value VERSION)`
			// imports an environment variable exactly like a command-line one,
			// so without the $(origin ...) filter a bare make in a shell or CI
			// job that exports VERSION would cut whatever it inherited.
			ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
			defer cancel()
			cmd := exec.CommandContext(ctx, "make", "-C", dir, target.name, "AGENT_GIT="+stubAgentGit)
			cmd.Env = append(os.Environ(), "GIT_TERMINAL_PROMPT=0", "VERSION="+target.good)
			out, err := cmd.CombinedOutput()
			if err == nil {
				t.Fatalf("%s accepted an inherited VERSION:\n%s", target.name, out)
			}
			if !strings.Contains(string(out), "state the version explicitly") {
				t.Errorf("an environment VERSION was treated as explicit; it must be command-line only.\n%s", out)
			}

			// REPO_NAME is `override`, so a command-line value is DISCARDED.
			// It IS this repository's name; no caller value could be correct,
			// and deriving it from the checkout directory signs a release as
			// the wrong agent.
			out2 := run("VERSION="+target.good, "REPO_NAME=")
			if strings.Contains(out2, "REPO_NAME is empty") {
				t.Errorf("a command-line REPO_NAME took effect; it must be `override` in the Makefile.\n%s", out2)
			}

			// The recipe's own inputs are `override` too: a command-line
			// REL_* would otherwise replace the value derived above and walk
			// around every guard on it — a REL_VERSION past the command-line
			// VERSION filter, a REL_REPO past REPO_NAME's override, a
			// REL_AGENT_GIT or REL_REMOTE past their Makefile values.
			if out := run("REL_VERSION=" + target.good); !strings.Contains(out, "state the version explicitly") {
				t.Errorf("a command-line REL_VERSION stood in for VERSION:\n%s", out)
			}
			for _, tc := range []struct{ override, refusal string }{
				{"REL_REPO=", "REPO_NAME is empty"},
				{"REL_AGENT_GIT=/nonexistent/agent-git.sh", "AGENT_GIT does not point"},
				{"REL_REMOTE=", "RELEASE_REMOTE is empty"},
			} {
				out := run("VERSION="+target.good, tc.override)
				if strings.Contains(out, tc.refusal) {
					t.Errorf("a command-line %s took effect; it must be `override` in the Makefile.\n%s", tc.override, out)
				}
				// And the recipe must have got PAST every input guard, to the
				// remote lookup this fixture cannot satisfy: an absent refusal
				// proves nothing if an earlier guard stopped it first.
				if !strings.Contains(out, "no git remote named") {
					t.Errorf("with %s the recipe stopped before the remote lookup, so the override check proved nothing.\n%s", tc.override, out)
				}
			}
		})
	}

	versionAfter, err := os.ReadFile(filepath.Join(dir, "VERSION"))
	if err != nil {
		t.Fatalf("read VERSION: %v", err)
	}
	if string(versionAfter) != string(versionBefore) {
		t.Fatalf("a REFUSED release rewrote VERSION: %q -> %q.\n"+
			"Every guard must run before the file is touched; writing it and then "+
			"failing leaves the tree claiming a version that was never released.",
			versionBefore, versionAfter)
	}
}

// TestReleaseRemoteDefaultsToDev pins the DEFAULT, which is the only production
// behaviour this file's other release tests do not cover.
//
// They all pass RELEASE_REMOTE explicitly — deliberately, so a rename of the
// repo's real remote cannot break them — and that leaves the default itself
// unverified: reverting it to `origin`, or to any other name, would keep the
// suite green while `make release-alpha` selected the wrong remote.
//
// So this fixture gives its disposable repository exactly one remote, named
// `dev`, and does NOT override RELEASE_REMOTE. If the default is `dev` the run
// resolves it and stops at a later, read-only guard; if the default is anything
// else the run stops at "no git remote named <that>", naming the wrong value.
func TestReleaseRemoteDefaultsToDev(t *testing.T) {
	dir := t.TempDir()
	remote := filepath.Join(t.TempDir(), "remote.git")

	for _, name := range []string{"Makefile", "VERSION", "CHANGELOG.md", ".release-allowed-signers"} {
		raw, err := os.ReadFile(filepath.Join(repoRoot, name))
		if err != nil {
			t.Fatalf("read %s: %v", name, err)
		}
		if err := os.WriteFile(filepath.Join(dir, name), raw, 0o644); err != nil {
			t.Fatalf("write %s: %v", name, err)
		}
	}
	stub := filepath.Join(dir, "stub-agent-git.sh")
	if err := os.WriteFile(stub, []byte("#!/usr/bin/env bash\nexit 0\n"), 0o755); err != nil {
		t.Fatalf("write stub: %v", err)
	}

	git := func(args ...string) {
		t.Helper()
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		cmd := exec.CommandContext(ctx, "git", args...)
		cmd.Dir = dir
		if out, err := cmd.CombinedOutput(); err != nil {
			t.Fatalf("git %v: %v\n%s", args, err, out)
		}
	}
	initCtx, initCancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer initCancel()
	bare := exec.CommandContext(initCtx, "git", "init", "-q", "--bare", remote)
	bare.Dir = filepath.Dir(remote)
	if out, err := bare.CombinedOutput(); err != nil {
		t.Fatalf("git init --bare: %v\n%s", err, out)
	}
	git("init", "-q", "-b", "main", ".")
	git("-c", "commit.gpgsign=false", "-c", "user.name=t", "-c", "user.email=t@t", "add", ".")
	git("-c", "commit.gpgsign=false", "-c", "user.name=t", "-c", "user.email=t@t", "commit", "-qm", "fixture")
	// EXACTLY ONE remote, named dev. No RELEASE_REMOTE override below.
	git("remote", "add", "dev", remote)

	runCtx, runCancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer runCancel()
	cmd := exec.CommandContext(runCtx, "make", "-C", dir, "release-alpha",
		"VERSION=v99.0.0-alpha", "AGENT_GIT="+stub)
	cmd.Env = append(os.Environ(), "GIT_TERMINAL_PROMPT=0")
	out, _ := cmd.CombinedOutput()

	if strings.Contains(string(out), "no git remote named") {
		t.Errorf("release-alpha did not resolve the `dev` remote, so RELEASE_REMOTE does "+
			"not default to `dev`. conventions/version-control.md 1 names the Gitea "+
			"remote `dev`, and a default that points elsewhere makes the target "+
			"unrunnable on a conforming checkout (#63).\n%s", out)
	}
	// It must still refuse — the fixture's CHANGELOG has no v99.0.0-alpha section
	// — which is what proves the run got PAST remote resolution rather than
	// succeeding for some unrelated reason.
	if !strings.Contains(string(out), "CHANGELOG.md has no") {
		t.Errorf("expected the run to reach the CHANGELOG guard, which sits after remote "+
			"resolution; got:\n%s", out)
	}
}

// TestReleaseAlphaRefusesAnUnsignedOrMisattributedRelease is the security
// property behind AGENT_GIT staying settable.
//
// AGENT_GIT must vary — the company checkout is not at one absolute path on
// every machine — so the recipe cannot enforce its identity claim by trusting
// the tool. It verifies the OUTCOME instead: after the commit and tag exist and
// BEFORE anything is pushed, the release commit must be authored by
// agent-go-quicraft and both objects must carry a signature.
//
// This test substitutes an AGENT_GIT that gets the author RIGHT, does not sign,
// and plants "gpgsig" and "SSH SIGNATURE" inside the commit and tag MESSAGES —
// the precise shape that defeats grepping `cat-file` output, and the reason the
// recipe asks git to verify the cryptography instead. It requires the recipe to
// stop. It gives the fixture a real bare remote, so the push
// WOULD succeed if the checks were absent; that is what makes the assertion
// mean something rather than being satisfied by some earlier guard.
func TestReleaseAlphaRefusesAnUnsignedOrMisattributedRelease(t *testing.T) {
	dir := t.TempDir()
	remote := filepath.Join(t.TempDir(), "remote.git")

	for _, name := range []string{"Makefile", "VERSION", "CHANGELOG.md", ".release-allowed-signers"} {
		raw, err := os.ReadFile(filepath.Join(repoRoot, name))
		if err != nil {
			t.Fatalf("read %s: %v", name, err)
		}
		if err := os.WriteFile(filepath.Join(dir, name), raw, 0o644); err != nil {
			t.Fatalf("write %s: %v", name, err)
		}
	}
	// A version the fixture CHANGELOG has a heading for, so the run reaches the
	// identity checks rather than stopping at the changelog guard.
	ver := "v" + strings.TrimSpace(readFile(t, filepath.Join(dir, "VERSION")))
	release := bumpPatch(ver)
	appendFile(t, filepath.Join(dir, "CHANGELOG.md"), "\n## ["+strings.TrimPrefix(release, "v")+"] - 2026-01-01\n\n- fixture\n")

	// An AGENT_GIT that gets the AUTHOR RIGHT, does not sign, and plants the
	// marker strings a naive check would look for INSIDE the commit and tag
	// MESSAGES. This is the precise bypass that makes grepping `cat-file`
	// output insufficient: the object text contains "gpgsig" and
	// "SSH SIGNATURE" while nothing is actually signed. Only real verification
	// against the agent's published key rejects it.
	shim := filepath.Join(dir, "fake-agent-git.sh")
	writeFile(t, shim, "#!/usr/bin/env bash\n"+
		"repo=\"$1\"; shift\n"+
		"msg='forged\n\ngpgsig -----BEGIN SSH SIGNATURE-----\nnot actually a signature\n'\n"+
		"args=()\n"+
		"for a in \"$@\"; do\n"+
		"  case \"$a\" in -m) args+=(-m); shift_msg=1;; *) args+=(\"$a\");; esac\n"+
		"done\n"+
		"case \"$1\" in\n"+
		"  commit) exec git -c commit.gpgsign=false -c user.name=\"agent-$repo\" "+
		"-c user.email=\"agent-$repo@example.internal\" commit -m \"$msg\" ;;\n"+
		"  tag)    exec git -c tag.gpgsign=false -c user.name=\"agent-$repo\" "+
		"-c user.email=\"agent-$repo@example.internal\" tag -a \"$3\" -m \"$msg\" ;;\n"+
		"  *)      exec git -c commit.gpgsign=false -c tag.gpgsign=false -c user.name=\"agent-$repo\" "+
		"-c user.email=\"agent-$repo@example.internal\" \"$@\" ;;\n"+
		"esac\n", 0o755)

	git := func(args ...string) {
		t.Helper()
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		cmd := exec.CommandContext(ctx, "git", args...)
		cmd.Dir = dir
		if out, err := cmd.CombinedOutput(); err != nil {
			t.Fatalf("git %v: %v\n%s", args, err, out)
		}
	}
	// Every git invocation below sets Dir explicitly. Without it the command
	// inherits the test process's CWD, and in a linked worktree that is a .git
	// FILE pointing at the parent checkout — unresolvable wherever the parent
	// is not present (a container mount, a CI workspace), which makes git fail
	// before it even reads its arguments.
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	initBare := exec.CommandContext(ctx, "git", "init", "-q", "--bare", remote)
	initBare.Dir = filepath.Dir(remote)
	if out, err := initBare.CombinedOutput(); err != nil {
		t.Fatalf("git init --bare: %v\n%s", err, out)
	}
	git("init", "-q", "-b", "main", ".")
	git("-c", "commit.gpgsign=false", "-c", "user.name=t", "-c", "user.email=t@t", "add", ".")
	git("-c", "commit.gpgsign=false", "-c", "user.name=t", "-c", "user.email=t@t", "commit", "-qm", "fixture")
	// Named explicitly and passed below, so these tests do not depend on
	// whatever RELEASE_REMOTE defaults to — that default has changed once
	// already (origin -> dev, #63) and broke them.
	git("remote", "add", fixtureRemote, remote)

	runCtx, runCancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer runCancel()
	cmd := exec.CommandContext(runCtx, "make", "-C", dir, "release-alpha",
		"VERSION="+release, "AGENT_GIT="+shim, "RELEASE_REMOTE="+fixtureRemote)
	cmd.Env = append(os.Environ(), "GIT_TERMINAL_PROMPT=0")
	out, err := cmd.CombinedOutput()
	if err == nil {
		t.Fatalf("release-alpha SUCCEEDED with an AGENT_GIT that writes as the wrong author "+
			"and does not sign. The identity claim is unenforced.\n%s", out)
	}
	if !strings.Contains(string(out), "does not verify") && !strings.Contains(string(out), "authored by") &&
		!strings.Contains(string(out), "cannot be VERIFIED") {
		t.Errorf("refused, but not for the identity/signature reason — some earlier guard fired "+
			"instead, so this test is not exercising the check it exists for:\n%s", out)
	}

	// The whole point of checking BEFORE the push: nothing reached the remote.
	lsCtx, lsCancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer lsCancel()
	lsCmd := exec.CommandContext(lsCtx, "git", "ls-remote", remote)
	lsCmd.Dir = dir
	refs, lsErr := lsCmd.CombinedOutput()
	if lsErr != nil {
		t.Fatalf("git ls-remote: %v\n%s", lsErr, refs)
	}
	if strings.TrimSpace(string(refs)) != "" {
		t.Fatalf("a REFUSED release published refs to the remote:\n%s\n"+
			"The identity checks must run before the push; a published tag cannot be retracted.", refs)
	}
}

func readFile(t *testing.T, p string) string {
	t.Helper()
	b, err := os.ReadFile(p)
	if err != nil {
		t.Fatalf("read %s: %v", p, err)
	}
	return string(b)
}

func writeFile(t *testing.T, p, content string, mode os.FileMode) {
	t.Helper()
	if err := os.WriteFile(p, []byte(content), mode); err != nil {
		t.Fatalf("write %s: %v", p, err)
	}
}

func appendFile(t *testing.T, p, content string) {
	t.Helper()
	f, err := os.OpenFile(p, os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		t.Fatalf("open %s: %v", p, err)
	}
	defer f.Close()
	if _, err := f.WriteString(content); err != nil {
		t.Fatalf("append %s: %v", p, err)
	}
}

// bumpPatch turns vX.Y.Z-alpha into vX.Y.(Z+1)-alpha so the fixture never names
// a version that already exists as a tag.
func bumpPatch(v string) string {
	base := strings.TrimSuffix(strings.TrimPrefix(v, "v"), "-alpha")
	parts := strings.Split(base, ".")
	if len(parts) != 3 {
		return "v9.9.9-alpha"
	}
	n, err := strconv.Atoi(parts[2])
	if err != nil {
		return "v9.9.9-alpha"
	}
	return "v" + parts[0] + "." + parts[1] + "." + strconv.Itoa(n+1) + "-alpha"
}

// TestReleaseAlphaRefusesAKeyItWasNotPinnedTo closes the trust-anchor
// substitution, which is the failure mode that makes "we verify the signature"
// worth anything.
//
// An earlier version built the allowed-signers file from a key found under
// AGENT_GIT's own directory tree. That verifies nothing: a substituted helper
// supplies the key it is checked against, so it signs the release with its own
// keypair, drops the matching public half where the recipe looks, and every
// pre-push check passes against the attacker's key.
//
// This test is that attack. The helper generates a fresh ed25519 key, signs the
// release commit and tag with it as the correct author, and plants the matching
// public key at the exact path the old implementation read
// (<root>/infra/gitea/agent-keys/agent-go-quicraft.key.pub) beneath a fabricated
// company root it also controls. The recipe must still refuse, because its
// anchor is the in-repo .release-allowed-signers and nothing else.
func TestReleaseAlphaRefusesAKeyItWasNotPinnedTo(t *testing.T) {
	if _, err := exec.LookPath("ssh-keygen"); err != nil {
		t.Skipf("ssh-keygen not available: %v", err)
	}
	dir := t.TempDir()
	remote := filepath.Join(t.TempDir(), "remote.git")
	fakeRoot := filepath.Join(t.TempDir(), "company")

	for _, name := range []string{"Makefile", "VERSION", "CHANGELOG.md", ".release-allowed-signers"} {
		raw, err := os.ReadFile(filepath.Join(repoRoot, name))
		if err != nil {
			t.Fatalf("read %s: %v", name, err)
		}
		if err := os.WriteFile(filepath.Join(dir, name), raw, 0o644); err != nil {
			t.Fatalf("write %s: %v", name, err)
		}
	}
	release := bumpPatch("v" + strings.TrimSpace(readFile(t, filepath.Join(dir, "VERSION"))))
	appendFile(t, filepath.Join(dir, "CHANGELOG.md"), "\n## ["+strings.TrimPrefix(release, "v")+"] - 2026-01-01\n\n- fixture\n")

	// The attacker's own signing key.
	keys := filepath.Join(fakeRoot, "infra", "gitea", "agent-keys")
	if err := os.MkdirAll(filepath.Join(fakeRoot, "software", "scripts"), 0o755); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	if err := os.MkdirAll(keys, 0o755); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	key := filepath.Join(keys, "agent-go-quicraft.key")
	kgCtx, kgCancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer kgCancel()
	if out, err := exec.CommandContext(kgCtx, "ssh-keygen", "-q", "-t", "ed25519", "-N", "", "-C", "forged", "-f", key).CombinedOutput(); err != nil {
		t.Fatalf("ssh-keygen: %v\n%s", err, out)
	}

	// A helper that signs as the right author with the WRONG key, planted where
	// the old, unsafe implementation would have looked for its trust anchor.
	shim := filepath.Join(fakeRoot, "software", "scripts", "agent-git.sh")
	writeFile(t, shim, "#!/usr/bin/env bash\nrepo=\"$1\"; shift\n"+
		"exec git -c gpg.format=ssh -c user.signingkey="+key+" -c commit.gpgsign=true -c tag.gpgsign=true "+
		"-c user.name=\"agent-$repo\" -c user.email=\"agent-$repo@example.internal\" \"$@\"\n", 0o755)

	git := func(args ...string) {
		t.Helper()
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		cmd := exec.CommandContext(ctx, "git", args...)
		cmd.Dir = dir
		if out, err := cmd.CombinedOutput(); err != nil {
			t.Fatalf("git %v: %v\n%s", args, err, out)
		}
	}
	initCtx, initCancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer initCancel()
	initBare := exec.CommandContext(initCtx, "git", "init", "-q", "--bare", remote)
	initBare.Dir = filepath.Dir(remote)
	if out, err := initBare.CombinedOutput(); err != nil {
		t.Fatalf("git init --bare: %v\n%s", err, out)
	}
	git("init", "-q", "-b", "main", ".")
	git("-c", "commit.gpgsign=false", "-c", "user.name=t", "-c", "user.email=t@t", "add", ".")
	git("-c", "commit.gpgsign=false", "-c", "user.name=t", "-c", "user.email=t@t", "commit", "-qm", "fixture")
	// Named explicitly and passed below, so these tests do not depend on
	// whatever RELEASE_REMOTE defaults to — that default has changed once
	// already (origin -> dev, #63) and broke them.
	git("remote", "add", fixtureRemote, remote)

	runCtx, runCancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer runCancel()
	cmd := exec.CommandContext(runCtx, "make", "-C", dir, "release-alpha", "VERSION="+release, "AGENT_GIT="+shim, "RELEASE_REMOTE="+fixtureRemote)
	cmd.Env = append(os.Environ(), "GIT_TERMINAL_PROMPT=0")
	out, err := cmd.CombinedOutput()
	if err == nil {
		t.Fatalf("release-alpha PUBLISHED a release signed with a key it was never pinned to.\n"+
			"The trust anchor must not be reachable from AGENT_GIT.\n%s", out)
	}
	if !strings.Contains(string(out), "does not verify") {
		t.Errorf("refused, but not at signature verification — some earlier guard fired, so "+
			"this test is not exercising the trust-anchor substitution:\n%s", out)
	}

	lsCtx, lsCancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer lsCancel()
	lsCmd := exec.CommandContext(lsCtx, "git", "ls-remote", remote)
	lsCmd.Dir = dir
	refs, lsErr := lsCmd.CombinedOutput()
	if lsErr != nil {
		t.Fatalf("git ls-remote: %v\n%s", lsErr, refs)
	}
	if strings.TrimSpace(string(refs)) != "" {
		t.Fatalf("a forged release reached the remote:\n%s", refs)
	}
}
