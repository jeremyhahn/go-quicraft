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
// sense that matters here, since a version string, a remote name or a path gets
// copied out of an issue, a chat message or another repo's release notes and
// nothing about it looks like code.
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
	"VERSION", "RELEASE_REMOTE", "RELEASE_GIT", "RELEASE_ALLOWED_SIGNERS",
	"BUILD_VERSION", "GIT_COMMIT", "BUILD_DATE",
}

// fixtureRemote is the name the release fixtures give their throwaway bare
// repository. Deliberately not "origin" or "dev": a test that borrows the
// repo's real remote name breaks the day that name changes.
const fixtureRemote = "fixture-remote"

// makeTargets are the three places the payload was observed to fire before the
// fix: the release target itself, an unrelated target, and a target that does
// not exist at all. The last two matter most — they prove the expansion was
// happening at PARSE time, before target selection, where no recipe guard could
// ever have reached it.
var makeTargets = []string{"release-alpha", "release", "release-candidate", "build", "no-such-target-exists"}

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

// releaseFixture is a disposable checkout for the release recipe: a copy of the
// Makefile, VERSION and CHANGELOG.md committed to a fresh repository on main.
// Every test that runs the recipe for real runs it here, never in the actual
// checkout, so no test can cut a release of this repository.
type releaseFixture struct {
	t       *testing.T
	dir     string // the checkout make runs in
	remote  string // bare repository registered as fixtureRemote; "" when none
	release string // the next patch version; CHANGELOG.md has a heading for it
}

// newReleaseFixture builds the fixture. With withRemote it also registers a bare
// repository as fixtureRemote, so a release that passes every check WOULD be
// published: that is what makes a refusal mean something rather than being
// satisfied by some earlier guard.
//
// The fixture does not copy .release-allowed-signers. Each test names the
// signers file it means, which keeps these tests independent of the private
// trust anchor and runnable in a published export.
func newReleaseFixture(t *testing.T, withRemote bool) *releaseFixture {
	t.Helper()
	f := &releaseFixture{t: t, dir: t.TempDir()}
	// The Makefile includes the tool pin file, so it comes along.
	// contrib/dragonboat/go.mod comes along because the release commit pins
	// its requirement of the parent.
	for _, sub := range []string{"scripts", "contrib/dragonboat"} {
		if err := os.MkdirAll(filepath.Join(f.dir, sub), 0o755); err != nil {
			t.Fatalf("mkdir %s: %v", sub, err)
		}
	}
	for _, name := range []string{"Makefile", "VERSION", "CHANGELOG.md", "scripts/tool-pins.sh", "contrib/dragonboat/go.mod"} {
		writeFile(t, filepath.Join(f.dir, name), readFile(t, filepath.Join(repoRoot, name)), 0o644)
	}
	f.release = bumpPatch("v" + strings.TrimSpace(f.version()))
	appendFile(t, filepath.Join(f.dir, "CHANGELOG.md"),
		"\n## ["+strings.TrimPrefix(f.release, "v")+"] - 2026-01-01\n\n- fixture\n")

	f.git("init", "-q", "-b", "main", ".")
	f.git("-c", "commit.gpgsign=false", "-c", "user.name=t", "-c", "user.email=t@t", "add", ".")
	f.git("-c", "commit.gpgsign=false", "-c", "user.name=t", "-c", "user.email=t@t", "commit", "-qm", "fixture")
	if withRemote {
		f.remote = filepath.Join(t.TempDir(), "remote.git")
		f.gitIn(filepath.Dir(f.remote), "init", "-q", "--bare", f.remote)
		// Named explicitly and passed below, so these tests do not depend on
		// whatever RELEASE_REMOTE defaults to, which can change.
		f.git("remote", "add", fixtureRemote, f.remote)
	}
	return f
}

// isolatedEnv is the environment every fixture git and make runs with. The
// global and system git configuration are ignored: a developer's own
// commit.gpgsign or user.signingkey would otherwise sign the "unsigned" cases
// and make them pass for the wrong reason. GIT_TERMINAL_PROMPT=0 makes an
// unreachable remote fail instead of blocking on a credential prompt.
func isolatedEnv() []string {
	env := make([]string, 0, len(os.Environ())+3)
	for _, kv := range os.Environ() {
		// GIT_CONFIG_COUNT/KEY_n/VALUE_n and GIT_CONFIG_PARAMETERS inject git
		// configuration that GIT_CONFIG_GLOBAL and GIT_CONFIG_NOSYSTEM do not
		// switch off: a launcher that sets user.signingkey or gpg.ssh.program
		// that way would sign the "unsigned" cases and verify the wrong key.
		if strings.HasPrefix(kv, "GIT_CONFIG_COUNT=") ||
			strings.HasPrefix(kv, "GIT_CONFIG_KEY_") ||
			strings.HasPrefix(kv, "GIT_CONFIG_VALUE_") ||
			strings.HasPrefix(kv, "GIT_CONFIG_PARAMETERS=") {
			continue
		}
		env = append(env, kv)
	}
	return append(env,
		"GIT_TERMINAL_PROMPT=0", "GIT_CONFIG_GLOBAL="+os.DevNull, "GIT_CONFIG_NOSYSTEM=1")
}

// git runs git in the fixture checkout and fails the test on error.
func (f *releaseFixture) git(args ...string) string {
	f.t.Helper()
	return f.gitIn(f.dir, args...)
}

// gitIn runs git in dir. Dir is always set explicitly: without it the command
// inherits the test process's CWD, which in a linked worktree is a .git FILE
// pointing at a parent checkout that may not be present (a container mount, a
// CI workspace).
func (f *releaseFixture) gitIn(dir string, args ...string) string {
	f.t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, "git", args...)
	cmd.Dir = dir
	cmd.Env = isolatedEnv()
	out, err := cmd.CombinedOutput()
	if err != nil {
		f.t.Fatalf("git %v: %v\n%s", args, err, out)
	}
	return string(out)
}

// make runs `make -C <fixture> <target> <vars...>` and returns its combined
// output and exit error.
func (f *releaseFixture) make(target string, vars ...string) (string, error) {
	f.t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, "make", append([]string{"-C", f.dir, target}, vars...)...)
	cmd.Env = isolatedEnv()
	out, err := cmd.CombinedOutput()
	if ctx.Err() != nil {
		f.t.Fatalf("make timed out: %v\n%s", ctx.Err(), out)
	}
	return string(out), err
}

// releaseAlpha runs release-alpha for f.release against fixtureRemote.
func (f *releaseFixture) releaseAlpha(vars ...string) (string, error) {
	f.t.Helper()
	return f.make("release-alpha", append([]string{"VERSION=" + f.release, "RELEASE_REMOTE=" + fixtureRemote}, vars...)...)
}

func (f *releaseFixture) version() string {
	f.t.Helper()
	return readFile(f.t, filepath.Join(f.dir, "VERSION"))
}

// remoteRefs lists every ref the fixture remote holds.
func (f *releaseFixture) remoteRefs() string {
	f.t.Helper()
	return strings.TrimSpace(f.git("ls-remote", f.remote))
}

// requireNothingPublished fails the test if a refused release reached the
// remote. The whole point of checking before the push is that a published tag
// cannot be retracted.
func (f *releaseFixture) requireNothingPublished() {
	f.t.Helper()
	if refs := f.remoteRefs(); refs != "" {
		f.t.Fatalf("a REFUSED release published refs to the remote:\n%s", refs)
	}
}

// signWith configures the fixture checkout so plain git writes as email and
// signs with key: the configuration a maintainer cutting a release with
// RELEASE_GIT's default carries in their own git config.
func (f *releaseFixture) signWith(email, key string, signCommits, signTags bool) {
	f.t.Helper()
	f.git("config", "user.name", "Release Maintainer")
	f.git("config", "user.email", email)
	f.git("config", "gpg.format", "ssh")
	f.git("config", "user.signingkey", key)
	f.git("config", "commit.gpgsign", strconv.FormatBool(signCommits))
	f.git("config", "tag.gpgsign", strconv.FormatBool(signTags))
}

// newSigningKey generates an unencrypted ed25519 key in a fresh directory and
// returns the private key path and the "<type> <base64>" public half an
// allowed-signers line carries. It skips the test when ssh-keygen is absent.
func newSigningKey(t *testing.T) (key, pub string) {
	t.Helper()
	if _, err := exec.LookPath("ssh-keygen"); err != nil {
		t.Skipf("ssh-keygen not available: %v", err)
	}
	key = filepath.Join(t.TempDir(), "id_ed25519")
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	if out, err := exec.CommandContext(ctx, "ssh-keygen", "-q", "-t", "ed25519", "-N", "", "-C", "fixture", "-f", key).CombinedOutput(); err != nil {
		t.Fatalf("ssh-keygen: %v\n%s", err, out)
	}
	fields := strings.Fields(readFile(t, key+".pub"))
	if len(fields) < 2 {
		t.Fatalf("unparseable public key %s.pub", key)
	}
	return key, fields[0] + " " + fields[1]
}

// writeSigners writes an allowed-signers file OUTSIDE the fixture checkout (a
// file inside it would fail the clean-tree guard) and returns its path.
func writeSigners(t *testing.T, lines ...string) string {
	t.Helper()
	p := filepath.Join(t.TempDir(), "allowed-signers")
	writeFile(t, p, "# fixture trust anchor\n"+strings.Join(lines, "\n")+"\n", 0o644)
	return p
}

// TestReleaseAlphaRefusesBeforeMutatingAnything pins the guard chain.
//
// Every guard in release-alpha is read-only, and the first thing that changes
// state is `printf ... > VERSION`. A guard that stopped refusing would
// therefore not merely permit a bad release — it would write VERSION and commit
// before anything downstream noticed, and a published tag cannot be retracted.
//
// The fixture has no remote, which keeps the happy path unreachable by
// construction: the recipe refuses at the remote check, several guards before
// the first write, so this test cannot cut a release however wrong the recipe
// becomes.
func TestReleaseAlphaRefusesBeforeMutatingAnything(t *testing.T) {
	f := newReleaseFixture(t, false)
	versionBefore := f.version()
	// Existence is all the guards before the first write check; the
	// verification that reads the file runs after the commit, which no case
	// here reaches.
	signers := writeSigners(t)

	// Both release targets run ONE recipe (define release-body) and differ only
	// in the version form they accept, so every guard is exercised on each —
	// the stable `release` as much as `release-alpha`.
	for _, target := range []struct {
		name  string
		good  string // a well-formed version, which reaches the remote guard
		wrong []string
	}{
		{"release-alpha", "v0.9.1-alpha", []string{"VERSION=0.9.1-alpha", "VERSION=v0.9.1", "VERSION=v01.9.1-alpha"}},
		{"release", "v0.9.1", []string{"VERSION=0.9.1", "VERSION=v0.9.1-alpha", "VERSION=v01.9.1"}},
		{"release-candidate", "v0.9.1-rc.1", []string{"VERSION=0.9.1-rc.1", "VERSION=v0.9.1", "VERSION=v0.9.1-alpha", "VERSION=v0.9.1-rc.0", "VERSION=v0.9.1-rc.01", "VERSION=v0.9.1-rc", "VERSION=v0.9.1-rc.1.2", "VERSION=v01.9.1-rc.1"}},
	} {
		t.Run(target.name, func(t *testing.T) {
			run := func(vars ...string) string {
				t.Helper()
				out, err := f.make(target.name, append([]string{"RELEASE_ALLOWED_SIGNERS=" + signers}, vars...)...)
				if err == nil {
					t.Fatalf("%s SUCCEEDED with %v — every case here must be "+
						"refused.\noutput:\n%s", target.name, vars, out)
				}
				return out
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

			// A RELEASE_GIT that names nothing runnable, and a signers file that
			// does not exist, are refused before VERSION is written: the recipe
			// fails closed rather than committing and then discovering it cannot
			// sign or verify.
			if out := run("VERSION="+target.good, "RELEASE_GIT=/nonexistent/git"); !strings.Contains(out, "RELEASE_GIT does not name") {
				t.Errorf("a RELEASE_GIT naming no executable was not refused up front:\n%s", out)
			}
			if out := run("VERSION="+target.good, "RELEASE_ALLOWED_SIGNERS="+filepath.Join(t.TempDir(), "absent")); !strings.Contains(out, "allowed-signers file") {
				t.Errorf("a missing allowed-signers file was not refused up front:\n%s", out)
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
			cmd := exec.CommandContext(ctx, "make", "-C", f.dir, target.name, "RELEASE_ALLOWED_SIGNERS="+signers)
			cmd.Env = append(isolatedEnv(), "VERSION="+target.good)
			out, err := cmd.CombinedOutput()
			if err == nil {
				t.Fatalf("%s accepted an inherited VERSION:\n%s", target.name, out)
			}
			if !strings.Contains(string(out), "state the version explicitly") {
				t.Errorf("an environment VERSION was treated as explicit; it must be command-line only.\n%s", out)
			}

			// The recipe's own inputs are `override`: a command-line REL_*
			// would otherwise replace the value derived above and walk around
			// every guard on it — a REL_VERSION past the command-line VERSION
			// filter, a REL_GIT, REL_SIGNERS or REL_REMOTE past the variable
			// the caller is meant to set.
			if out := run("REL_VERSION=" + target.good); !strings.Contains(out, "state the version explicitly") {
				t.Errorf("a command-line REL_VERSION stood in for VERSION:\n%s", out)
			}
			for _, tc := range []struct{ override, refusal string }{
				{"REL_GIT=/nonexistent/git", "RELEASE_GIT does not name"},
				{"REL_SIGNERS=/nonexistent/allowed-signers", "allowed-signers file"},
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

	if versionAfter := f.version(); versionAfter != versionBefore {
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
	f := newReleaseFixture(t, false)
	remote := filepath.Join(t.TempDir(), "remote.git")
	f.gitIn(filepath.Dir(remote), "init", "-q", "--bare", remote)
	// EXACTLY ONE remote, named dev. No RELEASE_REMOTE override below.
	f.git("remote", "add", "dev", remote)

	out, _ := f.make("release-alpha", "VERSION=v99.0.0-alpha", "RELEASE_ALLOWED_SIGNERS="+writeSigners(t))
	if strings.Contains(out, "no git remote named") {
		t.Errorf("release-alpha did not resolve the `dev` remote, so RELEASE_REMOTE does "+
			"not default to `dev`. The release remote is named "+
			"`dev`, and a default that points elsewhere makes the target "+
			"unrunnable on a conforming checkout.\n%s", out)
	}
	// It must still refuse — the fixture's CHANGELOG has no v99.0.0-alpha section
	// — which is what proves the run got PAST remote resolution rather than
	// succeeding for some unrelated reason.
	if !strings.Contains(out, "CHANGELOG.md has no") {
		t.Errorf("expected the run to reach the CHANGELOG guard, which sits after remote "+
			"resolution; got:\n%s", out)
	}
}

// TestReleaseAllowedSignersDefaultsToTheCheckoutFile pins the default trust
// anchor and that the recipe fails closed without it.
//
// The fixture carries no .release-allowed-signers and the run names none, so
// the recipe must refuse, naming the default file, before anything is written:
// a release that cannot be verified is not attempted.
func TestReleaseAllowedSignersDefaultsToTheCheckoutFile(t *testing.T) {
	f := newReleaseFixture(t, true)
	versionBefore := f.version()

	out, err := f.releaseAlpha()
	if err == nil {
		t.Fatalf("release-alpha SUCCEEDED with no allowed-signers file:\n%s", out)
	}
	if !strings.Contains(out, "allowed-signers file .release-allowed-signers") {
		t.Errorf("the refusal does not name the default .release-allowed-signers, so "+
			"RELEASE_ALLOWED_SIGNERS no longer defaults to it (or the guard moved):\n%s", out)
	}
	if f.version() != versionBefore {
		t.Errorf("a refused release rewrote VERSION")
	}
	if n := strings.TrimSpace(f.git("rev-list", "--count", "HEAD")); n != "1" {
		t.Errorf("a refused release committed: %s commits on main, want 1", n)
	}
	f.requireNothingPublished()
}

// TestReleaseCandidateCutsAnRcTag runs release-candidate end to end in the
// scratch repository against a scratch bare remote, never the real one. It pins
// what an -rc.N version becomes: the unprefixed VERSION file, the tag names, the
// release commit touching VERSION alone, and a refusal of a duplicate candidate.
func TestReleaseCandidateCutsAnRcTag(t *testing.T) {
	const maintainer = "release@example.invalid"
	key, pub := newSigningKey(t)
	f := newReleaseFixture(t, true)
	f.signWith(maintainer, key, true, true)
	signers := writeSigners(t, maintainer+" "+pub)

	const rc = "v0.9.5-rc.2"
	appendFile(t, filepath.Join(f.dir, "CHANGELOG.md"), "\n## [0.9.5-rc.2] - 2026-01-02\n\n- fixture rc\n")
	f.git("add", "CHANGELOG.md")
	f.git("commit", "-qm", "changelog for the candidate")

	run := func(version string) (string, error) {
		return f.make("release-candidate", "VERSION="+version, "RELEASE_REMOTE="+fixtureRemote, "RELEASE_ALLOWED_SIGNERS="+signers)
	}

	// A stable or alpha version is refused before anything is written.
	headBefore := strings.TrimSpace(f.git("rev-parse", "HEAD"))
	versionBefore := f.version()
	for _, bad := range []string{"v0.9.5", "v0.9.5-alpha", "v0.9.5-rc.0", "v0.9.5-rc.01"} {
		if out, err := run(bad); err == nil || !strings.Contains(out, "not of the form vX.Y.Z-rc.N") {
			t.Fatalf("release-candidate did not refuse %s on the form guard (err=%v):\n%s", bad, err, out)
		}
	}
	if f.version() != versionBefore || strings.TrimSpace(f.git("rev-parse", "HEAD")) != headBefore {
		t.Fatalf("a refused candidate wrote VERSION or committed")
	}
	f.requireNothingPublished()

	if out, err := run(rc); err != nil {
		t.Fatalf("release-candidate refused %s: %v\n%s", rc, err, out)
	}
	if got := f.version(); got != "0.9.5-rc.2\n" {
		t.Errorf("VERSION = %q, want %q (unprefixed)", got, "0.9.5-rc.2\n")
	}
	refs := f.remoteRefs()
	for _, want := range []string{"refs/heads/main", "refs/tags/" + rc, "refs/tags/test/performance/" + rc, "refs/tags/contrib/dragonboat/" + rc} {
		if !strings.Contains(refs, want) {
			t.Errorf("remote is missing %s after a successful candidate:\n%s", want, refs)
		}
	}
	if files := strings.TrimSpace(f.git("diff-tree", "-r", "--no-commit-id", "--name-only", "HEAD")); files != "VERSION\ncontrib/dragonboat/go.mod" {
		t.Errorf("the release commit changed %q, want VERSION and contrib/dragonboat/go.mod alone", files)
	}
	// contrib/dragonboat is importable at the candidate: its go.mod requires
	// the parent at the same version, the version its tag sits beside.
	if mod := f.git("show", "HEAD:contrib/dragonboat/go.mod"); !strings.Contains(mod, "\nrequire github.com/jeremyhahn/go-quicraft "+rc+"\n") {
		t.Errorf("contrib/dragonboat/go.mod does not require the parent at %s:\n%s", rc, mod)
	}

	// The same candidate cannot be cut twice.
	if out, err := run(rc); err == nil || !strings.Contains(out, "already exists") {
		t.Errorf("a second %s was not refused as a duplicate (err=%v):\n%s", rc, err, out)
	}
}

// TestReleaseVerifiesAgainstTheAllowedSigners drives the whole recipe, through
// the push, with plain git and throwaway keys.
//
// The recipe trusts no tool. After the release commit and tags exist and BEFORE
// anything is pushed, the commit's author must be a principal in the
// allowed-signers file, and the commit and both tags must carry signatures that
// verify against a key the file lists FOR THAT PRINCIPAL. Each refusal case
// has a real bare remote, so the push would succeed if the check were absent.
func TestReleaseVerifiesAgainstTheAllowedSigners(t *testing.T) {
	const maintainer = "release@example.invalid"
	keyA, pubA := newSigningKey(t)
	keyB, pubB := newSigningKey(t)

	published := func(t *testing.T, f *releaseFixture) {
		t.Helper()
		refs := f.remoteRefs()
		for _, want := range []string{"refs/heads/main", "refs/tags/" + f.release, "refs/tags/test/performance/" + f.release, "refs/tags/contrib/dragonboat/" + f.release} {
			if !strings.Contains(refs, want) {
				t.Errorf("remote is missing %s after a successful release:\n%s", want, refs)
			}
		}
	}

	t.Run("plain git signed by a listed maintainer", func(t *testing.T) {
		f := newReleaseFixture(t, true)
		f.signWith(maintainer, keyA, true, true)
		// No RELEASE_GIT: this pins its default as plain git.
		out, err := f.releaseAlpha("RELEASE_ALLOWED_SIGNERS=" + writeSigners(t, maintainer+" "+pubA))
		if err != nil {
			t.Fatalf("release-alpha refused a release signed by a listed maintainer: %v\n%s", err, out)
		}
		published(t, f)
	})

	t.Run("principal on a multi-principal line", func(t *testing.T) {
		f := newReleaseFixture(t, true)
		f.signWith(maintainer, keyA, true, true)
		out, err := f.releaseAlpha("RELEASE_ALLOWED_SIGNERS=" + writeSigners(t, "other@example.invalid,"+maintainer+" "+pubA))
		if err != nil {
			t.Fatalf("release-alpha did not match the author against the second principal of a line: %v\n%s", err, out)
		}
		published(t, f)
	})

	t.Run("signing wrapper with a leading argument", func(t *testing.T) {
		// A wrapper in the shape of a per-project signing helper: its first
		// argument names the project and it sets identity and signing itself.
		// The project name becomes the author's address, so a recipe that did
		// not pass the leading argument would fail the principal check.
		f := newReleaseFixture(t, true)
		wrapper := filepath.Join(t.TempDir(), "signing-git.sh")
		writeFile(t, wrapper, "#!/usr/bin/env bash\nset -eu\nproject=\"$1\"; shift\n"+
			"exec git -c gpg.format=ssh -c user.signingkey="+keyA+" -c commit.gpgsign=true -c tag.gpgsign=true "+
			"-c user.name=\"release-$project\" -c user.email=\"$project@example.invalid\" \"$@\"\n", 0o755)
		out, err := f.releaseAlpha("RELEASE_GIT="+wrapper+" go-quicraft",
			"RELEASE_ALLOWED_SIGNERS="+writeSigners(t, "go-quicraft@example.invalid "+pubA))
		if err != nil {
			t.Fatalf("release-alpha refused a release cut through a signing wrapper: %v\n%s", err, out)
		}
		published(t, f)
	})

	for _, tc := range []struct {
		name                  string
		author, key           string
		signCommits, signTags bool
		signers               []string
		refusal               string
	}{
		{
			name: "unsigned commit", author: maintainer, key: keyA, signTags: true,
			signers: []string{maintainer + " " + pubA},
			refusal: "release commit's signature does not verify",
		},
		{
			name: "unsigned tag", author: maintainer, key: keyA, signCommits: true,
			signers: []string{maintainer + " " + pubA},
			refusal: "signature does not verify",
		},
		{
			name: "author not in the signers file", author: "stranger@example.invalid", key: keyA, signCommits: true, signTags: true,
			signers: []string{maintainer + " " + pubA},
			refusal: "is not a principal in",
		},
		{
			// Key B IS in the file, so verifying against the whole file would
			// pass. It is listed for someone else, and the author must have
			// signed with a key listed for the author.
			name: "signed with a key listed for another principal", author: maintainer, key: keyB, signCommits: true, signTags: true,
			signers: []string{maintainer + " " + pubA, "other@example.invalid " + pubB},
			refusal: "release commit's signature does not verify",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newReleaseFixture(t, true)
			f.signWith(tc.author, tc.key, tc.signCommits, tc.signTags)
			out, err := f.releaseAlpha("RELEASE_ALLOWED_SIGNERS=" + writeSigners(t, tc.signers...))
			if err == nil {
				t.Fatalf("release-alpha SUCCEEDED; it must refuse (%s):\n%s", tc.refusal, out)
			}
			if !strings.Contains(out, tc.refusal) {
				t.Errorf("refused, but not with %q — some other guard fired, so this case is "+
					"not exercising the check it exists for:\n%s", tc.refusal, out)
			}
			f.requireNothingPublished()
		})
	}
}

// TestReleaseRefusesForgedSignatureMarkers pins that the recipe asks git to
// verify the cryptography rather than looking for signature-shaped text.
//
// The substituted RELEASE_GIT gets the author RIGHT, does not sign, and plants
// "gpgsig" and "SSH SIGNATURE" inside the commit and tag MESSAGES — the precise
// shape that defeats grepping `cat-file` output. Only real verification against
// the pinned key rejects it.
func TestReleaseRefusesForgedSignatureMarkers(t *testing.T) {
	const maintainer = "release@example.invalid"
	_, pub := newSigningKey(t)
	f := newReleaseFixture(t, true)

	shim := filepath.Join(t.TempDir(), "forging-git.sh")
	writeFile(t, shim, "#!/usr/bin/env bash\n"+
		"shift\n"+
		"msg='forged\n\ngpgsig -----BEGIN SSH SIGNATURE-----\nnot actually a signature\n'\n"+
		"id=(-c commit.gpgsign=false -c tag.gpgsign=false -c user.name=forger -c user.email="+maintainer+")\n"+
		"case \"$1\" in\n"+
		"  commit) exec git \"${id[@]}\" commit -m \"$msg\" ;;\n"+
		"  tag)    exec git \"${id[@]}\" tag -a \"$3\" -m \"$msg\" ;;\n"+
		"  *)      exec git \"${id[@]}\" \"$@\" ;;\n"+
		"esac\n", 0o755)

	out, err := f.releaseAlpha("RELEASE_GIT="+shim+" go-quicraft",
		"RELEASE_ALLOWED_SIGNERS="+writeSigners(t, maintainer+" "+pub))
	if err == nil {
		t.Fatalf("release-alpha SUCCEEDED with a RELEASE_GIT that does not sign. "+
			"Signature-shaped text was accepted as a signature.\n%s", out)
	}
	if !strings.Contains(out, "does not verify") {
		t.Errorf("refused, but not at signature verification — some earlier guard fired, so "+
			"this test is not exercising the check it exists for:\n%s", out)
	}
	f.requireNothingPublished()
}

// TestReleaseRefusesAKeyTheAnchorDoesNotPin closes the trust-anchor
// substitution, which is the failure mode that makes "we verify the signature"
// worth anything.
//
// Building the allowed-signers file from a key found beside RELEASE_GIT would
// verify nothing: a substituted helper supplies the key it is checked against,
// so it signs the release with its own keypair, drops the matching public half
// where the recipe looks, and every pre-push check passes against the
// attacker's key.
//
// This test is that attack. The helper generates its own key, signs the release
// commit and tag with it as the correct author, and plants a matching
// allowed-signers file beside itself, which it also hands git as
// gpg.ssh.allowedSignersFile. The recipe must still refuse, because its anchor
// is the allowed-signers file the caller named and nothing the helper reaches.
func TestReleaseRefusesAKeyTheAnchorDoesNotPin(t *testing.T) {
	const maintainer = "release@example.invalid"
	_, pinned := newSigningKey(t)
	forged, forgedPub := newSigningKey(t)
	f := newReleaseFixture(t, true)

	helperRoot := t.TempDir()
	helperSigners := filepath.Join(helperRoot, "allowed-signers")
	writeFile(t, helperSigners, maintainer+" "+forgedPub+"\n", 0o644)
	shim := filepath.Join(helperRoot, "signing-git.sh")
	writeFile(t, shim, "#!/usr/bin/env bash\nshift\n"+
		"exec git -c gpg.format=ssh -c user.signingkey="+forged+" -c commit.gpgsign=true -c tag.gpgsign=true "+
		"-c gpg.ssh.allowedSignersFile="+helperSigners+" "+
		"-c user.name=forger -c user.email="+maintainer+" \"$@\"\n", 0o755)

	out, err := f.releaseAlpha("RELEASE_GIT="+shim+" go-quicraft",
		"RELEASE_ALLOWED_SIGNERS="+writeSigners(t, maintainer+" "+pinned))
	if err == nil {
		t.Fatalf("release-alpha PUBLISHED a release signed with a key it was never pinned to.\n"+
			"The trust anchor must not be reachable from RELEASE_GIT.\n%s", out)
	}
	if !strings.Contains(out, "does not verify") {
		t.Errorf("refused, but not at signature verification — some earlier guard fired, so "+
			"this test is not exercising the trust-anchor substitution:\n%s", out)
	}
	f.requireNothingPublished()
}

// signingHelper writes a RELEASE_GIT in the shape of the per-project signing
// helper: its first argument names the project, and every command runs as
// maintainer, SSH-signed with key. onWrite runs before every command other
// than push, and onPush replaces the push; both are bash, run with "$@"
// holding the git arguments and g the signing git command. For a push, remote,
// branch, tag and ptag hold the remote and the three refspecs, classified by
// destination so the helper does not depend on their order. An empty onPush
// performs the push it was asked for.
func signingHelper(t *testing.T, key, maintainer, onWrite, onPush string) string {
	t.Helper()
	if onPush == "" {
		onPush = `exec "${g[@]}" "$@"`
	}
	p := filepath.Join(t.TempDir(), "signing-git.sh")
	writeFile(t, p, "#!/usr/bin/env bash\nset -eu\nshift\n"+
		"g=(git -c gpg.format=ssh -c user.signingkey="+key+" -c commit.gpgsign=true -c tag.gpgsign=true "+
		"-c user.name=release -c user.email="+maintainer+")\n"+
		"if [ \"$1\" != push ]; then\n"+onWrite+"\nexec \"${g[@]}\" \"$@\"\nfi\n"+
		"remote= branch= tag= ptag= ctag=\n"+
		"for a in \"${@:2}\"; do\n"+
		"  case \"$a\" in\n"+
		"    -*) ;;\n"+
		"    *:refs/heads/*) branch=$a ;;\n"+
		"    *:refs/tags/test/performance/*) ptag=$a ;;\n"+
		"    *:refs/tags/contrib/dragonboat/*) ctag=$a ;;\n"+
		"    *:refs/tags/*) tag=$a ;;\n"+
		"    *) remote=$a ;;\n"+
		"  esac\n"+
		"done\n"+
		onPush+"\n", 0o755)
	return p
}

// TestReleasePushesObjectIDsThroughTheHelper pins the publication contract an
// honest helper sees. The push still runs through RELEASE_GIT, because in the
// company flow that wrapper is what authenticates to the remote, and it names
// the verified objects by ID, atomically, so moving a ref after verification
// cannot change what is published.
func TestReleasePushesObjectIDsThroughTheHelper(t *testing.T) {
	const maintainer = "release@example.invalid"
	key, pub := newSigningKey(t)
	f := newReleaseFixture(t, true)
	log := filepath.Join(t.TempDir(), "pushes.log")
	helper := signingHelper(t, key, maintainer, "", `printf '%s\n' "$*" >> `+log+`; exec "${g[@]}" "$@"`)

	out, err := f.releaseAlpha("RELEASE_GIT="+helper+" go-quicraft",
		"RELEASE_ALLOWED_SIGNERS="+writeSigners(t, maintainer+" "+pub))
	if err != nil {
		t.Fatalf("release-alpha refused an honest signing helper: %v\n%s", err, out)
	}
	pushes := strings.Split(strings.TrimSpace(readFile(t, log)), "\n")
	if len(pushes) != 1 {
		t.Fatalf("RELEASE_GIT pushed %d times, want exactly one atomic push:\n%s", len(pushes), strings.Join(pushes, "\n"))
	}
	commit := strings.TrimSpace(f.git("rev-parse", "main"))
	tag := strings.TrimSpace(f.git("rev-parse", "refs/tags/"+f.release))
	ptag := strings.TrimSpace(f.git("rev-parse", "refs/tags/test/performance/"+f.release))
	ctag := strings.TrimSpace(f.git("rev-parse", "refs/tags/contrib/dragonboat/"+f.release))
	want := "push --atomic " + fixtureRemote + " " +
		commit + ":refs/heads/main " +
		tag + ":refs/tags/" + f.release + " " +
		ptag + ":refs/tags/test/performance/" + f.release + " " +
		ctag + ":refs/tags/contrib/dragonboat/" + f.release
	if pushes[0] != want {
		t.Errorf("the push does not name the verified objects by ID, atomically:\n got: %s\nwant: %s", pushes[0], want)
	}
}

// TestReleaseVerifiesWhatTheRemoteHolds is the defective or hostile helper at
// the one step the recipe still delegates to it. Signatures verified locally
// say nothing about what a helper that ignores its arguments publishes, so
// after the push the recipe reads the remote back with plain git and fails
// unless the branch and both tags name exactly the verified objects. Each
// helper signs correctly, so every pre-push check passes, then misbehaves at
// the push and exits 0.
func TestReleaseVerifiesWhatTheRemoteHolds(t *testing.T) {
	const maintainer = "release@example.invalid"
	key, pub := newSigningKey(t)

	for _, tc := range []struct{ name, onPush string }{
		{"pushes a different commit to the branch", `"${g[@]}" push --atomic "$remote" "HEAD~1:${branch#*:}" "$tag" "$ptag" "$ctag"`},
		{"pushes a different object under the release tag", `"${g[@]}" push --atomic "$remote" "$branch" "HEAD:${tag#*:}" "$ptag" "$ctag"`},
		{"skips the test/performance tag", `"${g[@]}" push --atomic "$remote" "$branch" "$tag" "$ctag"`},
		{"skips the contrib/dragonboat tag", `"${g[@]}" push --atomic "$remote" "$branch" "$tag" "$ptag"`},
		{"drops --atomic and stops after the branch", `"${g[@]}" push "$remote" "$branch"`},
		{"reports success without pushing", `:`},
		{"pushes to a different remote", `"${g[@]}" push --atomic "$DECOY" "$branch" "$tag" "$ptag" "$ctag"`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newReleaseFixture(t, true)
			decoy := filepath.Join(t.TempDir(), "decoy.git")
			f.gitIn(filepath.Dir(decoy), "init", "-q", "--bare", decoy)
			helper := signingHelper(t, key, maintainer, "", "DECOY="+decoy+"\n"+tc.onPush+"\nexit 0")
			out, err := f.releaseAlpha("RELEASE_GIT="+helper+" go-quicraft",
				"RELEASE_ALLOWED_SIGNERS="+writeSigners(t, maintainer+" "+pub))
			if err == nil {
				t.Fatalf("release-alpha SUCCEEDED although the remote does not hold the verified release:\n%s\nremote:\n%s", out, f.remoteRefs())
			}
			if !strings.Contains(out, "does not hold the verified release") {
				t.Errorf("failed, but not at the remote read-back, so this case is not exercising it:\n%s", out)
			}
			if strings.Contains(out, "Released ") {
				t.Errorf("the recipe announced a release it did not verify:\n%s", out)
			}
		})
	}
}

// TestReleaseBindsTheVerifiedObjectsToTheRelease pins that the objects the
// recipe signs off are the release it asked for, not merely objects the
// anchor's key signed: the commit is exactly the VERSION bump on top of the
// main it started from, and each tag object targets that commit under its own
// name. A helper that signs whatever it likes passes signature verification,
// so each case must be refused before anything is pushed.
func TestReleaseBindsTheVerifiedObjectsToTheRelease(t *testing.T) {
	const maintainer = "release@example.invalid"
	key, pub := newSigningKey(t)

	for _, tc := range []struct{ name, onWrite, refusal string }{
		{
			"commit carries more than VERSION",
			`if [ "$1" = commit ]; then echo planted > planted.txt; "${g[@]}" add planted.txt; fi`,
			"changes more than VERSION",
		},
		{
			"commit edits contrib/dragonboat/go.mod beyond the parent pin",
			`if [ "$1" = commit ]; then echo '// planted' >> contrib/dragonboat/go.mod; "${g[@]}" add contrib/dragonboat/go.mod; fi`,
			"changes more than VERSION",
		},
		{
			"commit is not on top of the release base",
			`if [ "$1" = commit ]; then "${g[@]}" update-ref HEAD "$("${g[@]}" commit-tree -p HEAD -m sneak 'HEAD^{tree}')"; fi`,
			"is not a single commit on top of",
		},
		{
			"tag targets another commit",
			`if [ "$1" = tag ]; then exec "${g[@]}" "$@" HEAD~1; fi`,
			"does not point at the release commit",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newReleaseFixture(t, true)
			out, err := f.releaseAlpha("RELEASE_GIT="+signingHelper(t, key, maintainer, tc.onWrite, "")+" go-quicraft",
				"RELEASE_ALLOWED_SIGNERS="+writeSigners(t, maintainer+" "+pub))
			if err == nil {
				t.Fatalf("release-alpha SUCCEEDED; it must refuse (%s):\n%s", tc.refusal, out)
			}
			if !strings.Contains(out, tc.refusal) {
				t.Errorf("refused, but not with %q:\n%s", tc.refusal, out)
			}
			f.requireNothingPublished()
		})
	}
}

// TestReleaseAllowedSignersIsWellFormed checks the private repository's own
// trust anchor: every entry names at least one principal and carries a public
// key ssh-keygen can parse. A malformed anchor fails every release closed; this
// reports it at review time instead of at release time.
func TestReleaseAllowedSignersIsWellFormed(t *testing.T) {
	requirePrivateWorkflowFile(t, ".release-allowed-signers")
	if _, err := exec.LookPath("ssh-keygen"); err != nil {
		t.Skipf("ssh-keygen not available: %v", err)
	}
	entries := 0
	for i, line := range strings.Split(readFile(t, filepath.Join(repoRoot, ".release-allowed-signers")), "\n") {
		line = strings.TrimSpace(line)
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		entries++
		fields := strings.Fields(line)
		if len(fields) < 3 || strings.Trim(fields[0], ",") == "" {
			t.Errorf(".release-allowed-signers:%d is not `principals keytype key`: %q", i+1, line)
			continue
		}
		// The key follows the principals and any options: it is the field
		// after the first one naming an SSH key type.
		var pub string
		for j := 1; j+1 < len(fields); j++ {
			if strings.HasPrefix(fields[j], "ssh-") || strings.HasPrefix(fields[j], "ecdsa-") || strings.HasPrefix(fields[j], "sk-") {
				pub = fields[j] + " " + fields[j+1]
				break
			}
		}
		if pub == "" {
			t.Errorf(".release-allowed-signers:%d carries no SSH public key: %q", i+1, line)
			continue
		}
		p := filepath.Join(t.TempDir(), "key.pub")
		writeFile(t, p, pub+"\n", 0o644)
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		out, err := exec.CommandContext(ctx, "ssh-keygen", "-l", "-f", p).CombinedOutput()
		cancel()
		if err != nil {
			t.Errorf(".release-allowed-signers:%d: ssh-keygen cannot parse the key: %v\n%s", i+1, err, out)
		}
	}
	if entries == 0 {
		t.Error(".release-allowed-signers lists no signer; every release would be refused")
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
