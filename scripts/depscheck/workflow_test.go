package main

import (
	"bufio"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

// The scans.yml bump job's ref mutations run as shell steps on a GitHub runner,
// which no Go test reaches through runWith. These tests take each step's
// run: script out of the workflow and run it with bash against a fake gh
// whose refs live in a real bare repository, the step's origin, so a
// deletion pushed with --force-with-lease meets git's own old-value check.

const workflowFile = "../../.github/workflows/scans.yml"

const bumpBranch = "ci/deps-bump"

// stepScript returns the run: block of the step named name, dedented.
func stepScript(t *testing.T, name string) string {
	t.Helper()
	data, err := os.ReadFile(workflowFile)
	require.NoError(t, err)
	lines := strings.Split(string(data), "\n")
	indent := func(s string) int { return len(s) - len(strings.TrimLeft(s, " ")) }
	for i, l := range lines {
		if strings.TrimSpace(l) != "- name: "+name {
			continue
		}
		for j := i + 1; j < len(lines); j++ {
			if strings.HasPrefix(strings.TrimSpace(lines[j]), "- ") {
				break
			}
			if strings.TrimSpace(lines[j]) != "run: |" {
				continue
			}
			key := indent(lines[j])
			var body []string
			cut := -1
			for _, b := range lines[j+1:] {
				if strings.TrimSpace(b) == "" {
					body = append(body, "")
					continue
				}
				if indent(b) <= key {
					break
				}
				if cut < 0 {
					cut = indent(b)
				}
				body = append(body, b[cut:])
			}
			return strings.Join(body, "\n") + "\n"
		}
	}
	t.Fatalf("%s has no step %q with a run: | block", workflowFile, name)
	return ""
}

// fakeGh answers the gh api calls the steps make from the bare repository
// $FAKE_ORIGIN. FAKE_MOVE_ON names the call (compare, graphql or delete)
// just before which the branch moves to FAKE_MOVE_TO, as a maintainer's
// push would.
const fakeGh = `#!/usr/bin/env bash
set -uo pipefail
G() { "$FAKE_REAL_GIT" --git-dir="$FAKE_ORIGIN" "$@"; }
move() { if [ "${FAKE_MOVE_ON:-}" = "$1" ]; then G update-ref "refs/heads/$BRANCH" "$FAKE_MOVE_TO"; fi; }
echo "gh $*" >> "$FAKE_LOG"
[ "$1" = api ] || exit 99
shift
method=GET
if [ "$1" = -X ]; then method=$2; shift 2; fi
path=$1
shift
ref="refs/heads/$BRANCH"
case "$method $path" in
"GET repos/$REPO/pulls?state=open"*) echo "${FAKE_OPEN_PR:-}" ;;
"GET repos/$REPO/pulls?state=closed"*)
  case "$*" in
  *merged_at*) echo "${FAKE_MERGED:-}" ;;
  *head.sha*) echo "${FAKE_MERGED_HEAD:-}" ;;
  *) exit 98 ;;
  esac ;;
"GET repos/$REPO/pulls")
  cat > /dev/null
  move pulls
  if [ -n "${FAKE_PULLS_FAIL:-}" ]; then echo "pulls: failed" >&2; exit 1; fi
  echo 41 ;;
"GET repos/$REPO/pulls/"*) G rev-parse "$ref" ;;
"PATCH repos/$REPO/pulls/"*) : ;;
"GET repos/$REPO/issues/"*/labels) : ;;
"GET repos/$REPO/git/ref/heads/main") G rev-parse refs/heads/main ;;
"GET repos/$REPO/git/ref/heads/$BRANCH")
  G rev-parse -q --verify "$ref" || { echo '{"message":"Not Found"}' >&2; exit 1; } ;;
"GET repos/$REPO/compare/main...$BRANCH") move compare; G merge-base refs/heads/main "$ref" ;;
"GET repos/$REPO/compare/"*)
  r=${path#"repos/$REPO/compare/"}
  G rev-list --count "${r%%...*}..${r#*...}"
  G rev-list --reverse "${r%%...*}..${r#*...}" ;;
"DELETE repos/$REPO/git/refs/heads/$BRANCH") move delete; G update-ref -d "$ref" ;;
"GET repos/$REPO/git/refs")
  sha=
  for a in "$@"; do case "$a" in sha=*) sha=${a#sha=} ;; esac; done
  G update-ref "$ref" "$sha" "" || { echo "Reference already exists" >&2; exit 1; } ;;
"GET graphql")
  in=$(cat)
  exp=$(jq -r .variables.input.expectedHeadOid <<<"$in")
  msg=$(jq -r .variables.input.message.headline <<<"$in")
  move graphql
  if [ -n "${FAKE_GRAPHQL_FAIL:-}" ]; then echo "graphql: failed" >&2; exit "$FAKE_GRAPHQL_FAIL"; fi
  new=$(G commit-tree -p "$exp" -m "$msg" "$exp^{tree}") || exit 1
  G update-ref "$ref" "$new" "$exp" || { echo "graphql: expectedHeadOid $exp is not the head" >&2; exit 1; }
  echo "$new" >> "$FAKE_ORIGIN/web-flow"
  echo "$new" ;;
"GET repos/$REPO/commits/"*)
  sha=${path##*/}
  web=
  if [ -f "$FAKE_ORIGIN/web-flow" ]; then
    while read -r w; do [ "$w" = "$sha" ] && web=1; done < "$FAKE_ORIGIN/web-flow"
  fi
  subject=$(G log -1 --format=%s "$sha") || exit 1
  if [ -n "$web" ]; then
    printf 'true\tnoreply@github.com\tweb-flow\t%s\n' "$subject"
  else
    printf 'false\tt@example.com\tnone\t%s\n' "$subject"
  fi ;;
*) echo "fake gh: unexpected call: $method $path $*" >&2; exit 99 ;;
esac
`

// fakeGit is git, except that a push first moves the branch when
// FAKE_MOVE_ON=delete: the push then meets a ref that moved after the
// step last read it.
const fakeGit = `#!/usr/bin/env bash
if [ "${1:-}" = push ]; then
  echo "git $*" >> "$FAKE_LOG"
  if [ "${FAKE_MOVE_ON:-}" = delete ]; then
    "$FAKE_REAL_GIT" --git-dir="$FAKE_ORIGIN" update-ref "refs/heads/$BRANCH" "$FAKE_MOVE_TO"
  fi
fi
exec "$FAKE_REAL_GIT" "$@"
`

// forge is a bare origin with main and the side commits the cases put the
// branch on, a clone of it to run the steps in, and the fakes.
type forge struct {
	t       *testing.T
	git     string
	origin  string
	work    string
	bin     string
	log     string
	baseEnv []string
	// a is main's parent; m is main; b and c are commits on a that main
	// lacks; d is a commit on c.
	a, m, b, c, d string
}

func newForge(t *testing.T) *forge {
	t.Helper()
	for _, tool := range []string{"bash", "jq"} {
		if _, err := exec.LookPath(tool); err != nil {
			t.Skipf("no %s: the workflow steps need it", tool)
		}
	}
	git, err := exec.LookPath("git")
	if err != nil {
		t.Skip("no git")
	}
	dir := t.TempDir()
	f := &forge{t: t, git: git, origin: filepath.Join(dir, "origin.git"), work: filepath.Join(dir, "work"), bin: filepath.Join(dir, "bin"), log: filepath.Join(dir, "calls.log")}
	f.baseEnv = append(os.Environ(),
		"GIT_CONFIG_GLOBAL=/dev/null", "GIT_CONFIG_NOSYSTEM=1",
		"GIT_AUTHOR_NAME=t", "GIT_AUTHOR_EMAIL=t@example.com",
		"GIT_COMMITTER_NAME=t", "GIT_COMMITTER_EMAIL=t@example.com")
	f.run("", "init", "-q", "--bare", f.origin)
	seed := filepath.Join(dir, "seed")
	f.run("", "init", "-q", seed)
	write := func(name, text string) {
		require.NoError(t, os.WriteFile(filepath.Join(seed, name), []byte(text), 0o600))
		f.run(seed, "add", name)
		f.run(seed, "commit", "-q", "-m", name+": "+text)
	}
	write("go.mod", "module example.com/app\n")
	f.a = f.run(seed, "rev-parse", "HEAD")
	write("main.txt", "main\n")
	f.m = f.run(seed, "rev-parse", "HEAD")
	f.run(seed, "checkout", "-q", "-b", "side", f.a)
	write("b.txt", "b\n")
	f.b = f.run(seed, "rev-parse", "HEAD")
	f.run(seed, "checkout", "-q", "-B", "side", f.a)
	write("c.txt", "c\n")
	f.c = f.run(seed, "rev-parse", "HEAD")
	write("d.txt", "d\n")
	f.d = f.run(seed, "rev-parse", "HEAD")
	f.run(seed, "push", "-q", f.origin, f.m+":refs/heads/main", f.b+":refs/keep/b", f.d+":refs/keep/d")
	f.run("", "clone", "-q", f.origin, f.work)
	require.NoError(t, os.MkdirAll(f.bin, 0o700))
	require.NoError(t, os.WriteFile(filepath.Join(f.bin, "gh"), []byte(fakeGh), 0o700))   //#nosec G306 -- an executable test fake
	require.NoError(t, os.WriteFile(filepath.Join(f.bin, "git"), []byte(fakeGit), 0o700)) //#nosec G306 -- an executable test fake
	return f
}

func (f *forge) run(dir string, args ...string) string {
	f.t.Helper()
	if dir != "" {
		args = append([]string{"-C", dir}, args...)
	}
	cmd := exec.CommandContext(f.t.Context(), f.git, args...)
	cmd.Env = f.baseEnv
	out, err := cmd.CombinedOutput()
	require.NoError(f.t, err, string(out))
	return strings.TrimSpace(string(out))
}

// setBranch puts the branch at sha, or deletes it when sha is empty.
func (f *forge) setBranch(sha string) {
	f.t.Helper()
	if sha == "" {
		f.run("", "--git-dir="+f.origin, "update-ref", "-d", "refs/heads/"+bumpBranch)
		return
	}
	f.run("", "--git-dir="+f.origin, "update-ref", "refs/heads/"+bumpBranch, sha)
}

// branch is the branch's tip on the origin, empty when it does not exist.
func (f *forge) branch() string {
	f.t.Helper()
	cmd := exec.CommandContext(f.t.Context(), f.git, "--git-dir="+f.origin, "rev-parse", "-q", "--verify", "refs/heads/"+bumpBranch)
	cmd.Env = f.baseEnv
	out, err := cmd.Output()
	if err != nil {
		return ""
	}
	return strings.TrimSpace(string(out))
}

// step runs the named step's script in the clone with env added, and
// returns its exit status, its output and the GITHUB_OUTPUT values.
func (f *forge) step(name string, env ...string) (int, string, map[string]string) {
	f.t.Helper()
	dir := f.t.TempDir()
	script := filepath.Join(dir, "step.sh")
	require.NoError(f.t, os.WriteFile(script, []byte(stepScript(f.t, name)), 0o600))
	outputs := filepath.Join(dir, "github_output")
	require.NoError(f.t, os.WriteFile(outputs, nil, 0o600))
	cmd := exec.CommandContext(f.t.Context(), "bash", "--noprofile", "--norc", "-e", script)
	cmd.Dir = f.work
	cmd.Env = append(append([]string(nil), f.baseEnv...),
		"PATH="+f.bin+string(os.PathListSeparator)+os.Getenv("PATH"),
		"REPO=owner/repo", "BRANCH="+bumpBranch, "GH_TOKEN=fake",
		"GITHUB_OUTPUT="+outputs, "GITHUB_ENV="+filepath.Join(dir, "github_env"), "RUNNER_TEMP="+dir,
		"PR_TITLE=deps: bump dependencies to latest (ruling 640)",
		"FAKE_ORIGIN="+f.origin, "FAKE_REAL_GIT="+f.git, "FAKE_LOG="+f.log)
	cmd.Env = append(cmd.Env, env...)
	out, err := cmd.CombinedOutput()
	code := 0
	if err != nil {
		exit, ok := err.(*exec.ExitError)
		require.True(f.t, ok, "%v: %s", err, out)
		code = exit.ExitCode()
	}
	got := make(map[string]string)
	data, rerr := os.Open(outputs) //#nosec G304 -- a file this test made
	require.NoError(f.t, rerr)
	defer data.Close()
	sc := bufio.NewScanner(data)
	for sc.Scan() {
		k, v, _ := strings.Cut(sc.Text(), "=")
		got[k] = v
	}
	return code, string(out), got
}

// calls is every gh and git push call the fakes logged.
func (f *forge) calls() string {
	data, err := os.ReadFile(f.log)
	if os.IsNotExist(err) {
		return ""
	}
	require.NoError(f.t, err)
	return string(data)
}

const (
	findBase     = "Find the base commit"
	deleteMerged = "Delete the merged PR's branch"
	commitStep   = "Commit to ci/deps-bump"
	openPR       = "Open the PR and label it"
)

func TestDepsBumpBaseStep(t *testing.T) {
	t.Run("an abandoned placeholder is reused and committed onto", func(t *testing.T) {
		f := newForge(t)
		f.setBranch(f.a)
		code, out, got := f.step(findBase)
		require.Equal(t, 0, code, out)
		assert.Equal(t, f.a, got["sha"], "the bump builds on the placeholder's tip")
		assert.Empty(t, got["stale"])
		assert.Equal(t, f.a, f.branch())
		assert.NotContains(t, f.calls(), "DELETE")

		code, out, _ = f.step(commitStep, "BASE_SHA="+got["sha"], "CHANGED=go.mod")
		require.Equal(t, 0, code, out)
		tip := f.branch()
		assert.NotEqual(t, f.a, tip)
		assert.Equal(t, f.a, f.run("", "--git-dir="+f.origin, "rev-parse", tip+"^"), "one commit on the placeholder")
	})
	t.Run("a placeholder is reused after an earlier PR merged", func(t *testing.T) {
		// An earlier bump PR (head c) merged; a later run re-created the
		// branch at a, a commit of main, and was cancelled before its
		// commit. The closed-PR lookup still returns the merged PR, but
		// the placeholder carries nothing main lacks: it is reused.
		f := newForge(t)
		f.setBranch(f.a)
		code, out, got := f.step(findBase, "FAKE_MERGED=2026-11-01T00:00:00Z", "FAKE_MERGED_HEAD="+f.c)
		require.Equal(t, 0, code, out)
		assert.Equal(t, f.a, got["sha"], "the bump builds on the placeholder's tip")
		assert.Empty(t, got["stale"])
		assert.Equal(t, f.a, f.branch())
		assert.NotContains(t, f.calls(), "DELETE")

		code, out, _ = f.step(commitStep, "BASE_SHA="+got["sha"], "CHANGED=go.mod")
		require.Equal(t, 0, code, out)
		assert.Equal(t, f.a, f.run("", "--git-dir="+f.origin, "rev-parse", f.branch()+"^"), "one commit on the placeholder")
	})
	t.Run("a placeholder that moves after the tip check is not deleted", func(t *testing.T) {
		// The tip reads a; b, a commit main lacks, lands before the merge
		// base is read, and the merge base of main and b is still a.
		f := newForge(t)
		f.setBranch(f.a)
		code, out, got := f.step(findBase, "FAKE_MOVE_ON=compare", "FAKE_MOVE_TO="+f.b)
		require.Equal(t, 0, code, out)
		assert.Equal(t, f.b, f.branch(), "b survives the base step")

		code, out, _ = f.step(commitStep, "BASE_SHA="+got["sha"], "CHANGED=go.mod")
		assert.NotEqual(t, 0, code, out)
		assert.Equal(t, f.b, f.branch(), "the commit fails on the moved head, and b survives")
	})
	t.Run("an unmerged branch with unique commits is refused", func(t *testing.T) {
		f := newForge(t)
		f.setBranch(f.c)
		code, out, _ := f.step(findBase)
		assert.Equal(t, 1, code, out)
		assert.Contains(t, out, "its last PR was not merged")
		assert.Equal(t, f.c, f.branch())
	})
	t.Run("a merged PR's branch with later commits is refused", func(t *testing.T) {
		f := newForge(t)
		f.setBranch(f.d)
		code, out, _ := f.step(findBase, "FAKE_MERGED=2026-11-01T00:00:00Z", "FAKE_MERGED_HEAD="+f.c)
		assert.Equal(t, 1, code, out)
		assert.Contains(t, out, "carries later commits")
		assert.Equal(t, f.d, f.branch())
	})
	t.Run("a merged PR's branch is deleted while still at its head", func(t *testing.T) {
		f := newForge(t)
		f.setBranch(f.c)
		code, out, got := f.step(findBase, "FAKE_MERGED=2026-11-01T00:00:00Z", "FAKE_MERGED_HEAD="+f.c)
		require.Equal(t, 0, code, out)
		assert.Equal(t, f.m, got["sha"])
		assert.Equal(t, f.c, got["stale"])
		assert.Equal(t, f.c, f.branch(), "nothing is deleted before the checkout")

		code, out, _ = f.step(deleteMerged, "STALE="+got["stale"])
		require.Equal(t, 0, code, out)
		assert.Empty(t, f.branch())
	})
	t.Run("a merged PR's branch that moves before the delete is kept", func(t *testing.T) {
		f := newForge(t)
		f.setBranch(f.c)
		code, out, _ := f.step(deleteMerged, "STALE="+f.c, "FAKE_MOVE_ON=delete", "FAKE_MOVE_TO="+f.d)
		assert.Equal(t, 1, code, out)
		assert.Contains(t, out, "moved from "+f.c)
		assert.Equal(t, f.d, f.branch())
	})
}

func TestDepsBumpCommitStep(t *testing.T) {
	t.Run("a commit failure deletes the branch this run created and keeps its status", func(t *testing.T) {
		f := newForge(t)
		code, out, _ := f.step(commitStep, "BASE_SHA="+f.m, "CHANGED=go.mod", "FAKE_GRAPHQL_FAIL=7")
		assert.Equal(t, 7, code, out)
		assert.Contains(t, out, "deleted "+bumpBranch+", created by this run")
		assert.Empty(t, f.branch())
	})
	t.Run("a commit failure keeps a branch that existed before the run", func(t *testing.T) {
		f := newForge(t)
		f.setBranch(f.m)
		code, out, _ := f.step(commitStep, "BASE_SHA="+f.m, "CHANGED=go.mod", "FAKE_GRAPHQL_FAIL=7")
		assert.Equal(t, 7, code, out)
		assert.Equal(t, f.m, f.branch())
		assert.NotContains(t, f.calls(), "git push")
	})
	t.Run("a branch that advances before the commit is kept", func(t *testing.T) {
		f := newForge(t)
		code, out, _ := f.step(commitStep, "BASE_SHA="+f.m, "CHANGED=go.mod", "FAKE_MOVE_ON=graphql", "FAKE_MOVE_TO="+f.b)
		assert.Equal(t, 1, code, out)
		assert.Contains(t, out, "is not the head")
		assert.Equal(t, f.b, f.branch())
	})
	t.Run("a branch that moves between the check and the delete is kept", func(t *testing.T) {
		f := newForge(t)
		code, out, _ := f.step(commitStep, "BASE_SHA="+f.m, "CHANGED=go.mod", "FAKE_GRAPHQL_FAIL=7", "FAKE_MOVE_ON=delete", "FAKE_MOVE_TO="+f.b)
		assert.Equal(t, 7, code, out)
		assert.Equal(t, f.b, f.branch())
	})
	t.Run("a commit lands on a new branch", func(t *testing.T) {
		f := newForge(t)
		code, out, _ := f.step(commitStep, "BASE_SHA="+f.m, "CHANGED=go.mod")
		require.Equal(t, 0, code, out)
		assert.Contains(t, out, "GitHub web-flow, verified")
		assert.Equal(t, f.m, f.run("", "--git-dir="+f.origin, "rev-parse", f.branch()+"^"))
	})
}

// landWithoutPR runs the commit step on a new branch at main and then the PR
// step with PR creation failing: the window the recovery closes. It returns
// the commit left on the branch.
func landWithoutPR(t *testing.T, f *forge) string {
	t.Helper()
	code, out, _ := f.step(commitStep, "BASE_SHA="+f.m, "CHANGED=go.mod")
	require.Equal(t, 0, code, out)
	landed := f.branch()
	code, out, _ = f.step(openPR, "PR=", "EXPECT="+landed, "CHANGED=go.mod", "FAKE_PULLS_FAIL=1")
	require.NotEqual(t, 0, code, out)
	require.Contains(t, out, "pulls: failed", "the PR creation is what failed")
	require.Equal(t, landed, f.branch())
	return landed
}

func TestDepsBumpRecovery(t *testing.T) {
	t.Run("a commit that landed without its PR is recovered and its PR opened", func(t *testing.T) {
		f := newForge(t)
		landed := landWithoutPR(t, f)
		code, out, got := f.step(findBase)
		require.Equal(t, 0, code, out)
		assert.Equal(t, landed, got["recover"])
		assert.Equal(t, landed, got["sha"], "the bump builds on the recovered tip")
		assert.Empty(t, got["stale"])
		assert.NotContains(t, f.calls(), "DELETE")

		code, out, opened := f.step(openPR, "PR=", "EXPECT="+got["sha"])
		require.Equal(t, 0, code, out)
		assert.Equal(t, "41", opened["number"])
		assert.Contains(t, out, "opened #41 at "+landed)
		assert.Equal(t, landed, f.branch())
	})
	t.Run("a recovered branch takes this run's commit on its tip", func(t *testing.T) {
		f := newForge(t)
		landed := landWithoutPR(t, f)
		code, out, got := f.step(findBase)
		require.Equal(t, 0, code, out)
		code, out, _ = f.step(commitStep, "BASE_SHA="+got["sha"], "CHANGED=go.mod")
		require.Equal(t, 0, code, out)
		tip := f.branch()
		assert.Equal(t, landed, f.run("", "--git-dir="+f.origin, "rev-parse", tip+"^"))
		code, out, opened := f.step(openPR, "PR=", "EXPECT="+tip, "CHANGED=go.mod")
		require.Equal(t, 0, code, out)
		assert.Equal(t, "41", opened["number"])
	})
	t.Run("a branch with one commit not the workflow's is refused", func(t *testing.T) {
		f := newForge(t)
		f.setBranch(f.c)
		code, out, _ := f.step(commitStep, "BASE_SHA="+f.c, "CHANGED=go.mod")
		require.Equal(t, 0, code, out)
		tip := f.branch()
		code, out, got := f.step(findBase)
		assert.Equal(t, 1, code, out)
		assert.Contains(t, out, f.c+" is not this workflow's commit")
		assert.Contains(t, out, "its last PR was not merged")
		assert.Empty(t, got["recover"])
		assert.Equal(t, tip, f.branch())
	})
	t.Run("a branch whose PR was closed unmerged at its tip is refused", func(t *testing.T) {
		f := newForge(t)
		landed := landWithoutPR(t, f)
		code, out, _ := f.step(findBase, "FAKE_MERGED_HEAD="+landed)
		assert.Equal(t, 1, code, out)
		assert.Contains(t, out, "its last PR was not merged")
		assert.Equal(t, landed, f.branch())
	})
	t.Run("a branch that moves before the PR step gets no PR", func(t *testing.T) {
		f := newForge(t)
		landed := landWithoutPR(t, f)
		code, out, got := f.step(findBase)
		require.Equal(t, 0, code, out)
		require.Equal(t, landed, got["recover"])
		f.setBranch(f.d)
		code, out, _ = f.step(openPR, "PR=", "EXPECT="+got["sha"])
		assert.Equal(t, 1, code, out)
		assert.Contains(t, out, "no PR opened")
		assert.NotContains(t, f.calls(), "labels")
		assert.Equal(t, f.d, f.branch(), "the moved branch is not overwritten")
	})
	t.Run("a PR opened on a branch that moved is closed, the branch kept", func(t *testing.T) {
		f := newForge(t)
		landed := landWithoutPR(t, f)
		code, out, got := f.step(findBase)
		require.Equal(t, 0, code, out)
		require.Equal(t, landed, got["recover"])
		code, out, _ = f.step(openPR, "PR=", "EXPECT="+got["sha"], "FAKE_MOVE_ON=pulls", "FAKE_MOVE_TO="+f.d)
		assert.Equal(t, 1, code, out)
		assert.Contains(t, out, "the PR is closed")
		assert.Contains(t, f.calls(), "-X PATCH repos/owner/repo/pulls/41")
		assert.NotContains(t, f.calls(), "labels")
		assert.Equal(t, f.d, f.branch(), "the moved branch is not overwritten")
	})
	t.Run("a recovered branch that moves before the commit is kept", func(t *testing.T) {
		f := newForge(t)
		landWithoutPR(t, f)
		code, out, got := f.step(findBase)
		require.Equal(t, 0, code, out)
		code, out, _ = f.step(commitStep, "BASE_SHA="+got["sha"], "CHANGED=go.mod", "FAKE_MOVE_ON=graphql", "FAKE_MOVE_TO="+f.d)
		assert.Equal(t, 1, code, out)
		assert.Contains(t, out, "is not the head")
		assert.Equal(t, f.d, f.branch())
	})
}
