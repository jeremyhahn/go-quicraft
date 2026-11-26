package buildgates

import (
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

// sigFixture is a throwaway repository whose commits are signed (or not)
// with a key the repository's .github/allowed_signers registers.
type sigFixture struct {
	dir string
	env []string
}

const sigFixtureEmail = "signer@example.com"

func newSigFixture(t *testing.T) *sigFixture {
	t.Helper()
	if _, err := exec.LookPath("ssh-keygen"); err != nil {
		t.Skip("ssh-keygen is not on PATH")
	}
	root := t.TempDir()
	f := &sigFixture{
		dir: filepath.Join(root, "repo"),
		env: []string{
			"PATH=/usr/local/bin:/usr/bin:/bin",
			"HOME=" + root,
			"GIT_CONFIG_NOSYSTEM=1",
			"GIT_CONFIG_GLOBAL=" + os.DevNull,
			"GIT_AUTHOR_NAME=Signer", "GIT_AUTHOR_EMAIL=" + sigFixtureEmail,
			"GIT_COMMITTER_NAME=Signer", "GIT_COMMITTER_EMAIL=" + sigFixtureEmail,
		},
	}
	key := filepath.Join(root, "key")
	f.run(t, root, "ssh-keygen", "-q", "-t", "ed25519", "-N", "", "-C", "fixture", "-f", key)
	pub, err := os.ReadFile(key + ".pub")
	if err != nil {
		t.Fatal(err)
	}
	f.run(t, root, "git", "init", "-q", "-b", "main", f.dir)
	f.git(t, "config", "gpg.format", "ssh")
	f.git(t, "config", "user.signingkey", key)
	if err := os.MkdirAll(filepath.Join(f.dir, ".github"), 0o755); err != nil {
		t.Fatal(err)
	}
	fields := strings.Fields(string(pub))
	reg := sigFixtureEmail + ` namespaces="git" ` + fields[0] + " " + fields[1] + "\n"
	if err := os.WriteFile(filepath.Join(f.dir, ".github", "allowed_signers"), []byte(reg), 0o644); err != nil {
		t.Fatal(err)
	}
	f.git(t, "add", ".github/allowed_signers")
	f.git(t, "-c", "commit.gpgsign=true", "commit", "-q", "-m", "registry")
	return f
}

func (f *sigFixture) run(t *testing.T, dir string, name string, args ...string) string {
	t.Helper()
	cmd := exec.CommandContext(t.Context(), name, args...)
	cmd.Dir = dir
	cmd.Env = f.env
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("%s %v: %v\n%s", name, args, err, out)
	}
	return strings.TrimSpace(string(out))
}

func (f *sigFixture) git(t *testing.T, args ...string) string {
	t.Helper()
	return f.run(t, f.dir, "git", args...)
}

// commits adds one empty commit per entry, signed when the entry is true.
func (f *sigFixture) commits(t *testing.T, signed ...bool) {
	t.Helper()
	for i, s := range signed {
		sign := "commit.gpgsign=false"
		if s {
			sign = "commit.gpgsign=true"
		}
		f.git(t, "-c", sign, "commit", "-q", "--allow-empty", "-m", "c"+strings.Repeat("+", i))
	}
}

func (f *sigFixture) rev(t *testing.T, r string) string {
	t.Helper()
	return f.git(t, "rev-parse", r)
}

// check runs scripts/check-signature-window.sh as the Signatures job does
// for a push of HEAD~1..HEAD to ref, and returns its output and whether it passed.
func (f *sigFixture) check(t *testing.T, ref string) (string, bool) {
	t.Helper()
	script, err := filepath.Abs(filepath.Join("..", "..", "scripts", "check-signature-window.sh"))
	if err != nil {
		t.Fatal(err)
	}
	cmd := exec.CommandContext(t.Context(), "bash", script)
	cmd.Dir = f.dir
	cmd.Env = append(append([]string{}, f.env...),
		"EVENT=push", "GITHUB_REF="+ref,
		"HEAD_SHA="+f.rev(t, "HEAD"), "BEFORE="+f.rev(t, "HEAD~1"), "BASE_SHA=",
		"REPO=", "API=", "GH_TOKEN=", "RUNNER_TEMP="+t.TempDir())
	out, err := cmd.CombinedOutput()
	if err != nil {
		if _, ok := err.(*exec.ExitError); !ok {
			t.Fatalf("check-signature-window.sh: %v\n%s", err, out)
		}
	}
	return string(out), err == nil
}

// signedRun is n signed commits.
func signedRun(n int) []bool {
	s := make([]bool, n)
	for i := range s {
		s[i] = true
	}
	return s
}

// withUnsigned is n commits, all signed except the one at HEAD~back.
func withUnsigned(n, back int) []bool {
	s := signedRun(n)
	s[n-1-back] = false
	return s
}

func TestSignatureWindowValidHistoryPasses(t *testing.T) {
	f := newSigFixture(t)
	f.commits(t, signedRun(55)...)
	if out, ok := f.check(t, "refs/heads/main"); !ok {
		t.Fatalf("a fully signed history failed:\n%s", out)
	} else if !strings.Contains(out, "checking 50 commit(s)") {
		t.Fatalf("the window is not 50 commits:\n%s", out)
	}
}

func TestSignatureWindowCatchesOlderUnsignedCommit(t *testing.T) {
	// HEAD~49 is outside BEFORE..HEAD but the oldest commit in the window.
	f := newSigFixture(t)
	f.commits(t, withUnsigned(55, 49)...)
	out, ok := f.check(t, "refs/heads/main")
	if ok || !strings.Contains(out, "unsigned") {
		t.Fatalf("an unsigned commit inside the window passed:\n%s", out)
	}
}

func TestSignatureWindowBoundaryNotChecked(t *testing.T) {
	// HEAD~50 is just outside HEAD~50..HEAD.
	f := newSigFixture(t)
	f.commits(t, withUnsigned(55, 50)...)
	if out, ok := f.check(t, "refs/heads/main"); !ok {
		t.Fatalf("a commit outside the window was checked:\n%s", out)
	}
}

func TestSignatureWindowShortHistory(t *testing.T) {
	f := newSigFixture(t)
	f.commits(t, signedRun(4)...)
	out, ok := f.check(t, "refs/heads/main")
	if !ok || !strings.Contains(out, "checking 5 commit(s)") {
		t.Fatalf("a history shorter than the window is not checked from the root:\n%s", out)
	}
	g := newSigFixture(t)
	g.commits(t, withUnsigned(4, 2)...)
	if out, ok := g.check(t, "refs/heads/main"); ok || !strings.Contains(out, "unsigned") {
		t.Fatalf("an unsigned commit in a short history passed:\n%s", out)
	}
}

func TestSignatureWindowOnlyOnMain(t *testing.T) {
	f := newSigFixture(t)
	f.commits(t, withUnsigned(6, 2)...)
	if out, ok := f.check(t, "refs/heads/feature"); !ok {
		t.Fatalf("a push to another branch checked past its range:\n%s", out)
	}
}
