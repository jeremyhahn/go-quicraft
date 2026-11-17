package main

import (
	"bytes"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"
	"testing"
)

// The tests below cover -base, -vuln and -bump (ruling 640): fake git,
// govulncheck and go get seams over the testdata fixtures.

// fakeBase answers GoModAt from memory.
type fakeBase struct {
	data  string
	found bool
	err   error
	asked []string
}

func (f *fakeBase) GoModAt(dir, commit string) ([]byte, bool, error) {
	f.asked = append(f.asked, dir+"@"+commit)
	return []byte(f.data), f.found, f.err
}

// merge returns the union of scans.
func merge(scans ...map[string][]advisory) map[string][]advisory {
	out := make(map[string][]advisory)
	for _, m := range scans {
		for k, v := range m {
			out[k] = append(out[k], v...)
		}
	}
	return out
}

// fakeVuln answers Scan from memory.
type fakeVuln struct {
	hits map[string][]advisory
	err  error
}

func (f fakeVuln) Scan(string) (map[string][]advisory, error) { return f.hits, f.err }

// fakeEditor records go get and go mod tidy calls.
type fakeEditor struct {
	gets  []string
	tidy  int
	err   error
	after int // fail on this get (1-based); 0 never
}

func (f *fakeEditor) Get(dir, pathAtVersion string) error {
	f.gets = append(f.gets, dir+" "+pathAtVersion)
	if f.after > 0 && len(f.gets) == f.after {
		return f.err
	}
	return nil
}

func (f *fakeEditor) Tidy(string) error {
	f.tidy++
	if f.after == 0 {
		return f.err
	}
	return nil
}

var proxied = fixtureSource{goproxy: "https://proxy.golang.org,direct"}

func runTools(t *testing.T, tools toolset, args ...string) (int, string, string) {
	t.Helper()
	if tools.src == nil {
		tools.src = proxied
	}
	if tools.base == nil {
		tools.base = &fakeBase{err: errors.New("base must not be read")}
	}
	if tools.vuln == nil {
		tools.vuln = fakeVuln{err: errors.New("scan must not run")}
	}
	if tools.edit == nil {
		tools.edit = &fakeEditor{err: errors.New("edit must not run")}
	}
	var stdout, stderr bytes.Buffer
	code := runWith(args, tools, testNow, &stdout, &stderr)
	return code, stdout.String(), stderr.String()
}

func failGoMod(t *testing.T) string {
	t.Helper()
	data, err := os.ReadFile("testdata/fail/go.mod")
	require.NoError(t, err)
	return string(data)
}

func TestBaseFailsOnlyWhatTheChangeTouches(t *testing.T) {
	file := filepath.Join("testdata", "fail", "go.mod")
	gomod := failGoMod(t)
	cases := []struct {
		name     string
		base     *fakeBase
		code     int
		failed   []string
		reported []string
	}{
		{
			name: "every lag was already there",
			base: &fakeBase{data: gomod, found: true},
			code: exitOK,
			reported: []string{
				file + ":14: behind, untouched by this change: example.com/behind v1.2.0, latest v1.3.0: reported, not failed (ruling 640)",
				"behind, untouched by this change: example.com/pseudo v0.0.0-20250101000000-abcdefabcdef",
				"behind, untouched by this change: github.com/jeremyhahn/go-lib v0.3.0-alpha",
				"or the change did not touch it",
			},
		},
		{
			name:     "the change bumps a lag but not to latest",
			base:     &fakeBase{data: strings.Replace(gomod, "example.com/behind v1.2.0", "example.com/behind v1.1.0", 1), found: true},
			code:     exitUndocumented,
			failed:   []string{file + ":14: example.com/behind v1.2.0 is behind latest v1.3.0", "FAILED: 1 module(s) behind latest"},
			reported: []string{"untouched by this change: example.com/pseudo", "untouched by this change: github.com/jeremyhahn/go-lib"},
		},
		{
			name:     "the change adds a lagging require",
			base:     &fakeBase{data: strings.Replace(gomod, "\texample.com/behind v1.2.0\n", "", 1), found: true},
			code:     exitUndocumented,
			failed:   []string{"example.com/behind v1.2.0 is behind latest v1.3.0", "FAILED: 1 module(s)"},
			reported: []string{"untouched by this change: example.com/pseudo"},
		},
		{
			name:   "the change adds the module",
			base:   &fakeBase{found: false},
			code:   exitUndocumented,
			failed: []string{"FAILED: 3 module(s) behind latest"},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			code, stdout, stderr := runTools(t, toolset{base: tc.base}, "-base", "abc123", "testdata/fail")
			assert.Equal(t, tc.code, code, stderr)
			assert.Equal(t, []string{"testdata/fail@abc123"}, tc.base.asked)
			for _, want := range tc.failed {
				assert.Contains(t, stderr, want)
			}
			for _, want := range tc.reported {
				assert.Contains(t, stdout, want)
			}
			assert.Contains(t, stdout, "held: example.com/documented v1.2.0", "a documented hold is still listed")
		})
	}
}

func TestBaseErrors(t *testing.T) {
	t.Run("git fails", func(t *testing.T) {
		code, _, stderr := runTools(t, toolset{base: &fakeBase{err: &BaseError{Dir: "testdata/fail", Commit: "abc123", Err: errors.New("exit status 128"), Stderr: "bad object"}}}, "-base", "abc123", "testdata/fail")
		assert.Equal(t, exitLookup, code)
		assert.Contains(t, stderr, "depscheck: reading testdata/fail/go.mod at abc123: exit status 128: bad object")
	})
	t.Run("base go.mod does not parse", func(t *testing.T) {
		code, _, stderr := runTools(t, toolset{base: &fakeBase{data: "require (\n", found: true}}, "-base", "abc123", "testdata/fail")
		assert.Equal(t, exitLookup, code)
		assert.Contains(t, stderr, "go.mod@abc123")
	})
}

func TestTouched(t *testing.T) {
	dep := dependency{Kind: kindRequire, Module: "example.com/a", Path: "example.com/a", Version: "v1.0.0"}
	cases := []struct {
		name string
		base map[string]string
		want bool
	}{
		{"new module", nil, true},
		{"same line", map[string]string{"require example.com/a": "example.com/a@v1.0.0"}, false},
		{"version changed", map[string]string{"require example.com/a": "example.com/a@v0.9.0"}, true},
		{"added", map[string]string{"require example.com/b": "example.com/b@v1.0.0"}, true},
		{"was a replace only", map[string]string{"replace example.com/a": "example.com/a@v1.0.0"}, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.want, touched(dep, tc.base))
		})
	}
}

func TestGitBase(t *testing.T) {
	git, err := exec.LookPath("git")
	if err != nil {
		t.Skip("no git")
	}
	repo := t.TempDir()
	gitRun := func(dir string, args ...string) string {
		t.Helper()
		cmd := exec.CommandContext(t.Context(), git, append([]string{"-c", "user.name=t", "-c", "user.email=t@example.com", "-c", "commit.gpgsign=false", "-C", dir}, args...)...)
		cmd.Env = append(os.Environ(), "GIT_CONFIG_GLOBAL=/dev/null", "GIT_CONFIG_NOSYSTEM=1")
		out, err := cmd.CombinedOutput()
		require.NoError(t, err, string(out))
		return strings.TrimSpace(string(out))
	}
	gitRun(repo, "init", "-q")
	require.NoError(t, os.WriteFile(filepath.Join(repo, "go.mod"), []byte("module example.com/app\n"), 0o600))
	require.NoError(t, os.MkdirAll(filepath.Join(repo, "sdk"), 0o700))
	require.NoError(t, os.WriteFile(filepath.Join(repo, "sdk", "keep"), nil, 0o600))
	gitRun(repo, "add", ".")
	gitRun(repo, "commit", "-q", "-m", "base")
	head := gitRun(repo, "rev-parse", "HEAD")
	t.Setenv("GIT_CONFIG_GLOBAL", "/dev/null")

	g := gitBase{path: git}
	data, found, err := g.GoModAt(repo, head)
	require.NoError(t, err)
	assert.True(t, found)
	assert.Equal(t, "module example.com/app\n", string(data))

	_, found, err = g.GoModAt(filepath.Join(repo, "sdk"), head)
	require.NoError(t, err)
	assert.False(t, found, "a module added after base has no go.mod there")

	_, _, err = g.GoModAt(repo, "0123456789abcdef0123456789abcdef01234567")
	var baseErr *BaseError
	require.ErrorAs(t, err, &baseErr)
	assert.Equal(t, "0123456789abcdef0123456789abcdef01234567", baseErr.Commit)
}

func TestVuln(t *testing.T) {
	hit := map[string][]advisory{"example.com/current@v1.4.0": {{ID: "GO-2026-0001", Fixed: "v1.4.1"}}}
	stdlibHit := map[string][]advisory{"stdlib@v1.27.0": {{ID: "GO-2026-0100", Fixed: "v1.27.1"}}}
	indirectHit := map[string][]advisory{"golang.org/x/net@v0.29.0": {{ID: "GO-2026-0002", Fixed: "v0.30.0"}}}
	gomod := failGoMod(t)
	cases := []struct {
		name   string
		dir    string
		args   []string
		tools  toolset
		code   int
		stderr []string
		stdout string
	}{
		{name: "clean", dir: "testdata/pass", tools: toolset{src: fixtureSource{goproxy: "direct"}, vuln: fakeVuln{}}, code: exitOK, stdout: "deps-check: every dependency"},
		{
			name:   "a current version that is vulnerable still fails",
			dir:    "testdata/pass",
			tools:  toolset{src: fixtureSource{goproxy: "direct"}, vuln: fakeVuln{hits: hit}},
			code:   exitVulnerable,
			stderr: []string{"testdata/pass: vulnerable: example.com/current v1.4.0: GO-2026-0001 (fixed in v1.4.1)", "FAILED: 1 required module version(s) govulncheck reports vulnerable"},
		},
		{
			name:   "an untouched vulnerable version fails",
			dir:    "testdata/fail",
			args:   []string{"-base", "abc123"},
			tools:  toolset{base: &fakeBase{data: gomod, found: true}, vuln: fakeVuln{hits: hit}},
			code:   exitVulnerable,
			stderr: []string{"vulnerable: example.com/current v1.4.0", "FAILED: 1 required module version(s) govulncheck reports vulnerable"},
		},
		{
			name:   "a vulnerability outranks an undocumented lag",
			dir:    "testdata/fail",
			tools:  toolset{vuln: fakeVuln{hits: hit}},
			code:   exitVulnerable,
			stderr: []string{"vulnerable: example.com/current v1.4.0", "FAILED: 1 required module version(s) govulncheck reports vulnerable"},
		},
		{
			name:   "stdlib only is reported, not failed",
			dir:    "testdata/pass",
			tools:  toolset{src: fixtureSource{goproxy: "direct"}, vuln: fakeVuln{hits: stdlibHit}},
			code:   exitOK,
			stdout: "testdata/pass: reported, not failed: stdlib go1.27.0: GO-2026-0100 (fixed in v1.27.1), the portfolio-wide toolchain pass carries it (CLAUDE.md Go toolchain rule); the security job gates reachable stdlib findings",
		},
		{
			name:   "toolchain is reported, not failed",
			dir:    "testdata/pass",
			tools:  toolset{src: fixtureSource{goproxy: "direct"}, vuln: fakeVuln{hits: map[string][]advisory{"toolchain@v1.27.0": {{ID: "GO-2026-0101"}}}}},
			code:   exitOK,
			stdout: "reported, not failed: toolchain go1.27.0: GO-2026-0101, the portfolio-wide toolchain pass carries it",
		},
		{
			name:   "an indirect-only module not in go.mod is reported, not failed",
			dir:    "testdata/pass",
			tools:  toolset{src: fixtureSource{goproxy: "direct"}, vuln: fakeVuln{hits: indirectHit}},
			code:   exitOK,
			stdout: "testdata/pass: reported, not failed: golang.org/x/net v0.29.0: GO-2026-0002 (fixed in v0.30.0), no require line in go.mod",
		},
		{
			name:   "stdlib and indirect-only beside a required hit: only the requirement fails",
			dir:    "testdata/pass",
			tools:  toolset{src: fixtureSource{goproxy: "direct"}, vuln: fakeVuln{hits: merge(hit, stdlibHit, indirectHit)}},
			code:   exitVulnerable,
			stderr: []string{"vulnerable: example.com/current v1.4.0", "FAILED: 1 required module version(s) govulncheck reports vulnerable"},
			stdout: "reported, not failed: stdlib go1.27.0",
		},
		{
			name:   "a // indirect require line in go.mod is a requirement and fails",
			dir:    "testdata/fail",
			args:   []string{"-base", "abc123"},
			tools:  toolset{base: &fakeBase{data: gomod, found: true}, vuln: fakeVuln{hits: map[string][]advisory{"example.com/dep@v1.0.0": {{ID: "GO-2026-0003"}}}}},
			code:   exitVulnerable,
			stderr: []string{"vulnerable: example.com/dep v1.0.0: GO-2026-0003", "FAILED: 1 required module version(s)"},
		},
		{
			name:   "scanner fails",
			dir:    "testdata/pass",
			tools:  toolset{src: fixtureSource{goproxy: "direct"}, vuln: fakeVuln{err: &VulnError{Dir: "testdata/pass", Err: errors.New("exit status 1"), Stderr: "no go.sum"}}},
			code:   exitLookup,
			stderr: []string{"depscheck: govulncheck in testdata/pass: exit status 1: no go.sum"},
		},
		{
			name:   "scanner missing",
			dir:    "testdata/pass",
			args:   []string{"-govulncheck", "/nonexistent/govulncheck"},
			tools:  toolset{src: fixtureSource{goproxy: "direct"}, vuln: govulncheck{}},
			code:   exitLookup,
			stderr: []string{"depscheck: govulncheck in testdata/pass"},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			args := append(append([]string{"-vuln"}, tc.args...), tc.dir)
			code, stdout, stderr := runTools(t, tc.tools, args...)
			assert.Equal(t, tc.code, code, stderr)
			for _, want := range tc.stderr {
				assert.Contains(t, stderr, want)
			}
			assert.Contains(t, stdout, tc.stdout)
		})
	}
}

func TestDecodeVulns(t *testing.T) {
	stream := `{"config":{"protocol_version":"v1.0.0","scan_level":"module"}}
{"progress":{"message":"Scanning your code"}}
{"osv":{"id":"GO-2026-0002"}}
{"finding":{"osv":"GO-2026-0002","fixed_version":"v0.30.0","trace":[{"module":"golang.org/x/net","version":"v0.29.0"}]}}
{"finding":{"osv":"GO-2026-0002","fixed_version":"v0.30.0","trace":[{"module":"golang.org/x/net","version":"v0.29.0","package":"golang.org/x/net/html"}]}}
{"finding":{"osv":"GO-2026-0001","fixed_version":"v0.30.0","trace":[{"module":"golang.org/x/net","version":"v0.29.0"}]}}
{"finding":{"osv":"GO-2026-0003","fixed_version":"v1.27.5","trace":[{"module":"stdlib","version":"v1.27.4"}]}}
`
	got, err := decodeVulns("d", strings.NewReader(stream))
	require.NoError(t, err)
	assert.Equal(t, map[string][]advisory{
		"golang.org/x/net@v0.29.0": {{ID: "GO-2026-0001", Fixed: "v0.30.0"}, {ID: "GO-2026-0002", Fixed: "v0.30.0"}},
		"stdlib@v1.27.4":           {{ID: "GO-2026-0003", Fixed: "v1.27.5"}},
	}, got)

	empty, err := decodeVulns("d", strings.NewReader(`{"config":{"scan_level":"module"}}`))
	require.NoError(t, err)
	assert.Empty(t, empty)

	for name, in := range map[string]string{"garbage": "not json", "no config": `{"finding":{}}`, "nothing": ""} {
		t.Run(name, func(t *testing.T) {
			_, err := decodeVulns("d", strings.NewReader(in))
			var vulnErr *VulnError
			require.ErrorAs(t, err, &vulnErr)
		})
	}
}

func TestBump(t *testing.T) {
	t.Run("moves every failing lag and tidies", func(t *testing.T) {
		edit := &fakeEditor{}
		code, stdout, stderr := runTools(t, toolset{edit: edit, vuln: fakeVuln{}}, "-bump", "testdata/fail")
		assert.Equal(t, exitOK, code, stderr)
		assert.Equal(t, []string{
			"testdata/fail example.com/behind@v1.3.0",
			"testdata/fail example.com/pseudo@v0.0.0-20260101000000-123456123456",
			"testdata/fail github.com/jeremyhahn/go-lib@v0.4.0-alpha",
		}, edit.gets, "documented, forked, fresh and current lines stay")
		assert.Equal(t, 1, edit.tidy)
		assert.Contains(t, stdout, "bumped example.com/behind v1.2.0 -> v1.3.0")
		assert.Contains(t, stdout, "deps-bump: done; re-run deps-check to verify (depscheck v1.0.0)")
	})
	t.Run("nothing to move", func(t *testing.T) {
		edit := &fakeEditor{}
		code, stdout, _ := runTools(t, toolset{src: fixtureSource{goproxy: "direct"}, edit: edit, vuln: fakeVuln{}}, "-bump", "testdata/pass")
		assert.Equal(t, exitOK, code)
		assert.Empty(t, edit.gets, "a fresh release waits out its 7 days")
		assert.Equal(t, 0, edit.tidy)
		assert.NotContains(t, stdout, "bumped")
	})
	t.Run("a vulnerable indirect requirement with a fix is bumped", func(t *testing.T) {
		// testdata/pass has no lag past the 7-day window: without the scan
		// the bump would move nothing and the deps-check after it would
		// fail on the vulnerable // indirect line.
		edit := &fakeEditor{}
		vuln := fakeVuln{hits: map[string][]advisory{
			"golang.org/x/net@v0.29.0":  {{ID: "GO-2026-0002", Fixed: "v0.30.0"}, {ID: "GO-2026-0004", Fixed: "v0.31.0"}},
			"example.com/nofix@v1.0.0":  {{ID: "GO-2026-0005"}},
			"stdlib@v1.27.0":            {{ID: "GO-2026-0100", Fixed: "v1.27.1"}},
			"example.com/notreq@v1.0.0": {{ID: "GO-2026-0006", Fixed: "v1.0.1"}},
		}}
		code, stdout, stderr := runTools(t, toolset{src: fixtureSource{goproxy: "direct"}, edit: edit, vuln: vuln}, "-bump", "testdata/vulnbump")
		assert.Equal(t, exitOK, code, stderr)
		assert.Equal(t, []string{"testdata/vulnbump golang.org/x/net@v0.31.0"}, edit.gets,
			"the highest fix of a required // indirect module; no fix, stdlib and unrequired modules stay")
		assert.Equal(t, 1, edit.tidy)
		assert.Contains(t, stdout, "bumped golang.org/x/net v0.29.0 -> v0.31.0")
	})
	t.Run("a vulnerable lag moves to the higher of latest and its fix", func(t *testing.T) {
		edit := &fakeEditor{}
		vuln := fakeVuln{hits: map[string][]advisory{
			"example.com/behind@v1.2.0":  {{ID: "GO-2026-0007", Fixed: "v1.2.1"}},
			"example.com/current@v1.4.0": {{ID: "GO-2026-0001", Fixed: "v1.4.1"}},
			"example.com/dep@v1.0.0":     {{ID: "GO-2026-0003", Fixed: "v1.0.2"}},
		}}
		code, _, stderr := runTools(t, toolset{edit: edit, vuln: vuln}, "-bump", "testdata/fail")
		assert.Equal(t, exitOK, code, stderr)
		assert.Equal(t, []string{
			"testdata/fail example.com/behind@v1.3.0",
			"testdata/fail example.com/pseudo@v0.0.0-20260101000000-123456123456",
			"testdata/fail github.com/jeremyhahn/go-lib@v0.4.0-alpha",
			"testdata/fail example.com/current@v1.4.1",
			"testdata/fail example.com/dep@v1.0.2",
		}, edit.gets)
	})
	t.Run("a scan failure stops the bump before any edit", func(t *testing.T) {
		edit := &fakeEditor{}
		code, _, stderr := runTools(t, toolset{edit: edit, vuln: fakeVuln{err: &VulnError{Dir: "testdata/fail", Err: errors.New("exit status 1")}}}, "-bump", "testdata/fail")
		assert.Equal(t, exitLookup, code)
		assert.Contains(t, stderr, "govulncheck in testdata/fail")
		assert.Empty(t, edit.gets)
	})
	t.Run("go get fails", func(t *testing.T) {
		edit := &fakeEditor{err: &GoCommandError{Dir: "testdata/fail", Args: []string{"get"}, Err: errors.New("exit status 1")}, after: 2}
		code, _, stderr := runTools(t, toolset{edit: edit, vuln: fakeVuln{}}, "-bump", "testdata/fail")
		assert.Equal(t, exitLookup, code)
		assert.Len(t, edit.gets, 2)
		assert.Contains(t, stderr, "exit status 1")
	})
	t.Run("go mod tidy fails", func(t *testing.T) {
		edit := &fakeEditor{err: errors.New("tidy refused")}
		code, _, stderr := runTools(t, toolset{edit: edit, vuln: fakeVuln{}}, "-bump", "testdata/fail")
		assert.Equal(t, exitLookup, code)
		assert.Contains(t, stderr, "tidy refused")
	})
	t.Run("a lookup failure stops the bump", func(t *testing.T) {
		code, _, _ := runTools(t, toolset{src: fixtureSource{goproxy: "off"}}, "-bump", "testdata/fail")
		assert.Equal(t, exitLookup, code)
	})
	t.Run("a go list lookup error stops the bump before any edit", func(t *testing.T) {
		gomod := filepath.Join("testdata", "lookup", "go.mod")
		before, err := os.ReadFile(gomod)
		require.NoError(t, err)
		edit := &fakeEditor{}
		code, stdout, stderr := runTools(t, toolset{src: fixtureSource{goproxy: "direct"}, edit: edit}, "-bump", "testdata/lookup")
		assert.Equal(t, exitLookup, code, stdout)
		assert.Contains(t, stderr, "cannot look up the latest version of github.com/jeremyhahn/go-private: git ls-remote -q origin: exit status 128")
		assert.Contains(t, stderr, "deps-bump: FAILED: the latest version of 1 module(s) in testdata/lookup could not be looked up; go.mod left unchanged")
		assert.NotContains(t, stdout, "deps-bump: done")
		// testdata/lookup lists example.com/behind, outdated and eligible,
		// before the failing lookup: a check that ran after (or inside) the
		// edit loop would already have called Get on it.
		assert.NotContains(t, stdout, "bumped example.com/behind")
		assert.Empty(t, edit.gets)
		assert.Equal(t, 0, edit.tidy)
		after, err := os.ReadFile(gomod)
		require.NoError(t, err)
		assert.Equal(t, string(before), string(after), "go.mod unchanged")
	})
	for _, extra := range [][]string{{"-base", "abc"}, {"-vuln"}} {
		t.Run("-bump with "+extra[0], func(t *testing.T) {
			code, _, stderr := runTools(t, toolset{}, append(append([]string{"-bump"}, extra...), "testdata/fail")...)
			assert.Equal(t, exitLookup, code)
			assert.Contains(t, stderr, ErrUsage.Error())
		})
	}
}

// graphEditor is a moduleEditor over a tiny module graph that rewrites
// the require block of go.mod the way go get does: upgrading a module
// raises the requirements of its new version (minimal version
// selection), and go get of an exact version lowers any module whose
// selected version needs more than that, back to v1.0.0.
type graphEditor struct {
	deps   map[string]map[string]string // path@version -> path -> minimum version
	gets   []string
	tidied []string
}

func (g *graphEditor) Get(dir, pathAtVersion string) error {
	g.gets = append(g.gets, pathAtVersion)
	sel, err := goModRequires(dir)
	if err != nil {
		return err
	}
	path, version, _ := strings.Cut(pathAtVersion, "@")
	sel[path] = version
	for p, min := range g.deps[pathAtVersion] {
		if semverLess(sel[p], min) {
			sel[p] = min
		}
	}
	for m, v := range sel {
		if need, ok := g.deps[m+"@"+v][path]; ok && semverLess(version, need) {
			sel[m] = "v1.0.0"
		}
	}
	paths := make([]string, 0, len(sel))
	for p := range sel {
		paths = append(paths, p)
	}
	sort.Strings(paths)
	var b strings.Builder
	b.WriteString("module example.com/m\n\ngo 1.22\n\nrequire (\n")
	for _, p := range paths {
		b.WriteString("\t" + p + " " + sel[p] + "\n")
	}
	b.WriteString(")\n")
	return os.WriteFile(filepath.Join(dir, "go.mod"), []byte(b.String()), 0o600)
}

func (g *graphEditor) Tidy(dir string) error {
	g.tidied = append(g.tidied, dir)
	return nil
}

// behindFinding is a bumpable lag of the go.mod require of path.
// goModRequires reads the require versions of dir/go.mod.
func goModRequires(dir string) (map[string]string, error) {
	file := filepath.Join(dir, "go.mod")
	data, err := os.ReadFile(file) //#nosec G304 -- a test fixture
	if err != nil {
		return nil, err
	}
	directives, err := parseGoMod(file, data)
	if err != nil {
		return nil, err
	}
	out := make(map[string]string)
	for _, d := range directives {
		if d.Kind == kindRequire {
			out[d.Path] = d.Version
		}
	}
	return out, nil
}

func behindFinding(dir, path, version, latest string) finding {
	return finding{
		Dep:    dependency{Kind: kindRequire, Module: path, Path: path, Version: version, File: filepath.Join(dir, "go.mod"), Line: 1},
		Latest: latest, Behind: true,
	}
}

func TestBumpFixIsAFloor(t *testing.T) {
	// Upgrading example.com/a to v1.1.0 raises example.com/b to v1.5.0;
	// b's advisory is fixed in v1.1.0. A go get b@v1.1.0 after the upgrade
	// would lower a again; the fix is a floor already met, so it is
	// skipped.
	dir := t.TempDir()
	writeFile(t, dir, "go.mod", "module example.com/m\n\ngo 1.22\n\nrequire (\n\texample.com/a v1.0.0\n\texample.com/b v1.0.0 // indirect\n)\n")
	edit := &graphEditor{deps: map[string]map[string]string{
		"example.com/a@v1.1.0": {"example.com/b": "v1.5.0"},
	}}
	vuln := fakeVuln{hits: map[string][]advisory{"example.com/b@v1.0.0": {{ID: "GO-2026-0020", Fixed: "v1.1.0"}}}}
	var stdout bytes.Buffer
	err := bumpModule(edit, vuln, dir, []finding{behindFinding(dir, "example.com/a", "v1.0.0", "v1.1.0")}, &stdout)
	require.NoError(t, err)
	assert.Equal(t, []string{"example.com/a@v1.1.0"}, edit.gets, "no exact go get below the selected b")
	sel, err := goModRequires(dir)
	require.NoError(t, err)
	assert.Equal(t, map[string]string{"example.com/a": "v1.1.0", "example.com/b": "v1.5.0"}, sel)
	assert.Equal(t, []string{dir}, edit.tidied)
}

func TestBumpFixBelowSelectionStillApplies(t *testing.T) {
	// The fix is not met by the upgrade: it is applied after it, and the
	// final selection keeps both.
	dir := t.TempDir()
	writeFile(t, dir, "go.mod", "module example.com/m\n\ngo 1.22\n\nrequire (\n\texample.com/a v1.0.0\n\texample.com/b v1.0.0 // indirect\n)\n")
	edit := &graphEditor{deps: map[string]map[string]string{
		"example.com/a@v1.1.0": {"example.com/b": "v1.2.0"},
	}}
	vuln := fakeVuln{hits: map[string][]advisory{"example.com/b@v1.0.0": {{ID: "GO-2026-0021", Fixed: "v1.3.0"}}}}
	var stdout bytes.Buffer
	require.NoError(t, bumpModule(edit, vuln, dir, []finding{behindFinding(dir, "example.com/a", "v1.0.0", "v1.1.0")}, &stdout))
	assert.Equal(t, []string{"example.com/a@v1.1.0", "example.com/b@v1.3.0"}, edit.gets)
	assert.Contains(t, stdout.String(), "bumped example.com/b v1.2.0 -> v1.3.0")
}

func TestBumpTidiesLocalReplaceDependents(t *testing.T) {
	// root/dep replaces example.com/root with ../, so none of its own
	// requirements is a bump candidate; the root bump still raises its
	// floors, so it is tidied after the root moved, and only then.
	newPair := func(t *testing.T) (string, string) {
		root := t.TempDir()
		writeFile(t, root, "go.mod", "module example.com/m\n\ngo 1.22\n\nrequire example.com/a v1.0.0\n")
		dep := filepath.Join(root, "dep")
		writeFile(t, dep, "go.mod", "module example.com/dep\n\ngo 1.22\n\nrequire example.com/m v0.0.0\n\nreplace example.com/m => ../\n")
		return root, dep
	}
	t.Run("after a root bump", func(t *testing.T) {
		root, dep := newPair(t)
		edit := &graphEditor{}
		var stdout bytes.Buffer
		require.NoError(t, bumpModule(edit, fakeVuln{}, root, []finding{behindFinding(root, "example.com/a", "v1.0.0", "v1.1.0")}, &stdout))
		require.NoError(t, bumpModule(edit, fakeVuln{}, dep, nil, &stdout))
		assert.Equal(t, []string{root, dep}, edit.tidied)
	})
	t.Run("not when the root did not move", func(t *testing.T) {
		root, dep := newPair(t)
		edit := &graphEditor{}
		var stdout bytes.Buffer
		require.NoError(t, bumpModule(edit, fakeVuln{}, root, nil, &stdout))
		require.NoError(t, bumpModule(edit, fakeVuln{}, dep, nil, &stdout))
		assert.Empty(t, edit.tidied)
	})
}

func TestBumpable(t *testing.T) {
	dep := dependency{Kind: kindRequire, Module: "m", Path: "m", Version: "v1"}
	cases := []struct {
		name string
		f    finding
		want bool
	}{
		{"behind", finding{Dep: dep, Behind: true, Latest: "v2"}, true},
		{"current", finding{Dep: dep, Latest: "v1"}, false},
		{"fresh", finding{Dep: dep, Behind: true, Fresh: true, Latest: "v2"}, false},
		{"documented", finding{Dep: dependency{Kind: kindRequire, Module: "m", Path: "m", Version: "v1", Reason: "held"}, Behind: true, Latest: "v2"}, false},
		{"lookup", finding{Dep: dep, Lookup: &LookupError{Module: "m"}}, false},
		{"fork", finding{Dep: dependency{Kind: kindReplace, Module: "m", Path: "f", Version: "v1"}, Behind: true, Latest: forkLatest}, false},
		{"unused", finding{Dep: dep, Unused: true}, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.want, bumpable(tc.f))
		})
	}
}

func TestReportUntouchedVersusFresh(t *testing.T) {
	dep := dependency{File: "go.mod", Line: 3, Kind: kindRequire, Module: "m", Path: "m", Version: "v1.0.0"}
	var stdout, stderr bytes.Buffer
	code := report([]finding{{Dep: dep, Behind: true, Latest: "v1.1.0", Untouched: true}}, nil, &stdout, &stderr)
	assert.Equal(t, exitOK, code)
	assert.Equal(t, "go.mod:3: behind, untouched by this change: m v1.0.0, latest v1.1.0: reported, not failed (ruling 640)\n", strings.SplitAfter(stdout.String(), "\n")[0])
	assert.Empty(t, stderr.String())
}

func TestRequiredModules(t *testing.T) {
	got, err := requiredModules("testdata/fail")
	require.NoError(t, err)
	assert.True(t, got["example.com/current"])
	assert.True(t, got["example.com/dep"], "a // indirect require line is a requirement")
	assert.False(t, got["example.com/gone"], "a replace with no require line is not")
	assert.False(t, got["stdlib"])

	_, err = requiredModules("testdata/missing")
	assert.Error(t, err)
}

// TestScanDir: a module scan runs in a package directory of the module,
// because govulncheck -scan module takes no pattern and fails with "no Go
// files" in a root that has none.
func TestScanDir(t *testing.T) {
	if _, err := exec.LookPath("go"); err != nil {
		t.Skip("no go command")
	}
	write := func(t *testing.T, root string, files map[string]string) {
		t.Helper()
		for f, body := range files {
			p := filepath.Join(root, f)
			require.NoError(t, os.MkdirAll(filepath.Dir(p), 0o755))
			require.NoError(t, os.WriteFile(p, []byte(body), 0o600))
		}
	}
	const mod = "module example.com/x\n\ngo 1.21\n"
	const pkg = "package x\n"
	const tagged = "//go:build never\n\npackage x\n"
	cases := []struct {
		name  string
		files map[string]string
		tags  string
		want  string
	}{
		{"root package", map[string]string{"go.mod": mod, "x.go": pkg}, "", "."},
		{"empty root", map[string]string{"go.mod": mod, "doc_test.go": pkg, "pkg/b/b.go": pkg, "pkg/c/c.go": pkg}, "", "pkg/b"},
		{"every file tagged", map[string]string{"go.mod": mod, "a/t.go": tagged}, "never", "a"},
		{"skips build-tagged, nested module and ignored dirs", map[string]string{
			"go.mod": mod, "a/t.go": tagged, "b/go.mod": "module example.com/b\n\ngo 1.21\n", "b/b.go": pkg,
			"_x/x.go": pkg, "testdata/t.go": pkg, "z/z.go": pkg,
		}, "", "z"},
		{"no package", map[string]string{"go.mod": mod, "x_test.go": pkg}, "", "."},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			root := t.TempDir()
			write(t, root, tc.files)
			t.Setenv("GOFLAGS", "-mod=mod")
			t.Setenv("GOWORK", "off")
			got, err := scanDir("go", tc.tags, root)
			require.NoError(t, err)
			want, err := filepath.EvalSymlinks(filepath.Join(root, tc.want))
			require.NoError(t, err)
			gotReal, err := filepath.EvalSymlinks(got)
			require.NoError(t, err)
			assert.Equal(t, want, gotReal)
		})
	}

	_, err := scanDir("go", "", filepath.Join(t.TempDir(), "missing"))
	assert.Error(t, err)
}
