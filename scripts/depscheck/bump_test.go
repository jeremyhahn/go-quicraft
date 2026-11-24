package main

import (
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

// listScanner runs the real package listing govulncheck's Scan runs first
// (scanDir, the go list -e ./... that finds the directory to scan), then
// answers the scan itself from memory, keyed by module directory.
type listScanner struct {
	hits map[string]map[string][]advisory
}

func (s listScanner) Scan(dir string) (map[string][]advisory, error) {
	if _, err := scanDir("go", "", dir); err != nil {
		return nil, err
	}
	return s.hits[dir], nil
}

// rootEditor raises the root's requirement the way go get does, and runs the
// real go mod tidy (offline: every module of the fixture is a local
// directory).
type rootEditor struct {
	tidied []string
}

func (e *rootEditor) Get(dir, pathAtVersion string) error {
	file := filepath.Join(dir, "go.mod")
	data, err := os.ReadFile(file) //#nosec G304 -- a test fixture
	if err != nil {
		return err
	}
	path, version, _ := strings.Cut(pathAtVersion, "@")
	moved := strings.Replace(string(data), path+" v1.0.0", path+" "+version, 1)
	return os.WriteFile(file, []byte(moved), 0o600)
}

func (e *rootEditor) Tidy(dir string) error {
	e.tidied = append(e.tidied, dir)
	return goCommand{path: "go"}.Tidy(dir)
}

// localUpstreamFixture writes a root module that requires example.com/lib
// v1.0.0 (a local directory), and a dependent module that replaces the root
// and lib with local directories and records lib v1.0.0 as // indirect.
// It returns the root and dependent directories.
func localUpstreamFixture(t *testing.T) (string, string) {
	t.Helper()
	if _, err := exec.LookPath("go"); err != nil {
		t.Skip("no go command")
	}
	t.Setenv("GOWORK", "off")
	t.Setenv("GOPROXY", "off")
	t.Setenv("GOFLAGS", "")
	t.Setenv("GOTOOLCHAIN", "local")
	base := t.TempDir()
	files := map[string]string{
		"root/go.mod":     "module example.com/root\n\ngo 1.22\n\nrequire example.com/lib v1.0.0\n\nreplace example.com/lib => ./lib\n",
		"root/root.go":    "package root\n\nimport _ \"example.com/lib\"\n",
		"root/lib/go.mod": "module example.com/lib\n\ngo 1.22\n",
		"root/lib/lib.go": "package lib\n",
		"dep/go.mod": "module example.com/dep\n\ngo 1.22\n\nrequire example.com/root v0.0.0\n\nrequire example.com/lib v1.0.0 // indirect\n\n" +
			"replace example.com/root => ../root\n\nreplace example.com/lib => ../root/lib\n",
		"dep/dep.go": "package dep\n\nimport _ \"example.com/root\"\n",
	}
	for name, body := range files {
		p := filepath.Join(base, name)
		require.NoError(t, os.MkdirAll(filepath.Dir(p), 0o700))
		require.NoError(t, os.WriteFile(p, []byte(body), 0o600))
	}
	return filepath.Join(base, "root"), filepath.Join(base, "dep")
}

// TestBumpLocalUpstreamMoved pins the deps-bump run over a root module and a
// module that replaces it with a local directory (contrib/dragonboat): the
// root's bump leaves the dependent's go.mod stale, so listing the dependent's
// packages fails with "updates to go.mod needed" until it is tidied.
func TestBumpLocalUpstreamMoved(t *testing.T) {
	vulnLib := map[string][]advisory{"example.com/lib@v1.0.0": {{ID: "GO-2026-0008", Fixed: "v1.1.0"}}}

	t.Run("the bump tidies the dependent before it lists it", func(t *testing.T) {
		root, dep := localUpstreamFixture(t)
		edit := &rootEditor{}
		scan := listScanner{hits: map[string]map[string][]advisory{root: vulnLib}}
		code, stdout, stderr := runTools(t, toolset{src: fixtureSource{goproxy: "direct"}, edit: edit, vuln: scan}, "-bump", root, dep)
		require.Equal(t, exitOK, code, stderr)
		assert.Contains(t, stdout, "bumped example.com/lib v1.0.0 -> v1.1.0")
		assert.Contains(t, edit.tidied, dep, "the dependent is tidied")
		data, err := os.ReadFile(filepath.Join(dep, "go.mod")) //#nosec G304 -- a test fixture
		require.NoError(t, err)
		assert.Contains(t, string(data), "example.com/lib v1.1.0", "the dependent's floor follows the root")
	})

	t.Run("deps-check still fails a stale go.mod", func(t *testing.T) {
		root, dep := localUpstreamFixture(t)
		require.NoError(t, (&rootEditor{}).Get(root, "example.com/lib@v1.1.0"))
		code, _, stderr := runTools(t, toolset{src: fixtureSource{goproxy: "direct"}, vuln: listScanner{}}, "-vuln", dep)
		assert.Equal(t, exitLookup, code)
		assert.Contains(t, stderr, "updates to go.mod needed")
	})
}
