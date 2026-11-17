package main

import (
	"os"
	"path/filepath"
	"testing"
)

// writeFile writes content to dir/name, creating its parent directories.
func writeFile(t *testing.T, dir, name, content string) {
	t.Helper()
	p := filepath.Join(dir, name)
	require.NoError(t, os.MkdirAll(filepath.Dir(p), 0o750))
	require.NoError(t, os.WriteFile(p, []byte(content), 0o600))
}

// TestSemverLess pins SemVer 2.0 precedence, numeric prerelease
// identifiers and Go pseudo-versions included: each pair is strictly
// ascending.
func TestSemverLess(t *testing.T) {
	for _, pair := range [][2]string{
		{"v1.0.0", "v1.0.1"},
		{"v1.9.0", "v1.10.0"},
		{"v1.0.0-rc.9", "v1.0.0-rc.10"},
		{"v1.0.0-rc.2", "v1.0.0-rc.10"},
		{"v1.0.0-alpha", "v1.0.0-alpha.1"},
		{"v1.0.0-alpha.1", "v1.0.0-alpha.beta"},
		{"v1.0.0-alpha.beta", "v1.0.0-beta"},
		{"v1.0.0-beta.11", "v1.0.0-rc.1"},
		{"v1.0.0-rc.1", "v1.0.0"},
		{"v1.0.0-1", "v1.0.0-alpha"},
		{"v0.3.0-alpha", "v0.3.1-0.20260101000000-abcdefabcdef"},
		{"v0.3.0-alpha", "v0.3.0-alpha.0.20260101000000-abcdefabcdef"},
		{"v0.3.0-alpha.0.20260101000000-abcdefabcdef", "v0.3.0-alpha.1"},
		{"v0.3.1-0.20260101000000-abcdefabcdef", "v0.3.1"},
		{"v0.0.0-20250101000000-abcdefabcdef", "v0.0.0-20260101000000-123456123456"},
		{"v1.0.0-rc.99999999999999999999", "v1.0.0-rc.100000000000000000000"},
	} {
		assert.True(t, semverLess(pair[0], pair[1]), pair[0]+" < "+pair[1])
		assert.False(t, semverLess(pair[1], pair[0]), pair[1]+" > "+pair[0])
	}
	for _, v := range []string{"v1.0.0", "v1.0.0-rc.10", "v0.3.1-0.20260101000000-abcdefabcdef"} {
		assert.False(t, semverLess(v, v), v)
	}
	assert.False(t, semverLess("v1.0.0+meta", "v1.0.0"), "build metadata has no precedence")
	assert.False(t, semverLess("v1.0.0", "v1.0.0+meta"), "build metadata has no precedence")
}

// TestVulnFixNumericPrerelease: a requirement at rc.9 whose advisory is
// fixed in rc.10 is planned to rc.10, and two advisories combine to the
// higher numeric prerelease.
func TestVulnFixNumericPrerelease(t *testing.T) {
	dir := t.TempDir()
	writeFile(t, dir, "go.mod", "module example.com/m\n\ngo 1.22\n\nrequire example.com/pre v1.0.0-rc.9\n")
	fixes, err := vulnFixes(fakeVuln{hits: map[string][]advisory{
		"example.com/pre@v1.0.0-rc.9": {{ID: "GO-2026-0010", Fixed: "v1.0.0-rc.10"}, {ID: "GO-2026-0011", Fixed: "v1.0.0-rc.2"}},
	}}, dir)
	require.NoError(t, err)
	require.Len(t, fixes, 1)
	assert.Equal(t, "v1.0.0-rc.10", fixes[0].fixed)
}
