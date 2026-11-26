package buildgates

import (
	"os/exec"
	"path/filepath"
	"sort"
	"strings"
	"testing"
)

// runMaskLAN runs scripts/ci-mask-lan.sh with only PATH and the given
// variables exported, and returns the values it asks the runner to mask.
func runMaskLAN(t *testing.T, env map[string]string) []string {
	t.Helper()
	cmd := exec.CommandContext(t.Context(), "bash", filepath.Join("..", "..", "scripts", "ci-mask-lan.sh"))
	cmd.Env = []string{"PATH=/usr/bin:/bin"}
	for k, v := range env {
		cmd.Env = append(cmd.Env, k+"="+v)
	}
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("ci-mask-lan.sh: %v\n%s", err, out)
	}
	var masks []string
	for _, line := range strings.Split(strings.TrimSpace(string(out)), "\n") {
		if v, ok := strings.CutPrefix(line, "::add-mask::"); ok && v != "" {
			masks = append(masks, v)
		}
	}
	return masks
}

// masked applies the masks to line the way the runner does, longest first.
func masked(line string, masks []string) string {
	sorted := append([]string(nil), masks...)
	sort.Slice(sorted, func(i, j int) bool { return len(sorted[i]) > len(sorted[j]) })
	for _, m := range sorted {
		line = strings.ReplaceAll(line, m, "***")
	}
	return line
}

// TestCIMaskLANMasksUserinfo: a configured URL that carries userinfo has
// both its credential and its private host masked in every later log line.
func TestCIMaskLANMasksUserinfo(t *testing.T) {
	cases := []struct {
		name, variable, value string
		hidden                []string
	}{
		{"http with user and password", "DEVCACHE_APK", "http://user:s3cr3t-http@10.1.2.3:3142/alpine", []string{"s3cr3t-http", "10.1.2.3"}},
		{"https with user and password in a list", "GOPROXY", "https://user:s3cr3t-https@192.168.5.6/goproxy,direct", []string{"s3cr3t-https", "192.168.5.6"}},
		{"ssh with user and password", "CI_REGISTRY", "ssh://git:s3cr3t-ssh@172.16.0.9:2222/repo", []string{"s3cr3t-ssh", "172.16.0.9"}},
		{"https with a token as the user", "RELEASE_REGISTRY", "https://tok3n-0123456789@10.9.9.9/v2", []string{"tok3n-0123456789", "10.9.9.9"}},
		{"public host with a password", "GOPROXY", "https://user:s3cr3t-public@proxy.example.com/,direct", []string{"s3cr3t-public"}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			masks := runMaskLAN(t, map[string]string{c.variable: c.value})
			// A later line may print the URL, or its host or credential alone.
			line := "echo " + c.value + " " + strings.Join(c.hidden, " ")
			got := masked(line, masks)
			for _, h := range c.hidden {
				if strings.Contains(got, h) {
					t.Errorf("%q still shows %q after masking %q", got, h, masks)
				}
			}
		})
	}
}

// TestCIMaskLANLeavesPublicHosts: public hosts and keywords without a
// credential are not masked.
func TestCIMaskLANLeavesPublicHosts(t *testing.T) {
	masks := runMaskLAN(t, map[string]string{"GOPROXY": "https://proxy.golang.org,direct", "GITHUB_SERVER_URL": "https://github.com"})
	if len(masks) != 0 {
		t.Fatalf("public entries masked: %q", masks)
	}
	masks = runMaskLAN(t, map[string]string{"DEVCACHE_GO": "http://10.4.5.6:8080/go"})
	if got := masked("GET http://10.4.5.6:8080/go and 10.4.5.6", masks); strings.Contains(got, "10.4.5.6") {
		t.Fatalf("a private host is not masked: %q", got)
	}
}
