package buildgates

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// topLevelPermissions returns the workflow's top-level permissions block:
// the `permissions:` line and its indented lines. ok is false without one.
func topLevelPermissions(workflow string) (block []string, ok bool) {
	lines := strings.Split(workflow, "\n")
	for i, l := range lines {
		if !strings.HasPrefix(l, "permissions:") {
			continue
		}
		block = []string{l}
		for _, next := range lines[i+1:] {
			if next != "" && !strings.HasPrefix(next, " ") && !strings.HasPrefix(next, "#") {
				break
			}
			block = append(block, next)
		}
		return block, true
	}
	return nil, false
}

// checkoutsWithoutPersistFalse names each actions/checkout step whose
// `with:` does not set persist-credentials: false.
func checkoutsWithoutPersistFalse(workflow string) []string {
	lines := strings.Split(workflow, "\n")
	var missing []string
	for i, l := range lines {
		if !strings.Contains(l, "uses: actions/checkout@") {
			continue
		}
		// The step ends at the next line indented no deeper than its "- ".
		start := i
		for start > 0 && !strings.HasPrefix(strings.TrimLeft(lines[start], " "), "- ") {
			start--
		}
		dash := len(lines[start]) - len(strings.TrimLeft(lines[start], " "))
		found := false
		for _, next := range lines[start+1:] {
			trimmed := strings.TrimLeft(next, " ")
			if trimmed != "" && len(next)-len(trimmed) <= dash {
				break
			}
			if trimmed == "persist-credentials: false" {
				found = true
			}
		}
		if !found {
			missing = append(missing, strings.TrimSpace(l))
		}
	}
	return missing
}

// TestWorkflowsGrantLeastPrivilege: the release, scans and nightly
// workflows default to read-only permissions, so a write scope is granted
// per job only where that job uses it.
func TestWorkflowsGrantLeastPrivilege(t *testing.T) {
	for _, name := range []string{"release.yml", "scans.yml", "nightly.yml"} {
		b, err := os.ReadFile(filepath.Join("..", "..", ".github", "workflows", name))
		if err != nil {
			t.Fatal(err)
		}
		block, ok := topLevelPermissions(string(b))
		if !ok {
			t.Errorf("%s has no top-level permissions block", name)
			continue
		}
		for _, l := range block {
			if strings.Contains(l, ": write") || strings.Contains(l, "write-all") {
				t.Errorf("%s grants %q at workflow level", name, strings.TrimSpace(l))
			}
		}
	}
}

// TestScansCheckoutsDropCredentials: no scans.yml job pushes, so no
// checkout leaves the token in the clone's git config.
func TestScansCheckoutsDropCredentials(t *testing.T) {
	b, err := os.ReadFile(filepath.Join("..", "..", ".github", "workflows", "scans.yml"))
	if err != nil {
		t.Fatal(err)
	}
	// The one exception: the bump job deletes its merged branch with
	// git push, so its checkout keeps the credentials that push uses.
	// Every other job's checkout must drop them.
	before, bump, ok := strings.Cut(string(b), "\n  bump:\n")
	if !ok {
		t.Fatal("scans.yml has no bump job")
	}
	// Jobs after bump, if any, are checked too.
	rest := ""
	for _, l := range strings.Split(bump, "\n") {
		if len(l) > 2 && strings.HasPrefix(l, "  ") && l[2] != ' ' && l[2] != '#' {
			_, rest, _ = strings.Cut(bump, "\n"+l+"\n")
			rest = l + "\n" + rest
			break
		}
	}
	for _, step := range checkoutsWithoutPersistFalse(before + "\n" + rest) {
		t.Errorf("scans.yml: %s keeps its credentials", step)
	}
}

func TestWorkflowPermissionHelpers_CanFail(t *testing.T) {
	const bad = "on: push\npermissions:\n  contents: write\njobs:\n  a:\n    steps:\n      - uses: actions/checkout@x # v4\n      - run: true\n"
	block, ok := topLevelPermissions(bad)
	if !ok || !strings.Contains(strings.Join(block, "\n"), ": write") {
		t.Fatalf("a workflow-level write is not seen: %q", block)
	}
	if got := checkoutsWithoutPersistFalse(bad); len(got) != 1 {
		t.Fatalf("a checkout without persist-credentials: false is not reported: %q", got)
	}
	const good = "on: push\npermissions:\n  contents: read\njobs:\n  a:\n    steps:\n      - uses: actions/checkout@x # v4\n        with:\n          persist-credentials: false\n      - run: true\n"
	block, _ = topLevelPermissions(good)
	if strings.Contains(strings.Join(block, "\n"), ": write") {
		t.Fatalf("a read-only block reads as a write: %q", block)
	}
	if got := checkoutsWithoutPersistFalse(good); len(got) != 0 {
		t.Fatalf("a checkout with persist-credentials: false is reported: %q", got)
	}
}
