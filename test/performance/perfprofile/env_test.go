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

package perfprofile

import (
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

// partialEnvJSON is a capture from a machine where two probes came up empty:
// docker was not installed, so capture-env.sh emitted "", and the socket count
// could not be read, so the physical core count (a product of two probes) could
// not be computed either. Both are exactly the shape capture-env.sh writes.
const partialEnvJSON = `{
  "profile": "testbox",
  "captured_at": "2026-08-17T09:00:00Z",
  "hostname": "testbox-01",
  "cpu_model": "Intel(R) Xeon(R) Platinum 8260 CPU @ 2.40GHz",
  "sockets": "",
  "physical_cores": "",
  "threads": "96",
  "numa_nodes": "2",
  "memory_total": "376.4 GiB",
  "os_name": "Ubuntu",
  "os_version": "24.04.4 LTS (Noble Numbat)",
  "kernel": "6.8.0-106-generic",
  "docker_version": "",
  "go_version": "go1.26.5 linux/amd64"
}`

// writeEnvJSON drops a raw capture into a directory.
func writeEnvJSON(t *testing.T, dir, content string) string {
	t.Helper()
	path := filepath.Join(dir, EnvFile)
	if err := os.WriteFile(path, []byte(content), 0644); err != nil {
		t.Fatalf("write %s: %v", path, err)
	}
	return path
}

// TestEnvMissingFieldIsVisiblyMissing is the core promise of the capture: a
// field the target could not supply must be legible as absent, and must not be
// confusable with a measurement. A blank cell reads as "nothing to say" and a
// defaulted 0 reads as a hardware fact, so neither is acceptable.
func TestEnvMissingFieldIsVisiblyMissing(t *testing.T) {
	env, err := LoadEnv(writeEnvJSON(t, t.TempDir(), partialEnvJSON))
	if err != nil {
		t.Fatalf("LoadEnv: %v", err)
	}

	// The fields that were captured must survive parsing intact, or the test
	// below would be asserting NotCaptured against an empty parse.
	if env.Threads != "96" || env.NUMANodes != "2" {
		t.Fatalf("captured fields lost in parse: threads=%q numa=%q", env.Threads, env.NUMANodes)
	}
	if !strings.Contains(env.CPUModel, "8260") {
		t.Fatalf("cpu_model = %q; want the captured model string", env.CPUModel)
	}

	missing := env.Missing()
	want := []string{"Sockets", "Physical cores", "Docker"}
	if strings.Join(missing, ",") != strings.Join(want, ",") {
		t.Fatalf("Missing() = %v; want %v", missing, want)
	}

	table := env.MarkdownTable()
	rows := map[string]string{}
	for _, line := range strings.Split(table, "\n") {
		cols := strings.Split(strings.Trim(line, "|"), "|")
		if len(cols) != 2 {
			continue
		}
		rows[strings.TrimSpace(cols[0])] = strings.TrimSpace(cols[1])
	}

	for _, label := range want {
		got, ok := rows[label]
		if !ok {
			t.Fatalf("table has no row for the uncaptured field %q:\n%s", label, table)
		}
		if got != NotCaptured {
			t.Fatalf("row %q = %q; want %q — an uncaptured field must not render blank",
				label, got, NotCaptured)
		}
	}

	// Every field is either a captured value or the marker. A row that is
	// neither is the silent-default failure this test exists to catch.
	for _, f := range envFields {
		got, ok := rows[f.label]
		if !ok {
			t.Fatalf("table omits field %q entirely:\n%s", f.label, table)
		}
		if got == "" {
			t.Fatalf("field %q rendered as an empty cell", f.label)
		}
	}
	if rows["Threads"] != "96" {
		t.Fatalf("captured field Threads = %q; want 96", rows["Threads"])
	}
}

// TestEnvSummaryNamesWhatIsAbsent covers the inline one-liner every generated
// document carries. A missing piece must say which piece is missing rather than
// closing the gap silently.
func TestEnvSummaryNamesWhatIsAbsent(t *testing.T) {
	env, err := LoadEnv(writeEnvJSON(t, t.TempDir(), partialEnvJSON))
	if err != nil {
		t.Fatalf("LoadEnv: %v", err)
	}
	summary := env.Summary()
	for _, want := range []string{"8260", "96 threads", "376.4 GiB", "Ubuntu", "6.8.0-106-generic"} {
		if !strings.Contains(summary, want) {
			t.Fatalf("summary %q omits captured detail %q", summary, want)
		}
	}
	if !strings.Contains(summary, "sockets not captured") {
		t.Fatalf("summary %q does not say the socket count is missing", summary)
	}
}

// TestLoadEnvAbsentIsTyped keeps "never captured" distinguishable from
// "captured and unreadable".
func TestLoadEnvAbsentIsTyped(t *testing.T) {
	_, err := LoadEnv(filepath.Join(t.TempDir(), EnvFile))
	if err == nil {
		t.Fatal("LoadEnv on a missing file returned nil error")
	}
	if _, ok := err.(*EnvNotCapturedError); !ok {
		t.Fatalf("LoadEnv error = %T (%v); want *EnvNotCapturedError", err, err)
	}

	dir := t.TempDir()
	writeEnvJSON(t, dir, "{not json")
	if _, err := LoadEnv(filepath.Join(dir, EnvFile)); err == nil {
		t.Fatal("LoadEnv on malformed JSON returned nil error")
	} else if _, ok := err.(*EnvParseError); !ok {
		t.Fatalf("LoadEnv error = %T (%v); want *EnvParseError", err, err)
	}
}

// TestProfileRejectsForeignCapture stops one machine's specifications from
// being printed beside another machine's numbers, which is the single failure
// the capture exists to prevent.
func TestProfileRejectsForeignCapture(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "omega")
	if err := os.MkdirAll(dir, 0755); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	writeEnvJSON(t, dir, strings.Replace(partialEnvJSON, `"testbox"`, `"r640"`, 1))

	p, err := Open(dir)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	_, err = p.LoadEnv()
	if err == nil {
		t.Fatal("LoadEnv accepted a capture naming a different profile")
	}
	var mismatch *EnvProfileMismatchError
	if m, ok := err.(*EnvProfileMismatchError); ok {
		mismatch = m
	} else {
		t.Fatalf("error = %T (%v); want *EnvProfileMismatchError", err, err)
	}
	if mismatch.Captured != "r640" || mismatch.Directory != "omega" {
		t.Fatalf("mismatch = %+v; want captured r640 in directory omega", mismatch)
	}
}

// TestCaptureEnvScriptOutputParses runs the real capture on this machine and
// feeds it to the real parser. It is the only check that the shell script and
// the Go struct still agree on the wire shape; a renamed key or an unescaped
// quote shows up here and nowhere else.
func TestCaptureEnvScriptOutputParses(t *testing.T) {
	script, err := filepath.Abs(filepath.Join("..", "capture-env.sh"))
	if err != nil {
		t.Fatalf("resolve script path: %v", err)
	}
	if _, err := os.Stat(script); err != nil {
		t.Fatalf("capture-env.sh not found at %s: %v", script, err)
	}
	bash, err := exec.LookPath("bash")
	if err != nil {
		t.Skipf("bash unavailable: %v", err)
	}

	out, err := exec.Command(bash, script, "unit-test-profile").Output()
	if err != nil {
		t.Fatalf("capture-env.sh failed: %v", err)
	}

	dir := t.TempDir()
	path := filepath.Join(dir, EnvFile)
	if err := os.WriteFile(path, out, 0644); err != nil {
		t.Fatalf("write capture: %v", err)
	}
	env, err := LoadEnv(path)
	if err != nil {
		t.Fatalf("the capture script emitted something LoadEnv rejects: %v\n%s", err, out)
	}
	if env.Profile != "unit-test-profile" {
		t.Fatalf("profile = %q; want the name passed on the command line", env.Profile)
	}
	if env.CapturedAt == "" {
		t.Fatal("capture recorded no date")
	}
	// The capture must never invent a value: whatever it could not obtain is
	// reported by Missing(), and this run is only meaningful if it obtained
	// something. Kernel and hostname come from uname, which exists anywhere the
	// benchmarks can run.
	if env.Kernel == "" || env.Hostname == "" {
		t.Fatalf("capture obtained neither kernel nor hostname: %+v", env)
	}
	t.Logf("capture on this machine could not obtain: %v", env.Missing())
}
