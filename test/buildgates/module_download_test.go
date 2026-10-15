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
	"errors"
	"io/fs"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	"gopkg.in/yaml.v3"
)

// ---------------------------------------------------------------------------
// One credential mechanism for every image that downloads a private module.
//
// The devcontainer images used to run `go mod download 2>/dev/null || true`.
// For a build list holding a module served only from the project forge
// (github.com/jeremyhahn/go-enclave), that download failed, the failure was
// discarded, and the image built anyway -- broken, and reporting nothing until
// a `make` inside it tried to compile. The benchmark images had already solved
// the problem with an optional BuildKit secret (id=gomodauth). Every image
// whose build list holds a forge-only module now runs that one RUN
// instruction, byte for byte, so one secret, one payload format and one
// failure behaviour serve them all.
// ---------------------------------------------------------------------------

// referenceDownloadDockerfile holds the module download RUN instruction every
// other private-module image must repeat verbatim. Dockerfiles cannot include
// one another, so identity is enforced here instead.
const referenceDownloadDockerfile = ".devcontainer/Dockerfile.dev"

// forgeOnlyModule is the first-party module that is served from the project
// forge and not from the public proxy. A build list that holds it cannot be
// downloaded without the forge credentials.
const forgeOnlyModule = "github.com/jeremyhahn/go-enclave"

// moduleDownloadDockerfiles maps every Dockerfile that runs `go mod download`
// to the module directory whose build list it downloads. A new Dockerfile
// that downloads modules fails TestEveryModuleDownloadIsClassified until it is
// listed here, so whether it needs the credential block is decided from its
// go.sum rather than left to whoever adds it.
var moduleDownloadDockerfiles = map[string]string{
	".devcontainer/Dockerfile":               ".",
	".devcontainer/Dockerfile.dev":           ".",
	".devcontainer/Dockerfile.integration":   ".",
	"build/docker/devcontainer/Dockerfile":   ".",
	"test/performance/quicraft/Dockerfile":   "test/performance/quicraft",
	"test/performance/etcd/Dockerfile":       "test/performance/etcd",
	"test/performance/dragonboat/Dockerfile": "test/performance/dragonboat",
}

// secretMountPath is where BuildKit mounts the gomodauth secret.
const secretMountPath = "/run/secrets/gomodauth"

// moduleDownloadRun returns the RUN instruction in the Dockerfile at rel that
// runs `go mod download`, as written: its lines, trailing whitespace trimmed,
// joined with newlines. ok is false when the file has no such instruction.
func moduleDownloadRun(t *testing.T, rel string) (run string, ok bool) {
	t.Helper()
	lines := strings.Split(readFile(t, filepath.Join(repoRoot, rel)), "\n")
	for i := 0; i < len(lines); i++ {
		if !strings.HasPrefix(strings.TrimSpace(lines[i]), "RUN ") {
			continue
		}
		var block []string
		for ; i < len(lines); i++ {
			cur := strings.TrimRight(lines[i], " \t")
			block = append(block, cur)
			if !strings.HasSuffix(cur, `\`) {
				break
			}
		}
		if text := strings.Join(block, "\n"); strings.Contains(text, "go mod download") {
			return text, true
		}
	}
	return "", false
}

// dockerfilesThatDownloadModules walks the repository for Dockerfiles with a
// `go mod download` RUN instruction and returns their slash-separated paths.
func dockerfilesThatDownloadModules(t *testing.T) []string {
	t.Helper()
	skip := map[string]bool{".git": true, ".claude": true, ".serena": true, "target": true, "node_modules": true}
	var found []string
	err := filepath.WalkDir(repoRoot, func(p string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			if skip[d.Name()] {
				return filepath.SkipDir
			}
			return nil
		}
		if !strings.HasPrefix(d.Name(), "Dockerfile") {
			return nil
		}
		rel, err := filepath.Rel(repoRoot, p)
		if err != nil {
			return err
		}
		rel = filepath.ToSlash(rel)
		if _, ok := moduleDownloadRun(t, rel); ok {
			found = append(found, rel)
		}
		return nil
	})
	if err != nil {
		t.Fatalf("walk %s: %v", repoRoot, err)
	}
	sort.Strings(found)
	return found
}

// needsForgeCredentials reports whether the build list of the module in dir
// holds the forge-only module, judged by its go.sum.
func needsForgeCredentials(t *testing.T, dir string) bool {
	t.Helper()
	sum := readFile(t, filepath.Join(repoRoot, dir, "go.sum"))
	return strings.Contains(sum, forgeOnlyModule+" ")
}

func TestEveryModuleDownloadIsClassified(t *testing.T) {
	found := dockerfilesThatDownloadModules(t)
	if len(found) == 0 {
		t.Fatal("no Dockerfile runs `go mod download`; the walk is broken or " +
			"every image moved, and the credential checks below prove nothing")
	}
	for _, rel := range found {
		if _, ok := moduleDownloadDockerfiles[rel]; !ok {
			t.Errorf("%s runs `go mod download` but is not in moduleDownloadDockerfiles. "+
				"List it with the module directory it downloads for, so whether it "+
				"needs the gomodauth credential block is decided from that module's go.sum.", rel)
		}
	}
	for rel := range moduleDownloadDockerfiles {
		if _, ok := moduleDownloadRun(t, rel); !ok {
			t.Errorf("%s is listed in moduleDownloadDockerfiles but no longer runs "+
				"`go mod download`; remove the stale entry", rel)
		}
	}
}

func TestPrivateModuleDownloadsShareOneCredentialBlock(t *testing.T) {
	want, ok := moduleDownloadRun(t, referenceDownloadDockerfile)
	if !ok {
		t.Fatalf("%s has no `go mod download` RUN instruction; it is the reference "+
			"every private-module image is compared against", referenceDownloadDockerfile)
	}
	if !needsForgeCredentials(t, moduleDownloadDockerfiles[referenceDownloadDockerfile]) {
		t.Fatalf("the reference image's module no longer lists %s in go.sum; "+
			"pick a reference that still needs the forge credentials", forgeOnlyModule)
	}
	checked := 0
	for rel, dir := range moduleDownloadDockerfiles {
		if !needsForgeCredentials(t, dir) {
			continue
		}
		checked++
		got, _ := moduleDownloadRun(t, rel)
		if got != want {
			t.Errorf("%s downloads a build list holding %s, but its module download "+
				"RUN instruction differs from %s's. Every such image carries the same "+
				"block, byte for byte, so one secret and one payload serve them all.\n"+
				"got:\n%s\n\nwant:\n%s", rel, forgeOnlyModule, referenceDownloadDockerfile, got, want)
		}
	}
	if checked < 2 {
		t.Fatalf("only %d image(s) need the forge credentials; the identity check "+
			"compared nothing", checked)
	}
}

// downloadOutcome is what running the reference download block produced.
type downloadOutcome struct {
	exitedZero bool
	// seenKey is the GIT_CONFIG_KEY_0 the stub `go` saw in its environment.
	seenKey string
	// seenArgs is the argument list the stub `go` was invoked with.
	seenArgs string
	output   string
}

// runDownloadBlock executes the reference module download RUN instruction
// under sh with a stub `go` first on PATH. secret, when non-empty, is written
// where the block reads the BuildKit secret; goExit is the stub's exit status.
// Only the mount flags and the secret path are substituted; every other token
// is the Dockerfile's own text.
func runDownloadBlock(t *testing.T, secret string, goExit int) downloadOutcome {
	t.Helper()

	run, ok := moduleDownloadRun(t, referenceDownloadDockerfile)
	if !ok {
		t.Fatalf("%s has no `go mod download` RUN instruction", referenceDownloadDockerfile)
	}
	body := strings.ReplaceAll(run, "\\\n", " ")
	body = strings.TrimSpace(strings.TrimPrefix(strings.TrimSpace(body), "RUN"))
	for strings.HasPrefix(body, "--") {
		_, rest, _ := strings.Cut(body, " ")
		body = strings.TrimSpace(rest)
	}

	dir := t.TempDir()
	secretPath := filepath.Join(dir, "gomodauth")
	if secret != "" {
		if err := os.WriteFile(secretPath, []byte(secret), 0o400); err != nil {
			t.Fatalf("write secret: %v", err)
		}
	}
	body = strings.ReplaceAll(body, secretMountPath, secretPath)

	seen := filepath.Join(dir, "seen")
	stub := "#!/bin/sh\nprintf '%s\\n%s\\n' \"${GIT_CONFIG_KEY_0:-}\" \"$*\" > '" + seen + "'\nexit " + strconv.Itoa(goExit) + "\n"
	binDir := filepath.Join(dir, "bin")
	if err := os.Mkdir(binDir, 0o700); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	if err := os.WriteFile(filepath.Join(binDir, "go"), []byte(stub), 0o700); err != nil {
		t.Fatalf("write stub go: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, "sh", "-c", body)
	cmd.Env = []string{"PATH=" + binDir + string(os.PathListSeparator) + os.Getenv("PATH")}
	out, err := cmd.CombinedOutput()

	var got downloadOutcome
	got.exitedZero = err == nil
	got.output = string(out)
	if raw, readErr := os.ReadFile(seen); readErr == nil {
		key, args, _ := strings.Cut(strings.TrimSuffix(string(raw), "\n"), "\n")
		got.seenKey, got.seenArgs = key, args
	} else if !errors.Is(readErr, fs.ErrNotExist) {
		t.Fatalf("read %s: %v", seen, readErr)
	}
	return got
}

func TestModuleDownloadFailsTheBuildWhenTheDownloadFails(t *testing.T) {
	got := runDownloadBlock(t, "", 1)
	if got.exitedZero {
		t.Fatalf("the module download step exited 0 although `go mod download` failed; "+
			"the image would build without its modules and fail only when a make "+
			"inside it compiled.\noutput:\n%s", got.output)
	}
	if got.seenArgs != "mod download" {
		t.Fatalf("the block did not run `go mod download` (stub saw %q)", got.seenArgs)
	}
}

func TestModuleDownloadSourcesTheMountedCredentials(t *testing.T) {
	const key = "url.https://x:token@forge.example/jeremyhahn/.insteadOf"
	secret := "GOPRIVATE=github.com/jeremyhahn/*\nGIT_CONFIG_COUNT=1\n" +
		"GIT_CONFIG_KEY_0=" + key + "\nGIT_CONFIG_VALUE_0=https://github.com/jeremyhahn/\n"
	got := runDownloadBlock(t, secret, 0)
	if !got.exitedZero {
		t.Fatalf("the module download step failed with credentials mounted.\noutput:\n%s", got.output)
	}
	if got.seenKey != key {
		t.Fatalf("`go mod download` saw GIT_CONFIG_KEY_0=%q, want the mounted value: "+
			"the secret must be sourced AND exported for the git that go runs", got.seenKey)
	}
	if strings.Contains(got.output, "token") {
		t.Fatalf("the module download step printed credential content:\n%s", got.output)
	}
}

func TestModuleDownloadRunsWithoutCredentials(t *testing.T) {
	got := runDownloadBlock(t, "", 0)
	if !got.exitedZero {
		t.Fatalf("the module download step failed with no secret mounted although "+
			"`go mod download` succeeded; the mount is optional so a build that "+
			"needs no forge module still works.\noutput:\n%s", got.output)
	}
	if got.seenKey != "" {
		t.Fatalf("`go mod download` saw GIT_CONFIG_KEY_0=%q with no secret mounted", got.seenKey)
	}
}

// ---------------------------------------------------------------------------
// .devcontainer/.env is the single place credentials live.
//
// The compose services take the build secret from it and carry the same four
// variables at run time, so `go` inside a container rewrites
// github.com/jeremyhahn/* to the forge for anything it fetches later. The
// committed .env.example documents the format; its values must be
// placeholders and its lines must read identically to sh (the build secret is
// sourced), to compose and to `docker run --env-file` (which takes quotes
// literally), so no line may quote its value.
// ---------------------------------------------------------------------------

// moduleCredentialVars are the four variables of the handbook's
// dependency-resolution §6, in the order .env.example lists them.
var moduleCredentialVars = []string{"GOPRIVATE", "GIT_CONFIG_COUNT", "GIT_CONFIG_KEY_0", "GIT_CONFIG_VALUE_0"}

func TestTheCredentialExampleListsTheFourVariablesUnquoted(t *testing.T) {
	body := readFile(t, filepath.Join(repoRoot, moduleCredentialFile+".example"))
	// No quotes (docker takes them literally) and no shell metacharacters (sh
	// would act on them when it sources the file as the build secret).
	line := regexp.MustCompile("^([A-Z_0-9]+)=([^'\"\\s<>|;&$`()\\\\]+)$")
	var keys []string
	for _, l := range strings.Split(body, "\n") {
		l = strings.TrimSpace(l)
		if l == "" || strings.HasPrefix(l, "#") {
			continue
		}
		m := line.FindStringSubmatch(l)
		if m == nil {
			t.Errorf("%s.example: %q is not an unquoted KEY=value line; sh, compose "+
				"and `docker run --env-file` must all read it the same way", moduleCredentialFile, l)
			continue
		}
		keys = append(keys, m[1])
		if m[1] == "GIT_CONFIG_KEY_0" && !strings.Contains(m[2], "forge.example") {
			t.Errorf("%s.example: GIT_CONFIG_KEY_0 must point at the forge.example placeholder, got %q",
				moduleCredentialFile, m[2])
		}
	}
	if strings.Join(keys, ",") != strings.Join(moduleCredentialVars, ",") {
		t.Errorf("%s.example lists %v, want exactly %v", moduleCredentialFile, keys, moduleCredentialVars)
	}
}

// composeFile is the subset of a compose file these checks read.
type composeFile struct {
	Services map[string]struct {
		Build struct {
			Dockerfile string   `yaml:"dockerfile"`
			Secrets    []string `yaml:"secrets"`
		} `yaml:"build"`
		EnvFile []struct {
			Path     string `yaml:"path"`
			Required *bool  `yaml:"required"`
		} `yaml:"env_file"`
		Environment yaml.Node `yaml:"environment"`
	} `yaml:"services"`
	Secrets map[string]struct {
		File string `yaml:"file"`
	} `yaml:"secrets"`
}

func TestComposeServicesTakeCredentialsFromTheEnvFile(t *testing.T) {
	for _, tc := range []struct {
		file     string
		services []string
	}{
		{".devcontainer/docker-compose.yml", []string{"devcontainer", "integration"}},
		{".devcontainer/docker-compose.integration.yml", []string{"test-runner"}},
	} {
		var cf composeFile
		if err := yaml.Unmarshal([]byte(readFile(t, filepath.Join(repoRoot, tc.file))), &cf); err != nil {
			t.Fatalf("parse %s: %v", tc.file, err)
		}
		if got := cf.Secrets["gomodauth"].File; got != ".env" {
			t.Errorf("%s: secret gomodauth comes from %q, want .env (the single "+
				"credential file, beside the compose file)", tc.file, got)
		}
		for _, name := range tc.services {
			svc, ok := cf.Services[name]
			if !ok {
				t.Errorf("%s: no service %q", tc.file, name)
				continue
			}
			if !strings.Contains(strings.Join(svc.Build.Secrets, ","), "gomodauth") {
				t.Errorf("%s: service %s does not pass the gomodauth secret to its build", tc.file, name)
			}
			// The runner stage of Dockerfile.integration holds a compiled
			// binary and no toolchain, so it fetches nothing at run time.
			if name == "test-runner" {
				continue
			}
			found := false
			for _, ef := range svc.EnvFile {
				if ef.Path == ".env" && ef.Required != nil && !*ef.Required {
					found = true
				}
			}
			if !found {
				t.Errorf("%s: service %s does not load .env (required: false) at run time; "+
					"go inside the container could not reach the forge", tc.file, name)
			}
			for _, k := range moduleCredentialVars {
				if strings.Contains(nodeText(&svc.Environment), k) {
					t.Errorf("%s: service %s sets %s inline; credentials live only in .env", tc.file, name, k)
				}
			}
		}
	}
}

// nodeText flattens a YAML node's scalar content for a containment check.
func nodeText(n *yaml.Node) string {
	if n == nil {
		return ""
	}
	parts := []string{n.Value}
	for _, c := range n.Content {
		parts = append(parts, nodeText(c))
	}
	return strings.Join(parts, " ")
}
