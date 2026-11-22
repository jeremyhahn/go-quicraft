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
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strings"
	"testing"

	"gopkg.in/yaml.v3"
)

var (
	envRefRe    = regexp.MustCompile(`\benv\.([A-Z_][A-Z0-9_]*)`)
	shellRefRe  = regexp.MustCompile(`\$\{?([A-Z_][A-Z0-9_]*)`)
	envAssignRe = regexp.MustCompile(`\b([A-Z_][A-Z0-9_]*)=`)
	envKeyRe    = regexp.MustCompile(`(?m)^ +([A-Z_][A-Z0-9_]*):`)
)

// imageHistoryProblems returns, for each step of job that runs
// scripts/ci-image-history.sh, the variables its `if:` or `run:` reads that
// no earlier step of the job writes to GITHUB_ENV and no env: block sets.
// A guard on such a variable is always false, so the check never runs.
func imageHistoryProblems(job, workflowEnv string) []string {
	declared := map[string]bool{}
	for _, m := range envKeyRe.FindAllStringSubmatch(workflowEnv, -1) {
		declared[m[1]] = true
	}
	var problems []string
	steps := strings.Split(job, "\n      - ")
	for _, step := range steps {
		if strings.Contains(step, "scripts/ci-image-history.sh") {
			var refs []string
			for _, line := range strings.Split(step, "\n") {
				t := strings.TrimSpace(line)
				switch {
				case strings.HasPrefix(t, "if:"):
					for _, m := range envRefRe.FindAllStringSubmatch(t, -1) {
						refs = append(refs, m[1])
					}
				case strings.HasPrefix(t, "run:") && strings.Contains(t, "ci-image-history.sh"):
					for _, m := range shellRefRe.FindAllStringSubmatch(t, -1) {
						refs = append(refs, m[1])
					}
				}
			}
			if len(refs) == 0 {
				problems = append(problems, "an image-history step names no image variable")
			}
			for _, r := range refs {
				if !declared[r] {
					problems = append(problems, r+" is read by an image-history step but no earlier step sets it")
				}
			}
		}
		for _, line := range strings.Split(step, "\n") {
			if strings.Contains(line, "GITHUB_ENV") {
				for _, m := range envAssignRe.FindAllStringSubmatch(line, -1) {
					declared[m[1]] = true
				}
			}
		}
		for _, m := range envKeyRe.FindAllStringSubmatch(step, -1) {
			declared[m[1]] = true
		}
	}
	return problems
}

// TestImageHistoryStepsReadSetVariables pins every image-history check to
// variables the job sets before it. A check keyed on a variable nothing sets
// is skipped on every run, and the images go unchecked without a red step.
func TestImageHistoryStepsReadSetVariables(t *testing.T) {
	raw, err := os.ReadFile(ciWorkflowPath)
	if err != nil {
		t.Fatalf("read %s: %v", ciWorkflowPath, err)
	}
	body := string(raw)
	head, _, ok := strings.Cut(body, "\njobs:\n")
	if !ok {
		t.Fatalf("no jobs: block in %s", ciWorkflowPath)
	}
	checked := 0
	for _, j := range ciJobs(t, body) {
		job := regexp.MustCompile(`(?m)^  ` + regexp.QuoteMeta(j) + `:\n(?:(?:    |      |\n).*\n)*`).FindString(body)
		checked += strings.Count(job, "scripts/ci-image-history.sh")
		for _, p := range imageHistoryProblems(job, head) {
			t.Errorf("job %q: %s", j, p)
		}
	}
	if checked == 0 {
		t.Fatal("no job runs scripts/ci-image-history.sh; step discovery has broken")
	}
}

// TestImageHistoryProblemsCatchesAnUnsetVariable is the check's own check:
// a guard on a variable no step sets must be reported.
func TestImageHistoryProblemsCatchesAnUnsetVariable(t *testing.T) {
	good := "  it:\n    steps:\n" +
		"      - name: Name the integration image\n" +
		"        run: echo \"IT_IMAGE=$(make -s it-image)\" >> \"$GITHUB_ENV\"\n" +
		"      - name: Check\n" +
		"        if: ${{ !cancelled() && env.IT_IMAGE != '' && runner.environment != 'github-hosted' }}\n" +
		"        run: scripts/ci-image-history.sh \"$IT_IMAGE\"\n"
	if p := imageHistoryProblems(good, ""); len(p) != 0 {
		t.Errorf("a set variable was reported: %v", p)
	}
	bad := strings.ReplaceAll(good, "env.IT_IMAGE", "env.IT_LEASE_ID")
	bad = strings.ReplaceAll(bad, `"$IT_IMAGE"`, `"$IT_LEASE_ID"`)
	if p := imageHistoryProblems(bad, ""); len(p) == 0 {
		t.Error("a guard on IT_LEASE_ID, which no step sets, was not reported")
	}
	late := "  it:\n    steps:\n" +
		"      - name: Check\n" +
		"        if: ${{ env.IT_IMAGE != '' }}\n" +
		"        run: scripts/ci-image-history.sh \"$IT_IMAGE\"\n" +
		"      - name: Name the integration image\n" +
		"        run: echo \"IT_IMAGE=x\" >> \"$GITHUB_ENV\"\n"
	if p := imageHistoryProblems(late, ""); len(p) == 0 {
		t.Error("a variable set only after the check was not reported")
	}
}

// integrationHistoryGateProblems reports an integration-image history step
// that can run when the job built no integration image. A job whose build
// step can select the toolchain target alone (the PR tier without ci:full)
// leaves IT_IMAGE named but unbuilt, so the step that checks "$IT_IMAGE"
// must also require the lease the job takes only on a present image.
func integrationHistoryGateProblems(job string) []string {
	if !strings.Contains(job, "target=docker-build-toolchain") {
		return nil
	}
	var problems []string
	for _, step := range strings.Split(job, "\n      - ") {
		if !strings.Contains(step, `scripts/ci-image-history.sh "$IT_IMAGE"`) {
			continue
		}
		guarded := false
		for _, line := range strings.Split(step, "\n") {
			t := strings.TrimSpace(line)
			if strings.HasPrefix(t, "if:") && strings.Contains(t, "env.IT_LEASE != ''") {
				guarded = true
			}
		}
		if !guarded {
			problems = append(problems, "the integration-image history step runs when the build step built only the toolchain image (its if: does not require env.IT_LEASE != '')")
		}
	}
	return problems
}

// TestIntegrationImageHistoryRunsOnlyWhenBuilt checks ci.yml's image job: an
// ordinary pull request builds only the toolchain image there, and the
// integration-image history step must skip rather than fail the job.
func TestIntegrationImageHistoryRunsOnlyWhenBuilt(t *testing.T) {
	raw, err := os.ReadFile(ciWorkflowPath)
	if err != nil {
		t.Fatalf("read %s: %v", ciWorkflowPath, err)
	}
	body := string(raw)
	job := regexp.MustCompile(`(?m)^  image:\n(?:(?:    |      |\n).*\n)*`).FindString(body)
	if !strings.Contains(job, `scripts/ci-image-history.sh "$IT_IMAGE"`) || !strings.Contains(job, "target=docker-build-toolchain") {
		t.Fatal("ci.yml's image job no longer has the toolchain-only build and the integration-image history step this test checks")
	}
	for _, p := range integrationHistoryGateProblems(job) {
		t.Errorf("job \"image\": %s", p)
	}

	prTier := "  image:\n    steps:\n" +
		"      - name: Build\n" +
		"        run: |\n" +
		"          target=docker-build-toolchain\n" +
		"          [ \"$CI_FULL_TIER\" != true ] || target=docker-build\n" +
		"      - name: Lease the integration image\n" +
		"        run: echo \"IT_LEASE=x\" >> \"$GITHUB_ENV\"\n" +
		"      - name: Check the integration image carries no private address\n" +
		"        if: ${{ !cancelled() && env.IT_IMAGE != '' && runner.environment != 'github-hosted' }}\n" +
		"        run: scripts/ci-image-history.sh \"$IT_IMAGE\"\n"
	if p := integrationHistoryGateProblems(prTier); len(p) == 0 {
		t.Error("a history step that runs on a toolchain-only build was not reported")
	}
	gated := strings.Replace(prTier, "env.IT_IMAGE != ''", "env.IT_IMAGE != '' && env.IT_LEASE != ''", 1)
	if p := integrationHistoryGateProblems(gated); len(p) != 0 {
		t.Errorf("a history step gated on the lease was reported: %v", p)
	}
	fullOnly := strings.Replace(prTier, "target=docker-build-toolchain\n", "target=docker-build\n", 1)
	if p := integrationHistoryGateProblems(fullOnly); len(p) != 0 {
		t.Errorf("a job that always builds the integration image was reported: %v", p)
	}
}

// suiteStep is the part of a workflow step the integration-suite check reads.
type suiteStep struct {
	Name string `yaml:"name"`
	ID   string `yaml:"id"`
	If   string `yaml:"if"`
	Run  string `yaml:"run"`
}

// integrationSuiteProblems reports how an integration job's steps could run
// the suite on an image the image-history check has not passed: no step
// before the check builds a missing image, the check is skipped on some
// condition other than a hosted runner (or has no id to key on), or the
// suite step does not require the check's success off hosted runners.
func integrationSuiteProblems(steps []suiteStep) []string {
	var probs []string
	check := -1
	for i, s := range steps {
		if strings.Contains(s.Run, "scripts/ci-image-history.sh") && strings.Contains(s.Run, "$IT_IMAGE") {
			check = i
			break
		}
	}
	if check < 0 {
		return []string{"no step runs scripts/ci-image-history.sh on $IT_IMAGE"}
	}
	built := false
	for _, s := range steps[:check] {
		if strings.Contains(s.Run, "docker image inspect") && strings.Contains(s.Run, "make docker-build-it") {
			built = true
		}
	}
	if !built {
		probs = append(probs, "no step before the image-history check builds the "+
			"integration image when the daemon lacks it (make docker-build-it)")
	}
	c := steps[check]
	if c.ID == "" {
		probs = append(probs, "the image-history check has no id for the suite to key on")
	}
	if strings.Contains(c.If, "IT_LEASE") {
		probs = append(probs, "the image-history check is skipped when IT_LEASE is "+
			"empty, so a missing image runs unchecked")
	}
	suite := -1
	for i, s := range steps {
		if strings.Contains(s.Run, "make integration-test") {
			suite = i
		}
	}
	switch {
	case suite < 0:
		probs = append(probs, "no step runs make integration-test")
	case suite < check:
		probs = append(probs, "make integration-test runs before the image-history check")
	default:
		cond := steps[suite].If
		if c.ID == "" || !strings.Contains(cond, "steps."+c.ID+".outcome == 'success'") ||
			!strings.Contains(cond, "runner.environment == 'github-hosted'") {
			probs = append(probs, fmt.Sprintf("make integration-test's if: %q does not "+
				"require the image-history check's success off hosted runners", cond))
		}
	}
	return probs
}

func TestIntegrationSuiteRunsOnlyOnACheckedImage(t *testing.T) {
	raw, err := os.ReadFile(ciWorkflowPath)
	if err != nil {
		t.Fatalf("read %s: %v", ciWorkflowPath, err)
	}
	var wf struct {
		Jobs map[string]struct {
			Steps []suiteStep `yaml:"steps"`
		} `yaml:"jobs"`
	}
	if err := yaml.Unmarshal(raw, &wf); err != nil {
		t.Fatalf("parse %s: %v", ciWorkflowPath, err)
	}
	job, ok := wf.Jobs["integration"]
	if !ok {
		t.Fatalf("%s: no integration job", ciWorkflowPath)
	}
	for _, p := range integrationSuiteProblems(job.Steps) {
		t.Errorf("integration job: %s", p)
	}

	// The form this replaces: the lease step leases only an image already on
	// the daemon, the check is keyed on the lease, and the suite builds a
	// missing image and runs it unchecked.
	old := []suiteStep{
		{Name: "Lease the integration image", Run: "if docker image inspect \"$IT_IMAGE\"; then docker create; fi"},
		{Name: "Check", If: "${{ !cancelled() && env.IT_IMAGE != '' && env.IT_LEASE != '' && runner.environment != 'github-hosted' }}",
			Run: "scripts/ci-image-history.sh \"$IT_IMAGE\""},
		{Name: "make integration-test", Run: "make integration-test IT_IMAGE=\"$IT_IMAGE\""},
	}
	if p := integrationSuiteProblems(old); len(p) < 3 {
		t.Errorf("the lease-keyed form was not fully reported: %v", p)
	}
}

// stubDocker is a docker first on PATH. `history` and `image inspect` print
// the history and config files in dir (or fail when STUB_FAIL is set);
// `run` prints dir/mirror when the probe is handed an APK_MIRROR and
// dir/public otherwise, standing in for the indexes apk would fetch.
const stubDocker = `#!/bin/sh
case "$1" in
history) [ -z "${STUB_FAIL:-}" ] || exit 1; cat "$STUB_DIR/history" ;;
image) [ -z "${STUB_FAIL:-}" ] || exit 1; cat "$STUB_DIR/config" ;;
run)
	m=
	for a in "$@"; do case $a in APK_MIRROR=*) m=${a#APK_MIRROR=} ;; esac; done
	cat >/dev/null
	if [ -n "$m" ]; then cat "$STUB_DIR/mirror"; else cat "$STUB_DIR/public"; fi ;;
*) exit 9 ;;
esac
`

// runWithStubDocker runs a repository script with stubDocker first on PATH,
// files written into the stub's directory, and extra environment; it returns
// the combined output and the exit code.
func runWithStubDocker(t *testing.T, script string, files, env map[string]string, args ...string) (string, int) {
	t.Helper()
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "docker"), []byte(stubDocker), 0o755); err != nil {
		t.Fatal(err)
	}
	for name, body := range files {
		if err := os.WriteFile(filepath.Join(dir, name), []byte(body), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	root, err := filepath.Abs(repoRoot)
	if err != nil {
		t.Fatal(err)
	}
	cmd := exec.Command("bash", append([]string{filepath.Join(root, "scripts", script)}, args...)...)
	cmd.Dir = root
	cmd.Env = append(os.Environ(), "PATH="+dir+string(os.PathListSeparator)+os.Getenv("PATH"), "STUB_DIR="+dir)
	for k, v := range env {
		cmd.Env = append(cmd.Env, k+"="+v)
	}
	out, err := cmd.CombinedOutput()
	code := 0
	if err != nil {
		var ee *exec.ExitError
		if !errors.As(err, &ee) {
			t.Fatalf("run %s: %v", script, err)
		}
		code = ee.ExitCode()
	}
	return string(out), code
}

// TestCIImageHistoryRejectsPrivateAddresses runs scripts/ci-image-history.sh
// against a stub docker: a private address in a history entry or in the
// config fails without being printed, a public-only image passes, and an
// image docker cannot read fails.
func TestCIImageHistoryRejectsPrivateAddresses(t *testing.T) {
	const public = "/bin/sh -c apk add curl # fetched from 198.51.100.7\n"
	const publicConfig = `{"Env":["PATH=/usr/bin","SRC=http://203.0.113.9/x"],"Labels":null}`
	cases := []struct {
		name, history, config, fail, secret string
		wantFail                            bool
	}{
		{"private address in a history entry", public + "/bin/sh -c curl http://10.20.30.40:3142/alpine \n", publicConfig, "", "10.20.30.40", true},
		{"private address in the config", public, `{"Env":["APK_MIRROR=http://192.168.1.10:3142"]}`, "", "192.168.1.10", true},
		{"private address in a label", public, `{"Labels":{"src":"172.16.5.4"}}`, "", "172.16.5.4", true},
		{"public addresses only", public, publicConfig, "", "", false},
		{"docker cannot read the image", public, publicConfig, "1", "", true},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			env := map[string]string{}
			if c.fail != "" {
				env["STUB_FAIL"] = c.fail
			}
			out, code := runWithStubDocker(t, "ci-image-history.sh",
				map[string]string{"history": c.history, "config": c.config}, env, "example/image:tag")
			if (code != 0) != c.wantFail {
				t.Fatalf("exit %d, want failure=%v; output:\n%s", code, c.wantFail, out)
			}
			if c.secret != "" && strings.Contains(out, c.secret) {
				t.Fatalf("the output prints the private address %s:\n%s", c.secret, out)
			}
		})
	}
}

// TestAPKIndexSumHashesThePublicIndexes runs scripts/apk-index-sum.sh with a
// mirror configured and a stub docker. The image build fetches its packages
// from the public repositories, so the sum must follow the public indexes:
// a public update changes it even while the mirror's indexes stay the same.
func TestAPKIndexSumHashesThePublicIndexes(t *testing.T) {
	mirror := strings.Repeat("c", 64) + "\n"
	env := map[string]string{
		"APK_BASE_IMAGES":     "alpine:3.20",
		"DEVCACHE_APK_MIRROR": "http://mirror.example:3142/alpine",
	}
	sum := func(public string) string {
		out, code := runWithStubDocker(t, "apk-index-sum.sh",
			map[string]string{"public": "https://dl-cdn.alpinelinux.org/alpine/v3.20/main\n" + public, "mirror": mirror}, env)
		if code != 0 {
			t.Fatalf("apk-index-sum.sh exit %d:\n%s", code, out)
		}
		lines := strings.Split(strings.TrimSpace(out), "\n")
		return lines[len(lines)-1]
	}
	before := sum(strings.Repeat("a", 64) + "\n")
	after := sum(strings.Repeat("b", 64) + "\n")
	if len(before) != 64 {
		t.Fatalf("apk-index-sum.sh printed %q, want a sha256", before)
	}
	if before == after {
		t.Fatalf("a public index update left APK_INDEX_SUM at %s: the probe hashes the mirror, not the repositories the image build uses", before)
	}
}
