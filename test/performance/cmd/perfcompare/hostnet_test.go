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

package main

import (
	"errors"
	"slices"
	"strings"
	"testing"
)

// TestHostNetworkArgsRaiseEveryBufferLimit pins the one-shot container that
// sets the host's socket buffer limits before any engine runs: privileged, on
// the host network namespace, writing all four limits to the same value.
func TestHostNetworkArgsRaiseEveryBufferLimit(t *testing.T) {
	args := hostNetworkArgs()
	for _, want := range []string{"run", "--rm", "--privileged", "--network=host"} {
		if !slices.Contains(args, want) {
			t.Errorf("hostNetworkArgs() = %q, missing %q", args, want)
		}
	}
	script := args[len(args)-1]
	for _, key := range []string{"rmem_max", "wmem_max", "rmem_default", "wmem_default"} {
		if !strings.Contains(script, "/proc/sys/net/core/"+key) {
			t.Errorf("script %q does not set net.core.%s", script, key)
		}
	}
	if !strings.Contains(script, hostSocketBufferBytes) {
		t.Errorf("script %q does not use %s", script, hostSocketBufferBytes)
	}
}

// TestNoEngineContainerRunsPrivileged guards the fairness rule: host tuning is
// applied once for every engine, so no engine's own container gets extra
// privileges.
func TestNoEngineContainerRunsPrivileged(t *testing.T) {
	for sys, cfg := range dockerGoSystems {
		if slices.Contains(goRunArgs(cfg), "--privileged") {
			t.Errorf("%s container runs --privileged", sys)
		}
	}
}

// stubPrepareHost replaces prepareHost for one test and restores it after.
func stubPrepareHost(t *testing.T, err error) {
	t.Helper()
	orig := prepareHost
	prepareHost = func() error { return err }
	t.Cleanup(func() { prepareHost = orig })
}

// TestRunBenchmarksStopsWhenHostPreparationFails pins the no-fallback rule: if
// the host's socket buffer limits cannot be set, no engine runs, because a run
// whose engines see different host settings is not a fair comparison.
func TestRunBenchmarksStopsWhenHostPreparationFails(t *testing.T) {
	stubPrepareHost(t, errors.New("set host socket buffer limits: denied"))
	err := runBenchmarks([]string{"--benchtime=1x", "--no-build", "--profile=" + newProfileDir(t), "dragonboat"})
	if err == nil || !strings.Contains(err.Error(), "host socket buffer") {
		t.Fatalf("runBenchmarks error = %v, want the host preparation failure", err)
	}
}
