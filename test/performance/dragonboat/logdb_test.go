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

package dragonboat_perf

import "testing"

// TestNodeHostConfigUsesTan pins the LogDB the harness benchmarks. Without an
// explicit factory Dragonboat falls back to its sharded Pebble LogDB, which is
// not the log store Dragonboat v4 is built around, so the comparison would
// measure a different storage engine than the one it reports.
func TestNodeHostConfigUsesTan(t *testing.T) {
	cfg := nodeHostConfig(t.TempDir(), "127.0.0.1:1", tlsCertBundle{})
	if cfg.Expert.LogDBFactory == nil {
		t.Fatal("NodeHostConfig.Expert.LogDBFactory is nil: Dragonboat would run its default sharded Pebble LogDB")
	}
	if got := cfg.Expert.LogDBFactory.Name(); got != "Tan" {
		t.Fatalf("LogDB factory = %q, want %q", got, "Tan")
	}
}
