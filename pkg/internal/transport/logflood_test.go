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

package transport

import (
	"sync/atomic"
	"testing"
	"time"
)

// TestLogSampled_BoundsPeerDrivenDiagnostics pins the rate limiter that keeps
// the inbound receive path off the log.
//
// Every warning on that path is triggerable at frame rate by one authenticated
// peer. slog's default handler writes synchronously to stderr under a single
// mutex, so an unsampled line costs a write syscall on the raft receive path
// AND serialises every other log call in the process behind it. The attacker's
// cost is a few dozen bytes per line produced.
func TestLogSampled_BoundsPeerDrivenDiagnostics(t *testing.T) {
	var stamp atomic.Int64

	// A burst of 10,000 "frames" must produce exactly one line.
	emitted := 0
	for i := 0; i < 10000; i++ {
		if LogSampled(&stamp, time.Hour) {
			emitted++
		}
	}
	if emitted != 1 {
		t.Fatalf("LogSampled emitted %d lines for a 10,000-frame burst, want 1: "+
			"one peer can drive this path at frame rate", emitted)
	}

	// Once the interval elapses, a line is emitted again — the limiter samples,
	// it does not silence.
	stamp.Store(time.Now().Add(-2 * time.Hour).UnixNano())
	if !LogSampled(&stamp, time.Hour) {
		t.Fatal("LogSampled never re-armed: the diagnostic is silenced, not sampled")
	}
}
