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
	"testing"
	"time"
)

// pollUntil polls condition() every interval until it returns true or timeout
// elapses. Reports a fatal error with msg on timeout.
func pollUntil(t *testing.T, timeout, interval time.Duration, msg string, condition func() bool) {
	t.Helper()
	deadline := time.NewTimer(timeout)
	defer deadline.Stop()
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		if condition() {
			return
		}
		select {
		case <-ticker.C:
		case <-deadline.C:
			t.Fatal(msg)
		}
	}
}

// assertNever verifies condition() stays false for the entire duration,
// sampling every interval. Reports a fatal error if condition() becomes true.
func assertNever(t *testing.T, duration, interval time.Duration, msg string, condition func() bool) {
	t.Helper()
	deadline := time.NewTimer(duration)
	defer deadline.Stop()
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			if condition() {
				t.Fatal(msg)
			}
		case <-deadline.C:
			// Passed: condition never became true.
			return
		}
	}
}

// waitForChan waits for a channel to be closed or for timeout.
func waitForChan(t *testing.T, ch <-chan struct{}, timeout time.Duration, msg string) {
	t.Helper()
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	select {
	case <-ch:
	case <-timer.C:
		t.Fatal(msg)
	}
}
