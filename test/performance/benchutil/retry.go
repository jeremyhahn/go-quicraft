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

// Package benchutil provides small helpers shared by the engine benchmark
// modules so retry budgets and warmup behavior stay identical across all
// benchmarked systems.
package benchutil

import "time"

// Warmup retry budget shared by every engine's warmup proposals. Transient
// startup errors (connection establishment, shard readiness) are expected
// during warmup; each engine must tolerate them with the SAME budget so no
// engine's suite is discarded for a flake another engine would survive.
const (
	// WarmupAttempts is the total number of attempts per warmup operation.
	WarmupAttempts = 4

	// WarmupBackoff is the delay between warmup retry attempts.
	WarmupBackoff = 500 * time.Millisecond
)

// Retry invokes fn up to attempts times, sleeping backoff between attempts.
// It returns nil on the first success, or the last error when all attempts
// fail.
func Retry(attempts int, backoff time.Duration, fn func() error) error {
	var lastErr error
	for i := 0; i < attempts; i++ {
		if i > 0 {
			time.Sleep(backoff)
		}
		if lastErr = fn(); lastErr == nil {
			return nil
		}
	}
	return lastErr
}
