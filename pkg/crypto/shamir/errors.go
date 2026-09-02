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

package shamir

import "fmt"

// SplitError is returned when splitting a secret fails.
type SplitError struct {
	Reason string
}

func (e *SplitError) Error() string {
	return fmt.Sprintf("quicraft/shamir: split failed: %s", e.Reason)
}

// CombineError is returned when combining shares fails.
type CombineError struct {
	Reason string
}

func (e *CombineError) Error() string {
	return fmt.Sprintf("quicraft/shamir: combine failed: %s", e.Reason)
}

// ProtectedMemoryError is returned when locked memory could not be allocated —
// the kernel refused an mlock, typically because RLIMIT_MEMLOCK is exhausted.
//
// It is deliberately NOT a ValidationError. The distinction is an ownership
// contract, not cosmetics: a ValidationError from NewProtectedShare leaves the
// caller's value untouched, whereas by the time this can be returned the value
// has already been WIPED — go-enclave's constructor wipes its source before it
// reports a refused lock. A caller that could not tell them apart would retry a
// legitimate-looking failure with a buffer that is now zeroes.
//
// It is also the actionable one for an operator: it says the host or container
// is out of locked memory, not that the input was wrong.
type ProtectedMemoryError struct {
	// Op names the allocation that was refused, for diagnosis.
	Op string
	// Err is the underlying refusal from the locked-memory allocator.
	Err error
}

func (e *ProtectedMemoryError) Error() string {
	return fmt.Sprintf("quicraft/shamir: %s: locked memory unavailable: %v", e.Op, e.Err)
}

// Unwrap exposes the allocator's own error, so errors.Is reaches go-enclave's
// sentinels.
func (e *ProtectedMemoryError) Unwrap() error { return e.Err }

// ValidationError is returned when share validation fails.
type ValidationError struct {
	Field  string
	Reason string
}

func (e *ValidationError) Error() string {
	return fmt.Sprintf("quicraft/shamir: validation failed for %s: %s", e.Field, e.Reason)
}

// IntegrityError was returned when a reconstructed secret's SHA-256 digest did
// not match a digest stored in the shares.
//
// Deprecated: shares no longer carry a secret digest (it was an offline
// brute-force oracle — see Share), so Combine performs no self-verification and
// NEVER returns this error. Integrity must be established out-of-band by the
// caller. The type is retained only for API/SDK compatibility.
type IntegrityError struct{}

func (e *IntegrityError) Error() string {
	return "quicraft/shamir: integrity verification failed: reconstructed secret digest does not match share digest"
}
