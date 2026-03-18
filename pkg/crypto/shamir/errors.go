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

// ValidationError is returned when share validation fails.
type ValidationError struct {
	Field  string
	Reason string
}

func (e *ValidationError) Error() string {
	return fmt.Sprintf("quicraft/shamir: validation failed for %s: %s", e.Field, e.Reason)
}

// IntegrityError is returned when the reconstructed secret's SHA-256 digest
// does not match the digest stored in the shares.
type IntegrityError struct{}

func (e *IntegrityError) Error() string {
	return "quicraft/shamir: integrity verification failed: reconstructed secret digest does not match share digest"
}
