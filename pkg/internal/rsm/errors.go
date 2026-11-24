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

package rsm

import "errors"

// rsm sentinel errors, separate from the quicraft package to avoid an import cycle.
var (
	// ErrInvalidSMType is returned when a CreateFunc returns an object
	// that does not implement any known state machine interface.
	ErrInvalidSMType = errors.New("rsm: invalid state machine type")

	// ErrStopped is returned when an operation is interrupted because
	// the stopper channel was closed.
	ErrStopped = errors.New("rsm: operation stopped")

	// ErrClosed is returned by read operations (Lookup/NALookup/NALookupBuf)
	// when the state machine has been closed.
	ErrClosed = errors.New("rsm: state machine is closed")

	// ErrApplyGap is returned by Apply when a batch does not start at lastApplied+1.
	ErrApplyGap = errors.New("rsm: committed entry gap before apply")

	// ErrSessionPolicyMismatch is returned when local session settings disagree with the shard's committed session policy.
	ErrSessionPolicyMismatch = errors.New("quicraft: configured session settings disagree with the shard's committed session policy")
)
