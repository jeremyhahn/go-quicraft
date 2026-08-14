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

// RSM-specific sentinel errors. These are independent of the quicraft
// package to avoid import cycles (quicraft imports rsm, rsm must not
// import quicraft). The quicraft package defines equivalent errors for
// the public API; these are the rsm-internal variants.
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

	// ErrApplyGap is returned by Apply when the offered batch does not
	// continue exactly where the previous one stopped. It mirrors the
	// engine-level ErrApplyGap one layer down and exists because the state
	// machine keeps its OWN applied counter: a batch that starts above
	// lastApplied+1 means either committed entries were dropped or the
	// counter was never seeded from the recovered index, and both make the
	// session expiry checkpoint walk run against a different entry sequence
	// than every other replica ran it against.
	ErrApplyGap = errors.New("rsm: committed entry gap before apply")
)
