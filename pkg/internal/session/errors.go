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

package session

import (
	"errors"
	"fmt"
)

// Session sentinel errors, kept here to avoid an import cycle; the quicraft package translates them.
var (
	// ErrSessionLimitExceeded is returned when the total session limit
	// for a shard has been reached.
	ErrSessionLimitExceeded = errors.New("session: session limit exceeded")

	// ErrSessionNotFound is returned when an operation references a
	// client session that does not exist.
	ErrSessionNotFound = errors.New("session: session not found")

	// ErrSessionAlreadyExists is returned when registering a client
	// session that is already registered.
	ErrSessionAlreadyExists = errors.New("session: session already exists")

	// ErrResponseLimitExceeded is returned when a session's response
	// cache exceeds the configured maximum, indicating a misbehaving client.
	ErrResponseLimitExceeded = errors.New("session: response limit exceeded")

	// ErrResponseEvicted is returned by CheckDuplicate when seriesID was completed and its cached response evicted.
	ErrResponseEvicted = errors.New("session: response already evicted")

	// ErrSnapshotCorrupted is the sentinel for BoundsError, enabling
	// errors.Is(err, ErrSnapshotCorrupted) checks.
	ErrSnapshotCorrupted = errors.New("session: snapshot data corrupted or malicious")
)

// maxResponseDataSize bounds one response payload during unmarshal.
const maxResponseDataSize = 64 << 20 // 64 MiB

// BoundsError is returned during snapshot unmarshal when a decoded count
// exceeds the configured or hardcoded limit. This guards against OOM
// from corrupted or malicious snapshot data.
type BoundsError struct {
	Field string
	Value uint64
	Limit uint64
}

// Error returns a human-readable description of the bounds violation.
func (e *BoundsError) Error() string {
	return fmt.Sprintf(
		"session: snapshot bounds exceeded: %s = %d, limit = %d",
		e.Field, e.Value, e.Limit,
	)
}

// Is reports whether target matches the sentinel ErrSnapshotCorrupted.
func (e *BoundsError) Is(target error) bool {
	return target == ErrSnapshotCorrupted
}
