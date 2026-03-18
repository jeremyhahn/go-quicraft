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

// Package sm defines the state machine interfaces and supporting types
// for user-implemented state machines in QuicRaft. Users implement one
// of the three state machine interfaces (StateMachine, ConcurrentStateMachine,
// or DiskStateMachine) and provide a factory function to create instances.
// QuicRaft handles consensus, replication, snapshotting, and recovery.
package sm

// Reserved Result.Value constants used internally by the RSM layer to
// signal session-related conditions back through the apply pipeline.
// These values occupy the high end of the uint64 range (alongside the
// session series ID sentinels) and must not be used by application code.
const (
	// ResultSessionExpired is the reserved Result.Value set by the RSM
	// when a session-managed entry references an unregistered clientID.
	// This happens after session expiry or snapshot restore that did not
	// include the session. The host translates this into ErrSessionExpired
	// on the RequestResult so the client can distinguish it from a
	// legitimate zero-value result (Raft PhD Figure 6.1, step 3).
	ResultSessionExpired uint64 = 1<<64 - 3 // math.MaxUint64 - 2
)

// Result is returned from state machine Update operations. The Value field
// is an application-defined result code. The Data field carries optional
// result data that is returned to the caller through RequestState.Result().
type Result struct {
	Value uint64
	Data  []byte
}

// Entry represents a committed log entry applied to the state machine.
// This is a simplified view of the internal proto.Entry -- users do not
// see Raft-internal fields (Term, Key, ClientID, etc.). Term is excluded
// because state machines should be term-agnostic.
//
// By default, Cmd is an owned copy. When Config.ZeroCopyEntryCmd is true,
// Cmd aliases an internal buffer that is reused after Update() returns.
// The state machine must copy Cmd if it needs the data to outlive the call.
type Entry struct {
	// Index is the Raft log index of this entry.
	Index uint64

	// Cmd is the proposal payload. Ownership depends on
	// Config.ZeroCopyEntryCmd (see package documentation).
	Cmd []byte
}

// CopyCmd returns an owned copy of the entry's Cmd. Use this method
// when Config.ZeroCopyEntryCmd is true and you need to retain Cmd
// after Update() returns.
func (e *Entry) CopyCmd() []byte {
	if len(e.Cmd) == 0 {
		return nil
	}
	cp := make([]byte, len(e.Cmd))
	copy(cp, e.Cmd)
	return cp
}
