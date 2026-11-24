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

// Package sm defines the StateMachine, ConcurrentStateMachine and DiskStateMachine interfaces users implement for QuicRaft.
package sm

// Result.Value constants reserved by the RSM layer; applications must not use them.
const (
	// ResultSessionExpired marks an entry for an unregistered client; the host reports ErrSessionExpired.
	ResultSessionExpired uint64 = 1<<64 - 3 // math.MaxUint64 - 2

	// ResultInvalidCommand marks a committed entry whose payload cannot be decoded; the state is left unchanged.
	ResultInvalidCommand uint64 = 1<<64 - 4 // math.MaxUint64 - 3
)

// Result is returned from state machine Update operations. The Value field
// is an application-defined result code. The Data field carries optional
// result data that is returned to the caller through RequestState.Result().
type Result struct {
	Value uint64
	Data  []byte
}

// Entry is a committed log entry as the state machine sees it; Cmd aliases an internal buffer when Config.ZeroCopyEntryCmd is set.
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
