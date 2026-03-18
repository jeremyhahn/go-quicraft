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

package proto

// State represents the persistent Raft hard state: the current term,
// the candidate voted for, and the highest committed log index. The
// wire format is exactly 24 bytes (3 fixed uint64 fields):
//
//	[Term:8][Vote:8][Commit:8]
type State struct {
	// Term is the current Raft term.
	Term uint64
	// Vote is the candidate this node voted for in the current term.
	// Zero if no vote has been cast.
	Vote uint64
	// Commit is the highest log index known to be committed.
	Commit uint64
}

// stateSize is the fixed wire format size: 3 uint64 fields = 24 bytes.
const stateSize = 3 * 8 // 24 bytes

// Size returns the marshaled size in bytes. Always returns stateSize (24).
func (s *State) Size() int {
	return stateSize
}

// MarshalTo writes the state into buf and returns bytes written.
// buf must be at least 24 bytes. Returns ErrBufferTooSmall if buf
// is too short.
func (s *State) MarshalTo(buf []byte) (int, error) {
	if len(buf) < stateSize {
		return 0, ErrBufferTooSmall
	}
	putUint64(buf[0:], s.Term)
	putUint64(buf[8:], s.Vote)
	putUint64(buf[16:], s.Commit)
	return stateSize, nil
}

// UnmarshalFrom reads the state from buf and returns bytes consumed.
// Returns ErrBufferTooSmall on truncated input.
func (s *State) UnmarshalFrom(buf []byte) (int, error) {
	if len(buf) < stateSize {
		return 0, ErrBufferTooSmall
	}
	s.Term = getUint64(buf[0:])
	s.Vote = getUint64(buf[8:])
	s.Commit = getUint64(buf[16:])
	return stateSize, nil
}

// IsEmpty returns true if the state contains all zero values,
// indicating no hard state has been persisted.
func (s *State) IsEmpty() bool {
	return s.Term == 0 && s.Vote == 0 && s.Commit == 0
}

// Reset clears all fields for reuse.
func (s *State) Reset() {
	s.Term = 0
	s.Vote = 0
	s.Commit = 0
}
