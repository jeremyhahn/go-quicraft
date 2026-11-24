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

// State is the persistent Raft hard state: term, vote, commit index and the vote window this replica may still owe a read lease.
type State struct {
	// Term is the current Raft term.
	Term uint64
	// Vote is the candidate this node voted for in the current term.
	// Zero if no vote has been cast.
	Vote uint64
	// Commit is the highest log index known to be committed.
	Commit uint64
	// VoteWindow is the longest vote window, in nanoseconds, this replica may have promised in a heartbeat acknowledgement and not yet outlived.
	VoteWindow uint64
}

// stateSize is the fixed wire format size: 4 uint64 fields = 32 bytes.
const stateSize = 4 * 8 // 32 bytes

// Size returns the marshaled size in bytes. Always returns stateSize (32).
func (s *State) Size() int {
	return stateSize
}

// MarshalTo writes the state into buf and returns bytes written.
// buf must be at least 32 bytes. Returns ErrBufferTooSmall if buf
// is too short.
func (s *State) MarshalTo(buf []byte) (int, error) {
	if len(buf) < stateSize {
		return 0, ErrBufferTooSmall
	}
	putUint64(buf[0:], s.Term)
	putUint64(buf[8:], s.Vote)
	putUint64(buf[16:], s.Commit)
	putUint64(buf[24:], s.VoteWindow)
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
	s.VoteWindow = getUint64(buf[24:])
	return stateSize, nil
}

// IsEmpty returns true if the state contains all zero values,
// indicating no hard state has been persisted.
func (s *State) IsEmpty() bool {
	return s.Term == 0 && s.Vote == 0 && s.Commit == 0 && s.VoteWindow == 0
}

// Reset clears all fields for reuse.
func (s *State) Reset() {
	s.Term = 0
	s.Vote = 0
	s.Commit = 0
	s.VoteWindow = 0
}
