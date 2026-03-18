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

// Message represents a Raft protocol message exchanged between peers.
// It has 11 fixed uint64 fields (88 bytes) followed by a count-prefixed
// list of entries and an inline snapshot. The wire format is:
//
//	[Type:8][From:8][To:8][ShardID:8][ReplicaID:8][Term:8][LogTerm:8]
//	[LogIndex:8][Commit:8][Reject:8][Hint:8]
//	[EntriesCount:4][entries...][HasSnapshot:8][snapshot...]
//
// Reject is stored as uint64 on wire (0 or 1) for fixed-width encoding.
type Message struct {
	// Type identifies the message type (Replicate, RequestVote, etc.).
	Type uint64
	// From is the sender replica ID.
	From uint64
	// To is the recipient replica ID.
	To uint64
	// ShardID identifies the Raft shard.
	ShardID uint64
	// ReplicaID is the source replica within the shard.
	ReplicaID uint64
	// Term is the sender's current Raft term.
	Term uint64
	// LogTerm is the term of the log entry at LogIndex.
	LogTerm uint64
	// LogIndex is a log index relevant to the message type.
	LogIndex uint64
	// Commit is the sender's commit index.
	Commit uint64
	// Reject indicates whether this is a rejection response.
	Reject bool
	// Hint carries type-specific hint data (e.g., rejected log index).
	Hint uint64
	// Snapshot carries snapshot metadata for InstallSnapshot messages.
	Snapshot Snapshot
	// Entries carries log entries for Replicate messages.
	Entries []Entry
}

// messageFixedSize is the wire size of the 11 fixed uint64 fields.
const messageFixedSize = 11 * 8 // 88 bytes

// HeartbeatMessageSize is the exact marshaled size of a heartbeat Message
// (zero entries, empty snapshot). Used by the transport heartbeat fast path
// to avoid per-message Size() calls.
//
// Layout: fixed fields (88) + entries count (4) + hasSnapshot flag (8) = 100.
const HeartbeatMessageSize = messageFixedSize + 4 + 8 // 100 bytes

// Size returns the total marshaled size of the message in bytes.
func (m *Message) Size() int {
	sz := messageFixedSize + 4 // fixed fields + entries count prefix
	for i := range m.Entries {
		sz += m.Entries[i].Size()
	}
	sz += 8 // HasSnapshot flag
	if !m.Snapshot.IsEmpty() {
		sz += m.Snapshot.Size()
	}
	return sz
}

// MarshalTo writes the message into buf and returns bytes written.
// buf must be at least Size() bytes. Returns ErrBufferTooSmall if buf
// is too short.
func (m *Message) MarshalTo(buf []byte) (int, error) {
	required := m.Size()
	if len(buf) < required {
		return 0, ErrBufferTooSmall
	}
	putUint64(buf[0:], m.Type)
	putUint64(buf[8:], m.From)
	putUint64(buf[16:], m.To)
	putUint64(buf[24:], m.ShardID)
	putUint64(buf[32:], m.ReplicaID)
	putUint64(buf[40:], m.Term)
	putUint64(buf[48:], m.LogTerm)
	putUint64(buf[56:], m.LogIndex)
	putUint64(buf[64:], m.Commit)
	putBool(buf[72:], m.Reject)
	putUint64(buf[80:], m.Hint)
	offset := messageFixedSize
	// Entries: [count:4][entries...]
	putUint32(buf[offset:], uint32(len(m.Entries)))
	offset += 4
	for i := range m.Entries {
		n, err := m.Entries[i].MarshalTo(buf[offset:])
		if err != nil {
			return 0, err
		}
		offset += n
	}
	// Snapshot: [HasSnapshot:8][snapshot...]
	hasSnapshot := !m.Snapshot.IsEmpty()
	putBool(buf[offset:], hasSnapshot)
	offset += 8
	if hasSnapshot {
		n, err := m.Snapshot.MarshalTo(buf[offset:])
		if err != nil {
			return 0, err
		}
		offset += n
	}
	return offset, nil
}

// UnmarshalFrom reads the message from buf and returns bytes consumed.
// Validates Message.Type against the known range and entries count
// against maxMessagesPerBatch before allocation. Returns ErrBufferTooSmall
// on truncated input, ErrInvalidMessageType for unknown types, or
// ErrTooManyMessages if the entry count exceeds the limit.
func (m *Message) UnmarshalFrom(buf []byte) (int, error) {
	// Need at least fixed fields + entries count
	if len(buf) < messageFixedSize+4 {
		return 0, ErrBufferTooSmall
	}
	m.Type = getUint64(buf[0:])
	m.From = getUint64(buf[8:])
	m.To = getUint64(buf[16:])
	m.ShardID = getUint64(buf[24:])
	m.ReplicaID = getUint64(buf[32:])
	m.Term = getUint64(buf[40:])
	m.LogTerm = getUint64(buf[48:])
	m.LogIndex = getUint64(buf[56:])
	m.Commit = getUint64(buf[64:])
	m.Reject = getBool(buf[72:])
	m.Hint = getUint64(buf[80:])
	// Validate Message.Type against known range.
	if m.Type > RateLimit {
		return 0, ErrInvalidMessageType
	}
	offset := messageFixedSize
	// Entries
	entryCount := getUint32(buf[offset:])
	offset += 4
	if entryCount > maxMessagesPerBatch {
		return 0, ErrTooManyMessages
	}
	if entryCount > 0 {
		if cap(m.Entries) >= int(entryCount) {
			m.Entries = m.Entries[:entryCount]
		} else {
			m.Entries = make([]Entry, entryCount)
		}
		for i := uint32(0); i < entryCount; i++ {
			n, err := m.Entries[i].UnmarshalFrom(buf[offset:])
			if err != nil {
				return 0, err
			}
			offset += n
		}
	} else {
		m.Entries = m.Entries[:0]
	}
	// Snapshot: [HasSnapshot:8][snapshot...]
	if len(buf) < offset+8 {
		return 0, ErrBufferTooSmall
	}
	hasSnapshot := getBool(buf[offset:])
	offset += 8
	if hasSnapshot {
		n, err := m.Snapshot.UnmarshalFrom(buf[offset:])
		if err != nil {
			return 0, err
		}
		offset += n
	} else {
		m.Snapshot.Reset()
	}
	return offset, nil
}

// Reset clears all fields for reuse.
func (m *Message) Reset() {
	m.Type = 0
	m.From = 0
	m.To = 0
	m.ShardID = 0
	m.ReplicaID = 0
	m.Term = 0
	m.LogTerm = 0
	m.LogIndex = 0
	m.Commit = 0
	m.Reject = false
	m.Hint = 0
	m.Snapshot.Reset()
	m.Entries = m.Entries[:0]
}
