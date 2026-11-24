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

// MessageOrigin records how a message reached the raft state machine.
//
// It is PROCESS-LOCAL state, deliberately NOT part of the wire format:
// MarshalTo never writes it, UnmarshalFrom never reads it, and Reset zeroes
// it. The zero value is therefore OriginRemote, so a message decoded from
// the network — or recycled through the message pool — is untrusted by
// construction and no peer can forge a local origin. Only in-process
// injectors stamp OriginLocal, and they do it through the delivery API
// (Node.DeliverLocal / Engine.DeliverLocal) rather than by hand.
type MessageOrigin uint8

const (
	// OriginRemote marks a message that was decoded from the wire. It is the
	// zero value so that "untrusted" is the default for every code path that
	// forgets to say otherwise.
	OriginRemote MessageOrigin = iota
	// OriginLocal marks a message injected by this process: the engine's
	// Propose/ReadIndex batching, the snapshot receive path's InstallSnapshot,
	// the transport's SnapshotStatus/Unreachable notifications and the host's
	// LeaderTransfer request. These carry no term and are exempt from the
	// term gate in raft.step, so the exemption must never be reachable from
	// the network.
	OriginLocal
)

// IsLocal reports whether the message was injected by this process rather
// than decoded from the wire.
func (o MessageOrigin) IsLocal() bool { return o == OriginLocal }

// String returns the origin name for logs and errors.
func (o MessageOrigin) String() string {
	if o == OriginLocal {
		return "local"
	}
	return "remote"
}

// Message represents a Raft protocol message exchanged between peers.
// It has 11 fixed uint64 fields (88 bytes) followed by a count-prefixed
// list of entries and an inline snapshot. The wire format is:
//
//	[Type:8][From:8][To:8][ShardID:8][ReplicaID:8][Term:8][LogTerm:8]
//	[LogIndex:8][Commit:8][Reject:8][Hint:8]
//	[EntriesCount:4][entries...][HasSnapshot:8][snapshot...]
//
// Reject is stored as uint64 on wire (0 or 1) for fixed-width encoding.
//
// Origin is process-local and is NOT part of that layout; see MessageOrigin.
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
	// Origin records whether this message was injected by the local process
	// or decoded from the wire. It never crosses the wire; see MessageOrigin.
	Origin MessageOrigin
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
// buf must be at least Size() bytes. Returns ErrBufferTooSmall if buf is too
// short, ErrTooManyMessages if Entries exceeds MaxEntriesPerMessage, or
// ErrInvalidMessageType if Type is outside the range UnmarshalFrom accepts —
// the read side's bounds applied on write, so a producer learns at the SENDER
// that its message is unparseable instead of having every peer silently reject
// the frame (a rejection the sender cannot tell from a broken connection; see
// MaxEntriesPerMessage).
//
// It also returns ErrSnapshotWithoutPosition rather than silently dropping a
// snapshot the layout cannot express; see Snapshot.hasContentBeyondPosition.
func (m *Message) MarshalTo(buf []byte) (int, error) {
	if len(m.Entries) > MaxEntriesPerMessage {
		return 0, ErrTooManyMessages
	}
	// The same range gate UnmarshalFrom applies. Message.Type selects the
	// handler on the receiving side, so a value outside the enum is not a
	// cosmetic mismatch: the frame is refused wholesale and the sender sees a
	// dead connection.
	if m.Type > maxMessageType {
		return 0, ErrInvalidMessageType
	}
	// The snapshot presence flag is derived from Snapshot.IsEmpty, which asks
	// only about Index and Term. A snapshot carrying a filepath, a membership
	// and a size but no log position therefore encodes as ABSENT, and every
	// field of it is discarded with no error — the receiver gets an
	// InstallSnapshot with nothing in it. Refuse instead: no position is not a
	// state any producer here can legitimately reach (a raft snapshot always
	// covers at least index 1), so the value is a construction bug, and the one
	// outcome that must not happen is losing it quietly.
	if m.Snapshot.IsEmpty() && m.Snapshot.hasContentBeyondPosition() {
		return 0, ErrSnapshotWithoutPosition
	}
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
// against MaxEntriesPerMessage before allocation. Returns ErrBufferTooSmall
// on truncated input, ErrInvalidMessageType for unknown types, or
// ErrTooManyMessages if the entry count exceeds the limit.
func (m *Message) UnmarshalFrom(buf []byte) (int, error) {
	// Need at least fixed fields + entries count
	if len(buf) < messageFixedSize+4 {
		return 0, ErrBufferTooSmall
	}
	// Anything reconstructed from bytes is remote by definition. Setting it
	// here (rather than relying on the caller to have Reset the message)
	// makes the wire decoder itself the place that establishes the property
	// raft.step depends on.
	m.Origin = OriginRemote
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
	if m.Type > maxMessageType {
		return 0, ErrInvalidMessageType
	}
	offset := messageFixedSize
	// Entries
	entryCount := getUint32(buf[offset:])
	offset += 4
	if entryCount > MaxEntriesPerMessage {
		return 0, ErrTooManyMessages
	}
	// entryHeaderSize is the minimum encoding of one Entry (empty Cmd), so
	// a count the remaining buffer cannot possibly hold is rejected before
	// the slice is allocated rather than after a partial parse.
	if uint64(entryCount)*entryHeaderSize > uint64(len(buf)-offset) {
		return 0, ErrBufferTooSmall
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

// maxRetainedEntries bounds the Entries capacity a message keeps across
// Reset, mirroring maxRetainedRequests for MessageBatch.Requests. A message
// lives inside a pooled MessageBatch, so without this a single crafted frame
// carrying many oversized messages would pin
// len(Requests) * cap(Entries) * unsafe.Sizeof(Entry) bytes in the pool for
// the process lifetime. The bound sits above the legitimate working set —
// UnmarshalFrom accepts up to MaxEntriesPerMessage (10000) entries per
// message, while a steady-state append carries at most
// batch.DefaultMaxBatchSize (1000) proposals — so a normal replication
// message still reuses its backing array.
const maxRetainedEntries = 1024

// Reset clears all fields for reuse. An Entries slice grown past
// maxRetainedEntries is dropped rather than truncated so a pooled message
// cannot retain attacker-influenced capacity.
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
	// A pooled message must come back as UNTRUSTED. Leaving a stale
	// OriginLocal on a recycled message would let the next wire decode inherit
	// the local-injection exemption in raft.step.
	m.Origin = OriginRemote
	m.Snapshot.Reset()
	if cap(m.Entries) > maxRetainedEntries {
		m.Entries = nil
		return
	}
	m.Entries = m.Entries[:0]
}
