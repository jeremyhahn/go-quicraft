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

// Entry is the fundamental unit of replication in the Raft log. It contains
// 8 fixed fields and one variable-length command payload. The wire format
// uses a 64-byte fixed header followed by the command data:
//
//	[Term:8][Index:8][Type:8][Key:8][ClientID:8][SeriesID:8][RespondedTo:8][CmdLen:4][Cmd:N]
//
// Entry.Type encodes both the entry type (lower 4 bits) and compression flag
// (bit 4). Always mask with EntryTypeMask before comparing entry types.
// Cmd is zero-copy aliased into the source buffer during UnmarshalFrom.
type Entry struct {
	// Term is the Raft term when this entry was proposed.
	Term uint64
	// Index is the log position of this entry.
	Index uint64
	// Type identifies the entry type and compression flag.
	// Lower 4 bits: EntryNormal, EntryConfigChange, EntryEncodedEntry, or EntryMetadata.
	// Bit 4: EntryCompressFlag (Snappy compression).
	Type uint64
	// Key is the client request key for deduplication.
	Key uint64
	// ClientID is the session client ID.
	ClientID uint64
	// SeriesID is the session series ID.
	SeriesID uint64
	// RespondedTo is the highest responded-to series for this client.
	RespondedTo uint64
	// Cmd is the state machine command payload. After UnmarshalFrom,
	// Cmd aliases the source buffer (zero-copy). The caller must copy
	// if the data needs to outlive the buffer.
	Cmd []byte
}

// entryHeaderSize is the fixed portion of the wire format: 7 uint64 fields
// (56 bytes) plus a 4-byte uint32 cmd length prefix.
const entryHeaderSize = 7*8 + 4 // 60 bytes

// Size returns the total marshaled size of the entry in bytes.
func (e *Entry) Size() int {
	return entryHeaderSize + len(e.Cmd)
}

// MarshalTo writes the entry into buf and returns the number of bytes
// written. buf must be at least Size() bytes. Returns ErrBufferTooSmall
// if buf is too short.
func (e *Entry) MarshalTo(buf []byte) (int, error) {
	required := e.Size()
	if len(buf) < required {
		return 0, ErrBufferTooSmall
	}
	putUint64(buf[0:], e.Term)
	putUint64(buf[8:], e.Index)
	putUint64(buf[16:], e.Type)
	putUint64(buf[24:], e.Key)
	putUint64(buf[32:], e.ClientID)
	putUint64(buf[40:], e.SeriesID)
	putUint64(buf[48:], e.RespondedTo)
	putUint32(buf[56:], uint32(len(e.Cmd)))
	copy(buf[60:], e.Cmd)
	return required, nil
}

// UnmarshalFrom reads the entry from buf and returns the number of bytes
// consumed. All fields are validated before use. Cmd is zero-copy aliased
// into buf. Returns ErrBufferTooSmall on truncated input,
// ErrEntrySizeLimitExceeded if CmdLen exceeds MaxEntrySize, or
// ErrInvalidEntryType if the entry type is unrecognized.
func (e *Entry) UnmarshalFrom(buf []byte) (int, error) {
	if len(buf) < entryHeaderSize {
		return 0, ErrBufferTooSmall
	}
	e.Term = getUint64(buf[0:])
	e.Index = getUint64(buf[8:])
	e.Type = getUint64(buf[16:])
	e.Key = getUint64(buf[24:])
	e.ClientID = getUint64(buf[32:])
	e.SeriesID = getUint64(buf[40:])
	e.RespondedTo = getUint64(buf[48:])
	cmdLen := getUint32(buf[56:])
	// Validate cmdLen before any allocation or slice creation.
	// MaxEntrySize (8MB) fits in uint32, so no overflow risk.
	if cmdLen > MaxEntrySize {
		return 0, ErrEntrySizeLimitExceeded
	}
	total := entryHeaderSize + int(cmdLen)
	if len(buf) < total {
		return 0, ErrBufferTooSmall
	}
	// Validate Entry.Type against known enum values (stripped of compression flag).
	entryType := e.Type & EntryTypeMask
	if entryType != EntryNormal && entryType != EntryConfigChange &&
		entryType != EntryEncodedEntry && entryType != EntryMetadata {
		return 0, ErrInvalidEntryType
	}
	if cmdLen > 0 {
		e.Cmd = buf[60:total] // zero-copy: aliases source buffer
	} else {
		e.Cmd = nil
	}
	return total, nil
}

// IsConfigChange returns true if this entry is a configuration change.
func (e *Entry) IsConfigChange() bool {
	return e.Type&EntryTypeMask == EntryConfigChange
}

// IsMetadataEntry returns true if this entry is a witness metadata entry.
// Metadata entries carry only Index and Term (no Cmd payload) and are used
// to replicate log position information to witnesses so they can participate
// in elections with correct up-to-date checks.
func (e *Entry) IsMetadataEntry() bool {
	return e.Type&EntryTypeMask == EntryMetadata
}

// IsSessionManaged returns true if this entry belongs to a client session
// (clientID != 0). Entries with clientID == 0 bypass session deduplication.
func (e *Entry) IsSessionManaged() bool {
	return e.ClientID != NotSessionManagedClientID
}

// IsNewSessionRequest returns true if this entry is a session registration
// request. Identified by: not a config change, empty Cmd, valid clientID,
// and SeriesID == SessionSeriesIDForRegister.
func (e *Entry) IsNewSessionRequest() bool {
	return !e.IsConfigChange() &&
		len(e.Cmd) == 0 &&
		e.ClientID != NotSessionManagedClientID &&
		e.SeriesID == SessionSeriesIDForRegister
}

// IsEndOfSessionRequest returns true if this entry is a session
// unregistration request. Identified by: not a config change, empty Cmd,
// valid clientID, and SeriesID == SessionSeriesIDForUnregister.
func (e *Entry) IsEndOfSessionRequest() bool {
	return !e.IsConfigChange() &&
		len(e.Cmd) == 0 &&
		e.ClientID != NotSessionManagedClientID &&
		e.SeriesID == SessionSeriesIDForUnregister
}

// IsEmpty returns true if the entry has no command payload.
func (e *Entry) IsEmpty() bool {
	return len(e.Cmd) == 0
}

// Reset clears all fields for reuse, enabling zero-allocation patterns
// when entries are pooled.
func (e *Entry) Reset() {
	e.Term = 0
	e.Index = 0
	e.Type = 0
	e.Key = 0
	e.ClientID = 0
	e.SeriesID = 0
	e.RespondedTo = 0
	e.Cmd = nil
}
