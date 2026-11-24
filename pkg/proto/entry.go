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

// Entry is a Raft log entry: a 68-byte fixed header then Cmd, which UnmarshalFrom aliases into the buffer. Mask Type with EntryTypeMask.
type Entry struct {
	// Term is the Raft term when this entry was proposed.
	Term uint64
	// Index is the log position of this entry.
	Index uint64
	// Type holds the entry type in bits 0-3 and EntryCompressFlag in bit 4; higher bits must be zero.
	Type uint64
	// Key is the client request key for deduplication.
	Key uint64
	// ClientID is the session client ID.
	ClientID uint64
	// SeriesID is the session series ID.
	SeriesID uint64
	// RespondedTo is the highest responded-to series for this client.
	RespondedTo uint64
	// OwnerID is the embedder-supplied principal the per-owner session cap counts against; 0 is unset and never capped. Only session registrations use it.
	OwnerID uint64
	// Cmd is the state machine command payload. After UnmarshalFrom,
	// Cmd aliases the source buffer (zero-copy). The caller must copy
	// if the data needs to outlive the buffer.
	Cmd []byte
}

// entryHeaderSize is the fixed portion of the wire format: 8 uint64 fields
// (64 bytes) plus a 4-byte uint32 cmd length prefix.
const entryHeaderSize = 8*8 + 4 // 68 bytes

// entryTypeReservedMask covers Entry.Type bits with no meaning; they are rejected on both read and write.
const entryTypeReservedMask = ^(EntryTypeMask | EntryCompressFlag)

// isValidEntryType reports whether t is a known type with an optional compression flag and nothing else; shared by MarshalTo and UnmarshalFrom.
func isValidEntryType(t uint64) bool {
	if t&entryTypeReservedMask != 0 {
		return false
	}
	switch t & EntryTypeMask {
	case EntryNormal, EntryConfigChange, EntryEncodedEntry, EntryMetadata,
		EntryInitialMembership:
		return true
	default:
		return false
	}
}

// Size returns the total marshaled size of the entry in bytes.
func (e *Entry) Size() int {
	return entryHeaderSize + len(e.Cmd)
}

// MarshalTo writes the entry into buf and returns the bytes written: ErrBufferTooSmall, ErrEntrySizeLimitExceeded or ErrInvalidEntryType on failure.
func (e *Entry) MarshalTo(buf []byte) (int, error) {
	if uint64(len(e.Cmd)) > uint64(MaxEntrySize) {
		return 0, ErrEntrySizeLimitExceeded
	}
	if !isValidEntryType(e.Type) {
		return 0, ErrInvalidEntryType
	}
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
	putUint64(buf[56:], e.OwnerID)
	putUint32(buf[64:], uint32(len(e.Cmd)))
	copy(buf[entryHeaderSize:], e.Cmd)
	return required, nil
}

// UnmarshalFrom reads the entry from buf, aliasing Cmd: ErrBufferTooSmall, ErrEntrySizeLimitExceeded or ErrInvalidEntryType on failure.
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
	e.OwnerID = getUint64(buf[56:])
	cmdLen := getUint32(buf[64:])
	// Validate cmdLen before any allocation or slice creation.
	// MaxEntrySize (8MB) fits in uint32, so no overflow risk.
	if cmdLen > MaxEntrySize {
		return 0, ErrEntrySizeLimitExceeded
	}
	total := entryHeaderSize + int(cmdLen)
	if len(buf) < total {
		return 0, ErrBufferTooSmall
	}
	// Validate Entry.Type: a known enum value under the mask, the compression
	// flag at most, and no reserved bits. See entryTypeReservedMask for why the
	// reserved bits are refused rather than masked off.
	if !isValidEntryType(e.Type) {
		return 0, ErrInvalidEntryType
	}
	if cmdLen > 0 {
		e.Cmd = buf[entryHeaderSize:total] // zero-copy: aliases source buffer
	} else {
		e.Cmd = nil
	}
	return total, nil
}

// IsConfigChange returns true if this entry is a configuration change.
func (e *Entry) IsConfigChange() bool {
	return e.Type&EntryTypeMask == EntryConfigChange
}

// IsInitialMembership reports whether this is the bootstrap membership entry at index 1.
func (e *Entry) IsInitialMembership() bool {
	return e.Type&EntryTypeMask == EntryInitialMembership
}

// IsMetadataEntry reports whether this is a witness metadata entry, carrying only Index and Term.
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
	e.OwnerID = 0
	e.Cmd = nil
}
