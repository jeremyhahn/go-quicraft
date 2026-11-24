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
// 9 fixed fields and one variable-length command payload. The wire format
// uses a 68-byte fixed header (entryHeaderSize) followed by the command data:
//
//	[Term:8][Index:8][Type:8][Key:8][ClientID:8][SeriesID:8][RespondedTo:8][OwnerID:8][CmdLen:4][Cmd:N]
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
	// Lower 4 bits: EntryNormal, EntryConfigChange, EntryEncodedEntry,
	// EntryMetadata, or EntryInitialMembership.
	// Bit 4: EntryCompressFlag (Snappy compression).
	// Bits 5 and above are RESERVED and must be zero: both MarshalTo and
	// UnmarshalFrom refuse a value that sets any of them (see
	// entryTypeReservedMask).
	Type uint64
	// Key is the client request key for deduplication.
	Key uint64
	// ClientID is the session client ID.
	ClientID uint64
	// SeriesID is the session series ID.
	SeriesID uint64
	// RespondedTo is the highest responded-to series for this client.
	RespondedTo uint64
	// OwnerID identifies the PRINCIPAL that owns the session this entry
	// registers — the embedder's tenant, account, or connection identity — and
	// is the key the per-owner concurrent-session cap is counted against (see
	// SessionPolicy.MaxSessionsPerOwner and session.Manager.Register).
	//
	// It is here, in the replicated entry, and not derived at the leader,
	// because the cap is a STATE MACHINE decision: every replica must accept or
	// refuse a registration identically from the log alone, and a leader-local
	// fact — the authenticated peer on the connection that carried the proposal
	// — is not in the log and is not the same on a replica replaying it after a
	// restart. Anything the apply path consults must ride in the entry.
	//
	// It is SUPPLIED BY THE EMBEDDER (Host.NewSessionWithOwner,
	// NewSessionWithOwner). This library has no notion of who a client is: a
	// clientID is eight bytes the caller chooses, so it cannot be used to group
	// sessions by principal, and the engine must not invent an identity it
	// cannot authenticate. An embedder that has one — a tenant ID, a subject
	// from a verified token — passes it in and gets the cap; one that does not
	// passes nothing.
	//
	// ZERO MEANS UNSET, and an unset owner is never counted or capped. That is
	// what keeps every existing embedder working unchanged: a session registered
	// without an owner behaves exactly as it did before this field existed. It
	// is deliberately NOT a security boundary on its own — the value is whatever
	// the embedder claims — so the cap it drives limits resource consumption per
	// declared owner, and is only as trustworthy as the embedder's own
	// authentication.
	//
	// Only session-registration entries carry a meaningful value; the apply path
	// reads it in no other branch.
	OwnerID uint64
	// Cmd is the state machine command payload. After UnmarshalFrom,
	// Cmd aliases the source buffer (zero-copy). The caller must copy
	// if the data needs to outlive the buffer.
	Cmd []byte
}

// entryHeaderSize is the fixed portion of the wire format: 8 uint64 fields
// (64 bytes) plus a 4-byte uint32 cmd length prefix.
const entryHeaderSize = 8*8 + 4 // 68 bytes

// entryTypeReservedMask covers every bit of Entry.Type that carries no meaning
// in this format: everything outside the type enum (EntryTypeMask) and the
// compression flag (EntryCompressFlag).
//
// They are REJECTED rather than masked away, at both ends. Masking would let a
// sender attach 59 bits of its own to an entry and have this build silently
// agree to a type it read as something else, which is the same trust-the-wire
// mistake as accepting an unknown enum value — and it would do it while the
// entry, bits and all, is committed to the raft log and replayed forever.
// Rejecting also keeps Type byte-stable across a round trip, which the fuzz
// round-trip targets assert and the log replay path depends on.
const entryTypeReservedMask = ^(EntryTypeMask | EntryCompressFlag)

// isValidEntryType reports whether t is an Entry.Type value this package will
// both write and read back: a known entry type in the low four bits, the
// compression flag optionally set, and nothing else.
//
// It is shared by MarshalTo and UnmarshalFrom deliberately. If the two
// disagreed, the package could emit an entry it refused to parse, and an entry
// is the one thing here that is committed to the raft log before anyone tries
// to read it back.
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

// MarshalTo writes the entry into buf and returns the number of bytes
// written. buf must be at least Size() bytes. Returns ErrBufferTooSmall
// if buf is too short, ErrEntrySizeLimitExceeded if Cmd exceeds MaxEntrySize,
// or ErrInvalidEntryType if Type is one UnmarshalFrom would refuse — the read
// side's bounds applied on write, so the encoder cannot emit an entry this
// package refuses to read back.
//
// Type is validated too, and the objection that kept it unchecked here for so
// long — the compression flag shares the field, so callers legitimately build
// values the enum alone does not describe — is exactly what isValidEntryType
// accounts for: the flag is admitted, the enum is checked under the mask, and
// only the 59 bits that mean nothing are refused. An entry is committed to the
// raft log before any peer parses it, so an encoder that emits a type its own
// decoder rejects writes an entry every replica faults on forever.
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

// IsInitialMembership reports whether this entry is the bootstrap membership
// entry a shard writes at index 1 on first start. Its Cmd is a marshaled
// Membership carrying the full initial configuration (voters, observers,
// witnesses, and the witness policy), so a replica reconstructs its membership
// by replaying the committed log rather than from an out-of-band seed.
func (e *Entry) IsInitialMembership() bool {
	return e.Type&EntryTypeMask == EntryInitialMembership
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
	e.OwnerID = 0
	e.Cmd = nil
}
