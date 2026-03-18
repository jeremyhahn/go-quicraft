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

//go:build quicraft_fuzz

package proto

import (
	"bytes"
	"math"
	"testing"
)

// FuzzEntryRoundTrip verifies that Entry marshal/unmarshal round-trips
// preserve all fields for arbitrary valid inputs.
func FuzzEntryRoundTrip(f *testing.F) {
	// Normal entry with payload.
	f.Add(uint64(1), uint64(1), uint64(EntryNormal), uint64(0),
		uint64(0), uint64(0), uint64(0), []byte("hello"))
	// Config change entry, no payload.
	f.Add(uint64(5), uint64(10), uint64(EntryConfigChange), uint64(42),
		uint64(0), uint64(0), uint64(0), []byte{})
	// Encoded entry with session fields.
	f.Add(uint64(100), uint64(200), uint64(EntryEncodedEntry), uint64(7),
		uint64(999), uint64(3), uint64(2), []byte("session-cmd"))
	// Metadata entry, no payload.
	f.Add(uint64(50), uint64(50), uint64(EntryMetadata), uint64(0),
		uint64(0), uint64(0), uint64(0), []byte{})
	// Compressed normal entry.
	f.Add(uint64(1), uint64(1), uint64(EntryNormal|EntryCompressFlag), uint64(0),
		uint64(0), uint64(0), uint64(0), []byte("compressed"))
	// Compressed config change entry.
	f.Add(uint64(2), uint64(3), uint64(EntryConfigChange|EntryCompressFlag), uint64(1),
		uint64(0), uint64(0), uint64(0), []byte("cc-compressed"))
	// Zero value entry.
	f.Add(uint64(0), uint64(0), uint64(0), uint64(0),
		uint64(0), uint64(0), uint64(0), []byte{})
	// Max uint64 fields, small payload.
	f.Add(uint64(math.MaxUint64), uint64(math.MaxUint64), uint64(EntryNormal), uint64(math.MaxUint64),
		uint64(math.MaxUint64), uint64(math.MaxUint64), uint64(math.MaxUint64), []byte{0xFF})
	// Session register sentinel.
	f.Add(uint64(10), uint64(20), uint64(EntryNormal), uint64(0),
		uint64(12345), SessionSeriesIDForRegister, uint64(0), []byte{})
	// Session unregister sentinel.
	f.Add(uint64(10), uint64(21), uint64(EntryNormal), uint64(0),
		uint64(12345), SessionSeriesIDForUnregister, uint64(0), []byte{})

	f.Fuzz(func(t *testing.T, term, index, typ, key, clientID, seriesID, respondedTo uint64, cmd []byte) {
		// Clamp type to valid entry type range, preserving compress flag.
		baseType := typ & EntryTypeMask
		if baseType > EntryMetadata {
			baseType = baseType % (EntryMetadata + 1)
		}
		compressFlag := typ & EntryCompressFlag
		typ = baseType | compressFlag

		if len(cmd) > int(MaxEntrySize) {
			return
		}

		e := Entry{
			Term:        term,
			Index:       index,
			Type:        typ,
			Key:         key,
			ClientID:    clientID,
			SeriesID:    seriesID,
			RespondedTo: respondedTo,
			Cmd:         cmd,
		}

		buf := make([]byte, e.Size())
		n, err := e.MarshalTo(buf)
		if err != nil {
			t.Fatalf("MarshalTo failed: %v", err)
		}
		if n != e.Size() {
			t.Fatalf("MarshalTo wrote %d bytes, expected %d", n, e.Size())
		}

		var e2 Entry
		n2, err := e2.UnmarshalFrom(buf)
		if err != nil {
			t.Fatalf("round-trip UnmarshalFrom failed: %v", err)
		}
		if n2 != n {
			t.Fatalf("UnmarshalFrom consumed %d bytes, MarshalTo wrote %d", n2, n)
		}

		if e.Term != e2.Term {
			t.Fatalf("Term mismatch: %d != %d", e.Term, e2.Term)
		}
		if e.Index != e2.Index {
			t.Fatalf("Index mismatch: %d != %d", e.Index, e2.Index)
		}
		if e.Type != e2.Type {
			t.Fatalf("Type mismatch: %d != %d", e.Type, e2.Type)
		}
		if e.Key != e2.Key {
			t.Fatalf("Key mismatch: %d != %d", e.Key, e2.Key)
		}
		if e.ClientID != e2.ClientID {
			t.Fatalf("ClientID mismatch: %d != %d", e.ClientID, e2.ClientID)
		}
		if e.SeriesID != e2.SeriesID {
			t.Fatalf("SeriesID mismatch: %d != %d", e.SeriesID, e2.SeriesID)
		}
		if e.RespondedTo != e2.RespondedTo {
			t.Fatalf("RespondedTo mismatch: %d != %d", e.RespondedTo, e2.RespondedTo)
		}
		if !bytes.Equal(e.Cmd, e2.Cmd) {
			t.Fatalf("Cmd mismatch: %v != %v", e.Cmd, e2.Cmd)
		}
	})
}

// FuzzMessageRoundTrip verifies that Message marshal/unmarshal round-trips
// preserve all fields including embedded entries.
func FuzzMessageRoundTrip(f *testing.F) {
	f.Add(uint64(Replicate), uint64(1), uint64(2), uint64(100), uint64(1),
		uint64(5), uint64(4), uint64(10), uint64(9), true, uint64(0),
		[]byte("entry-cmd"))
	f.Add(uint64(Heartbeat), uint64(1), uint64(2), uint64(100), uint64(1),
		uint64(5), uint64(0), uint64(0), uint64(9), false, uint64(0),
		[]byte{})
	f.Add(uint64(RequestVote), uint64(3), uint64(1), uint64(200), uint64(3),
		uint64(10), uint64(9), uint64(50), uint64(0), false, uint64(50),
		[]byte("vote-data"))
	f.Add(uint64(0), uint64(0), uint64(0), uint64(0), uint64(0),
		uint64(0), uint64(0), uint64(0), uint64(0), false, uint64(0),
		[]byte{})

	f.Fuzz(func(t *testing.T, msgType, from, to, shardID, replicaID,
		term, logTerm, logIndex, commit uint64, reject bool, hint uint64,
		entryCmd []byte) {

		// Clamp message type to valid range.
		if msgType > RateLimit {
			msgType = msgType % (RateLimit + 1)
		}
		if len(entryCmd) > int(MaxEntrySize) {
			return
		}

		m := Message{
			Type:      msgType,
			From:      from,
			To:        to,
			ShardID:   shardID,
			ReplicaID: replicaID,
			Term:      term,
			LogTerm:   logTerm,
			LogIndex:  logIndex,
			Commit:    commit,
			Reject:    reject,
			Hint:      hint,
		}

		// Add an entry if there is command data.
		if len(entryCmd) > 0 {
			m.Entries = []Entry{
				{
					Term:  term,
					Index: logIndex,
					Type:  EntryNormal,
					Cmd:   entryCmd,
				},
			}
		}

		buf := make([]byte, m.Size())
		n, err := m.MarshalTo(buf)
		if err != nil {
			t.Fatalf("MarshalTo failed: %v", err)
		}
		if n != m.Size() {
			t.Fatalf("MarshalTo wrote %d bytes, expected %d", n, m.Size())
		}

		var m2 Message
		n2, err := m2.UnmarshalFrom(buf)
		if err != nil {
			t.Fatalf("round-trip UnmarshalFrom failed: %v", err)
		}
		if n2 != n {
			t.Fatalf("UnmarshalFrom consumed %d bytes, MarshalTo wrote %d", n2, n)
		}

		if m.Type != m2.Type {
			t.Fatalf("Type mismatch: %d != %d", m.Type, m2.Type)
		}
		if m.From != m2.From {
			t.Fatalf("From mismatch: %d != %d", m.From, m2.From)
		}
		if m.To != m2.To {
			t.Fatalf("To mismatch: %d != %d", m.To, m2.To)
		}
		if m.ShardID != m2.ShardID {
			t.Fatalf("ShardID mismatch: %d != %d", m.ShardID, m2.ShardID)
		}
		if m.ReplicaID != m2.ReplicaID {
			t.Fatalf("ReplicaID mismatch: %d != %d", m.ReplicaID, m2.ReplicaID)
		}
		if m.Term != m2.Term {
			t.Fatalf("Term mismatch: %d != %d", m.Term, m2.Term)
		}
		if m.LogTerm != m2.LogTerm {
			t.Fatalf("LogTerm mismatch: %d != %d", m.LogTerm, m2.LogTerm)
		}
		if m.LogIndex != m2.LogIndex {
			t.Fatalf("LogIndex mismatch: %d != %d", m.LogIndex, m2.LogIndex)
		}
		if m.Commit != m2.Commit {
			t.Fatalf("Commit mismatch: %d != %d", m.Commit, m2.Commit)
		}
		if m.Reject != m2.Reject {
			t.Fatalf("Reject mismatch: %v != %v", m.Reject, m2.Reject)
		}
		if m.Hint != m2.Hint {
			t.Fatalf("Hint mismatch: %d != %d", m.Hint, m2.Hint)
		}
		if len(m.Entries) != len(m2.Entries) {
			t.Fatalf("Entries length mismatch: %d != %d", len(m.Entries), len(m2.Entries))
		}
		for i := range m.Entries {
			if m.Entries[i].Term != m2.Entries[i].Term {
				t.Fatalf("Entry[%d].Term mismatch", i)
			}
			if m.Entries[i].Index != m2.Entries[i].Index {
				t.Fatalf("Entry[%d].Index mismatch", i)
			}
			if !bytes.Equal(m.Entries[i].Cmd, m2.Entries[i].Cmd) {
				t.Fatalf("Entry[%d].Cmd mismatch", i)
			}
		}
	})
}

// FuzzMessageBatchRoundTrip verifies that MessageBatch marshal/unmarshal
// round-trips preserve the batch count and message contents.
func FuzzMessageBatchRoundTrip(f *testing.F) {
	f.Add(uint64(WireVersion), uint64(42), "127.0.0.1:9000",
		uint64(Heartbeat), uint64(1), uint64(2), []byte("batch-cmd"))
	f.Add(uint64(WireVersion), uint64(0), "node-1:5000",
		uint64(Replicate), uint64(3), uint64(4), []byte{})
	f.Add(uint64(0), uint64(0), "",
		uint64(0), uint64(0), uint64(0), []byte{})
	f.Add(uint64(WireVersion), uint64(math.MaxUint64), "a",
		uint64(RateLimit), uint64(math.MaxUint64), uint64(math.MaxUint64), []byte{0xFF})

	f.Fuzz(func(t *testing.T, binVer, deploymentID uint64, srcAddr string,
		msgType, from, to uint64, entryCmd []byte) {

		// Clamp wire version to valid range.
		if binVer > WireVersion {
			binVer = binVer % (WireVersion + 1)
		}
		// Clamp message type to valid range.
		if msgType > RateLimit {
			msgType = msgType % (RateLimit + 1)
		}
		// Clamp address length.
		if len(srcAddr) > MaxAddressLength {
			srcAddr = srcAddr[:MaxAddressLength]
		}
		if len(entryCmd) > int(MaxEntrySize) {
			return
		}

		msg := Message{
			Type: msgType,
			From: from,
			To:   to,
		}
		if len(entryCmd) > 0 {
			msg.Entries = []Entry{
				{Term: 1, Index: 1, Type: EntryNormal, Cmd: entryCmd},
			}
		}

		mb := MessageBatch{
			BinVer:        binVer,
			DeploymentID:  deploymentID,
			SourceAddress: srcAddr,
			Requests:      []Message{msg},
		}

		buf := make([]byte, mb.Size())
		n, err := mb.MarshalTo(buf)
		if err != nil {
			t.Fatalf("MarshalTo failed: %v", err)
		}
		if n != mb.Size() {
			t.Fatalf("MarshalTo wrote %d bytes, expected %d", n, mb.Size())
		}

		var mb2 MessageBatch
		n2, err := mb2.UnmarshalFrom(buf)
		if err != nil {
			t.Fatalf("round-trip UnmarshalFrom failed: %v", err)
		}
		if n2 != n {
			t.Fatalf("UnmarshalFrom consumed %d bytes, MarshalTo wrote %d", n2, n)
		}

		if mb.BinVer != mb2.BinVer {
			t.Fatalf("BinVer mismatch: %d != %d", mb.BinVer, mb2.BinVer)
		}
		if mb.DeploymentID != mb2.DeploymentID {
			t.Fatalf("DeploymentID mismatch: %d != %d", mb.DeploymentID, mb2.DeploymentID)
		}
		if mb.SourceAddress != mb2.SourceAddress {
			t.Fatalf("SourceAddress mismatch: %q != %q", mb.SourceAddress, mb2.SourceAddress)
		}
		if len(mb.Requests) != len(mb2.Requests) {
			t.Fatalf("Requests length mismatch: %d != %d", len(mb.Requests), len(mb2.Requests))
		}
		for i := range mb.Requests {
			if mb.Requests[i].Type != mb2.Requests[i].Type {
				t.Fatalf("Request[%d].Type mismatch", i)
			}
			if mb.Requests[i].From != mb2.Requests[i].From {
				t.Fatalf("Request[%d].From mismatch", i)
			}
			if mb.Requests[i].To != mb2.Requests[i].To {
				t.Fatalf("Request[%d].To mismatch", i)
			}
			if len(mb.Requests[i].Entries) != len(mb2.Requests[i].Entries) {
				t.Fatalf("Request[%d].Entries length mismatch", i)
			}
			for j := range mb.Requests[i].Entries {
				if !bytes.Equal(mb.Requests[i].Entries[j].Cmd, mb2.Requests[i].Entries[j].Cmd) {
					t.Fatalf("Request[%d].Entry[%d].Cmd mismatch", i, j)
				}
			}
		}
	})
}

// FuzzSnapshotRoundTrip verifies that Snapshot marshal/unmarshal round-trips
// preserve all fields including embedded membership data.
func FuzzSnapshotRoundTrip(f *testing.F) {
	f.Add(uint64(100), uint64(1), uint64(500), uint64(10),
		uint64(42), "/tmp/snap-001", uint64(1024),
		[]byte{0xDE, 0xAD}, uint64(RegularStateMachine), false, uint64(0), false, uint64(0))
	f.Add(uint64(200), uint64(2), uint64(1000), uint64(20),
		uint64(0), "", uint64(0),
		[]byte{}, uint64(OnDiskStateMachine), true, uint64(999), true, uint64(5))
	f.Add(uint64(0), uint64(0), uint64(0), uint64(0),
		uint64(0), "", uint64(0),
		[]byte{}, uint64(0), false, uint64(0), false, uint64(0))
	f.Add(uint64(math.MaxUint64), uint64(math.MaxUint64), uint64(math.MaxUint64), uint64(math.MaxUint64),
		uint64(math.MaxUint64), "/a", uint64(1),
		[]byte{0xFF}, uint64(ConcurrentStateMachine), true, uint64(math.MaxUint64), true, uint64(math.MaxUint64))

	f.Fuzz(func(t *testing.T, shardID, replicaID, index, term,
		ccID uint64, filepath string, fileSize uint64,
		checksum []byte, smType uint64, imported bool,
		onDiskIndex uint64, witness bool, epoch uint64) {

		// Clamp state machine type to valid range.
		if smType > maxStateMachineType {
			smType = smType % (maxStateMachineType + 1)
		}
		// Clamp filepath length.
		if len(filepath) > MaxFilepathLen {
			filepath = filepath[:MaxFilepathLen]
		}
		// Clamp checksum length.
		if len(checksum) > int(MaxChecksumLen) {
			checksum = checksum[:MaxChecksumLen]
		}

		s := Snapshot{
			ShardID:   shardID,
			ReplicaID: replicaID,
			Index:     index,
			Term:      term,
			Membership: Membership{
				ConfigChangeID: ccID,
				Addresses:      map[uint64]string{1: "addr-1"},
				Observers:      map[uint64]string{},
				Witnesses:      map[uint64]string{},
				Removed:        map[uint64]bool{},
			},
			Filepath:    filepath,
			FileSize:    fileSize,
			Checksum:    checksum,
			Type:        smType,
			Imported:    imported,
			OnDiskIndex: onDiskIndex,
			Witness:     witness,
			Epoch:       epoch,
		}

		buf := make([]byte, s.Size())
		n, err := s.MarshalTo(buf)
		if err != nil {
			t.Fatalf("MarshalTo failed: %v", err)
		}
		if n != s.Size() {
			t.Fatalf("MarshalTo wrote %d bytes, expected %d", n, s.Size())
		}

		var s2 Snapshot
		n2, err := s2.UnmarshalFrom(buf)
		if err != nil {
			t.Fatalf("round-trip UnmarshalFrom failed: %v", err)
		}
		if n2 != n {
			t.Fatalf("UnmarshalFrom consumed %d bytes, MarshalTo wrote %d", n2, n)
		}

		if s.ShardID != s2.ShardID {
			t.Fatalf("ShardID mismatch: %d != %d", s.ShardID, s2.ShardID)
		}
		if s.ReplicaID != s2.ReplicaID {
			t.Fatalf("ReplicaID mismatch: %d != %d", s.ReplicaID, s2.ReplicaID)
		}
		if s.Index != s2.Index {
			t.Fatalf("Index mismatch: %d != %d", s.Index, s2.Index)
		}
		if s.Term != s2.Term {
			t.Fatalf("Term mismatch: %d != %d", s.Term, s2.Term)
		}
		if s.Membership.ConfigChangeID != s2.Membership.ConfigChangeID {
			t.Fatalf("Membership.ConfigChangeID mismatch")
		}
		if s.Filepath != s2.Filepath {
			t.Fatalf("Filepath mismatch: %q != %q", s.Filepath, s2.Filepath)
		}
		if s.FileSize != s2.FileSize {
			t.Fatalf("FileSize mismatch: %d != %d", s.FileSize, s2.FileSize)
		}
		if !bytes.Equal(s.Checksum, s2.Checksum) {
			t.Fatalf("Checksum mismatch")
		}
		if s.Type != s2.Type {
			t.Fatalf("Type mismatch: %d != %d", s.Type, s2.Type)
		}
		if s.Imported != s2.Imported {
			t.Fatalf("Imported mismatch: %v != %v", s.Imported, s2.Imported)
		}
		if s.OnDiskIndex != s2.OnDiskIndex {
			t.Fatalf("OnDiskIndex mismatch: %d != %d", s.OnDiskIndex, s2.OnDiskIndex)
		}
		if s.Witness != s2.Witness {
			t.Fatalf("Witness mismatch: %v != %v", s.Witness, s2.Witness)
		}
		if s.Epoch != s2.Epoch {
			t.Fatalf("Epoch mismatch: %d != %d", s.Epoch, s2.Epoch)
		}
		// Verify membership addresses survived.
		if len(s.Membership.Addresses) != len(s2.Membership.Addresses) {
			t.Fatalf("Membership.Addresses length mismatch")
		}
		for k, v := range s.Membership.Addresses {
			if s2.Membership.Addresses[k] != v {
				t.Fatalf("Membership.Addresses[%d] mismatch: %q != %q", k, v, s2.Membership.Addresses[k])
			}
		}
	})
}

// FuzzEntryUnmarshalCrash feeds random bytes to Entry.UnmarshalFrom.
// The function must not panic regardless of input -- errors are acceptable.
func FuzzEntryUnmarshalCrash(f *testing.F) {
	f.Add([]byte{})
	f.Add([]byte{0})
	f.Add([]byte{0, 0, 0, 0})
	f.Add([]byte{0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF})
	f.Add(make([]byte, entryHeaderSize))
	f.Add(make([]byte, entryHeaderSize+10))
	// Entry with max cmd length field to trigger size validation.
	maxCmdLenBuf := make([]byte, entryHeaderSize)
	putUint32(maxCmdLenBuf[56:], math.MaxUint32)
	f.Add(maxCmdLenBuf)

	f.Fuzz(func(t *testing.T, data []byte) {
		var e Entry
		_, _ = e.UnmarshalFrom(data)
	})
}

// FuzzMessageUnmarshalCrash feeds random bytes to Message.UnmarshalFrom.
// The function must not panic regardless of input.
func FuzzMessageUnmarshalCrash(f *testing.F) {
	f.Add([]byte{})
	f.Add([]byte{0, 0, 0, 0})
	f.Add([]byte{0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF})
	f.Add(make([]byte, messageFixedSize+4))
	f.Add(make([]byte, HeartbeatMessageSize))
	// Message with huge entry count to trigger validation.
	hugeCountBuf := make([]byte, messageFixedSize+4)
	putUint32(hugeCountBuf[messageFixedSize:], math.MaxUint32)
	f.Add(hugeCountBuf)

	f.Fuzz(func(t *testing.T, data []byte) {
		var m Message
		_, _ = m.UnmarshalFrom(data)
	})
}

// FuzzMessageBatchUnmarshalCrash feeds random bytes to MessageBatch.UnmarshalFrom.
// The function must not panic regardless of input.
func FuzzMessageBatchUnmarshalCrash(f *testing.F) {
	f.Add([]byte{})
	f.Add([]byte{0, 0, 0, 0})
	f.Add([]byte{0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF})
	f.Add(make([]byte, messageBatchFixedSize+8))
	// Batch with future wire version to trigger version validation.
	futureVerBuf := make([]byte, messageBatchFixedSize+8)
	putUint64(futureVerBuf[0:], WireVersion+100)
	f.Add(futureVerBuf)

	f.Fuzz(func(t *testing.T, data []byte) {
		var mb MessageBatch
		_, _ = mb.UnmarshalFrom(data)
	})
}

// FuzzSnapshotUnmarshalCrash feeds random bytes to Snapshot.UnmarshalFrom.
// The function must not panic regardless of input.
func FuzzSnapshotUnmarshalCrash(f *testing.F) {
	f.Add([]byte{})
	f.Add([]byte{0, 0, 0, 0})
	f.Add([]byte{0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF})
	f.Add(make([]byte, 128))
	f.Add(make([]byte, 256))

	f.Fuzz(func(t *testing.T, data []byte) {
		var s Snapshot
		_, _ = s.UnmarshalFrom(data)
	})
}

// FuzzConfigChangeRoundTrip verifies that ConfigChange marshal/unmarshal
// round-trips preserve all fields.
func FuzzConfigChangeRoundTrip(f *testing.F) {
	f.Add(uint64(1), uint64(AddNode), uint64(100), "127.0.0.1:9000", true)
	f.Add(uint64(2), uint64(RemoveNode), uint64(200), "", false)
	f.Add(uint64(3), uint64(AddNonVoting), uint64(300), "observer:5000", true)
	f.Add(uint64(4), uint64(AddWitness), uint64(400), "witness:6000", false)
	f.Add(uint64(0), uint64(0), uint64(0), "", false)
	f.Add(uint64(math.MaxUint64), uint64(AddWitness), uint64(math.MaxUint64), "x", true)

	f.Fuzz(func(t *testing.T, ccID, ccType, replicaID uint64, addr string, initialize bool) {
		// Clamp config change type to valid range.
		if ccType > maxConfigChangeType {
			ccType = ccType % (maxConfigChangeType + 1)
		}
		// Clamp address length.
		if len(addr) > MaxAddressLength {
			addr = addr[:MaxAddressLength]
		}

		cc := ConfigChange{
			ConfigChangeID: ccID,
			Type:           ccType,
			ReplicaID:      replicaID,
			Address:        addr,
			Initialize:     initialize,
		}

		buf := make([]byte, cc.Size())
		n, err := cc.MarshalTo(buf)
		if err != nil {
			t.Fatalf("MarshalTo failed: %v", err)
		}
		if n != cc.Size() {
			t.Fatalf("MarshalTo wrote %d bytes, expected %d", n, cc.Size())
		}

		var cc2 ConfigChange
		n2, err := cc2.UnmarshalFrom(buf)
		if err != nil {
			t.Fatalf("round-trip UnmarshalFrom failed: %v", err)
		}
		if n2 != n {
			t.Fatalf("UnmarshalFrom consumed %d bytes, MarshalTo wrote %d", n2, n)
		}

		if cc.ConfigChangeID != cc2.ConfigChangeID {
			t.Fatalf("ConfigChangeID mismatch: %d != %d", cc.ConfigChangeID, cc2.ConfigChangeID)
		}
		if cc.Type != cc2.Type {
			t.Fatalf("Type mismatch: %d != %d", cc.Type, cc2.Type)
		}
		if cc.ReplicaID != cc2.ReplicaID {
			t.Fatalf("ReplicaID mismatch: %d != %d", cc.ReplicaID, cc2.ReplicaID)
		}
		if cc.Address != cc2.Address {
			t.Fatalf("Address mismatch: %q != %q", cc.Address, cc2.Address)
		}
		if cc.Initialize != cc2.Initialize {
			t.Fatalf("Initialize mismatch: %v != %v", cc.Initialize, cc2.Initialize)
		}
	})
}

// FuzzMembershipRoundTrip verifies that Membership marshal/unmarshal
// round-trips preserve the config change ID and all map entries.
func FuzzMembershipRoundTrip(f *testing.F) {
	f.Add(uint64(1), uint64(10), "addr-10", uint64(20), "obs-20",
		uint64(30), "wit-30", uint64(40))
	f.Add(uint64(0), uint64(0), "", uint64(0), "",
		uint64(0), "", uint64(0))
	f.Add(uint64(math.MaxUint64), uint64(1), "a", uint64(2), "b",
		uint64(3), "c", uint64(4))
	f.Add(uint64(999), uint64(100), "node-a:5000", uint64(200), "node-b:5001",
		uint64(300), "node-c:5002", uint64(400))

	f.Fuzz(func(t *testing.T, ccID uint64,
		addrKey uint64, addrVal string,
		obsKey uint64, obsVal string,
		witKey uint64, witVal string,
		removedKey uint64) {

		// Clamp address values to valid length.
		if len(addrVal) > MaxAddressLength {
			addrVal = addrVal[:MaxAddressLength]
		}
		if len(obsVal) > MaxAddressLength {
			obsVal = obsVal[:MaxAddressLength]
		}
		if len(witVal) > MaxAddressLength {
			witVal = witVal[:MaxAddressLength]
		}

		m := Membership{
			ConfigChangeID: ccID,
			Addresses:      map[uint64]string{addrKey: addrVal},
			Observers:      map[uint64]string{obsKey: obsVal},
			Witnesses:      map[uint64]string{witKey: witVal},
			Removed:        map[uint64]bool{removedKey: true},
		}

		buf := make([]byte, m.Size())
		n, err := m.MarshalTo(buf)
		if err != nil {
			t.Fatalf("MarshalTo failed: %v", err)
		}
		if n != m.Size() {
			t.Fatalf("MarshalTo wrote %d bytes, expected %d", n, m.Size())
		}

		var m2 Membership
		n2, err := m2.UnmarshalFrom(buf)
		if err != nil {
			t.Fatalf("round-trip UnmarshalFrom failed: %v", err)
		}
		if n2 != n {
			t.Fatalf("UnmarshalFrom consumed %d bytes, MarshalTo wrote %d", n2, n)
		}

		if m.ConfigChangeID != m2.ConfigChangeID {
			t.Fatalf("ConfigChangeID mismatch: %d != %d", m.ConfigChangeID, m2.ConfigChangeID)
		}
		if len(m.Addresses) != len(m2.Addresses) {
			t.Fatalf("Addresses length mismatch: %d != %d", len(m.Addresses), len(m2.Addresses))
		}
		for k, v := range m.Addresses {
			if m2.Addresses[k] != v {
				t.Fatalf("Addresses[%d] mismatch: %q != %q", k, v, m2.Addresses[k])
			}
		}
		if len(m.Observers) != len(m2.Observers) {
			t.Fatalf("Observers length mismatch: %d != %d", len(m.Observers), len(m2.Observers))
		}
		for k, v := range m.Observers {
			if m2.Observers[k] != v {
				t.Fatalf("Observers[%d] mismatch: %q != %q", k, v, m2.Observers[k])
			}
		}
		if len(m.Witnesses) != len(m2.Witnesses) {
			t.Fatalf("Witnesses length mismatch: %d != %d", len(m.Witnesses), len(m2.Witnesses))
		}
		for k, v := range m.Witnesses {
			if m2.Witnesses[k] != v {
				t.Fatalf("Witnesses[%d] mismatch: %q != %q", k, v, m2.Witnesses[k])
			}
		}
		if len(m.Removed) != len(m2.Removed) {
			t.Fatalf("Removed length mismatch: %d != %d", len(m.Removed), len(m2.Removed))
		}
		for k, v := range m.Removed {
			if m2.Removed[k] != v {
				t.Fatalf("Removed[%d] mismatch: %v != %v", k, v, m2.Removed[k])
			}
		}
	})
}

// FuzzStateRoundTrip verifies that State marshal/unmarshal round-trips
// preserve all fields.
func FuzzStateRoundTrip(f *testing.F) {
	f.Add(uint64(1), uint64(2), uint64(100))
	f.Add(uint64(0), uint64(0), uint64(0))
	f.Add(uint64(math.MaxUint64), uint64(math.MaxUint64), uint64(math.MaxUint64))
	f.Add(uint64(1000), uint64(0), uint64(500))
	f.Add(uint64(42), uint64(7), uint64(0))

	f.Fuzz(func(t *testing.T, term, vote, commit uint64) {
		s := State{
			Term:   term,
			Vote:   vote,
			Commit: commit,
		}

		buf := make([]byte, s.Size())
		n, err := s.MarshalTo(buf)
		if err != nil {
			t.Fatalf("MarshalTo failed: %v", err)
		}
		if n != s.Size() {
			t.Fatalf("MarshalTo wrote %d bytes, expected %d", n, s.Size())
		}

		var s2 State
		n2, err := s2.UnmarshalFrom(buf)
		if err != nil {
			t.Fatalf("round-trip UnmarshalFrom failed: %v", err)
		}
		if n2 != n {
			t.Fatalf("UnmarshalFrom consumed %d bytes, MarshalTo wrote %d", n2, n)
		}

		if s.Term != s2.Term {
			t.Fatalf("Term mismatch: %d != %d", s.Term, s2.Term)
		}
		if s.Vote != s2.Vote {
			t.Fatalf("Vote mismatch: %d != %d", s.Vote, s2.Vote)
		}
		if s.Commit != s2.Commit {
			t.Fatalf("Commit mismatch: %d != %d", s.Commit, s2.Commit)
		}
	})
}
