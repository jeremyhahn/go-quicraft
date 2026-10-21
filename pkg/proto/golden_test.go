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

import (
	"bytes"
	"reflect"
	"testing"

	"github.com/jeremyhahn/go-quicraft/pkg/internal/golden"
)

// goldenCodec is the encoding surface every pinned proto layout implements.
type goldenCodec interface {
	Size() int
	MarshalTo(buf []byte) (int, error)
	UnmarshalFrom(buf []byte) (int, error)
}

// goldenFullMembership is a membership with every map and the session policy
// populated, so a decoder that dropped any of them fails the field check.
func goldenFullMembership() Membership {
	m := goldenMembershipFixture()
	m.Identities = map[uint64]string{1: "node-a", 2: "node-b"}
	m.RemovedIdentities = map[uint64]string{5: "node-e"}
	m.SetSessionPolicy(goldenMembershipSessionPolicy())
	m.WireVersion = WireVersion
	return m
}

func goldenEntries() []Entry {
	return []Entry{
		{
			Term: 7, Index: 41, Type: EntryNormal,
			Key: 0x1111, ClientID: 0x2222, SeriesID: 0x3333, RespondedTo: 0x3332, OwnerID: 0x4444,
			Cmd: []byte("golden command"),
		},
		{
			Term: 7, Index: 42, Type: EntryConfigChange,
			Key: 0x5555, Cmd: []byte{0xde, 0xad, 0xbe, 0xef},
		},
	}
}

func goldenSnapshot() Snapshot {
	return Snapshot{
		ShardID: 3, ReplicaID: 2, Index: 40, Term: 6,
		Membership:  goldenFullMembership(),
		Filepath:    "/var/lib/quicraft/snapshot-0000000000000028",
		FileSize:    4096,
		OnDiskIndex: 39,
		Epoch:       9,
		Compression: 1,
	}
}

// goldenProtoCases is every pinned proto layout with its fixed input. The
// inputs are fixed values only: no clock, no randomness, no map whose
// iteration order the encoder could leak.
func goldenProtoCases() []struct {
	name  string
	in    goldenCodec
	fresh func() goldenCodec
} {
	e := goldenEntries()[0]
	cc := ConfigChange{
		ConfigChangeID: 0x0a0b0c0d, Type: AddNode, ReplicaID: 4, Address: "d:4",
		Initialize: true, MaxWitnesses: 1, RequireFullReplicaMajority: true,
		NodeIdentity: "node-d", WireVersion: WireVersion,
	}
	m := goldenFullMembership()
	s := goldenSnapshot()
	st := State{Term: 7, Vote: 2, Commit: 40, VoteWindow: 1500000000}
	sf := SnapshotFile{Filepath: "external/blob-1", FileSize: 512, FileID: 1, Metadata: []byte("meta")}
	msg := Message{
		Type: Replicate, From: 1, To: 2, ShardID: 3, ReplicaID: 2,
		Term: 7, LogTerm: 6, LogIndex: 40, Commit: 40, Reject: false, Hint: 0x99,
		Entries: goldenEntries(),
	}
	install := Message{
		Type: InstallSnapshot, From: 1, To: 2, ShardID: 3, ReplicaID: 2,
		Term: 7, Snapshot: goldenSnapshot(),
	}
	mb := MessageBatch{
		BinVer: WireVersion, MaxBinVer: WireVersion, DeploymentID: 0xfeedface, SourceEpoch: 12,
		SourceAddress: "a:1", Requests: []Message{msg, install},
	}
	chunk := SnapshotChunk{
		ShardID: 3, ReplicaID: 2, From: 1, Index: 40, Term: 6,
		ChunkID: 1, ChunkCount: 3, ChunkSize: 8, FileSize: 4096,
		Filepath: "snapshot-0000000000000028/snapshot.data", FileChunkID: 1, FileChunkCount: 3,
		HasFileInfo: true, FileInfo: sf, Membership: goldenFullMembership(),
		OnDiskIndex: 39, DeploymentID: 0xfeedface, BinVer: WireVersion, Epoch: 9,
		Data: []byte("chunk-01"),
	}
	return []struct {
		name  string
		in    goldenCodec
		fresh func() goldenCodec
	}{
		{"entry.bin", &e, func() goldenCodec { return &Entry{} }},
		{"configchange.bin", &cc, func() goldenCodec { return &ConfigChange{} }},
		{"membership.bin", &m, func() goldenCodec { return &Membership{} }},
		{"snapshot.bin", &s, func() goldenCodec { return &Snapshot{} }},
		{"state.bin", &st, func() goldenCodec { return &State{} }},
		{"snapshotfile.bin", &sf, func() goldenCodec { return &SnapshotFile{} }},
		{"message.bin", &msg, func() goldenCodec { return &Message{} }},
		{"messagebatch.bin", &mb, func() goldenCodec { return &MessageBatch{} }},
		{"snapshotchunk.bin", &chunk, func() goldenCodec { return &SnapshotChunk{} }},
	}
}

// TestGoldenFixturesEncode proves the encoders still write the pinned bytes:
// encoding each fixed input reproduces its fixture byte for byte.
func TestGoldenFixturesEncode(t *testing.T) {
	for _, tc := range goldenProtoCases() {
		t.Run(tc.name, func(t *testing.T) {
			buf := make([]byte, tc.in.Size())
			n, err := tc.in.MarshalTo(buf)
			if err != nil {
				t.Fatalf("MarshalTo: %v", err)
			}
			golden.Check(t, tc.name, buf[:n])
		})
	}
}

// TestGoldenFixturesDecode proves the decoders still read what this version
// wrote: each fixture decodes to its input field for field, consumes every
// byte, and encodes back to itself.
func TestGoldenFixturesDecode(t *testing.T) {
	for _, tc := range goldenProtoCases() {
		t.Run(tc.name, func(t *testing.T) {
			raw := golden.Load(t, tc.name)
			got := tc.fresh()
			n, err := got.UnmarshalFrom(raw)
			if err != nil {
				t.Fatalf("UnmarshalFrom: %v", err)
			}
			if n != len(raw) {
				t.Fatalf("UnmarshalFrom consumed %d of %d bytes", n, len(raw))
			}
			if !reflect.DeepEqual(got, tc.in) {
				t.Fatalf("decoded fixture differs from its input:\n got  %+v\n want %+v", got, tc.in)
			}
			buf := make([]byte, got.Size())
			n, err = got.MarshalTo(buf)
			if err != nil {
				t.Fatalf("re-encode: %v", err)
			}
			if !bytes.Equal(buf[:n], raw) {
				t.Fatalf("re-encoding the decoded fixture changed its bytes:\n got  %x\n want %x", buf[:n], raw)
			}
		})
	}
}
