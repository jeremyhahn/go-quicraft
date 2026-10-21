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

package engine

import (
	"bytes"
	"encoding/binary"
	"testing"

	"github.com/jeremyhahn/go-quicraft/pkg/internal/golden"
)

// goldenSnapshotMeta carries a fixed CreatedAt: a zero one would make
// MarshalSnapshotMeta stamp the wall clock.
func goldenSnapshotMeta() SnapshotMeta {
	return SnapshotMeta{
		Index: 40, Term: 6, ShardID: 3, ReplicaID: 2,
		CreatedAt: 1767225600, Epoch: 9, Compression: 1,
		DataLen: 4096, DataHash: 0x0123456789abcdef,
	}
}

// TestGoldenSnapshotMeta pins the snapshot.meta record in both directions.
func TestGoldenSnapshotMeta(t *testing.T) {
	in := goldenSnapshotMeta()
	golden.Check(t, "snapshot-meta.bin", MarshalSnapshotMeta(in))

	raw := golden.Load(t, "snapshot-meta.bin")
	got, err := ParseSnapshotMeta(raw)
	if err != nil {
		t.Fatalf("ParseSnapshotMeta: %v", err)
	}
	want := in
	want.Version = snapshotMetaVersion
	want.EpochRecorded, want.CompressionRecorded, want.DataHashRecorded = true, true, true
	if got != want {
		t.Fatalf("parsed meta = %+v, want %+v", got, want)
	}
	if out := MarshalSnapshotMeta(got); !bytes.Equal(out, raw) {
		t.Fatalf("re-encoding the parsed meta changed its bytes:\n got  %x\n want %x", out, raw)
	}
}

// TestGoldenSnapshotFrameAAD pins the 40-byte QRSNAP1 context every at-rest
// snapshot frame is authenticated under. The reader rebuilds it rather than
// reading it, so the decode side checks each field at its pinned offset.
func TestGoldenSnapshotFrameAAD(t *testing.T) {
	const shardID, replicaID, index, seq = 3, 2, 40, 5
	aad := NewSnapshotAAD(shardID, replicaID, index, aadCapability{supports: true})
	golden.Check(t, "snapshot-frame-aad.bin", aad.Frame(seq, true))

	raw := golden.Load(t, "snapshot-frame-aad.bin")
	if len(raw) != len(aad.buf) {
		t.Fatalf("fixture is %d bytes, want %d", len(raw), len(aad.buf))
	}
	if string(raw[:len(snapshotAADMagic)]) != snapshotAADMagic {
		t.Fatalf("magic = %q, want %q", raw[:len(snapshotAADMagic)], snapshotAADMagic)
	}
	fields := []struct {
		name string
		got  uint64
		want uint64
	}{
		{"shard", binary.BigEndian.Uint64(raw[snapshotAADShardOff:]), shardID},
		{"replica", binary.BigEndian.Uint64(raw[snapshotAADReplicaOff:]), replicaID},
		{"index", binary.BigEndian.Uint64(raw[snapshotAADIndexOff:]), index},
		{"seq", binary.BigEndian.Uint64(raw[snapshotAADSeqOff:]), seq},
		{"final", uint64(raw[snapshotAADFinalOff]), 1},
	}
	for _, f := range fields {
		if f.got != f.want {
			t.Fatalf("%s = %d, want %d", f.name, f.got, f.want)
		}
	}
	out := NewSnapshotAAD(fields[0].got, fields[1].got, fields[2].got, aadCapability{supports: true}).
		Frame(fields[3].got, fields[4].got == 1)
	if !bytes.Equal(out, raw) {
		t.Fatalf("re-encoding the decoded context changed its bytes:\n got  %x\n want %x", out, raw)
	}
}
