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

package engine

import (
	"testing"

	"github.com/jeremyhahn/go-quicraft/pkg/config"
)

// snapshotMetaFuzzSeeds are the records the snapshot_meta tests use: a full
// current record, a zero record, and the truncated and foreign shapes the
// parser must refuse.
func snapshotMetaFuzzSeeds() [][]byte {
	full := MarshalSnapshotMeta(SnapshotMeta{
		Index:       900,
		Term:        7,
		ShardID:     12,
		ReplicaID:   3,
		CreatedAt:   1_700_000_000,
		Epoch:       5,
		Compression: config.Snappy,
		DataLen:     4096,
		DataHash:    0xfeedface,
	})
	minimal := MarshalSnapshotMeta(SnapshotMeta{Index: 1, Epoch: 3})
	return [][]byte{
		full,
		minimal,
		minimal[:len(minimal)-8],
		full[:snapshotMetaHeaderSize],
		full[:snapshotMetaHeaderSize+metaBodyCreatedAtOff+8],
		{},
		[]byte("not a snapshot meta record"),
	}
}

// FuzzParseSnapshotMeta feeds arbitrary bytes to ParseSnapshotMeta. It must
// never panic, and a record it accepts must survive MarshalSnapshotMeta and a
// second parse with every field it carried unchanged.
func FuzzParseSnapshotMeta(f *testing.F) {
	for _, seed := range snapshotMetaFuzzSeeds() {
		f.Add(seed)
	}
	f.Fuzz(func(t *testing.T, data []byte) {
		m, err := ParseSnapshotMeta(data)
		if err != nil {
			if m != (SnapshotMeta{}) {
				t.Fatalf("refused record returned fields: %+v", m)
			}
			return
		}
		if m.CreatedAt == 0 {
			// MarshalSnapshotMeta stamps the clock over a zero CreatedAt,
			// so such a record is not a fixed point of the encoder.
			return
		}
		again, err := ParseSnapshotMeta(MarshalSnapshotMeta(m))
		if err != nil {
			t.Fatalf("re-encoded record refused: %v", err)
		}
		if again.Index != m.Index || again.Term != m.Term ||
			again.ShardID != m.ShardID || again.ReplicaID != m.ReplicaID ||
			again.CreatedAt != m.CreatedAt {
			t.Fatalf("identity fields changed: %+v -> %+v", m, again)
		}
		if m.EpochRecorded && again.Epoch != m.Epoch {
			t.Fatalf("epoch changed: %d -> %d", m.Epoch, again.Epoch)
		}
		if m.CompressionRecorded && again.Compression != m.Compression {
			t.Fatalf("compression changed: %d -> %d", m.Compression, again.Compression)
		}
		if m.DataHashRecorded && (again.DataLen != m.DataLen || again.DataHash != m.DataHash) {
			t.Fatalf("integrity fields changed: %+v -> %+v", m, again)
		}
		if m.DataHashRecorded {
			want := m
			want.Version = again.Version
			if again != want {
				t.Fatalf("full record not a round-trip fixed point: %+v -> %+v", m, again)
			}
		}
	})
}

// FuzzSnapshotMetaRoundTrip encodes arbitrary field values and requires the
// parser to hand every one of them back.
func FuzzSnapshotMetaRoundTrip(f *testing.F) {
	f.Add(uint64(900), uint64(7), uint64(12), uint64(3), uint64(1_700_000_000), uint64(5), uint16(config.Snappy), uint64(4096), uint64(0xfeedface))
	f.Add(uint64(0), uint64(0), uint64(0), uint64(0), uint64(1), uint64(0), uint16(0), uint64(0), uint64(0))
	f.Add(^uint64(0), ^uint64(0), ^uint64(0), ^uint64(0), ^uint64(0), ^uint64(0), ^uint16(0), ^uint64(0), ^uint64(0))
	f.Fuzz(func(t *testing.T, index, term, shard, replica, created, epoch uint64, compression uint16, dataLen, dataHash uint64) {
		if created == 0 {
			created = 1
		}
		in := SnapshotMeta{
			Index:       index,
			Term:        term,
			ShardID:     shard,
			ReplicaID:   replica,
			CreatedAt:   created,
			Epoch:       epoch,
			Compression: config.CompressionType(compression),
			DataLen:     dataLen,
			DataHash:    dataHash,
		}
		got, err := ParseSnapshotMeta(MarshalSnapshotMeta(in))
		if err != nil {
			t.Fatalf("ParseSnapshotMeta(MarshalSnapshotMeta(%+v)): %v", in, err)
		}
		want := in
		want.Version = snapshotMetaVersion
		want.EpochRecorded = true
		want.CompressionRecorded = true
		want.DataHashRecorded = true
		if got != want {
			t.Fatalf("round trip: got %+v, want %+v", got, want)
		}
	})
}
