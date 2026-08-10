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
	"errors"
	"testing"
)

// Regression tests for the WireVersion 3 bump (Membership version byte, on
// top of the v2 witness-policy fields) and the per-field over-limit
// sentinels.

// TestWireVersionIsThree pins the current wire version. The v1 and v2
// layouts have no legacy decode path, so any change here must ship with
// matching encoder/decoder changes across all consumers.
func TestWireVersionIsThree(t *testing.T) {
	if WireVersion != 3 {
		t.Fatalf("WireVersion = %d, want 3", WireVersion)
	}
	if uint64(membershipWireVersion) != WireVersion {
		t.Fatalf("membershipWireVersion = %d, want %d", membershipWireVersion, WireVersion)
	}
}

// TestMessageBatchRejectsOldWireVersion verifies that a batch stamped with
// the pre-witness-policy wire version 1 is rejected with a clear
// ErrUnsupportedVersion instead of being misparsed.
func TestMessageBatchRejectsOldWireVersion(t *testing.T) {
	mb := MessageBatch{BinVer: WireVersion, SourceAddress: "n1:9000"}
	buf := make([]byte, mb.Size())
	if _, err := mb.MarshalTo(buf); err != nil {
		t.Fatalf("MarshalTo: %v", err)
	}
	putUint64(buf[0:], 1) // legacy wire version

	var mb2 MessageBatch
	if _, err := mb2.UnmarshalFrom(buf); !errors.Is(err, ErrUnsupportedVersion) {
		t.Fatalf("UnmarshalFrom(v1 batch): got %v, want ErrUnsupportedVersion", err)
	}
}

// TestSnapshotChunkValidatesBinVer verifies that SnapshotChunk gates BinVer
// on exact equality with WireVersion, mirroring MessageBatch. The gate was
// one-sided (BinVer <= WireVersion), which admitted v1/v2 chunks whose
// embedded Membership uses a different layout, and admitted unstamped
// chunks that are indistinguishable from them.
func TestSnapshotChunkValidatesBinVer(t *testing.T) {
	newChunk := func(binVer uint64) []byte {
		sc := SnapshotChunk{
			ShardID: 1, ReplicaID: 2, Index: 3, Term: 4,
			BinVer:    binVer,
			ChunkSize: uint64(len("payload")),
			Data:      []byte("payload"),
		}
		buf := make([]byte, sc.Size())
		if _, err := sc.MarshalTo(buf); err != nil {
			t.Fatalf("MarshalTo: %v", err)
		}
		return buf
	}

	var sc SnapshotChunk
	for _, binVer := range []uint64{0, 1, WireVersion - 1, WireVersion + 1} {
		if _, err := sc.UnmarshalFrom(newChunk(binVer)); !errors.Is(err, ErrUnsupportedVersion) {
			t.Fatalf("UnmarshalFrom(BinVer=%d): got %v, want ErrUnsupportedVersion", binVer, err)
		}
	}
	if _, err := sc.UnmarshalFrom(newChunk(WireVersion)); err != nil {
		t.Fatalf("UnmarshalFrom(BinVer=%d): unexpected error %v", WireVersion, err)
	}
}

// TestConfigChangeRejectsLegacyLayout verifies that a v1 ConfigChange
// payload — which ends immediately after Initialize, with no witness-policy
// fields — is detected and rejected with ErrUnsupportedVersion rather than
// an opaque ErrBufferTooSmall.
func TestConfigChangeRejectsLegacyLayout(t *testing.T) {
	addr := "10.0.0.1:9000"
	// v1 layout: [ID:8][Type:8][ReplicaID:8][Address:4+len][Initialize:8]
	legacy := make([]byte, 24+4+len(addr)+8)
	putUint64(legacy[0:], 7)
	putUint64(legacy[8:], AddNode)
	putUint64(legacy[16:], 42)
	offset := 24
	offset += putString(legacy[offset:], addr)
	putBool(legacy[offset:], true)

	var cc ConfigChange
	if _, err := cc.UnmarshalFrom(legacy); !errors.Is(err, ErrUnsupportedVersion) {
		t.Fatalf("UnmarshalFrom(v1 layout): got %v, want ErrUnsupportedVersion", err)
	}

	// A genuinely truncated v2 payload (cut inside the policy fields) must
	// still surface ErrBufferTooSmall.
	cc2 := ConfigChange{ConfigChangeID: 7, Type: AddNode, ReplicaID: 42, Address: addr}
	buf := make([]byte, cc2.Size())
	if _, err := cc2.MarshalTo(buf); err != nil {
		t.Fatalf("MarshalTo: %v", err)
	}
	var cc3 ConfigChange
	if _, err := cc3.UnmarshalFrom(buf[:len(buf)-4]); !errors.Is(err, ErrBufferTooSmall) {
		t.Fatalf("UnmarshalFrom(truncated v2): got %v, want ErrBufferTooSmall", err)
	}
}

// marshalLegacyMembershipV1 encodes the v1 Membership layout:
// [ConfigChangeID:8][Addresses][Observers][Witnesses][Removed].
func marshalLegacyMembershipV1(ccID uint64, addrs map[uint64]string) []byte {
	buf := make([]byte, 8+mapSize(addrs)+3*4)
	putUint64(buf[0:], ccID)
	offset := 8
	offset += putMap(buf[offset:], addrs)
	offset += putMap(buf[offset:], nil)
	offset += putMap(buf[offset:], nil)
	putBoolMap(buf[offset:], nil)
	return buf
}

// marshalLegacyMembershipV2 encodes the v2 Membership layout: the v1 fields
// with MaxWitnesses and RequireFullReplicaMajority appended, and no version
// byte.
func marshalLegacyMembershipV2(ccID, maxWit uint64, policy bool, addrs map[uint64]string) []byte {
	buf := make([]byte, 24+mapSize(addrs)+3*4)
	putUint64(buf[0:], ccID)
	putUint64(buf[8:], maxWit)
	putBool(buf[16:], policy)
	offset := 24
	offset += putMap(buf[offset:], addrs)
	offset += putMap(buf[offset:], nil)
	offset += putMap(buf[offset:], nil)
	putBoolMap(buf[offset:], nil)
	return buf
}

// TestMembershipRejectsLegacyLayout verifies that pre-v3 Membership
// payloads are rejected with ErrUnsupportedVersion by the leading version
// byte.
//
// The EMPTY cases are the point of the version byte. The previous decoder
// discriminated layouts with a heuristic on bytes 16..24 ("must be a bare
// 0 or 1"), which is undecidable for an empty legacy membership: it encodes
// those bytes as [WitnessCount:4=0][RemovedCount:4=0], byte-identical to a
// membership with RequireFullReplicaMajority=false. Such a buffer used to
// parse on and surface as ErrBufferTooSmall / ErrTooManyMembershipEntries —
// or, embedded in a larger buffer, misparse into a plausible membership.
func TestMembershipRejectsLegacyLayout(t *testing.T) {
	addrs := map[uint64]string{1: "10.0.0.1:9000"}
	cases := []struct {
		name string
		buf  []byte
	}{
		{"v1 populated", marshalLegacyMembershipV1(5, addrs)},
		{"v1 empty", marshalLegacyMembershipV1(0, nil)},
		{"v2 populated", marshalLegacyMembershipV2(5, 2, true, addrs)},
		{"v2 empty", marshalLegacyMembershipV2(0, 0, false, nil)},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var m Membership
			if _, err := m.UnmarshalFrom(tc.buf); !errors.Is(err, ErrUnsupportedVersion) {
				t.Fatalf("UnmarshalFrom: got %v, want ErrUnsupportedVersion", err)
			}
		})
	}
}

// TestMembershipEmptyLegacyEmbeddedRejected covers the residual exposure the
// heuristic could not reach: an empty legacy membership embedded in a larger
// buffer, where no length or trailing-byte check applies. Both a v1 and a v2
// section are padded with enough trailing bytes to satisfy every downstream
// bounds check, so only the version byte can reject them.
func TestMembershipEmptyLegacyEmbeddedRejected(t *testing.T) {
	for _, tc := range []struct {
		name    string
		section []byte
	}{
		{"v1", marshalLegacyMembershipV1(0, nil)},
		{"v2", marshalLegacyMembershipV2(0, 0, false, nil)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			embedded := append(append([]byte{}, tc.section...), make([]byte, 256)...)
			var m Membership
			if _, err := m.UnmarshalFrom(embedded); !errors.Is(err, ErrUnsupportedVersion) {
				t.Fatalf("UnmarshalFrom(embedded %s): got %v, want ErrUnsupportedVersion", tc.name, err)
			}
		})
	}
}

// TestMembershipVersionByteOnWire pins the version byte's position and
// value, and asserts every legacy layout it must not collide with is
// rejected for every shape the encoder can produce.
func TestMembershipVersionByteOnWire(t *testing.T) {
	shapes := []Membership{
		{},
		{ConfigChangeID: 7},
		{ConfigChangeID: 7, MaxWitnesses: 3, RequireFullReplicaMajority: true},
		{
			ConfigChangeID:             9,
			MaxWitnesses:               1,
			RequireFullReplicaMajority: false,
			Addresses:                  map[uint64]string{1: "a:1", 2: "b:2"},
			Observers:                  map[uint64]string{3: "c:3"},
			Witnesses:                  map[uint64]string{4: "d:4"},
			Removed:                    map[uint64]bool{5: true},
		},
	}
	for i := range shapes {
		m := shapes[i]
		buf := make([]byte, m.Size())
		n, err := m.MarshalTo(buf)
		if err != nil {
			t.Fatalf("shape %d MarshalTo: %v", i, err)
		}
		if n != m.Size() {
			t.Fatalf("shape %d: MarshalTo wrote %d, Size() = %d", i, n, m.Size())
		}
		if buf[0] != membershipWireVersion {
			t.Fatalf("shape %d: wire byte 0 = %d, want %d", i, buf[0], membershipWireVersion)
		}
		var got Membership
		n2, err := got.UnmarshalFrom(buf)
		if err != nil {
			t.Fatalf("shape %d UnmarshalFrom: %v", i, err)
		}
		if n2 != n {
			t.Fatalf("shape %d: consumed %d, wrote %d", i, n2, n)
		}

		// Any other version byte must be rejected before a field is read.
		for _, bad := range []byte{0, 1, 2, membershipWireVersion + 1, 0xFF} {
			if bad == membershipWireVersion {
				continue
			}
			corrupt := append([]byte{}, buf...)
			corrupt[0] = bad
			var m2 Membership
			if _, err := m2.UnmarshalFrom(corrupt); !errors.Is(err, ErrUnsupportedVersion) {
				t.Fatalf("shape %d version %d: got %v, want ErrUnsupportedVersion", i, bad, err)
			}
		}
	}
}

// TestOverLimitSentinelsPerField verifies that each length-limited field
// reports its own dedicated sentinel (previously getBytes always returned
// ErrEntrySizeLimitExceeded and getString always ErrAddressTooLong, leaving
// ErrChecksumTooLong/ErrMetadataTooLong/ErrChunkDataTooLong/ErrFilepathTooLong
// unreachable).
func TestOverLimitSentinelsPerField(t *testing.T) {
	// SnapshotFile.Filepath over MaxFilepathLen.
	buf := make([]byte, 8)
	putUint32(buf, uint32(MaxFilepathLen+1))
	var sf SnapshotFile
	if _, err := sf.UnmarshalFrom(buf); !errors.Is(err, ErrFilepathTooLong) {
		t.Fatalf("SnapshotFile filepath: got %v, want ErrFilepathTooLong", err)
	}

	// SnapshotFile.Metadata over MaxMetadataLen.
	sfBuf := make([]byte, 4+16+4)
	putString(sfBuf, "")                      // empty filepath
	putUint32(sfBuf[4+16:], MaxMetadataLen+1) // oversized metadata prefix
	if _, err := sf.UnmarshalFrom(sfBuf); !errors.Is(err, ErrMetadataTooLong) {
		t.Fatalf("SnapshotFile metadata: got %v, want ErrMetadataTooLong", err)
	}

	// Snapshot.Checksum over MaxChecksumLen.
	var emptyMembership Membership
	msize := emptyMembership.Size()
	snapBuf := make([]byte, 32+msize+4+8+4)
	emptyMembership.MarshalTo(snapBuf[32:])
	offset := 32 + msize
	offset += putString(snapBuf[offset:], "") // empty filepath
	offset += 8                               // FileSize
	putUint32(snapBuf[offset:], MaxChecksumLen+1)
	var s Snapshot
	if _, err := s.UnmarshalFrom(snapBuf); !errors.Is(err, ErrChecksumTooLong) {
		t.Fatalf("Snapshot checksum: got %v, want ErrChecksumTooLong", err)
	}

	// SnapshotChunk.Data over MaxSnapshotChunkDataLen.
	sc := SnapshotChunk{BinVer: WireVersion}
	chunkBuf := make([]byte, sc.Size())
	n, err := sc.MarshalTo(chunkBuf)
	if err != nil {
		t.Fatalf("MarshalTo: %v", err)
	}
	putUint32(chunkBuf[n-4:], MaxSnapshotChunkDataLen+1) // oversized data prefix
	var sc2 SnapshotChunk
	if _, err := sc2.UnmarshalFrom(chunkBuf); !errors.Is(err, ErrChunkDataTooLong) {
		t.Fatalf("SnapshotChunk data: got %v, want ErrChunkDataTooLong", err)
	}
}
