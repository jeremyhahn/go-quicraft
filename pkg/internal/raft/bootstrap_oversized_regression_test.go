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

package raft

import (
	"encoding/binary"
	"errors"
	"sort"
	"strings"
	"testing"

	"github.com/jeremyhahn/go-quicraft/pkg/proto"
)

// ---------------------------------------------------------------------------
// The bootstrap entry must never be DURABLE BUT UNREADABLE
//
// BuildInitialMembershipEntry marshals the membership Host.StartShard assembled
// from the caller's members map and Config.MemberIdentities, and the result is
// committed at log index 1. A limit enforced on READ only would let an
// over-length address marshal happily and the shard bootstrap, and then the
// NEXT start of that shard could not decode its own index-1 entry:
// reconstructShardMembership -> ReconstructMembership -> Membership.UnmarshalFrom
// -> getMap returns ErrAddressTooLong, and StartShard propagates it. The shard
// would be permanently unstartable, with no remedy but deleting its data
// directory. The encoder therefore enforces every limit on WRITE as well.
//
// These tests drive the durable path itself — build the entry, then decode it
// the way recovery does — rather than the codec in isolation.
// ---------------------------------------------------------------------------

// oversizedBootstrapMembership is the shape Host.StartShard produces when the
// operator hands it an over-length address: the members map copied verbatim into
// Addresses.
func oversizedBootstrapMembership() proto.Membership {
	return proto.Membership{
		Addresses: map[uint64]string{
			1: "127.0.0.1:5001",
			2: strings.Repeat("x", proto.MaxAddressLength+1),
		},
	}
}

// TestBuildInitialMembershipEntry_RefusesAnUnreadableMembership pins the fix at
// the point the durable bytes are produced.
func TestBuildInitialMembershipEntry_RefusesAnUnreadableMembership(t *testing.T) {
	_, err := BuildInitialMembershipEntry(oversizedBootstrapMembership())
	if !errors.Is(err, proto.ErrAddressTooLong) {
		t.Fatalf("BuildInitialMembershipEntry: got %v, want proto.ErrAddressTooLong.\n"+
			"Producing this entry commits it at log index 1, and the next start of "+
			"the shard cannot decode it — StartShard then aborts permanently on data "+
			"the shard itself wrote", err)
	}
}

// TestBuildInitialMembershipEntry_TheRoundTripIsWhatBreaks demonstrates the
// durable consequence directly: encode the oversized membership the way the old
// encoder did, then run the RECOVERY decode over it and show that is the half
// that fails. Without this the test above could be satisfied by an encoder that
// refused something the decoder would happily have read.
func TestBuildInitialMembershipEntry_TheRoundTripIsWhatBreaks(t *testing.T) {
	ms := oversizedBootstrapMembership()

	// The bytes a read-only-checking encoder would emit, assembled by hand
	// because the encoder refuses to produce them. Size() and the map layout
	// are the encoder's own, so this is byte-for-byte what such an encoder
	// would commit at index 1.
	entry := proto.Entry{
		Index: BootstrapMembershipIndex,
		Term:  bootstrapMembershipTerm,
		Type:  proto.EntryInitialMembership,
		Cmd:   encodeMembershipUnchecked(ms),
	}

	// This is exactly what Host.reconstructShardMembership performs on the next
	// start of the shard.
	_, err := ReconstructMembership([]proto.Entry{entry})
	if !errors.Is(err, proto.ErrAddressTooLong) {
		t.Fatalf("ReconstructMembership over the committed index-1 entry: got %v, "+
			"want proto.ErrAddressTooLong. This is the recovery decode that made the "+
			"shard unstartable, and it is the half the write-side check exists to "+
			"prevent ever reaching", err)
	}

	// And the runtime fold reaches the same verdict, so the two paths agree.
	p := newTestPeer(t, testConfig(), proto.State{}, singleNodeMembership())
	if _, err := p.applyMembershipEntry(entry); !errors.Is(err, ErrCorruptConfigEntry) {
		t.Fatalf("applyMembershipEntry over the same entry: got %v, want "+
			"ErrCorruptConfigEntry: the runtime must not fail OPEN where recovery "+
			"fails closed", err)
	}
}

// TestBuildInitialMembershipEntry_LegalMembershipStillRoundTrips is the
// boundary control: the check must refuse only what the decoder refuses. A
// membership at exactly the limits must still build and reconstruct.
func TestBuildInitialMembershipEntry_LegalMembershipStillRoundTrips(t *testing.T) {
	ms := proto.Membership{
		Addresses:  map[uint64]string{1: strings.Repeat("x", proto.MaxAddressLength)},
		Identities: map[uint64]string{1: strings.Repeat("i", proto.MaxNodeIdentityLength)},
	}
	entry, err := BuildInitialMembershipEntry(ms)
	if err != nil {
		t.Fatalf("BuildInitialMembershipEntry at exactly the limits: %v", err)
	}
	got, err := ReconstructMembership([]proto.Entry{entry})
	if err != nil {
		t.Fatalf("ReconstructMembership at exactly the limits: %v", err)
	}
	if got.Addresses[1] != ms.Addresses[1] {
		t.Fatalf("the longest legal address did not survive the durable round trip")
	}
	if got.Identities[1] != ms.Identities[1] {
		t.Fatalf("the longest legal identity did not survive the durable round trip")
	}
}

// encodeMembershipUnchecked writes the current Membership layout for a value
// whose only maps are Addresses, with the per-value length bound BYPASSED. It
// reproduces exactly what proto.Membership.MarshalTo emitted before the
// write-side limits were added, which is the only way to obtain these bytes now.
//
// Layout, spelled out because that is half the point of the test:
//
//	[Version:1][ConfigChangeID:8][MaxWitnesses:8][RequireFullReplicaMajority:8]
//	[SessionExpiryEntries:8][MaxTotalSessions:8][MaxSessionsPerClient:8]
//	[Addresses][Observers][Witnesses][Removed][Identities][RemovedIdentities]
//
// with each map encoded as [count:4] followed by [key:8][len:4][value] per entry
// in ascending key order.
//
// The version byte must be the one proto writes, not a hard-coded historical
// number: the decoder gates on EXACT equality, so a stale value here would make
// the assertion below pass for the wrong reason — ErrUnsupportedVersion instead
// of the ErrAddressTooLong this test is about.
func encodeMembershipUnchecked(ms proto.Membership) []byte {
	const membershipVersionByte = 1
	const fixedSize = 1 + 8 + 8 + 8 + 8 + 8 + 8 // 49

	keys := make([]uint64, 0, len(ms.Addresses))
	for k := range ms.Addresses {
		keys = append(keys, k)
	}
	sort.Slice(keys, func(i, j int) bool { return keys[i] < keys[j] })

	size := fixedSize + 4
	for _, v := range ms.Addresses {
		size += 8 + 4 + len(v)
	}
	size += 5 * 4 // the five remaining maps, all empty

	buf := make([]byte, size)
	buf[0] = membershipVersionByte
	binary.LittleEndian.PutUint64(buf[1:], ms.ConfigChangeID)
	offset := fixedSize
	binary.LittleEndian.PutUint32(buf[offset:], uint32(len(keys)))
	offset += 4
	for _, k := range keys {
		v := ms.Addresses[k]
		binary.LittleEndian.PutUint64(buf[offset:], k)
		offset += 8
		binary.LittleEndian.PutUint32(buf[offset:], uint32(len(v)))
		offset += 4
		copy(buf[offset:], v)
		offset += len(v)
	}
	// Observers, Witnesses, Removed, Identities, RemovedIdentities: all empty.
	return buf[:offset+5*4]
}
