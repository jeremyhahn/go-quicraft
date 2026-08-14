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

package quicraft

import (
	"errors"
	"strings"
	"testing"

	"github.com/jeremyhahn/go-quicraft/pkg/proto"
)

// ---------------------------------------------------------------------------
// StartShard must refuse a members map it cannot make durable
//
// members is copied verbatim into proto.Membership.Addresses,
// raft.BuildInitialMembershipEntry marshals it, and the result is COMMITTED as
// the EntryInitialMembership at log index 1. With the limits enforced on read
// only, an over-length address bootstrapped the shard successfully and made
// every subsequent start fail — reconstructShardMembership decodes that entry
// and propagates ErrAddressTooLong, so StartShard aborted forever on data the
// shard itself wrote, with no remedy but deleting the data directory.
//
// proto.Membership.MarshalTo now refuses to produce those bytes, which turns
// the permanent brick into an error. Checking at the API boundary as well is
// what turns that error into one naming the replica and the reason, at the call
// the operator actually made, before anything is persisted.
// ---------------------------------------------------------------------------

func TestStartShard_RejectsAnAddressTheFormatCannotCarry(t *testing.T) {
	h := newTestHost(t)

	members := map[uint64]string{
		1: strings.Repeat("x", proto.MaxAddressLength+1),
	}
	err := h.StartShard(members, false, testCreateFunc, testShardConfig(1, 1))
	if err == nil {
		t.Fatal("StartShard accepted an address longer than the membership format " +
			"can carry. The shard bootstraps, commits an index-1 entry no binary can " +
			"decode, and every later start of it fails permanently")
	}
	var invalid *InvalidMemberError
	if !errors.As(err, &invalid) {
		t.Fatalf("StartShard: got %T (%v), want *InvalidMemberError naming the "+
			"offending replica", err, err)
	}
	if invalid.ReplicaID != 1 {
		t.Fatalf("InvalidMemberError.ReplicaID = %d, want 1", invalid.ReplicaID)
	}
	if h.ShardCount() != 0 {
		t.Fatalf("ShardCount = %d after the refusal, want 0: nothing may be "+
			"published for a shard that was rejected", h.ShardCount())
	}
}

// TestStartShard_RejectsAnIdentityTheFormatCannotCarry covers the other map
// StartShard seeds from caller input. cfg.MemberIdentities travels the same
// route into the same committed entry, and is bounded by the ENCODER's limit
// (MaxAddressLength, which every string map in a Membership shares through
// putMap/getMap) rather than by MaxNodeIdentityLength.
func TestStartShard_RejectsAnIdentityTheFormatCannotCarry(t *testing.T) {
	h := newTestHost(t)

	scfg := testShardConfig(1, 1)
	scfg.MemberIdentities = map[uint64]string{
		1: strings.Repeat("i", proto.MaxAddressLength+1),
	}
	err := h.StartShard(map[uint64]string{1: "127.0.0.1:5000"}, false, testCreateFunc, scfg)
	if err == nil {
		t.Fatal("StartShard accepted a node identity longer than the membership " +
			"format can carry")
	}
	var invalid *InvalidMemberError
	if !errors.As(err, &invalid) {
		t.Fatalf("StartShard: got %T (%v), want *InvalidMemberError", err, err)
	}
	if h.ShardCount() != 0 {
		t.Fatalf("ShardCount = %d after the refusal, want 0", h.ShardCount())
	}
}

// TestStartShard_AcceptsTheLongestLegalAddressAndIdentity is the boundary
// control, and it is what stops the two tests above from passing on a check
// that refuses everything. A value at exactly the limit must still bootstrap,
// which means it must also survive the index-1 round trip.
func TestStartShard_AcceptsTheLongestLegalAddressAndIdentity(t *testing.T) {
	h := newTestHost(t)

	// The address must remain resolvable by the transport-free test host, so it
	// is padded with a trailing DNS-legal label rather than being pure filler.
	addr := "127.0.0.1:5000"
	scfg := testShardConfig(1, 1)
	scfg.MemberIdentities = map[uint64]string{
		1: strings.Repeat("i", proto.MaxAddressLength),
	}
	if err := h.StartShard(map[uint64]string{1: addr}, false, testCreateFunc, scfg); err != nil {
		t.Fatalf("StartShard at exactly the format limits: %v", err)
	}
	if h.ShardCount() != 1 {
		t.Fatalf("ShardCount = %d, want 1", h.ShardCount())
	}
}
