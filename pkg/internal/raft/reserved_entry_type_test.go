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
	"context"
	"errors"
	"testing"

	"github.com/jeremyhahn/go-quicraft/pkg/proto"
)

// reservedTypeLeader returns replica 1 of a three-node shard elected leader,
// with the election's messages drained.
func reservedTypeLeader(ctx context.Context, t *testing.T) *raft {
	t.Helper()
	if err := ctx.Err(); err != nil {
		t.Fatalf("context done before the leader was built: %v", err)
	}
	r := newTestRaft(1)
	electLeader(r)
	if r.state != leader {
		t.Fatalf("replica 1 is %v, want leader", r.state)
	}
	return r
}

// reservedTypeSnapshot is the leader state a refused Propose must leave
// untouched.
type reservedTypeSnapshot struct {
	lastIndex       uint64
	msgs            int
	confChangeIndex uint64
}

// takeReservedTypeSnapshot records the leader state a refused batch must not
// move.
func takeReservedTypeSnapshot(ctx context.Context, t *testing.T, r *raft) reservedTypeSnapshot {
	t.Helper()
	if err := ctx.Err(); err != nil {
		t.Fatalf("context done before the snapshot: %v", err)
	}
	return reservedTypeSnapshot{
		lastIndex:       r.log.lastIndex(),
		msgs:            len(r.msgs),
		confChangeIndex: r.log.confChangeIndex,
	}
}

// reservedTypeConfigChange encodes an AddNode change the default policy
// admits.
func reservedTypeConfigChange(ctx context.Context, t *testing.T) []byte {
	t.Helper()
	if err := ctx.Err(); err != nil {
		t.Fatalf("context done before the change was encoded: %v", err)
	}
	return marshalConfigChange(t, proto.ConfigChange{
		Type:         proto.AddNode,
		ReplicaID:    4,
		Address:      "addr4",
		NodeIdentity: "spki:4",
	})
}

// reservedTypePropose builds a Propose for r: local (OriginLocal, From the leader) or
// follower-forwarded (OriginRemote, From replica 2, at the leader's term).
func reservedTypePropose(ctx context.Context, t *testing.T, r *raft, remote bool, entries []proto.Entry) proto.Message {
	t.Helper()
	if err := ctx.Err(); err != nil {
		t.Fatalf("context done before the Propose was built: %v", err)
	}
	msg := proto.Message{
		Type: proto.Propose, From: r.replicaID, To: r.replicaID,
		Origin: proto.OriginLocal, Entries: entries,
	}
	if remote {
		msg.From = 2
		msg.Term = r.term
		msg.Origin = proto.OriginRemote
	}
	return msg
}

// TestReservedEntryType_SingleEntryRefused pins that a leader refuses a
// Propose whose only entry is EntryInitialMembership or EntryMetadata with
// ErrReservedEntryType, whether it is local or follower-forwarded.
func TestReservedEntryType_SingleEntryRefused(t *testing.T) {
	ctx := context.Background()
	for _, remote := range []bool{false, true} {
		for _, typ := range []uint64{proto.EntryInitialMembership, proto.EntryMetadata} {
			r := reservedTypeLeader(ctx, t)
			before := takeReservedTypeSnapshot(ctx, t, r)
			err := r.step(reservedTypePropose(ctx, t, r, remote, []proto.Entry{{Type: typ, Cmd: []byte("x")}}))
			if !errors.Is(err, ErrReservedEntryType) {
				t.Fatalf("remote=%v type=%d: step returned %v, want ErrReservedEntryType", remote, typ, err)
			}
			if after := takeReservedTypeSnapshot(ctx, t, r); after != before {
				t.Fatalf("remote=%v type=%d: leader state moved %+v -> %+v on a refused Propose",
					remote, typ, before, after)
			}
		}
	}
}

// TestReservedEntryType_BatchRefusedWhole pins that a three-entry batch whose
// last entry is reserved is refused whole: no entry is appended, no message
// is queued and the config-change bound does not move, even though the batch
// carries a valid config change ahead of the reserved entry.
func TestReservedEntryType_BatchRefusedWhole(t *testing.T) {
	ctx := context.Background()
	for _, remote := range []bool{false, true} {
		for _, typ := range []uint64{proto.EntryInitialMembership, proto.EntryMetadata} {
			r := reservedTypeLeader(ctx, t)
			before := takeReservedTypeSnapshot(ctx, t, r)
			pendingBefore := r.pendingConfigChange
			err := r.step(reservedTypePropose(ctx, t, r, remote, []proto.Entry{
				{Type: proto.EntryNormal, Cmd: []byte("a")},
				{Type: proto.EntryConfigChange, Cmd: reservedTypeConfigChange(ctx, t)},
				{Type: typ, Cmd: []byte("c")},
			}))
			if !errors.Is(err, ErrReservedEntryType) {
				t.Fatalf("remote=%v type=%d: step returned %v, want ErrReservedEntryType", remote, typ, err)
			}
			if after := takeReservedTypeSnapshot(ctx, t, r); after != before {
				t.Fatalf("remote=%v type=%d: leader state moved %+v -> %+v on a refused batch",
					remote, typ, before, after)
			}
			if r.pendingConfigChange != pendingBefore {
				t.Fatalf("remote=%v type=%d: pendingConfigChange %v -> %v on a refused batch",
					remote, typ, pendingBefore, r.pendingConfigChange)
			}
		}
	}
}

// TestReservedEntryType_ProposableTypesAccepted is the positive control:
// EntryNormal, EntryConfigChange with a valid encoded change, EntryEncodedEntry
// and an EntryNormal carrying the compression flag above EntryTypeMask are
// each appended, local or follower-forwarded.
func TestReservedEntryType_ProposableTypesAccepted(t *testing.T) {
	ctx := context.Background()
	cases := []struct {
		name  string
		entry func() proto.Entry
	}{
		{"normal", func() proto.Entry { return proto.Entry{Type: proto.EntryNormal, Cmd: []byte("n")} }},
		{"config-change", func() proto.Entry {
			return proto.Entry{Type: proto.EntryConfigChange, Cmd: reservedTypeConfigChange(ctx, t)}
		}},
		{"encoded", func() proto.Entry { return proto.Entry{Type: proto.EntryEncodedEntry, Cmd: []byte("e")} }},
		{"normal-compress-flag", func() proto.Entry {
			return proto.Entry{Type: proto.EntryNormal | proto.EntryCompressFlag, Cmd: []byte("z")}
		}},
	}
	for _, remote := range []bool{false, true} {
		for _, tc := range cases {
			r := reservedTypeLeader(ctx, t)
			last := r.log.lastIndex()
			if err := r.step(reservedTypePropose(ctx, t, r, remote, []proto.Entry{tc.entry()})); err != nil {
				t.Fatalf("remote=%v %s: step returned %v, want nil", remote, tc.name, err)
			}
			if got := r.log.lastIndex(); got != last+1 {
				t.Fatalf("remote=%v %s: lastIndex %d -> %d, want %d", remote, tc.name, last, got, last+1)
			}
		}
	}
}
