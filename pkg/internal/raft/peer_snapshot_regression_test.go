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
	"testing"

	"github.com/jeremyhahn/go-quicraft/pkg/config"
	"github.com/jeremyhahn/go-quicraft/pkg/proto"
)

// installSnapshotMsg builds an InstallSnapshot message from leader 2 to
// replica 1 carrying the given snapshot at the snapshot's term.
func installSnapshotMsg(ss proto.Snapshot) proto.Message {
	return proto.Message{
		Type:     proto.InstallSnapshot,
		From:     2,
		To:       1,
		ShardID:  1,
		Term:     ss.Term,
		Snapshot: ss,
	}
}

// TestPeer_InstallSnapshot_PendingSnapshotIsNeverSuperseded verifies that a
// second InstallSnapshot processed in the SAME step-worker batch as a restore
// never leaves the log holding an obsolete pending snapshot.
//
// The step worker drains the whole inbox through Peer.Handle before it calls
// GetUpdate once, so a restore (snapshot A), the leader's follow-up entries,
// and a newer snapshot (snapshot B) can all be stepped before the engine ever
// sees snapshot A. Snapshot B then matches the log at its index, and the
// matching-log branch fast-forwards committed AND processed past snapshot A —
// while inmem still holds snapshot A, because the pending snapshot is cleared
// only by the engine's persistence ack (commitUpdate -> savedSnapshot).
//
// GetUpdate would then ship the superseded snapshot A: LogReader.ApplySnapshot
// rewinds the tracked range behind the apply frontier (or rejects it outright
// with ErrSnapshotOutOfDate, which fails the shard in Peer.Commit), and the
// entries in (A.Index, B.Index] are never emitted for apply because processed
// already moved past them. (F8)
func TestPeer_InstallSnapshot_PendingSnapshotIsNeverSuperseded(t *testing.T) {
	cfg := testConfig()
	cfg.PreVote = config.BoolPtr(false)
	p := newTestPeer(t, cfg, proto.State{}, threeNodeMembership())
	p.r.becomeFollower(9, 2)

	// A diverged local log: entries 1..12 from an older term.
	p.r.log.inmem.merge(makeEntries(1, 13, 2))

	// 1. Snapshot A at index 5 conflicts with the local log (term 9 vs
	// term 2), so the log is fully replaced and A becomes pending.
	snapA := proto.Snapshot{
		Index: 5,
		Term:  9,
		Membership: proto.Membership{
			ConfigChangeID: 3,
			Addresses:      map[uint64]string{1: "addr1", 2: "addr2", 3: "addr3"},
		},
	}
	if err := p.Handle(installSnapshotMsg(snapA)); err != nil {
		t.Fatalf("InstallSnapshot A: %v", err)
	}
	if ss := p.r.log.pendingSnapshot(); ss == nil || ss.Index != snapA.Index {
		t.Fatalf("precondition: snapshot %d must be pending after the restore, got %+v",
			snapA.Index, ss)
	}

	// 2. The leader's follow-up entries 6..12 land in the same batch and
	// extend the restored log.
	if err := p.Handle(proto.Message{
		Type:     proto.Replicate,
		From:     2,
		To:       1,
		ShardID:  1,
		Term:     9,
		LogIndex: snapA.Index,
		LogTerm:  snapA.Term,
		Commit:   snapA.Index,
		Entries:  makeEntries(6, 13, 9),
	}); err != nil {
		t.Fatalf("Replicate: %v", err)
	}

	// 3. Snapshot B at index 10 now MATCHES the log (index 10 is at term 9),
	// still before the engine has been handed snapshot A.
	snapB := proto.Snapshot{
		Index: 10,
		Term:  9,
		Membership: proto.Membership{
			ConfigChangeID: 8,
			Addresses:      map[uint64]string{1: "addr1", 2: "addr2", 3: "addr3"},
		},
	}
	if err := p.Handle(installSnapshotMsg(snapB)); err != nil {
		t.Fatalf("InstallSnapshot B: %v", err)
	}

	if ss := p.r.log.pendingSnapshot(); ss == nil || ss.Index != snapB.Index {
		t.Fatalf("pending snapshot must advance to the newest snapshot %d, got %+v",
			snapB.Index, ss)
	}

	update, err := p.GetUpdate(true, 0)
	if err != nil {
		t.Fatalf("GetUpdate: %v", err)
	}
	if update.Snapshot.Index != snapB.Index {
		t.Fatalf("GetUpdate emitted snapshot %d, want the newest snapshot %d "+
			"(processed=%d): a superseded snapshot rewinds the LogReader and "+
			"fails the shard", update.Snapshot.Index, snapB.Index, p.r.log.processed)
	}
	// The emitted snapshot must cover everything the log already counts as
	// applied; anything below it would leave a permanent apply hole.
	if update.Snapshot.Index < p.r.log.processed {
		t.Fatalf("emitted snapshot %d is behind the processed marker %d",
			update.Snapshot.Index, p.r.log.processed)
	}
}

// TestPeer_InstallSnapshot_RejectedSnapshotKeepsMembership verifies that an
// InstallSnapshot rejected by handleSnapshot (its index is at or below the
// committed index) never overwrites the local membership, even when an
// unrelated snapshot is still pending persistence.
//
// Peer.Handle adopts the snapshot's membership unconditionally when the log
// was restored, because a restore fast-forwards the processed marker past the
// config changes the snapshot subsumes. Deriving "restored" from a merely
// non-nil pending snapshot makes a REJECTED retransmission of an older
// snapshot inherit the newer snapshot's restore and roll the membership (and
// the raft remotes derived from it) backwards. (F8)
func TestPeer_InstallSnapshot_RejectedSnapshotKeepsMembership(t *testing.T) {
	cfg := testConfig()
	cfg.PreVote = config.BoolPtr(false)
	p := newTestPeer(t, cfg, proto.State{}, threeNodeMembership())
	p.r.becomeFollower(9, 2)

	// A diverged local log: entries 1..12 from an older term.
	p.r.log.inmem.merge(makeEntries(1, 13, 2))

	// The current snapshot at index 10 conflicts with the local log and
	// restores it, adding replica 4 to the membership. It stays pending
	// until the engine acknowledges its persistence.
	current := proto.Snapshot{
		Index: 10,
		Term:  9,
		Membership: proto.Membership{
			ConfigChangeID: 8,
			Addresses: map[uint64]string{
				1: "addr1", 2: "addr2", 3: "addr3", 4: "addr4",
			},
		},
	}
	if err := p.Handle(installSnapshotMsg(current)); err != nil {
		t.Fatalf("InstallSnapshot current: %v", err)
	}
	if p.r.log.pendingSnapshot() == nil {
		t.Fatal("precondition: the restored snapshot must still be pending")
	}

	// A retransmission of the leader's PREVIOUS snapshot arrives in the same
	// batch. It is at or below the committed index, so handleSnapshot rejects
	// it and the log keeps everything the newer snapshot installed.
	stale := proto.Snapshot{
		Index: 5,
		Term:  9,
		Membership: proto.Membership{
			ConfigChangeID: 3,
			Addresses:      map[uint64]string{1: "addr1", 2: "addr2", 3: "addr3"},
		},
	}
	if err := p.Handle(installSnapshotMsg(stale)); err != nil {
		t.Fatalf("InstallSnapshot stale: %v", err)
	}

	m := p.GetMembership()
	if _, ok := m.Addresses[4]; !ok {
		t.Fatalf("membership rolled back by a rejected snapshot: %v", m.Addresses)
	}
	if m.ConfigChangeID != current.Membership.ConfigChangeID {
		t.Fatalf("expected configChangeID %d, got %d",
			current.Membership.ConfigChangeID, m.ConfigChangeID)
	}
	if _, ok := p.r.remotes[4]; !ok {
		t.Fatal("raft remotes rolled back by a rejected snapshot")
	}
}

// TestPeer_InstallSnapshot_StaleTermDoesNotRewriteMembership is the F3
// regression.
//
// raft.step DROPS a message whose term is behind the local term and returns
// nil. Peer.Handle gated membership adoption on `err == nil` alone, so a
// stale-term InstallSnapshot — one raft refused to act on — still rewrote the
// membership: the set quorum() is computed over, replaced with no term check,
// no log proof and no committed entry behind it. The only guard left was the
// ConfigChangeID carried in the message itself, which the sender chooses.
//
// Any authenticated member of the shard could therefore hand a peer an
// arbitrary configuration — for example one in which it is the sole voter.
func TestPeer_InstallSnapshot_StaleTermDoesNotRewriteMembership(t *testing.T) {
	cfg := testConfig()
	cfg.PreVote = config.BoolPtr(false)
	p := newTestPeer(t, cfg, proto.State{}, threeNodeMembership())
	p.r.becomeFollower(9, 2)

	if len(p.membership.addresses) != 3 {
		t.Fatalf("precondition: expected 3 voters, got %d", len(p.membership.addresses))
	}

	// A deposed leader (or any shard member) at term 3 sends a snapshot
	// carrying a configuration of its choosing, with a ConfigChangeID high
	// enough to beat the strictly-newer test.
	forged := proto.Snapshot{
		Index: 50,
		Term:  3,
		Membership: proto.Membership{
			ConfigChangeID: 9999,
			Addresses:      map[uint64]string{2: "addr2"},
		},
	}
	msg := installSnapshotMsg(forged)
	msg.Term = 3 // stale: below the local term 9

	if err := p.Handle(msg); err != nil {
		t.Fatalf("Handle: %v", err)
	}

	if len(p.membership.addresses) != 3 {
		t.Fatalf("membership rewritten to %v by a STALE-TERM InstallSnapshot "+
			"that raft dropped without acting on: quorum() is now computed "+
			"over a configuration no committed entry ever established",
			p.membership.addresses)
	}
	if p.membership.configChangeID == forged.Membership.ConfigChangeID {
		t.Fatalf("adopted ConfigChangeID %d from a stale-term InstallSnapshot",
			p.membership.configChangeID)
	}
	if p.r.log.committed != 0 {
		t.Fatalf("committed advanced to %d on a dropped message", p.r.log.committed)
	}
}

// A CURRENT-term InstallSnapshot must still install its membership, so the
// stale-term guard cannot be satisfied by refusing every snapshot.
func TestPeer_InstallSnapshot_CurrentTermStillAdoptsMembership(t *testing.T) {
	cfg := testConfig()
	cfg.PreVote = config.BoolPtr(false)
	p := newTestPeer(t, cfg, proto.State{}, threeNodeMembership())
	p.r.becomeFollower(9, 2)

	ss := proto.Snapshot{
		Index: 50,
		Term:  9,
		Membership: proto.Membership{
			ConfigChangeID: 7,
			Addresses:      map[uint64]string{1: "addr1", 2: "addr2", 3: "addr3", 4: "addr4"},
		},
	}
	if err := p.Handle(installSnapshotMsg(ss)); err != nil {
		t.Fatalf("Handle: %v", err)
	}
	if len(p.membership.addresses) != 4 {
		t.Fatalf("current-term snapshot membership not adopted: %v", p.membership.addresses)
	}
}

// A LOCALLY injected InstallSnapshot carries no term (term 0 is the
// local-injection escape hatch that step exempts from every term comparison).
// It must not be mistaken for a stale-term message, or the snapshot-receive
// path — which is the only thing that can repair a joining node's incomplete
// membership — would stop adopting membership entirely.
func TestPeer_InstallSnapshot_ZeroTermIsLocalNotStale(t *testing.T) {
	cfg := testConfig()
	cfg.PreVote = config.BoolPtr(false)
	p := newTestPeer(t, cfg, proto.State{}, threeNodeMembership())
	p.r.becomeFollower(9, 2)

	ss := proto.Snapshot{
		Index: 50,
		Term:  9,
		Membership: proto.Membership{
			ConfigChangeID: 7,
			Addresses:      map[uint64]string{1: "addr1", 2: "addr2", 3: "addr3", 4: "addr4"},
		},
	}
	msg := installSnapshotMsg(ss)
	msg.Term = 0 // as built by the snapshot receive path

	if err := p.Handle(msg); err != nil {
		t.Fatalf("Handle: %v", err)
	}
	if len(p.membership.addresses) != 4 {
		t.Fatalf("locally injected snapshot membership not adopted: %v", p.membership.addresses)
	}
}
