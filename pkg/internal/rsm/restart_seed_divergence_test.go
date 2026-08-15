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

package rsm

import (
	"bytes"
	"context"
	"errors"
	"testing"

	"github.com/jeremyhahn/go-quicraft/pkg/proto"
	"github.com/jeremyhahn/go-quicraft/pkg/sm"
)

// A restarted replica and a peer that never restarted must apply the identical
// committed log to the identical state — and the state machine's own applied
// counter is the input that decides whether they do.
//
// rsm.StateMachine.lastApplied is the LOWER BOUND of Apply's session expiry
// checkpoint walk. RecoverFromSnapshot restores the session table (it travels
// in the snapshot stream) but not this counter, so a replica that resumes with
// it at 0 re-walks every checkpoint below the resume index against the RECOVERED
// table. That would be harmless if ExpireSessions were idempotent, and it is
// not: it deletes at most defaultMaxExpirePerCheckpoint (64) sessions per call,
// so each extra checkpoint drains another 64 sessions the peers still hold.
//
// The fixture below is sized to make that visible rather than to be small. The
// shard registers 500 clients across indices 1..500; the capped expiry leaves a
// backlog of idle-but-live sessions at the cut, and the entry right after the
// cut touches a client that sits inside that backlog. A peer that walks only the
// checkpoints above the cut still holds the client and applies the write; a
// replica that re-walks the checkpoints below it drains the backlog first,
// deletes that client before the entry is flushed, and answers
// ResultSessionExpired for a write its peers committed to the user state
// machine.

const (
	// seedFixtureClients is the number of clients registered at indices
	// 1..seedFixtureClients. It has to exceed the expiry cap by enough that
	// the capped scan leaves a backlog at the cut; at 500 the peer still
	// holds 244 idle sessions there.
	seedFixtureClients = 500
	// seedFixtureCut is the index the restarted replica resumes at.
	seedFixtureCut = seedFixtureClients
	// seedFixtureTouched is the client the entry after the cut belongs to. It
	// is inside the backlog the extra checkpoints drain.
	seedFixtureTouched = 300
	// seedFixtureExpiry is the replicated inactivity threshold, which is also
	// the checkpoint spacing.
	seedFixtureExpiry = 100
)

// seedFixturePolicy is the shard's replicated session policy for the fixture.
// MaxTotalSessions has to admit every client or the registrations themselves
// would be what differs.
func seedFixturePolicy() proto.SessionPolicy {
	return proto.SessionPolicy{
		ExpiryEntries:        seedFixtureExpiry,
		MaxTotalSessions:     4 * seedFixtureClients,
		MaxSessionsPerClient: 100,
	}
}

// seedFixtureReplica builds one replica of the fixture's shard.
func seedFixtureReplica(t *testing.T) *StateMachine {
	t.Helper()
	createFn := sm.NewCreateFunc(func(shardID, replicaID uint64) sm.StateMachine {
		return newTestKV()
	})
	s, err := NewStateMachine(1, 1, createFn, t.TempDir(), seedFixturePolicy(), nil, false)
	if err != nil {
		t.Fatalf("NewStateMachine = %v", err)
	}
	t.Cleanup(func() { _ = s.Close() })
	return s
}

// seedFixtureRegistrations is the committed prefix: client i registers at
// index i.
func seedFixtureRegistrations() []proto.Entry {
	entries := make([]proto.Entry, 0, seedFixtureClients)
	for i := uint64(1); i <= seedFixtureClients; i++ {
		entries = append(entries, proto.Entry{
			Index:    i,
			ClientID: i,
			SeriesID: proto.SessionSeriesIDForRegister,
			Type:     proto.EntryNormal,
		})
	}
	return entries
}

// seedFixtureTail is the committed suffix: a session-managed write from
// seedFixtureTouched at the first index after the cut, then filler out to the
// next checkpoint so the walk actually runs.
func seedFixtureTail() []proto.Entry {
	entries := []proto.Entry{{
		Index:    seedFixtureCut + 1,
		ClientID: seedFixtureTouched,
		SeriesID: 1,
		Cmd:      makeCmd("touched", "by-the-surviving-client"),
	}}
	for idx := uint64(seedFixtureCut + 2); idx <= seedFixtureCut+100; idx++ {
		entries = append(entries, proto.Entry{Index: idx, Type: proto.EntryNormal})
	}
	return entries
}

// registeredClients reports which of the fixture's clients a replica still
// holds, so two replicas' session tables can be compared as values.
func registeredClients(s *StateMachine) []uint64 {
	var held []uint64
	for i := uint64(1); i <= seedFixtureClients; i++ {
		if s.Sessions().IsRegistered(i) {
			held = append(held, i)
		}
	}
	return held
}

// restartAt snapshots s at the cut and returns a FRESH state machine recovered
// from that snapshot — the restart path, modelled exactly: the snapshot stream
// carries the session table, and nothing in it carries the applied index.
//
// When seed is true the caller's applied index is installed with SetLastApplied,
// which is what Host.StartShard does beside the engine node's SeedLastApplied.
func restartAt(t *testing.T, s *StateMachine, index uint64, seed bool) *StateMachine {
	t.Helper()
	var buf bytes.Buffer
	stopper := make(chan struct{})
	if err := s.SaveSnapshot(context.Background(), &buf, stopper); err != nil {
		t.Fatalf("SaveSnapshot at index %d: %v", index, err)
	}
	restarted := seedFixtureReplica(t)
	if err := restarted.RecoverFromSnapshot(context.Background(), &buf, stopper); err != nil {
		t.Fatalf("RecoverFromSnapshot: %v", err)
	}
	if got := restarted.LastApplied(); got != 0 {
		t.Fatalf("RecoverFromSnapshot set lastApplied to %d; this test's premise is "+
			"that it does NOT, so the fixture no longer models the restart it is "+
			"about", got)
	}
	if seed {
		restarted.SetLastApplied(index)
	}
	return restarted
}

// TestRestart_SeededReplicaMatchesPeerThatNeverRestarted drives two state
// machines through the identical committed log — one straight through, one
// across a restart at the cut — and requires identical session tables AND
// identical results.
func TestRestart_SeededReplicaMatchesPeerThatNeverRestarted(t *testing.T) {
	registrations, tail := seedFixtureRegistrations(), seedFixtureTail()

	peer := seedFixtureReplica(t)
	if err := peer.Apply(registrations, make([]sm.Result, len(registrations))); err != nil {
		t.Fatalf("peer.Apply(registrations) = %v", err)
	}

	restarted := seedFixtureReplica(t)
	if err := restarted.Apply(registrations, make([]sm.Result, len(registrations))); err != nil {
		t.Fatalf("restarted.Apply(registrations) = %v", err)
	}

	// Both replicas reached the cut through the same entries, so their session
	// tables must already agree — and the backlog the fixture depends on must
	// actually be there.
	if err := requireSameSessions(peer, restarted); err != nil {
		t.Fatalf("the two replicas disagree BEFORE the restart: %v", err)
	}
	if !peer.Sessions().IsRegistered(seedFixtureTouched) {
		t.Fatalf("client %d was already expired at the cut; the fixture no longer "+
			"exercises a live session across the restart", seedFixtureTouched)
	}

	resumed := restartAt(t, restarted, seedFixtureCut, true)

	peerResults := make([]sm.Result, len(tail))
	if err := peer.Apply(tail, peerResults); err != nil {
		t.Fatalf("peer.Apply(tail) = %v", err)
	}
	resumedResults := make([]sm.Result, len(tail))
	if err := resumed.Apply(tail, resumedResults); err != nil {
		t.Fatalf("resumed.Apply(tail) = %v; a replica resumed at its recovered "+
			"index must be able to apply the next committed entry", err)
	}

	if err := requireSameSessions(peer, resumed); err != nil {
		t.Fatalf("session tables diverged across the restart: %v. The restarted "+
			"replica walked a different set of expiry checkpoints, and expiry is "+
			"capped per checkpoint, so the extra rounds delete sessions the peer "+
			"still holds", err)
	}
	for i := range tail {
		if peerResults[i].Value != resumedResults[i].Value ||
			!bytes.Equal(peerResults[i].Data, resumedResults[i].Data) {
			t.Fatalf("result for committed index %d: peer %+v, restarted replica %+v. "+
				"The two replicas answered the same committed entry differently, "+
				"which means one of them applied it to the user state machine and "+
				"the other did not",
				tail[i].Index, peerResults[i], resumedResults[i])
		}
	}
	if resumedResults[0].Value == sm.ResultSessionExpired {
		t.Fatalf("both replicas rejected the write from client %d as SESSION_EXPIRED; "+
			"the fixture is meant to keep that client alive across the restart, so "+
			"it is no longer testing what it claims", seedFixtureTouched)
	}
}

// TestRestart_UnseededReplicaIsRefusedNotDiverged pins the other half. A replica
// resumed WITHOUT its applied index cannot be allowed to apply: the batch leaves
// a hole covering every index through the snapshot, and Apply must fail with
// ErrApplyGap rather than walk the checkpoints below the resume index.
//
// Reverting the contiguity guard in Apply turns this case back into silent
// divergence — the same one the test above forbids — which is exactly what the
// guard exists to convert into a loud failure.
func TestRestart_UnseededReplicaIsRefusedNotDiverged(t *testing.T) {
	registrations, tail := seedFixtureRegistrations(), seedFixtureTail()

	replica := seedFixtureReplica(t)
	if err := replica.Apply(registrations, make([]sm.Result, len(registrations))); err != nil {
		t.Fatalf("Apply(registrations) = %v", err)
	}
	unseeded := restartAt(t, replica, seedFixtureCut, false)

	err := unseeded.Apply(tail, make([]sm.Result, len(tail)))
	if !errors.Is(err, ErrApplyGap) {
		t.Fatalf("Apply on a replica resumed WITHOUT its applied index = %v, want "+
			"ErrApplyGap. Its counter reads %d while the batch starts at %d, so the "+
			"expiry checkpoint walk would run over every checkpoint in (0, %d] that "+
			"its peers already ran, draining the session backlog 64 at a time",
			err, unseeded.LastApplied(), tail[0].Index, seedFixtureCut)
	}
}

// requireSameSessions compares two replicas' session tables as values.
func requireSameSessions(a, b *StateMachine) error {
	held, other := registeredClients(a), registeredClients(b)
	if len(held) != len(other) {
		return &sessionSetMismatch{a: held, b: other}
	}
	for i := range held {
		if held[i] != other[i] {
			return &sessionSetMismatch{a: held, b: other}
		}
	}
	return nil
}

// sessionSetMismatch reports two session tables that hold different clients.
type sessionSetMismatch struct {
	a, b []uint64
}

func (e *sessionSetMismatch) Error() string {
	return "replica A holds " + summariseClients(e.a) +
		", replica B holds " + summariseClients(e.b)
}

// summariseClients renders a client set compactly: the whole set would be
// hundreds of IDs, and the count plus the endpoints is what identifies the
// difference.
func summariseClients(ids []uint64) string {
	if len(ids) == 0 {
		return "no sessions"
	}
	return itoa(len(ids)) + " sessions [" + itoa(int(ids[0])) + ".." +
		itoa(int(ids[len(ids)-1])) + "]"
}

// itoa avoids pulling fmt into an error path that only formats small integers.
func itoa(n int) string {
	if n == 0 {
		return "0"
	}
	var buf [20]byte
	i := len(buf)
	for n > 0 {
		i--
		buf[i] = byte('0' + n%10)
		n /= 10
	}
	return string(buf[i:])
}
