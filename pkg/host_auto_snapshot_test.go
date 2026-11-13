package quicraft

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"sync"
	"testing"
	"time"

	"github.com/jeremyhahn/go-quicraft/pkg/config"
	"github.com/jeremyhahn/go-quicraft/pkg/sm"
)

const (
	autoSnapShardID   uint64 = 1
	autoSnapReplicaID uint64 = 1
	autoSnapEntries   uint64 = 20
	autoSnapOverhead  uint64 = 5
)

// autoSnapSM is a key/value state machine that counts SaveSnapshot calls and
// records the applied index each one captured. Every command is "key=value".
type autoSnapSM struct {
	mu      sync.Mutex
	applied uint64
	kv      map[string]string
	saves   []uint64
}

func (s *autoSnapSM) Update(_ context.Context, entries []sm.Entry, results []sm.Result) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.kv == nil {
		s.kv = make(map[string]string)
	}
	for i := range entries {
		var k, v string
		if _, err := fmt.Sscanf(string(entries[i].Cmd), "%s %s", &k, &v); err == nil {
			s.kv[k] = v
		}
		s.applied = entries[i].Index
		results[i] = sm.Result{Value: entries[i].Index}
	}
	return nil
}

// Lookup returns a copy of the whole key/value map.
func (s *autoSnapSM) Lookup(_ context.Context, _ interface{}) (interface{}, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make(map[string]string, len(s.kv))
	for k, v := range s.kv {
		out[k] = v
	}
	return out, nil
}

func (s *autoSnapSM) SaveSnapshot(_ context.Context, w io.Writer, _ <-chan struct{}) error {
	s.mu.Lock()
	data, err := json.Marshal(s.kv)
	s.saves = append(s.saves, s.applied)
	s.mu.Unlock()
	if err != nil {
		return err
	}
	_, err = w.Write(data)
	return err
}

func (s *autoSnapSM) RecoverFromSnapshot(_ context.Context, r io.Reader, _ <-chan struct{}) error {
	data, err := io.ReadAll(r)
	if err != nil {
		return err
	}
	kv := make(map[string]string)
	if len(data) > 0 {
		if err := json.Unmarshal(data, &kv); err != nil {
			return err
		}
	}
	s.mu.Lock()
	s.kv = kv
	s.mu.Unlock()
	return nil
}

func (s *autoSnapSM) Close(_ context.Context) error { return nil }

// saveIndices returns the applied index of every SaveSnapshot so far.
func (s *autoSnapSM) saveIndices() []uint64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]uint64(nil), s.saves...)
}

func (s *autoSnapSM) lastApplied() uint64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.applied
}

// autoSnapShardConfig is the shard config every test here uses, with the
// given SnapshotEntries and CompactionOverhead 5.
func autoSnapShardConfig(snapshotEntries uint64) config.Config {
	c := testShardConfig(autoSnapShardID, autoSnapReplicaID)
	c.SnapshotEntries = snapshotEntries
	c.CompactionOverhead = autoSnapOverhead
	return c
}

// startAutoSnapHost starts a Host with an on-disk WAL in walDir and nodeDir
// and one shard whose state machine is sm.
func startAutoSnapHost(t *testing.T, walDir, nodeDir string, s *autoSnapSM, snapshotEntries uint64) *Host {
	t.Helper()
	h, err := newDirLockTestHost(t, walDir, nodeDir, 1)
	if err != nil {
		t.Fatalf("NewHost: %v", err)
	}
	create := func(_, _ uint64) interface{} { return s }
	if err := h.StartShard(map[uint64]string{autoSnapReplicaID: "127.0.0.1:5000"}, false,
		create, autoSnapShardConfig(snapshotEntries)); err != nil {
		mustCloseHost(t, h)
		t.Fatalf("StartShard: %v", err)
	}
	waitForLeader(t, h, autoSnapShardID)
	return h
}

// proposeAutoSnap proposes the keys [from, to) one at a time.
func proposeAutoSnap(ctx context.Context, t *testing.T, h *Host, from, to int) {
	t.Helper()
	for i := from; i < to; i++ {
		cmd := []byte(fmt.Sprintf("key-%04d value-%04d", i, i))
		if _, err := h.SyncPropose(ctx, autoSnapShardID, cmd); err != nil {
			t.Fatalf("SyncPropose %d: %v", i, err)
		}
	}
}

// waitAutoSnap polls cond until it holds or the bounded wait ends.
func waitAutoSnap(ctx context.Context, t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for !cond() {
		if time.Now().After(deadline) || ctx.Err() != nil {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

// settleAutoSnap waits until at least minSaves saves have been taken and the
// latest one has completed: the node's last snapshot index has reached the
// index the save captured, and the node's in-flight snapshotting flag has
// been released (the pool clears it only after persistence, compaction and
// the completion report). It returns every save so far, none still pending.
//
// SaveSnapshot records its index before the snapshot is persisted, and while
// the flag is held the apply path skips the automatic trigger, so a test that
// drives the next threshold, or compares a save with LogRange, before this
// returns can race the save still in flight.
//
// The flag is probed by claiming it and clearing it at once. Callers settle
// only while no proposal is in flight and the applied index is below the next
// threshold, so no automatic trigger can be refused during the probe.
func settleAutoSnap(ctx context.Context, t *testing.T, h *Host, s *autoSnapSM, minSaves int) []uint64 {
	t.Helper()
	node := h.engine.GetNode(autoSnapShardID)
	if node == nil {
		t.Fatalf("shard %d has no engine node", autoSnapShardID)
	}
	deadline := time.Now().Add(5 * time.Second)
	for {
		saves := s.saveIndices()
		if len(saves) >= minSaves &&
			(len(saves) == 0 || node.LastSnapshotIndex() >= saves[len(saves)-1]) &&
			node.TryStartSnapshot() {
			node.ClearSnapshotting()
			return saves
		}
		if time.Now().After(deadline) || ctx.Err() != nil {
			t.Fatalf("timed out waiting for %d completed snapshots: saves %v, last "+
				"snapshot index %d, applied %d", minSaves, saves,
				node.LastSnapshotIndex(), s.lastApplied())
		}
		time.Sleep(5 * time.Millisecond)
	}
}

// TestAutoSnapshot_TriggersAtSnapshotEntries checks (a): fewer than
// SnapshotEntries applied entries take no snapshot; SnapshotEntries more take
// one on their own, with no RequestSnapshot call.
func TestAutoSnapshot_TriggersAtSnapshotEntries(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	s := &autoSnapSM{}
	h := startAutoSnapHost(t, t.TempDir(), t.TempDir(), s, autoSnapEntries)
	defer mustCloseHost(t, h)

	// SnapshotEntries counts applied entries, and the bootstrap and leader
	// entries are applied entries too, so drive the applied index itself to
	// one short of SnapshotEntries.
	n := 0
	for s.lastApplied() < autoSnapEntries-1 {
		proposeAutoSnap(ctx, t, h, n, n+1)
		n++
	}
	if applied := s.lastApplied(); applied != autoSnapEntries-1 {
		t.Fatalf("applied index %d after %d proposals, want %d", applied, n, autoSnapEntries-1)
	}
	time.Sleep(100 * time.Millisecond)
	if saves := s.saveIndices(); len(saves) != 0 {
		t.Fatalf("applied index %d < SnapshotEntries %d after %d proposals, yet the "+
			"shard took %d snapshots %v; want none", autoSnapEntries-1, autoSnapEntries,
			n, len(saves), saves)
	}

	// One more applied entry reaches SnapshotEntries: a snapshot follows on
	// its own, with no RequestSnapshot call.
	proposeAutoSnap(ctx, t, h, n, n+1)
	n++
	// Wait for the save to complete and release the in-flight flag before
	// driving the next threshold: while it is held the apply path skips the
	// trigger, and no later batch would retry it.
	if saves := settleAutoSnap(ctx, t, h, s, 1); saves[0] != autoSnapEntries {
		t.Fatalf("first automatic snapshot at applied index %d, want SnapshotEntries %d",
			saves[0], autoSnapEntries)
	}

	// SnapshotEntries more applied entries take the next one.
	proposeAutoSnap(ctx, t, h, n, n+int(autoSnapEntries))
	saves := settleAutoSnap(ctx, t, h, s, 2)
	t.Logf("%d proposals reached applied index %d; automatic snapshots at %v",
		n-1, autoSnapEntries-1, saves)
	// The first save was settled before these proposals, so nothing defers
	// the trigger: the second save lands exactly SnapshotEntries later.
	if len(saves) != 2 || saves[1] != 2*autoSnapEntries {
		t.Fatalf("automatic snapshots at %v, want a second one at applied index %d",
			saves, 2*autoSnapEntries)
	}
}

// TestAutoSnapshot_CadenceCompactionAndRecovery checks (b) and (c): about 100
// proposals take one snapshot per SnapshotEntries applied entries, the log
// keeps CompactionOverhead entries below the latest snapshot and no more is
// required, and a restarted Host recovers every value.
func TestAutoSnapshot_CadenceCompactionAndRecovery(t *testing.T) {
	const proposals = 100
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	walDir, nodeDir := t.TempDir(), t.TempDir()
	s := &autoSnapSM{}
	h := startAutoSnapHost(t, walDir, nodeDir, s, autoSnapEntries)

	// Propose one key at a time, and whenever an applied entry reaches the
	// next threshold, settle that save (persisted, compacted, in-flight flag
	// released) before driving the next one. A threshold reached while a save
	// is still in flight is skipped by the apply path and retried only by a
	// later batch, so without settling the cadence depends on save latency.
	node := h.engine.GetNode(autoSnapShardID)
	if node == nil {
		mustCloseHost(t, h)
		t.Fatalf("shard %d has no engine node", autoSnapShardID)
	}
	var saves []uint64
	for i := 0; i < proposals; i++ {
		proposeAutoSnap(ctx, t, h, i, i+1)
		if s.lastApplied()-node.LastSnapshotIndex() >= autoSnapEntries {
			saves = settleAutoSnap(ctx, t, h, s, len(saves)+1)
		}
	}
	// Settle every save before reading the indices: a save still in flight
	// has recorded its index but not yet compacted the log, and the latest
	// index below is compared with LogRange.
	saves = settleAutoSnap(ctx, t, h, s, len(saves))
	applied := s.lastApplied()
	want := int(applied / autoSnapEntries)
	if len(saves) != want {
		mustCloseHost(t, h)
		t.Fatalf("applied index %d with SnapshotEntries %d took %d snapshots %v, want %d",
			applied, autoSnapEntries, len(saves), saves, want)
	}
	for i := 1; i < len(saves); i++ {
		if saves[i]-saves[i-1] != autoSnapEntries {
			mustCloseHost(t, h)
			t.Fatalf("snapshots %v are %d applied entries apart, want SnapshotEntries %d",
				saves, saves[i]-saves[i-1], autoSnapEntries)
		}
	}

	latest := saves[len(saves)-1]
	compactTo := latest - autoSnapOverhead
	var lr LogRange
	waitAutoSnap(ctx, t, fmt.Sprintf("log compaction to %d", compactTo), func() bool {
		var err error
		lr, err = h.LogRange(autoSnapShardID)
		if err != nil {
			t.Fatalf("LogRange: %v", err)
		}
		return lr.FirstIndex >= compactTo
	})
	t.Logf("applied %d, snapshots %v, log range [%d, %d]", applied, saves,
		lr.FirstIndex, lr.LastIndex)
	if lr.FirstIndex > compactTo+1 {
		mustCloseHost(t, h)
		t.Fatalf("LogRange first index %d after a snapshot at %d: the log keeps fewer "+
			"than CompactionOverhead %d entries below the snapshot", lr.FirstIndex, latest,
			autoSnapOverhead)
	}
	mustCloseHost(t, h)

	r := &autoSnapSM{}
	h = startAutoSnapHost(t, walDir, nodeDir, r, autoSnapEntries)
	defer mustCloseHost(t, h)
	var got interface{}
	var err error
	for {
		got, err = h.SyncRead(ctx, autoSnapShardID, nil)
		if !errors.Is(err, ErrReadIndexNotReady) || ctx.Err() != nil {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	if err != nil {
		t.Fatalf("SyncRead after restart: %v", err)
	}
	kv, ok := got.(map[string]string)
	if !ok || len(kv) != proposals {
		t.Fatalf("restarted Host holds %d values, want %d", len(kv), proposals)
	}
	for i := 0; i < proposals; i++ {
		k, v := fmt.Sprintf("key-%04d", i), fmt.Sprintf("value-%04d", i)
		if kv[k] != v {
			t.Fatalf("restarted Host has %s = %q, want %q", k, kv[k], v)
		}
	}
}

// TestAutoSnapshot_DisabledNeverSnapshots checks (d): SnapshotsDisabled takes
// no snapshot on its own, and SyncRequestSnapshot still takes one.
func TestAutoSnapshot_DisabledNeverSnapshots(t *testing.T) {
	const proposals = 100
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	s := &autoSnapSM{}
	h := startAutoSnapHost(t, t.TempDir(), t.TempDir(), s, config.SnapshotsDisabled)
	defer mustCloseHost(t, h)

	proposeAutoSnap(ctx, t, h, 0, proposals)
	time.Sleep(200 * time.Millisecond)
	if saves := s.saveIndices(); len(saves) != 0 {
		t.Fatalf("SnapshotsDisabled shard took %d snapshots %v on its own, want none",
			len(saves), saves)
	}
	index, err := h.SyncRequestSnapshot(ctx, autoSnapShardID, SnapshotOption{})
	if err != nil {
		t.Fatalf("SyncRequestSnapshot with SnapshotsDisabled: %v", err)
	}
	if applied := s.lastApplied(); index != applied {
		t.Fatalf("SyncRequestSnapshot index %d, want the applied index %d", index, applied)
	}
	if saves := s.saveIndices(); len(saves) != 1 {
		t.Fatalf("after SyncRequestSnapshot the shard took %d snapshots %v, want 1",
			len(saves), saves)
	}
}
