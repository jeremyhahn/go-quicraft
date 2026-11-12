package quicraft

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/jeremyhahn/go-quicraft/pkg/seal"
	"github.com/jeremyhahn/go-quicraft/pkg/sm/kv"
)

const (
	keyRotationLiveShardID   uint64 = 1
	keyRotationLiveReplicaID uint64 = 1
	// keyRotationLiveAdvances is how many scheduled rotations the test waits
	// for beyond the epoch the barrier starts in.
	keyRotationLiveAdvances = 3
)

// liveSealedStore is the StatePersister a real embedder supplies: it keeps the
// latest persisted SealedRootKey so a restarted process can load it.
type liveSealedStore struct {
	mu     sync.Mutex
	latest *seal.SealedRootKey
	writes int
}

func (s *liveSealedStore) persist(k *seal.SealedRootKey) error {
	if k == nil {
		return errors.New("nil sealed root key")
	}
	cp := *k
	cp.Salt = slices.Clone(k.Salt)
	cp.Ciphertext = slices.Clone(k.Ciphertext)
	cp.StateMAC = slices.Clone(k.StateMAC)
	cp.Metadata = slices.Clone(k.Metadata)
	cp.KnownEpochs = slices.Clone(k.KnownEpochs)
	cp.Nonce = slices.Clone(k.Nonce)
	cp.HardwarePayload = slices.Clone(k.HardwarePayload)
	s.mu.Lock()
	s.latest = &cp
	s.writes++
	s.mu.Unlock()
	return nil
}

func (s *liveSealedStore) load() *seal.SealedRootKey {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.latest
}

// newLiveRotationBarrier builds a real software-strategy barrier wired to
// store. With prior == nil it initialises a new root key; otherwise it loads
// prior, as a restarted process loads its persisted barrier state. Either way
// the barrier is returned unsealed.
func newLiveRotationBarrier(ctx context.Context, t *testing.T, store *liveSealedStore, prior *seal.SealedRootKey) *seal.Barrier {
	t.Helper()
	_ = seal.AllowWeakKDFParams()
	cfg := seal.DefaultBarrierConfig(1)
	cfg.StatePersister = store.persist
	b := seal.MustNewBarrier(cfg)
	strategy, err := newTestSealStrategy([]byte("key-rotation-live"))
	if err != nil {
		t.Fatalf("newTestSealStrategy: %v", err)
	}
	if prior == nil {
		if err := b.Initialize(ctx, strategy, seal.Credentials{}); err != nil {
			t.Fatalf("barrier Initialize: %v", err)
		}
	} else {
		if err := b.SetSealedRootKey(prior); err != nil {
			t.Fatalf("barrier SetSealedRootKey: %v", err)
		}
		if err := b.Unseal(ctx, strategy, seal.Credentials{}); err != nil {
			t.Fatalf("barrier Unseal: %v", err)
		}
	}
	if b.IsSealed() || b.IsPlaintextMode() {
		t.Fatal("fixture: the barrier must be unsealed with a real root key")
	}
	return b
}

// startLiveRotationHost opens a Host on walDir/nodeDir with barrier b and the
// rotation schedule under test, and starts the shard on a fresh KV store.
func startLiveRotationHost(t *testing.T, walDir, nodeDir string, b *seal.Barrier) *Host {
	t.Helper()
	cfg := testHostConfig(t)
	cfg.WALDir, cfg.NodeHostDir = walDir, nodeDir
	cfg.KeyRotationInterval = time.Second
	cfg.MaxRetainedEpochs = 2
	h, err := NewHost(cfg, WithoutTransport(), WithBarrier(b))
	if err != nil {
		t.Fatalf("NewHost: %v", err)
	}
	members := map[uint64]string{keyRotationLiveReplicaID: "127.0.0.1:5000"}
	if err := h.StartShard(members, false, kv.NewMemoryCreateFunc(),
		testShardConfig(keyRotationLiveShardID, keyRotationLiveReplicaID)); err != nil {
		mustCloseHost(t, h)
		t.Fatalf("StartShard: %v", err)
	}
	waitForLeader(t, h, keyRotationLiveShardID)
	return h
}

// proposeLiveValue writes key=value through raft.
func proposeLiveValue(ctx context.Context, t *testing.T, h *Host, key, value string) {
	t.Helper()
	cmd, err := kv.EncodePut([]byte(key), []byte(value))
	if err != nil {
		t.Fatalf("EncodePut: %v", err)
	}
	if _, err := h.SyncPropose(ctx, keyRotationLiveShardID, cmd); err != nil {
		t.Fatalf("SyncPropose %s: %v", key, err)
	}
}

// readLiveValue reads key with SyncRead, retrying the ErrReadIndexNotReady a
// restarted leader returns until it commits an entry in its term.
func readLiveValue(ctx context.Context, t *testing.T, h *Host, key string) string {
	t.Helper()
	for {
		got, err := h.SyncRead(ctx, keyRotationLiveShardID, []byte(key))
		if errors.Is(err, ErrReadIndexNotReady) && ctx.Err() == nil {
			time.Sleep(5 * time.Millisecond)
			continue
		}
		if err != nil {
			t.Fatalf("SyncRead %s: %v", key, err)
		}
		switch v := got.(type) {
		case []byte:
			return string(v)
		case string:
			return v
		default:
			t.Fatalf("SyncRead %s returned %T, want []byte", key, got)
			return ""
		}
	}
}

type liveValue struct {
	key, value string
	epoch      uint64
}

// TestKeyRotationLive_EntriesAndSnapshotSurviveRestart drives scheduled
// rotation on a real seal.Barrier and checks the documented contract of
// KeyRotationInterval and MaxRetainedEpochs end to end.
func TestKeyRotationLive_EntriesAndSnapshotSurviveRestart(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	walDir, nodeDir := t.TempDir(), t.TempDir()
	store := &liveSealedStore{}

	b := newLiveRotationBarrier(ctx, t, store, nil)
	h := startLiveRotationHost(t, walDir, nodeDir, b)
	closed := false
	defer func() {
		if !closed {
			mustCloseHost(t, h)
		}
	}()

	// (a) and (b): propose while the schedule rotates, recording the epoch
	// each value was written under (the epoch is the same before and after
	// the proposal, so the record is sealed under it).
	startEpoch := b.CurrentEpoch()
	var values []liveValue
	epochs := map[uint64]bool{}
	deadline := time.Now().Add(20 * time.Second)
	for i := 0; ; i++ {
		before := b.CurrentEpoch()
		v := liveValue{key: fmt.Sprintf("pre-%04d", i), value: fmt.Sprintf("value-%04d-epoch-%d", i, before)}
		proposeLiveValue(ctx, t, h, v.key, v.value)
		if after := b.CurrentEpoch(); after == before {
			v.epoch = before
			epochs[before] = true
		}
		values = append(values, v)
		if b.CurrentEpoch() >= startEpoch+keyRotationLiveAdvances && len(epochs) > keyRotationLiveAdvances {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("(a) epoch advanced %d -> %d in 20s with KeyRotationInterval 1s, want at least %d rotations",
				startEpoch, b.CurrentEpoch(), keyRotationLiveAdvances)
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Logf("(a) epoch advanced %d -> %d; values written under epochs %v", startEpoch, b.CurrentEpoch(), sortedKeys(epochs))
	for _, v := range values {
		if got := readLiveValue(ctx, t, h, v.key); got != v.value {
			t.Fatalf("(b) SyncRead %s (epoch %d) = %q, want %q", v.key, v.epoch, got, v.value)
		}
	}

	// (c): snapshot, more proposals, close.
	snapEpochBefore := b.CurrentEpoch()
	if _, err := h.SyncRequestSnapshot(ctx, keyRotationLiveShardID, SnapshotOption{}); err != nil {
		t.Fatalf("SyncRequestSnapshot: %v", err)
	}
	snapEpochAfter := b.CurrentEpoch()
	for i := 0; i < 20; i++ {
		v := liveValue{key: fmt.Sprintf("post-%04d", i), value: fmt.Sprintf("post-value-%04d", i), epoch: b.CurrentEpoch()}
		proposeLiveValue(ctx, t, h, v.key, v.value)
		epochs[v.epoch] = true
		values = append(values, v)
		time.Sleep(50 * time.Millisecond)
	}
	mustCloseHost(t, h)
	closed = true

	// (d): no purge removed an epoch a live WAL record or the retained
	// snapshot is sealed under. Nothing was compacted here, so every epoch a
	// value was written under is still needed, as is the snapshot's.
	persisted := store.load()
	if persisted == nil {
		t.Fatal("(d) the barrier never persisted its state")
	}
	needed := sortedKeys(epochs)
	for _, ep := range []uint64{snapEpochBefore, snapEpochAfter} {
		if !slices.Contains(needed, ep) {
			needed = append(needed, ep)
		}
	}
	for _, ep := range needed {
		if ep == 0 {
			continue
		}
		if !slices.Contains(persisted.KnownEpochs, ep) {
			t.Fatalf("(d) persisted KnownEpochs %v (epoch %d) lost epoch %d that live data is sealed under",
				persisted.KnownEpochs, persisted.Epoch, ep)
		}
	}
	t.Logf("(d) persisted epoch %d, KnownEpochs %v, %d state writes", persisted.Epoch, persisted.KnownEpochs, store.writes)

	// (c) continued: a new Host on the same directories with the persisted
	// barrier state, unsealed, reads every value back.
	b2 := newLiveRotationBarrier(ctx, t, store, persisted)
	h2 := startLiveRotationHost(t, walDir, nodeDir, b2)
	defer mustCloseHost(t, h2)
	for _, v := range values {
		if got := readLiveValue(ctx, t, h2, v.key); got != v.value {
			t.Fatalf("(c) after restart SyncRead %s (epoch %d) = %q, want %q", v.key, v.epoch, got, v.value)
		}
	}
}

func sortedKeys(m map[uint64]bool) []uint64 {
	out := make([]uint64, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	slices.Sort(out)
	return out
}
