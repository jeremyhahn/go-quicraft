package quicraft

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jeremyhahn/go-quicraft/pkg/logdb/waldb"
	"github.com/jeremyhahn/go-quicraft/pkg/seal"
	"github.com/jeremyhahn/go-quicraft/pkg/sm/kv"
)

const (
	keyRotationLiveShardID   uint64 = 1
	keyRotationLiveReplicaID uint64 = 1
	// keyRotationLiveAdvances is how many scheduled rotations the test waits
	// for beyond the epoch the barrier starts in.
	keyRotationLiveAdvances = 3
	// keyRotationLiveSegmentSize is the WAL segment size: small enough that
	// keyRotationLivePadding-sized values roll a segment within each epoch.
	keyRotationLiveSegmentSize int64 = 64 << 10
	// keyRotationLivePadding pads each value to about one WAL block.
	keyRotationLivePadding = 4096
)

// liveValueText is a value of about keyRotationLivePadding bytes named by
// prefix and i, so a few dozen proposals roll a WAL segment.
func liveValueText(prefix string, i int, epoch uint64) string {
	return fmt.Sprintf("%s-%04d-epoch-%d-", prefix, i, epoch) + strings.Repeat("x", keyRotationLivePadding)
}

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
	// Small segments so the WAL rolls several times within each epoch: segment
	// GC after a snapshot then removes whole old-epoch segments, the WAL's
	// MinLiveEpoch rises, and the next rotation really purges an old DEK. With
	// the default 64 MiB segment nothing ever rolls and the purge floor stays at
	// the first epoch. Host.Close closes the injected LogDB.
	db, err := waldb.Open(walDir, waldb.WithBarrier(b), waldb.WithBlockSize(4096),
		waldb.WithMaxFileSize(keyRotationLiveSegmentSize), waldb.WithNoSync(true))
	if err != nil {
		t.Fatalf("waldb.Open: %v", err)
	}
	h, err := NewHost(cfg, WithoutTransport(), WithBarrier(b), WithLogDB(db))
	if err != nil {
		_ = db.Close()
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
		v := liveValue{key: fmt.Sprintf("pre-%04d", i), value: liveValueText("value", i, before)}
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

	// (c): snapshot with no compaction overhead, so the log is compacted up to
	// the snapshot index and segment GC drops the whole old-epoch segments
	// below it. Then keep proposing across more rotations until the schedule
	// purges an epoch older than the snapshot, and close.
	preEpochs := sortedKeys(epochs)
	oldest := preEpochs[0]
	snapEpochBefore := b.CurrentEpoch()
	if _, err := h.SyncRequestSnapshot(ctx, keyRotationLiveShardID,
		SnapshotOption{OverrideCompactionOverhead: true, CompactionOverhead: 0}); err != nil {
		t.Fatalf("SyncRequestSnapshot: %v", err)
	}
	snapEpochAfter := b.CurrentEpoch()
	if oldest >= snapEpochBefore {
		t.Fatalf("fixture: oldest value epoch %d is not older than the snapshot epoch %d", oldest, snapEpochBefore)
	}
	postEpochs := map[uint64]bool{}
	purged := func() bool {
		p := store.load()
		return p != nil && !slices.Contains(p.KnownEpochs, oldest)
	}
	deadline = time.Now().Add(20 * time.Second)
	for i := 0; ; i++ {
		before := b.CurrentEpoch()
		v := liveValue{key: fmt.Sprintf("post-%04d", i), value: liveValueText("post-value", i, before)}
		proposeLiveValue(ctx, t, h, v.key, v.value)
		if after := b.CurrentEpoch(); after == before {
			v.epoch = before
			postEpochs[before] = true
		}
		values = append(values, v)
		if b.CurrentEpoch() > snapEpochAfter+h.cfg.MaxRetainedEpochs && purged() {
			break
		}
		if time.Now().After(deadline) {
			p := store.load()
			floor, floorOK := h.engine.GlobalSafePurgeFloor()
			var walMin uint64
			var walErr error
			if edb, ok := h.logdb.(interface{ MinLiveEpoch() (uint64, error) }); ok {
				walMin, walErr = edb.MinLiveEpoch()
			}
			var segs []string
			_ = filepath.WalkDir(walDir, func(path string, d fs.DirEntry, _ error) error {
				if d != nil && !d.IsDir() {
					segs = append(segs, strings.TrimPrefix(path, walDir))
				}
				return nil
			})
			t.Logf("DIAG floor=%d ok=%v walMin=%d walErr=%v files=%v", floor, floorOK, walMin, walErr, segs)
			t.Fatalf("(d) epoch %d -> %d after a compacting snapshot at epoch %d, MaxRetainedEpochs %d: "+
				"oldest value epoch %d never purged (persisted %+v)",
				snapEpochAfter, b.CurrentEpoch(), snapEpochBefore, h.cfg.MaxRetainedEpochs, oldest, p)
		}
		time.Sleep(50 * time.Millisecond)
	}
	mustCloseHost(t, h)
	closed = true

	// (d): the schedule really purged an epoch older than the snapshot (the
	// one the first values were written under), and kept every epoch a live
	// WAL record or the retained snapshot is still sealed under: the
	// snapshot's epoch and every post-snapshot value's.
	persisted := store.load()
	if persisted == nil {
		t.Fatal("(d) the barrier never persisted its state")
	}
	if slices.Contains(persisted.KnownEpochs, oldest) {
		t.Fatalf("(d) persisted KnownEpochs %v still holds epoch %d, older than the snapshot epoch %d: nothing was purged",
			persisted.KnownEpochs, oldest, snapEpochBefore)
	}
	needed := sortedKeys(postEpochs)
	if !slices.Contains(needed, snapEpochAfter) {
		needed = append(needed, snapEpochAfter)
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
	t.Logf("(d) purged epoch %d; persisted epoch %d, KnownEpochs %v, %d state writes",
		oldest, persisted.Epoch, persisted.KnownEpochs, store.writes)

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
