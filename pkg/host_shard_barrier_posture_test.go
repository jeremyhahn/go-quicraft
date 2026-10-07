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
	"bytes"
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/jeremyhahn/go-quicraft/pkg/logdb"
	"github.com/jeremyhahn/go-quicraft/pkg/logdb/memdb"
	"github.com/jeremyhahn/go-quicraft/pkg/logdb/waldb"
	"github.com/jeremyhahn/go-quicraft/pkg/seal"
)

// These tests pin per-shard barriers in every host-wide posture (#240): a
// tenant's Raft group keeps its own barrier whether the host runs with sealing
// disabled (WithoutEncryption) or under a host barrier still in plaintext mode.

// hostRoutedLogDB models a multiplexing LogDB: shards with a dedicated barrier
// are stored under it, every other shard on one host-wide route. routes lists
// every barrier it writes under, by identity; the host-wide route is plaintext
// when none of them is the host's barrier.
type hostRoutedLogDB struct {
	*memdb.MemDB
	routes []any
}

func newHostRoutedLogDB(routes ...any) *hostRoutedLogDB {
	return &hostRoutedLogDB{MemDB: memdb.New(), routes: routes}
}

func (db *hostRoutedLogDB) AtRestProtection() logdb.AtRestProtection {
	return logdb.AtRestHostRouted
}

func (db *hostRoutedLogDB) MinLiveEpochForBarrier(barrier any) (uint64, bool, error) {
	for _, r := range db.routes {
		if logdb.SameBarrier(r, barrier) {
			return 0, true, nil
		}
	}
	return 0, false, nil
}

// newPlaintextModeBarrier returns an initialized none-strategy barrier: a host
// barrier that writes plaintext until EnableEncryption.
func newPlaintextModeBarrier(t *testing.T) *seal.Barrier {
	t.Helper()
	cfg := seal.DefaultBarrierConfig(1)
	cfg.StatePersister = func(*seal.SealedRootKey) error { return nil }
	b := seal.MustNewBarrier(cfg)
	if err := b.Initialize(context.Background(), seal.NewNoneStrategy(), seal.Credentials{}); err != nil {
		t.Fatalf("none barrier Initialize: %v", err)
	}
	if !b.IsPlaintextMode() {
		t.Fatal("fixture: a none-strategy barrier must report IsPlaintextMode")
	}
	return b
}

// TestNewHost_AtRestPostures pins NewHost's verdict on every row of #240's
// table, and on the refusals that must stay.
func TestNewHost_AtRestPostures(t *testing.T) {
	tenant := func(t *testing.T) Barrier { return createTestBarrier(t) }
	resolver := func(b Barrier) func(uint64) Barrier {
		return func(shardID uint64) Barrier {
			if shardID == 1 {
				return b
			}
			return nil
		}
	}
	plaintextWAL := func(t *testing.T, opts ...waldb.Option) logdb.LogDB {
		t.Helper()
		db, err := waldb.Open(t.TempDir(), opts...)
		if err != nil {
			t.Fatalf("waldb.Open: %v", err)
		}
		t.Cleanup(func() { _ = db.Close() })
		return db
	}

	cases := []struct {
		name string
		opts func(t *testing.T) []HostOption
		want error
	}{
		// Row 1: sealing disabled, no per-shard barriers. Nothing to encrypt.
		{"WithoutEncryption + mux", func(t *testing.T) []HostOption {
			return []HostOption{WithoutEncryption(), WithLogDB(newHostRoutedLogDB())}
		}, nil},
		// Row 2: no declared host-wide route, so the mux's plaintext route has
		// nothing to be judged against.
		{"mux + WithShardBarriers, no declared host-wide route", func(t *testing.T) []HostOption {
			b := tenant(t)
			return []HostOption{WithLogDB(newHostRoutedLogDB(b)), WithShardBarriers(resolver(b))}
		}, ErrShardBarriersPlaintextWAL},
		// Row 3: sealing disabled with per-shard barriers.
		{"WithoutEncryption + mux + WithShardBarriers", func(t *testing.T) []HostOption {
			b := tenant(t)
			return []HostOption{WithoutEncryption(), WithLogDB(newHostRoutedLogDB(b)), WithShardBarriers(resolver(b))}
		}, nil},
		{"WithoutEncryption + non-durable log + WithShardBarriers", func(t *testing.T) []HostOption {
			b := tenant(t)
			return []HostOption{WithoutEncryption(), WithMemoryLogDB(), WithShardBarriers(resolver(b))}
		}, nil},
		// Row 4: a host barrier in plaintext mode with a supplied LogDB whose
		// host-wide route writes through it.
		{"plaintext-mode WithBarrier + mux routed through it", func(t *testing.T) []HostOption {
			host := newPlaintextModeBarrier(t)
			return []HostOption{WithBarrier(host), WithLogDB(newHostRoutedLogDB(host))}
		}, nil},
		{"plaintext-mode WithBarrier + mux routed through it + WithShardBarriers", func(t *testing.T) []HostOption {
			host, b := newPlaintextModeBarrier(t), tenant(t)
			return []HostOption{WithBarrier(host), WithLogDB(newHostRoutedLogDB(host, b)), WithShardBarriers(resolver(b))}
		}, nil},
		{"plaintext-mode WithBarrier + waldb under that barrier", func(t *testing.T) []HostOption {
			host := newPlaintextModeBarrier(t)
			return []HostOption{WithBarrier(host), WithLogDB(plaintextWAL(t, waldb.WithBarrier(host)))}
		}, nil},

		// Refusals that stay.
		{"WithoutEncryption + WithBarrier", func(t *testing.T) []HostOption {
			return []HostOption{WithoutEncryption(), WithMemoryLogDB(), WithBarrier(tenant(t))}
		}, ErrAtRestEncryptionConflict},
		{"WithoutEncryption + WithShardBarriers + default WAL", func(t *testing.T) []HostOption {
			return []HostOption{WithoutEncryption(), WithShardBarriers(resolver(tenant(t)))}
		}, ErrShardBarriersPlaintextWAL},
		{"WithoutEncryption + WithShardBarriers + plaintext waldb", func(t *testing.T) []HostOption {
			return []HostOption{WithoutEncryption(), WithLogDB(plaintextWAL(t)), WithShardBarriers(resolver(tenant(t)))}
		}, ErrShardBarriersPlaintextWAL},
		{"WithBarrier + mux routed through another barrier", func(t *testing.T) []HostOption {
			host, other := newPlaintextModeBarrier(t), newPlaintextModeBarrier(t)
			return []HostOption{WithBarrier(host), WithLogDB(newHostRoutedLogDB(other))}
		}, ErrBarrierPlaintextLogDB},
		{"WithBarrier + mux with a plaintext host-wide route", func(t *testing.T) []HostOption {
			host, b := newPlaintextModeBarrier(t), tenant(t)
			return []HostOption{WithBarrier(host), WithLogDB(newHostRoutedLogDB(b)), WithShardBarriers(resolver(b))}
		}, ErrBarrierPlaintextLogDB},
		{"WithBarrier + waldb under another plaintext-mode barrier", func(t *testing.T) []HostOption {
			host := newPlaintextModeBarrier(t)
			return []HostOption{WithBarrier(host), WithLogDB(plaintextWAL(t, waldb.WithBarrier(newPlaintextModeBarrier(t))))}
		}, ErrBarrierPlaintextLogDB},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			opts := append([]HostOption{WithoutTransport()}, tc.opts(t)...)
			h, err := NewHost(testHostConfig(t), opts...)
			if tc.want == nil {
				if err != nil {
					t.Fatalf("NewHost = %v, want nil", err)
				}
				if closeErr := h.Close(); closeErr != nil {
					t.Fatalf("Close: %v", closeErr)
				}
				return
			}
			if err == nil {
				_ = h.Close()
			}
			if !errors.Is(err, tc.want) {
				t.Fatalf("NewHost = %v, want %v", err, tc.want)
			}
		})
	}
}

// TestStartShard_AtRestPostures pins which shards start in each accepted
// posture. A shard's own barrier must be the one the LogDB stores it under,
// and a shard with no barrier starts only on a route the host declared.
func TestStartShard_AtRestPostures(t *testing.T) {
	members := map[uint64]string{1: "127.0.0.1:5000"}
	type shard struct {
		id   uint64
		want error
	}
	cases := []struct {
		name   string
		opts   func(t *testing.T) []HostOption
		shards []shard
	}{
		{"WithoutEncryption + mux + WithShardBarriers", func(t *testing.T) []HostOption {
			owned, foreign, sealed := createTestBarrier(t), createTestBarrier(t), createTestBarrier(t)
			if err := sealed.Seal(); err != nil {
				t.Fatalf("Seal: %v", err)
			}
			return []HostOption{WithoutEncryption(), WithLogDB(newHostRoutedLogDB(owned, sealed)),
				WithShardBarriers(func(shardID uint64) Barrier {
					switch shardID {
					case 1:
						return owned
					case 3:
						return foreign
					case 4:
						return sealed
					}
					return nil
				})}
		}, []shard{
			{1, nil},                          // its own barrier, owned by the LogDB
			{2, nil},                          // the declared plaintext host-wide route
			{3, ErrShardBarrierLogDBMismatch}, // logged under another key
			{4, ErrBarrierSealed},             // no silent fallback for a sealed barrier
		}},
		{"plaintext-mode WithBarrier + mux + WithShardBarriers", func(t *testing.T) []HostOption {
			host, owned, foreign := newPlaintextModeBarrier(t), createTestBarrier(t), createTestBarrier(t)
			return []HostOption{WithBarrier(host), WithLogDB(newHostRoutedLogDB(host, owned)),
				WithShardBarriers(func(shardID uint64) Barrier {
					switch shardID {
					case 1:
						return owned
					case 3:
						return foreign
					}
					return nil
				})}
		}, []shard{
			{1, nil},                          // its own barrier, owned by the LogDB
			{2, nil},                          // falls back to the host barrier
			{3, ErrShardBarrierLogDBMismatch}, // logged under another key
		}},
		{"WithShardBarriers + no declared host-wide route", func(t *testing.T) []HostOption {
			owned := createTestBarrier(t)
			return []HostOption{WithMemoryLogDB(),
				WithShardBarriers(func(shardID uint64) Barrier {
					if shardID == 1 {
						return owned
					}
					return nil
				})}
		}, []shard{
			{1, nil},
			{2, ErrShardBarrierUnresolved}, // nothing declared, so no plaintext
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			opts := append([]HostOption{WithoutTransport()}, tc.opts(t)...)
			h, err := NewHost(testHostConfig(t), opts...)
			if err != nil {
				t.Fatalf("NewHost: %v", err)
			}
			defer func() { _ = h.Close() }()
			for _, s := range tc.shards {
				err := h.StartShard(members, false, testCreateFunc, testShardConfig(s.id, 1))
				if s.want == nil && err != nil {
					t.Errorf("StartShard(%d) = %v, want nil", s.id, err)
				}
				if s.want != nil && !errors.Is(err, s.want) {
					t.Errorf("StartShard(%d) = %v, want %v", s.id, err, s.want)
				}
			}
		})
	}
}

// payloadSnapshotSM writes a fixed payload as its snapshot, so the bytes on
// disk can be searched for it.
type payloadSnapshotSM struct {
	mockStateMachine
	payload []byte
}

func (m *payloadSnapshotSM) SaveSnapshot(_ context.Context, w io.Writer, _ <-chan struct{}) error {
	_, err := w.Write(m.payload)
	return err
}

// TestShardBarriers_PlaintextHostRoute_SnapshotEncryptedOnDisk shows a
// barriered shard's snapshot encrypted on disk under its own barrier while the
// host-wide route is plaintext, and an unbarriered shard's snapshot on that
// plaintext route beside it.
func TestShardBarriers_PlaintextHostRoute_SnapshotEncryptedOnDisk(t *testing.T) {
	tenant := createTestBarrier(t)
	db := newHostRoutedLogDB(tenant)
	h, err := NewHost(testHostConfig(t), WithoutTransport(), WithoutEncryption(), WithLogDB(db),
		WithShardBarriers(func(shardID uint64) Barrier {
			if shardID == 1 {
				return tenant
			}
			return nil
		}))
	if err != nil {
		t.Fatalf("NewHost: %v", err)
	}
	defer func() { _ = h.Close() }()

	payload := bytes.Repeat([]byte("TENANT-SNAPSHOT-PAYLOAD-7c1e"), 64)
	create := func(uint64, uint64) interface{} { return &payloadSnapshotSM{payload: payload} }
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	for _, shardID := range []uint64{1, 2} {
		if startErr := h.StartShard(map[uint64]string{1: "127.0.0.1:5000"}, false, create,
			testShardConfig(shardID, 1)); startErr != nil {
			t.Fatalf("StartShard(%d): %v", shardID, startErr)
		}
		waitForLeader(t, h, shardID)
		if _, proposeErr := h.SyncPropose(ctx, shardID, []byte("cmd")); proposeErr != nil {
			t.Fatalf("SyncPropose(%d): %v", shardID, proposeErr)
		}
		if _, snapErr := h.SyncRequestSnapshot(ctx, shardID, SnapshotOption{}); snapErr != nil {
			t.Fatalf("SyncRequestSnapshot(%d): %v", shardID, snapErr)
		}
	}

	onDisk := func(shardID uint64) (logdb.Snapshot, []byte) {
		t.Helper()
		ss, getErr := db.GetSnapshot(shardID, 1)
		if getErr != nil {
			t.Fatalf("GetSnapshot(%d): %v", shardID, getErr)
		}
		raw, readErr := os.ReadFile(filepath.Join(ss.Filepath, "snapshot.dat"))
		if readErr != nil {
			t.Fatalf("read snapshot of shard %d: %v", shardID, readErr)
		}
		return ss, raw
	}

	ss, raw := onDisk(1)
	if ss.Epoch == 0 {
		t.Fatal("barriered shard's snapshot record has Epoch 0: it was written in plaintext")
	}
	if bytes.Contains(raw, payload[:64]) {
		t.Fatal("barriered shard's snapshot holds its payload in cleartext on disk")
	}
	var decrypted bytes.Buffer
	if _, decErr := decryptSnapshotFrames(tenant, 1, 1, ss.Index, bytes.NewReader(raw), &decrypted); decErr != nil {
		t.Fatalf("snapshot does not decrypt under the shard's own barrier: %v", decErr)
	}
	if !bytes.Contains(decrypted.Bytes(), payload[:64]) {
		t.Fatal("snapshot decrypted under the shard's barrier does not hold the payload")
	}

	ss, raw = onDisk(2)
	if ss.Epoch != 0 || !bytes.Contains(raw, payload[:64]) {
		t.Fatalf("unbarriered shard's snapshot (Epoch %d) is not on the declared plaintext "+
			"host-wide route", ss.Epoch)
	}
}
