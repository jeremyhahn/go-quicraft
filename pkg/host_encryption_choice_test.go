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
	"os"
	"path/filepath"
	"testing"

	"github.com/jeremyhahn/go-quicraft/pkg/logdb/waldb"
)

// TestNewHost_AtRestEncryptionIsAStatedChoice pins that plaintext at rest is
// never reached by omission: a host must configure a barrier or declare
// WithoutEncryption, and may not do both.
func TestNewHost_AtRestEncryptionIsAStatedChoice(t *testing.T) {
	t.Run("unspecified is refused before anything touches the disk", func(t *testing.T) {
		cfg := testHostConfig(t)
		// Point the host at directories that do not exist yet, so a refusal
		// that ran after NewHost's MkdirAll would leave them behind.
		cfg.NodeHostDir = filepath.Join(cfg.NodeHostDir, "nodehost")
		cfg.WALDir = filepath.Join(cfg.WALDir, "wal")
		h, err := NewHost(cfg, WithMemoryLogDB(), WithoutTransport())
		if !errors.Is(err, ErrAtRestEncryptionUnspecified) {
			if h != nil {
				_ = h.Close()
			}
			t.Fatalf("NewHost with no encryption option = %v, want ErrAtRestEncryptionUnspecified", err)
		}
		for _, dir := range []string{cfg.NodeHostDir, cfg.WALDir} {
			if _, statErr := os.Stat(dir); !os.IsNotExist(statErr) {
				t.Errorf("refused host created %s (stat err %v)", dir, statErr)
			}
		}
	})

	t.Run("WithoutEncryption with WithBarrier is refused", func(t *testing.T) {
		_, err := NewHost(testHostConfig(t), WithMemoryLogDB(), WithoutTransport(),
			WithoutEncryption(), WithBarrier(createTestBarrier(t)))
		if !errors.Is(err, ErrAtRestEncryptionConflict) {
			t.Fatalf("NewHost = %v, want ErrAtRestEncryptionConflict", err)
		}
	})

	t.Run("WithoutEncryption with WithShardBarriers is refused", func(t *testing.T) {
		b := createTestBarrier(t)
		_, err := NewHost(testHostConfig(t), WithMemoryLogDB(), WithoutTransport(),
			WithShardBarriers(func(uint64) Barrier { return b }), WithoutEncryption())
		if !errors.Is(err, ErrAtRestEncryptionConflict) {
			t.Fatalf("NewHost = %v, want ErrAtRestEncryptionConflict", err)
		}
	})

	accepted := []struct {
		name string
		opts func(t *testing.T) []HostOption
	}{
		{"WithoutEncryption", func(*testing.T) []HostOption {
			return []HostOption{WithoutEncryption()}
		}},
		{"WithBarrier", func(t *testing.T) []HostOption {
			return []HostOption{WithBarrier(createTestBarrier(t))}
		}},
		{"WithShardBarriers over a non-durable log", func(t *testing.T) []HostOption {
			b := createTestBarrier(t)
			return []HostOption{WithShardBarriers(func(uint64) Barrier { return b })}
		}},
	}
	for _, tc := range accepted {
		t.Run(tc.name+" is accepted", func(t *testing.T) {
			opts := append([]HostOption{WithMemoryLogDB(), WithoutTransport()}, tc.opts(t)...)
			h, err := NewHost(testHostConfig(t), opts...)
			if err != nil {
				t.Fatalf("NewHost = %v, want nil", err)
			}
			if err := h.Close(); err != nil {
				t.Fatalf("Close: %v", err)
			}
		})
	}
}

// TestNewHost_StatedBarrierMustCoverTheLog pins that a stated encryption covers
// the Raft log: the host-wide barrier is wired only into the default WAL, so a
// caller-supplied LogDB never receives it. Accepting WithBarrier plus an
// unencrypted waldb.Open(dir) — the one-line change that relocates the WAL —
// would encrypt snapshots and write every proposed command to disk in
// plaintext, with no error.
func TestNewHost_StatedBarrierMustCoverTheLog(t *testing.T) {
	t.Run("unencrypted supplied WAL is refused", func(t *testing.T) {
		db, err := waldb.Open(t.TempDir())
		if err != nil {
			t.Fatalf("waldb.Open: %v", err)
		}
		defer func() { _ = db.Close() }()
		_, err = NewHost(testHostConfig(t), WithLogDB(db), WithBarrier(createTestBarrier(t)), WithoutTransport())
		if !errors.Is(err, ErrBarrierPlaintextLogDB) {
			t.Fatalf("NewHost(WithBarrier, plaintext WAL) = %v, want ErrBarrierPlaintextLogDB", err)
		}
	})
	t.Run("supplied WAL encrypted under the barrier is accepted", func(t *testing.T) {
		b := createTestBarrier(t)
		db, err := waldb.Open(t.TempDir(), waldb.WithBarrier(b))
		if err != nil {
			t.Fatalf("waldb.Open: %v", err)
		}
		h, err := NewHost(testHostConfig(t), WithLogDB(db), WithBarrier(b), WithoutTransport())
		if err != nil {
			t.Fatalf("NewHost(WithBarrier, encrypted WAL) = %v, want nil", err)
		}
		if err := h.Close(); err != nil {
			t.Fatalf("Close: %v", err)
		}
	})
	t.Run("non-durable log is accepted", func(t *testing.T) {
		h, err := NewHost(testHostConfig(t), WithMemoryLogDB(), WithBarrier(createTestBarrier(t)), WithoutTransport())
		if err != nil {
			t.Fatalf("NewHost(WithBarrier, memdb) = %v, want nil", err)
		}
		if err := h.Close(); err != nil {
			t.Fatalf("Close: %v", err)
		}
	})
}
