package quicraft

import (
	"errors"
	"testing"

	"github.com/jeremyhahn/go-quicraft/pkg/logdb"
	"github.com/jeremyhahn/go-quicraft/pkg/logdb/waldb"
)

// TestNewHost_ShardBarriersWithoutHostBarrierRefused verifies per-shard encryption alone over a plaintext WAL is refused.
func TestNewHost_ShardBarriersWithoutHostBarrierRefused(t *testing.T) {
	cfg := testHostConfig(t)
	shardBarrier := createTestBarrier(t)

	_, err := NewHost(cfg,
		WithShardBarriers(func(shardID uint64) Barrier { return shardBarrier }),
		WithoutTransport(),
	)
	if !errors.Is(err, ErrShardBarriersPlaintextWAL) {
		t.Fatalf("NewHost error = %v, want ErrShardBarriersPlaintextWAL: "+
			"per-shard barriers with the default WAL would store the log in "+
			"plaintext while snapshots are encrypted", err)
	}
}

// TestNewHost_ShardBarriersWithHostBarrierAccepted verifies the legal
// combination still works: the host-wide barrier encrypts the WAL while the
// per-shard resolver covers snapshots.
func TestNewHost_ShardBarriersWithHostBarrierAccepted(t *testing.T) {
	cfg := testHostConfig(t)
	hostBarrier := createTestBarrier(t)
	shardBarrier := createTestBarrier(t)

	h, err := NewHost(cfg,
		WithBarrier(hostBarrier),
		WithShardBarriers(func(shardID uint64) Barrier { return shardBarrier }),
		WithoutTransport(),
	)
	if err != nil {
		t.Fatalf("NewHost with both a host-wide and per-shard barrier failed: %v", err)
	}
	defer h.Close()
}

// TestNewHost_ShardBarriersWithPlaintextLogDBRefused verifies per-shard barriers over a plaintext caller-supplied LogDB are refused.
func TestNewHost_ShardBarriersWithPlaintextLogDBRefused(t *testing.T) {
	cfg := testHostConfig(t)
	shardBarrier := createTestBarrier(t)

	// Opened without waldb.WithBarrier: records are cleartext.
	db, err := waldb.Open(t.TempDir())
	if err != nil {
		t.Fatalf("waldb.Open: %v", err)
	}
	defer db.Close()

	if got := db.AtRestProtection(); got != logdb.AtRestUnprotected {
		t.Fatalf("waldb opened without a barrier reports AtRestProtection = %v, "+
			"want AtRestUnprotected; the fixture is not modelling a plaintext WAL", got)
	}

	h, err := NewHost(cfg,
		WithLogDB(db),
		WithShardBarriers(func(shardID uint64) Barrier { return shardBarrier }),
		WithoutTransport(),
	)
	if err == nil {
		h.Close()
		t.Fatal("NewHost accepted per-shard barriers over a PLAINTEXT waldb: " +
			"snapshots are encrypted per tenant while every Raft log entry -- " +
			"every user command -- is written to disk in cleartext, and no " +
			"observable signal reports it")
	}
	if !errors.Is(err, ErrShardBarriersPlaintextWAL) {
		t.Fatalf("NewHost error = %v, want ErrShardBarriersPlaintextWAL", err)
	}
}

// TestNewHost_ShardBarriersWithEncryptedLogDBAccepted is the positive half of
// The same construction with the WAL actually encrypted must be accepted,
// so the guard above discriminates on the at-rest property and not merely on
// the presence of a caller-supplied LogDB.
func TestNewHost_ShardBarriersWithEncryptedLogDBAccepted(t *testing.T) {
	cfg := testHostConfig(t)
	walBarrier := createTestBarrier(t)
	shardBarrier := createTestBarrier(t)

	db, err := waldb.Open(t.TempDir(), waldb.WithBarrier(walBarrier))
	if err != nil {
		t.Fatalf("waldb.Open: %v", err)
	}
	defer db.Close()

	if got := db.AtRestProtection(); got != logdb.AtRestBarrierEncrypted {
		t.Fatalf("waldb opened with a barrier reports AtRestProtection = %v, "+
			"want AtRestBarrierEncrypted", got)
	}

	h, err := NewHost(cfg,
		WithLogDB(db),
		WithShardBarriers(func(shardID uint64) Barrier { return shardBarrier }),
		WithoutTransport(),
	)
	if err != nil {
		t.Fatalf("NewHost with an ENCRYPTED caller-supplied LogDB and per-shard "+
			"barriers failed: %v; the plaintext-WAL guard must key off the "+
			"LogDB's at-rest property, not off supplying a LogDB at all", err)
	}
	defer h.Close()
}

// TestNewHost_ShardBarriersWithCustomLogDBAccepted verifies a non-durable in-memory LogDB is accepted.
func TestNewHost_ShardBarriersWithCustomLogDBAccepted(t *testing.T) {
	cfg := testHostConfig(t)
	shardBarrier := createTestBarrier(t)

	h, err := NewHost(cfg,
		WithMemoryLogDB(),
		WithShardBarriers(func(shardID uint64) Barrier { return shardBarrier }),
		WithoutTransport(),
	)
	if err != nil {
		t.Fatalf("NewHost with a caller-supplied LogDB failed: %v", err)
	}
	defer h.Close()
}
