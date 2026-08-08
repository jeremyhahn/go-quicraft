package quicraft

import (
	"errors"
	"testing"

	"github.com/jeremyhahn/go-quicraft/pkg/logdb"
	"github.com/jeremyhahn/go-quicraft/pkg/logdb/waldb"
)

// TestNewHost_ShardBarriersWithoutHostBarrierRefused verifies that configuring
// per-shard encryption alone is refused rather than silently producing a
// plaintext WAL.
//
// The default WAL takes ONE barrier, so WithShardBarriers cannot reach it.
// Accepting this combination encrypted snapshots while every Raft log entry --
// i.e. every user command -- was written to disk in cleartext. An operator
// checking the snapshot directory would see encryption working and never learn
// the command log was readable. Same conflation class as the three sealed-vs-nil
// barrier downgrades already fixed, relocated to host construction.
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

// TestNewHost_ShardBarriersWithPlaintextLogDBRefused is the H-1 regression
// test: the SIXTH at-rest plaintext downgrade, and the first on the "which
// barrier applies to this shard" axis rather than the sealed-vs-nil axis the
// five before it shared.
//
// The refusal used to be written as `if options.logdb == nil`, so it
// discriminated on whether a LogDB was SUPPLIED rather than on what that LogDB
// IS. Supplying one is the natural thing an integrator does to put the WAL on a
// chosen directory, and it bought a full bypass: an unencrypted waldb.Open(dir)
// plus WithShardBarriers and no WithBarrier produced byte-for-byte the state
// ErrShardBarriersPlaintextWAL exists to prevent -- encrypted snapshots over a
// plaintext Raft log, every user command readable off the disk, while the
// operator's spot-check of the snapshot directory shows encryption working.
//
// A type assertion cannot decide this: *waldb.DB implements the per-barrier
// interfaces in BOTH cases. Only the LogDB's own AtRestProtection declaration
// separates them, and its zero value is AtRestUnprotected so a store that
// declares nothing is read as unsafe.
func TestNewHost_ShardBarriersWithPlaintextLogDBRefused(t *testing.T) {
	cfg := testHostConfig(t)
	shardBarrier := createTestBarrier(t)

	// Opened WITHOUT waldb.WithBarrier: every record reaches a WAL segment in
	// cleartext.
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
// H-1: the same construction with the WAL actually encrypted must be accepted,
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

// TestNewHost_ShardBarriersWithCustomLogDBAccepted verifies that an in-memory
// LogDB satisfies the requirement.
//
// It is accepted because it is NON-DURABLE, not because a LogDB was supplied:
// nothing is ever written to a file, so there is no at-rest artifact to read
// off the disk and the guarantee holds vacuously. Generalising this case to
// "any caller-supplied LogDB is fine" is exactly what produced the bypass
// pinned by TestNewHost_ShardBarriersWithPlaintextLogDBRefused above.
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
