package quicraft

import (
	"errors"
	"testing"
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

// TestNewHost_ShardBarriersWithCustomLogDBAccepted verifies that supplying a
// LogDB (which a caller can make multiplexing) satisfies the requirement.
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
