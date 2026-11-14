package waldb

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"testing"

	"github.com/jeremyhahn/go-quicraft/pkg/logdb"
)

// The write side refuses any membership map past maxMembershipMapEntries and
// any bootstrap DataDir past maxBootstrapDataDirLen with
// ErrMembershipTooLarge, because replay rejects such a record as corruption.
// These tests pin that every capped map is refused through every write path,
// that a refusal writes nothing (the previous record survives, also across a
// reopen), and that a record exactly at every cap is accepted and round-trips.

const (
	membershipSizeShard   = 7
	membershipSizeReplica = 3
)

// openMembershipSizeDB opens a WAL DB in dir with the options the existing
// oversized-membership regression test uses.
func openMembershipSizeDB(ctx context.Context, t *testing.T, dir string) *DB {
	t.Helper()
	if err := ctx.Err(); err != nil {
		t.Fatalf("context: %v", err)
	}
	db, err := Open(dir, WithBlockSize(1024), WithMaxFileSize(64*1024*1024))
	if err != nil {
		t.Fatalf("Open(%s): %v", dir, err)
	}
	return db
}

// reopenMembershipSizeDB closes db and opens dir again.
func reopenMembershipSizeDB(ctx context.Context, t *testing.T, db *DB, dir string) *DB {
	t.Helper()
	if err := db.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	return openMembershipSizeDB(ctx, t, dir)
}

// membershipAddrMap returns a map of n replica IDs starting at base.
func membershipAddrMap(ctx context.Context, t *testing.T, base uint64, n int) map[uint64]string {
	t.Helper()
	if err := ctx.Err(); err != nil {
		t.Fatalf("context: %v", err)
	}
	m := make(map[uint64]string, n)
	for i := 0; i < n; i++ {
		m[base+uint64(i)] = "a"
	}
	return m
}

// membershipRemovedMap returns a removed map of n replica IDs starting at base.
func membershipRemovedMap(ctx context.Context, t *testing.T, base uint64, n int) map[uint64]bool {
	t.Helper()
	if err := ctx.Err(); err != nil {
		t.Fatalf("context: %v", err)
	}
	m := make(map[uint64]bool, n)
	for i := 0; i < n; i++ {
		m[base+uint64(i)] = true
	}
	return m
}

// baselineMembershipSnapshot is a small, valid snapshot saved before each
// refused write, which must survive the refusal unchanged.
func baselineMembershipSnapshot(ctx context.Context, t *testing.T) logdb.Snapshot {
	t.Helper()
	return logdb.Snapshot{
		Index: 10,
		Term:  2,
		Membership: logdb.Membership{
			ConfigChangeID:    9,
			Addresses:         membershipAddrMap(ctx, t, 1, 3),
			Observers:         membershipAddrMap(ctx, t, 100, 1),
			Witnesses:         membershipAddrMap(ctx, t, 200, 1),
			Removed:           membershipRemovedMap(ctx, t, 300, 1),
			Identities:        membershipAddrMap(ctx, t, 1, 3),
			RemovedIdentities: membershipAddrMap(ctx, t, 300, 1),
		},
	}
}

// oversizedMembershipCases sets exactly one map to one entry past the cap.
func oversizedMembershipCases(ctx context.Context, t *testing.T) []struct {
	name string
	set  func(*logdb.Membership)
} {
	t.Helper()
	over := maxMembershipMapEntries + 1
	return []struct {
		name string
		set  func(*logdb.Membership)
	}{
		{"Observers", func(m *logdb.Membership) { m.Observers = membershipAddrMap(ctx, t, 1000, over) }},
		{"Witnesses", func(m *logdb.Membership) { m.Witnesses = membershipAddrMap(ctx, t, 1000, over) }},
		{"Removed", func(m *logdb.Membership) { m.Removed = membershipRemovedMap(ctx, t, 1000, over) }},
		{"Identities", func(m *logdb.Membership) { m.Identities = membershipAddrMap(ctx, t, 1000, over) }},
		{"RemovedIdentities", func(m *logdb.Membership) {
			m.RemovedIdentities = membershipAddrMap(ctx, t, 1000, over)
		}},
	}
}

// requireMembershipSnapshot asserts GetSnapshot returns want.
func requireMembershipSnapshot(ctx context.Context, t *testing.T, db *DB, want logdb.Snapshot, when string) {
	t.Helper()
	if err := ctx.Err(); err != nil {
		t.Fatalf("context: %v", err)
	}
	got, err := db.GetSnapshot(membershipSizeShard, membershipSizeReplica)
	if err != nil {
		t.Fatalf("%s: GetSnapshot: %v", when, err)
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("%s: GetSnapshot index=%d term=%d, want the saved record index=%d term=%d (maps equal: addresses %v observers %v witnesses %v removed %v identities %v removedIdentities %v)",
			when, got.Index, got.Term, want.Index, want.Term,
			reflect.DeepEqual(got.Membership.Addresses, want.Membership.Addresses),
			reflect.DeepEqual(got.Membership.Observers, want.Membership.Observers),
			reflect.DeepEqual(got.Membership.Witnesses, want.Membership.Witnesses),
			reflect.DeepEqual(got.Membership.Removed, want.Membership.Removed),
			reflect.DeepEqual(got.Membership.Identities, want.Membership.Identities),
			reflect.DeepEqual(got.Membership.RemovedIdentities, want.Membership.RemovedIdentities))
	}
}

// requireMembershipBootstrap asserts GetBootstrap returns want.
func requireMembershipBootstrap(ctx context.Context, t *testing.T, db *DB, want logdb.Bootstrap, when string) {
	t.Helper()
	if err := ctx.Err(); err != nil {
		t.Fatalf("context: %v", err)
	}
	got, ok, err := db.GetBootstrap(membershipSizeShard, membershipSizeReplica)
	if err != nil {
		t.Fatalf("%s: GetBootstrap: %v", when, err)
	}
	if !ok {
		t.Fatalf("%s: GetBootstrap found no record, want the saved one", when)
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("%s: GetBootstrap = {%d addresses, join=%v, type=%d, dataDir %d bytes}, want {%d addresses, join=%v, type=%d, dataDir %d bytes}",
			when, len(got.Addresses), got.Join, got.Type, len(got.DataDir),
			len(want.Addresses), want.Join, want.Type, len(want.DataDir))
	}
}

func TestSaveSnapshot_MembershipTooLarge_EveryMapRefusedAndNothingWritten(t *testing.T) {
	ctx := context.Background()
	for _, tc := range oversizedMembershipCases(ctx, t) {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			db := openMembershipSizeDB(ctx, t, dir)
			base := baselineMembershipSnapshot(ctx, t)
			if err := db.SaveSnapshot(membershipSizeShard, membershipSizeReplica, base); err != nil {
				t.Fatalf("SaveSnapshot baseline: %v", err)
			}

			bad := baselineMembershipSnapshot(ctx, t)
			bad.Index, bad.Term = 20, 3
			tc.set(&bad.Membership)
			err := db.SaveSnapshot(membershipSizeShard, membershipSizeReplica, bad)
			if !errors.Is(err, ErrMembershipTooLarge) {
				t.Fatalf("SaveSnapshot with %d %s = %v, want ErrMembershipTooLarge",
					maxMembershipMapEntries+1, tc.name, err)
			}
			requireMembershipSnapshot(ctx, t, db, base, "after refusal")

			db = reopenMembershipSizeDB(ctx, t, db, dir)
			defer db.Close()
			requireMembershipSnapshot(ctx, t, db, base, "after refusal and reopen")
		})
	}
}

func TestSaveState_MembershipTooLarge_EveryMapRefusedAndNothingWritten(t *testing.T) {
	ctx := context.Background()
	for _, tc := range oversizedMembershipCases(ctx, t) {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			db := openMembershipSizeDB(ctx, t, dir)
			base := baselineMembershipSnapshot(ctx, t)
			if err := db.SaveState([]logdb.Update{{
				ShardID: membershipSizeShard, ReplicaID: membershipSizeReplica,
				Snapshot: base,
			}}); err != nil {
				t.Fatalf("SaveState baseline: %v", err)
			}

			bad := baselineMembershipSnapshot(ctx, t)
			bad.Index, bad.Term = 20, 3
			tc.set(&bad.Membership)
			err := db.SaveState([]logdb.Update{{
				ShardID: membershipSizeShard, ReplicaID: membershipSizeReplica,
				Snapshot: bad,
			}})
			if !errors.Is(err, ErrMembershipTooLarge) {
				t.Fatalf("SaveState with a snapshot of %d %s = %v, want ErrMembershipTooLarge",
					maxMembershipMapEntries+1, tc.name, err)
			}
			requireMembershipSnapshot(ctx, t, db, base, "after refusal")

			db = reopenMembershipSizeDB(ctx, t, db, dir)
			defer db.Close()
			requireMembershipSnapshot(ctx, t, db, base, "after refusal and reopen")
		})
	}
}

func TestSaveBootstrap_MembershipTooLarge_RefusedAndNothingWritten(t *testing.T) {
	ctx := context.Background()
	cases := []struct {
		name string
		bs   logdb.Bootstrap
	}{
		{"Addresses", logdb.Bootstrap{
			Addresses: membershipAddrMap(ctx, t, 1, maxMembershipMapEntries+1),
			Type:      1,
			DataDir:   "/data",
		}},
		{"DataDir", logdb.Bootstrap{
			Addresses: membershipAddrMap(ctx, t, 1, 3),
			Type:      1,
			DataDir:   strings.Repeat("d", maxBootstrapDataDirLen+1),
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			db := openMembershipSizeDB(ctx, t, dir)
			base := logdb.Bootstrap{
				Addresses: membershipAddrMap(ctx, t, 1, 3),
				Join:      true,
				Type:      2,
				DataDir:   "/baseline",
			}
			if err := db.SaveBootstrap(membershipSizeShard, membershipSizeReplica, base); err != nil {
				t.Fatalf("SaveBootstrap baseline: %v", err)
			}

			err := db.SaveBootstrap(membershipSizeShard, membershipSizeReplica, tc.bs)
			if !errors.Is(err, ErrMembershipTooLarge) {
				t.Fatalf("SaveBootstrap with %d addresses and a %d-byte DataDir = %v, want ErrMembershipTooLarge",
					len(tc.bs.Addresses), len(tc.bs.DataDir), err)
			}
			requireMembershipBootstrap(ctx, t, db, base, "after refusal")

			db = reopenMembershipSizeDB(ctx, t, db, dir)
			defer db.Close()
			requireMembershipBootstrap(ctx, t, db, base, "after refusal and reopen")
		})
	}
}

func TestMembershipTooLarge_AtEveryCapSavesAndReadsBack(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	db := openMembershipSizeDB(ctx, t, dir)

	n := maxMembershipMapEntries
	ss := logdb.Snapshot{
		Index: 10,
		Term:  2,
		Membership: logdb.Membership{
			ConfigChangeID:    9,
			Addresses:         membershipAddrMap(ctx, t, 1, n),
			Observers:         membershipAddrMap(ctx, t, 100000, n),
			Witnesses:         membershipAddrMap(ctx, t, 200000, n),
			Removed:           membershipRemovedMap(ctx, t, 300000, n),
			Identities:        membershipAddrMap(ctx, t, 1, n),
			RemovedIdentities: membershipAddrMap(ctx, t, 300000, n),
		},
	}
	if err := db.SaveSnapshot(membershipSizeShard, membershipSizeReplica, ss); err != nil {
		t.Fatalf("SaveSnapshot with every map at %d entries: %v", n, err)
	}
	bs := logdb.Bootstrap{
		Addresses: membershipAddrMap(ctx, t, 1, n),
		Join:      false,
		Type:      1,
		DataDir:   strings.Repeat("d", maxBootstrapDataDirLen),
	}
	if err := db.SaveBootstrap(membershipSizeShard, membershipSizeReplica, bs); err != nil {
		t.Fatalf("SaveBootstrap with %d addresses and a %d-byte DataDir: %v", n, len(bs.DataDir), err)
	}
	requireMembershipSnapshot(ctx, t, db, ss, "before reopen")
	requireMembershipBootstrap(ctx, t, db, bs, "before reopen")

	db = reopenMembershipSizeDB(ctx, t, db, dir)
	defer db.Close()
	requireMembershipSnapshot(ctx, t, db, ss, "after reopen")
	requireMembershipBootstrap(ctx, t, db, bs, "after reopen")
}
