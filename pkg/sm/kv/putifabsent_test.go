package kv

import (
	"context"
	"testing"

	"github.com/jeremyhahn/go-quicraft/pkg/sm"
)

// mustEncodePutIfAbsent encodes a conditional put or fails the test.
func mustEncodePutIfAbsent(t *testing.T, key, value []byte) []byte {
	t.Helper()
	cmd, err := EncodePutIfAbsent(key, value)
	if err != nil {
		t.Fatalf("EncodePutIfAbsent: %v", err)
	}
	return cmd
}

// stores exercises both state-machine implementations through one test body.
//
// ConcurrentStore is not a variant of MemoryStore — it decodes in a separate
// validation pass and applies under per-bucket locks — so an opcode can be
// handled correctly by one and rejected by the other. That is exactly what
// happened while adding this op: the concurrent store's opcode whitelist was
// missed, and every conditional write there would have been answered
// ResultInvalidCommand.
// applier is the narrow surface these tests need. The two stores satisfy
// different snapshot interfaces, and snapshots are irrelevant here.
type applier interface {
	Update(ctx context.Context, entries []sm.Entry, results []sm.Result) error
	Lookup(ctx context.Context, query interface{}) (interface{}, error)
}

func stores(t *testing.T) map[string]applier {
	t.Helper()
	return map[string]applier{
		"MemoryStore":     NewMemoryStore(),
		"ConcurrentStore": NewConcurrentStore(),
	}
}

// TestPutIfAbsent_StoresOnlyWhenKeyIsAbsent is the operation's defining
// property.
//
// The first write takes effect and reports ResultStored; the second, for a key
// that now exists, is declined with ResultNotStored and — critically — must not
// overwrite. A conditional write that clobbered the existing value would be an
// ordinary Put with extra steps, and every single-use token built on it would
// be redeemable twice.
func TestPutIfAbsent_StoresOnlyWhenKeyIsAbsent(t *testing.T) {
	for name, store := range stores(t) {
		t.Run(name, func(t *testing.T) {
			ctx := context.Background()

			results := make([]sm.Result, 1)
			entries := []sm.Entry{{Index: 1, Cmd: mustEncodePutIfAbsent(t, []byte("k"), []byte("first"))}}
			if err := store.Update(ctx, entries, results); err != nil {
				t.Fatalf("Update(first): %v", err)
			}
			if results[0].Value != ResultStored {
				t.Fatalf("first write result = %d, want ResultStored(%d)", results[0].Value, ResultStored)
			}

			results = make([]sm.Result, 1)
			entries = []sm.Entry{{Index: 2, Cmd: mustEncodePutIfAbsent(t, []byte("k"), []byte("second"))}}
			if err := store.Update(ctx, entries, results); err != nil {
				t.Fatalf("Update(second): %v", err)
			}
			if results[0].Value != ResultNotStored {
				t.Fatalf("second write result = %d, want ResultNotStored(%d)", results[0].Value, ResultNotStored)
			}

			got, err := store.Lookup(ctx, []byte("k"))
			if err != nil {
				t.Fatalf("Lookup: %v", err)
			}
			value, ok := got.([]byte)
			if !ok {
				t.Fatalf("Lookup returned %T, want []byte", got)
			}
			if string(value) != "first" {
				t.Errorf("value = %q, want %q: a declined conditional write must not overwrite",
					value, "first")
			}
		})
	}
}

// TestPutIfAbsent_OneWinnerInASingleBatch is the property Raft actually relies
// on.
//
// Competing proposals for one key arrive as consecutive entries in a committed
// batch, and the state machine applies them in log order. Exactly one may
// succeed. If both did, two replicas replaying the same log would still agree
// with each other — but the cluster would have handed the same single-use
// credential to two callers, which is the failure this op exists to prevent.
func TestPutIfAbsent_OneWinnerInASingleBatch(t *testing.T) {
	for name, store := range stores(t) {
		t.Run(name, func(t *testing.T) {
			ctx := context.Background()

			entries := []sm.Entry{
				{Index: 1, Cmd: mustEncodePutIfAbsent(t, []byte("race"), []byte("a"))},
				{Index: 2, Cmd: mustEncodePutIfAbsent(t, []byte("race"), []byte("b"))},
				{Index: 3, Cmd: mustEncodePutIfAbsent(t, []byte("race"), []byte("c"))},
			}
			results := make([]sm.Result, len(entries))
			if err := store.Update(ctx, entries, results); err != nil {
				t.Fatalf("Update: %v", err)
			}

			winners := 0
			for _, r := range results {
				if r.Value == ResultStored {
					winners++
				}
			}
			if winners != 1 {
				t.Fatalf("%d writes reported stored, want exactly 1", winners)
			}
			if results[0].Value != ResultStored {
				t.Errorf("the first entry in log order must be the winner")
			}
		})
	}
}

// TestPutIfAbsent_DoesNotDisturbOtherKeys guards the bucket routing in the
// concurrent store: a conditional write must lock and inspect only its own key.
func TestPutIfAbsent_DoesNotDisturbOtherKeys(t *testing.T) {
	for name, store := range stores(t) {
		t.Run(name, func(t *testing.T) {
			ctx := context.Background()

			entries := []sm.Entry{
				{Index: 1, Cmd: mustEncodePutIfAbsent(t, []byte("alpha"), []byte("1"))},
				{Index: 2, Cmd: mustEncodePutIfAbsent(t, []byte("beta"), []byte("2"))},
				{Index: 3, Cmd: mustEncodePutIfAbsent(t, []byte("gamma"), []byte("3"))},
			}
			results := make([]sm.Result, len(entries))
			if err := store.Update(ctx, entries, results); err != nil {
				t.Fatalf("Update: %v", err)
			}
			for i, r := range results {
				if r.Value != ResultStored {
					t.Fatalf("entry %d result = %d, want ResultStored: distinct keys never conflict", i, r.Value)
				}
			}
		})
	}
}
