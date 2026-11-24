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

// stores runs a test body against both store implementations. applier is the narrow surface the tests need.
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

// TestPutIfAbsent_StoresOnlyWhenKeyIsAbsent checks the first write stores and a second is declined without overwriting.
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

// TestPutIfAbsent_OneWinnerInASingleBatch checks exactly one of competing writes in a batch succeeds.
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
