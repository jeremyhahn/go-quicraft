// Copyright 2026 Jeremy Hahn
// SPDX-License-Identifier: Apache-2.0

package sm_test

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"strings"
	"sync"

	"github.com/jeremyhahn/go-quicraft/pkg/sm"
)

// The examples in this file compile and run the snippets of
// docs/state-machines/user.md, so the guide cannot drift from the API.

// Counter is the guide's "Write a basic state machine" snippet, verbatim.
type Counter struct{ n uint64 }

func (c *Counter) Update(ctx context.Context, entries []sm.Entry, results []sm.Result) error {
	for i, e := range entries {
		if len(e.Cmd) != 8 {
			results[i] = sm.Result{Value: sm.ResultInvalidCommand}
			continue
		}
		c.n += binary.LittleEndian.Uint64(e.Cmd)
		results[i] = sm.Result{Value: c.n}
	}
	return nil
}

func (c *Counter) Lookup(ctx context.Context, query interface{}) (interface{}, error) {
	return c.n, nil
}

func (c *Counter) SaveSnapshot(ctx context.Context, w io.Writer, stopper <-chan struct{}) error {
	return binary.Write(w, binary.LittleEndian, c.n)
}

func (c *Counter) RecoverFromSnapshot(ctx context.Context, r io.Reader, stopper <-chan struct{}) error {
	return binary.Read(r, binary.LittleEndian, &c.n)
}

func (c *Counter) Close(ctx context.Context) error { return nil }

// GetHash is the guide's "Turn on divergence detection" snippet, verbatim.
func (c *Counter) GetHash() (uint64, error) { return c.n, nil }

var _ sm.Hasher = (*Counter)(nil) // compile-time check

// counterCmd encodes an 8-byte Counter increment.
func counterCmd(n uint64) []byte {
	cmd := make([]byte, 8)
	binary.LittleEndian.PutUint64(cmd, n)
	return cmd
}

// ExampleNewCreateFunc registers the guide's Counter through NewCreateFunc,
// the typed wrapper the guide passes to host.StartShard, and applies two
// valid commands and one that is not 8 bytes.
func ExampleNewCreateFunc() {
	ctx := context.Background()
	create := sm.NewCreateFunc(func(shardID, replicaID uint64) sm.StateMachine {
		return &Counter{}
	})

	machine, ok := create(1, 1).(sm.StateMachine)
	if !ok {
		fmt.Println("not a StateMachine")
		return
	}
	entries := []sm.Entry{
		{Index: 1, Cmd: counterCmd(5)},
		{Index: 2, Cmd: counterCmd(7)},
		{Index: 3, Cmd: []byte("bad")},
	}
	results := make([]sm.Result, len(entries))
	if err := machine.Update(ctx, entries, results); err != nil {
		fmt.Println("update:", err)
		return
	}
	for i, r := range results {
		fmt.Printf("entry %d: value=%d invalid=%t\n", i+1, r.Value, r.Value == sm.ResultInvalidCommand)
	}
	// Output:
	// entry 1: value=5 invalid=false
	// entry 2: value=12 invalid=false
	// entry 3: value=18446744073709551612 invalid=true
}

// ExampleHasher saves the guide's Counter to a snapshot, recovers it into a
// fresh Counter, and shows that Lookup and GetHash agree on the state.
func ExampleHasher() {
	ctx := context.Background()
	src := &Counter{}
	results := make([]sm.Result, 1)
	if err := src.Update(ctx, []sm.Entry{{Index: 1, Cmd: counterCmd(42)}}, results); err != nil {
		fmt.Println("update:", err)
		return
	}

	var snap bytes.Buffer
	if err := src.SaveSnapshot(ctx, &snap, nil); err != nil {
		fmt.Println("save:", err)
		return
	}
	dst := &Counter{}
	if err := dst.RecoverFromSnapshot(ctx, &snap, nil); err != nil {
		fmt.Println("recover:", err)
		return
	}

	value, err := dst.Lookup(ctx, nil)
	if err != nil {
		fmt.Println("lookup:", err)
		return
	}
	var h sm.Hasher = dst
	hash, err := h.GetHash()
	if err != nil {
		fmt.Println("hash:", err)
		return
	}
	srcHash, _ := src.GetHash()
	fmt.Println("lookup:", value)
	fmt.Println("hash:", hash)
	fmt.Println("replicas agree:", hash == srcHash)
	// Output:
	// lookup: 42
	// hash: 42
	// replicas agree: true
}

// memDisk is a small in-memory DiskStateMachine for the cache example. A
// command "k=v" sets k; a command "k?=v" sets k only when it is absent.
// Result.Value is 1 when the store changed and 0 when it declined.
type memDisk struct {
	mu   sync.RWMutex
	data map[string]string
}

func newMemDisk() *memDisk { return &memDisk{data: make(map[string]string)} }

func (d *memDisk) Open(ctx context.Context, initialDir string, stopper <-chan struct{}) (uint64, error) {
	return 0, nil
}

func (d *memDisk) Update(ctx context.Context, entries []sm.Entry, results []sm.Result) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	for i, e := range entries {
		key, value, conditional, ok := parseMemCmd(e.Cmd)
		if !ok {
			results[i] = sm.Result{Value: sm.ResultInvalidCommand}
			continue
		}
		if _, exists := d.data[key]; conditional && exists {
			results[i] = sm.Result{Value: 0}
			continue
		}
		d.data[key] = value
		results[i] = sm.Result{Value: 1}
	}
	return nil
}

func (d *memDisk) Lookup(ctx context.Context, query interface{}) (interface{}, error) {
	key, ok := query.(string)
	if !ok {
		return nil, errors.New("memDisk: query must be a string")
	}
	d.mu.RLock()
	defer d.mu.RUnlock()
	v, ok := d.data[key]
	if !ok {
		return nil, nil
	}
	return []byte(v), nil
}

func (d *memDisk) Sync() error { return nil }

func (d *memDisk) PrepareSnapshot() (interface{}, error) { return nil, nil }

func (d *memDisk) SaveSnapshot(ctx context.Context, snapshotCtx interface{}, w io.Writer, stopper <-chan struct{}) error {
	return nil
}

func (d *memDisk) RecoverFromSnapshot(ctx context.Context, r io.Reader, stopper <-chan struct{}) error {
	return nil
}

func (d *memDisk) Close(ctx context.Context) error { return nil }

// parseMemCmd decodes "k=v" (unconditional) or "k?=v" (put-if-absent).
func parseMemCmd(cmd []byte) (key, value string, conditional, ok bool) {
	s := string(cmd)
	if k, v, found := strings.Cut(s, "?="); found && k != "" {
		return k, v, true, true
	}
	if k, v, found := strings.Cut(s, "="); found && k != "" {
		return k, v, false, true
	}
	return "", "", false, false
}

// extractKV takes the key and value from the COMMAND bytes.
func extractKV(cmd []byte) (string, []byte) {
	key, value, _, ok := parseMemCmd(cmd)
	if !ok {
		return "", nil
	}
	return key, []byte(value)
}

// entryApplied reports whether the store actually changed: a declined
// put-if-absent must not reach the cache.
func entryApplied(cmd []byte, result sm.Result) bool { return result.Value == 1 }

// ExampleNewCachedDiskStateMachine builds the guide's cache over a small
// in-memory DiskStateMachine, reads a value back from the cache, shows that
// a declined conditional write is not cached, and prints the errors an
// invalid CacheConfig returns.
func ExampleNewCachedDiskStateMachine() {
	ctx := context.Background()
	inner := newMemDisk()
	cached, err := sm.NewCachedDiskStateMachine(inner, sm.CacheConfig{
		MaxEntries:   100000,
		ExtractKV:    extractKV,    // key and value from the COMMAND bytes
		EntryApplied: entryApplied, // did this entry actually change the store?
	})
	if err != nil {
		fmt.Println("new:", err)
		return
	}

	apply := func(cmd string) {
		results := make([]sm.Result, 1)
		if err := cached.Update(ctx, []sm.Entry{{Index: 1, Cmd: []byte(cmd)}}, results); err != nil {
			fmt.Println("update:", err)
			return
		}
		fmt.Printf("%s -> applied=%t\n", cmd, results[0].Value == 1)
	}
	read := func() {
		v, err := cached.Lookup(ctx, "color")
		if err != nil {
			fmt.Println("lookup:", err)
			return
		}
		b, _ := v.([]byte)
		hits, misses := cached.CacheStats()
		fmt.Printf("color=%s hits=%d misses=%d\n", b, hits, misses)
	}

	apply("color=blue")
	read()
	apply("color?=red") // put-if-absent: the store declines it
	read()

	_, err = sm.NewCachedDiskStateMachine(inner, sm.CacheConfig{MaxEntries: 0, ExtractKV: extractKV})
	var maxErr *sm.CacheMaxEntriesError
	fmt.Println(errors.As(err, &maxErr), err)

	_, err = sm.NewCachedDiskStateMachine(inner, sm.CacheConfig{MaxEntries: 10})
	var cfgErr *sm.InvalidCacheConfigError
	fmt.Println(errors.As(err, &cfgErr), err)
	// Output:
	// color=blue -> applied=true
	// color=blue hits=1 misses=0
	// color?=red -> applied=false
	// color=blue hits=1 misses=1
	// true sm: cache max entries must be > 0, got 0
	// true sm: invalid cache config: ExtractKV must not be nil
}
