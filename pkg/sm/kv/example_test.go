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

package kv_test

import (
	"bytes"
	"context"
	"errors"
	"fmt"

	"github.com/jeremyhahn/go-quicraft/pkg/sm"
	"github.com/jeremyhahn/go-quicraft/pkg/sm/kv"
)

// ExampleNewMemoryCreateFunc creates a key-value state machine through its
// factory, applies a Put and a Delete, reads the result back, and restores
// a snapshot into a second replica.
func ExampleNewMemoryCreateFunc() {
	ctx := context.Background()
	create := kv.NewMemoryCreateFunc()
	store := create(1, 1).(sm.StateMachine)
	defer store.Close(ctx)

	putA, _ := kv.EncodePut([]byte("a"), []byte("apple"))
	putB, _ := kv.EncodePut([]byte("b"), []byte("banana"))
	delA, _ := kv.EncodeDelete([]byte("a"))
	entries := []sm.Entry{{Index: 1, Cmd: putA}, {Index: 2, Cmd: putB}, {Index: 3, Cmd: delA}}
	results := make([]sm.Result, len(entries))
	if err := store.Update(ctx, entries, results); err != nil {
		fmt.Println("update:", err)
		return
	}
	fmt.Println("put results:", results[0].Value, results[1].Value)

	val, err := store.Lookup(ctx, []byte("b"))
	fmt.Printf("b = %s, err = %v\n", val, err)
	var notFound *kv.KeyNotFoundError
	_, err = store.Lookup(ctx, "a")
	fmt.Println("a deleted:", errors.As(err, &notFound))

	var snap bytes.Buffer
	if err := store.SaveSnapshot(ctx, &snap, make(chan struct{})); err != nil {
		fmt.Println("save:", err)
		return
	}
	replica := create(1, 2).(sm.StateMachine)
	defer replica.Close(ctx)
	if err := replica.RecoverFromSnapshot(ctx, &snap, make(chan struct{})); err != nil {
		fmt.Println("recover:", err)
		return
	}
	keys, _ := replica.Lookup(ctx, kv.ListQuery{Prefix: ""})
	fmt.Println("replica keys:", keys)
	// Output:
	// put results: 5 6
	// b = banana, err = <nil>
	// a deleted: true
	// replica keys: [b]
}

// ExampleNewConcurrentCreateFunc runs the same round trip on the concurrent
// store, whose snapshot is taken in two steps: PrepareSnapshot captures the
// state, and SaveSnapshot streams it while updates may continue.
func ExampleNewConcurrentCreateFunc() {
	ctx := context.Background()
	create := kv.NewConcurrentCreateFunc()
	store := create(1, 1).(sm.ConcurrentStateMachine)
	defer store.Close(ctx)

	put, _ := kv.EncodePut([]byte("k"), []byte("before"))
	if err := store.Update(ctx, []sm.Entry{{Index: 1, Cmd: put}}, make([]sm.Result, 1)); err != nil {
		fmt.Println("update:", err)
		return
	}
	snapCtx, err := store.PrepareSnapshot()
	if err != nil {
		fmt.Println("prepare:", err)
		return
	}
	put, _ = kv.EncodePut([]byte("k"), []byte("after"))
	if err := store.Update(ctx, []sm.Entry{{Index: 2, Cmd: put}}, make([]sm.Result, 1)); err != nil {
		fmt.Println("update:", err)
		return
	}

	var snap bytes.Buffer
	if err := store.SaveSnapshot(ctx, snapCtx, &snap, make(chan struct{})); err != nil {
		fmt.Println("save:", err)
		return
	}
	replica := create(1, 2).(sm.ConcurrentStateMachine)
	defer replica.Close(ctx)
	if err := replica.RecoverFromSnapshot(ctx, &snap, make(chan struct{})); err != nil {
		fmt.Println("recover:", err)
		return
	}
	current, _ := store.Lookup(ctx, "k")
	restored, _ := replica.Lookup(ctx, "k")
	fmt.Printf("current = %s, restored = %s\n", current, restored)
	// Output:
	// current = after, restored = before
}
