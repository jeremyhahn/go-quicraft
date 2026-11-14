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

package registry

import (
	"errors"
	"fmt"
	"testing"
)

// TestCountAfterCompareAndRemoveOfAbsentEntry checks that CompareAndRemove on
// a registry that holds no entry for the key refuses, whatever address it is
// asked to match, and leaves Count at zero.
func TestCountAfterCompareAndRemoveOfAbsentEntry(t *testing.T) {
	r := NewRegistry()

	if r.CompareAndRemove(1, 2, "x:1") {
		t.Fatal("CompareAndRemove(1, 2, \"x:1\") on a fresh registry = true, want false")
	}
	if got := r.Count(); got != 0 {
		t.Fatalf("Count() after refused CompareAndRemove = %d, want 0", got)
	}
	if r.CompareAndRemove(1, 2, "") {
		t.Fatal("CompareAndRemove(1, 2, \"\") on a fresh registry = true, want false")
	}
	if got := r.Count(); got != 0 {
		t.Fatalf("Count() after refused CompareAndRemove with empty want = %d, want 0", got)
	}
}

// TestCountAfterMatchingCompareAndRemove checks that a CompareAndRemove whose
// want matches the stored address removes the entry, lowers Count, makes
// Resolve fail with ErrNodeNotFound, and that repeating it refuses without
// driving Count below zero.
func TestCountAfterMatchingCompareAndRemove(t *testing.T) {
	r := NewRegistry()
	r.Register(1, 2, "x:1")
	if got := r.Count(); got != 1 {
		t.Fatalf("Count() after Register = %d, want 1", got)
	}

	if !r.CompareAndRemove(1, 2, "x:1") {
		t.Fatal("matching CompareAndRemove = false, want true")
	}
	if got := r.Count(); got != 0 {
		t.Fatalf("Count() after matching CompareAndRemove = %d, want 0", got)
	}
	addr, err := r.Resolve(1, 2)
	if err == nil {
		t.Fatalf("Resolve after CompareAndRemove = (%q, nil), want an error", addr)
	}
	if !errors.Is(err, ErrNodeNotFound) {
		t.Fatalf("Resolve error %v does not match ErrNodeNotFound", err)
	}

	if r.CompareAndRemove(1, 2, "x:1") {
		t.Fatal("second identical CompareAndRemove = true, want false")
	}
	if got := r.Count(); got != 0 {
		t.Fatalf("Count() after second CompareAndRemove = %d, want 0", got)
	}
}

// TestCountAfterRegisterIfAbsentFollowingRemove checks that once Remove has
// deleted an entry RegisterIfAbsent stores a new address for the same key,
// reports that it stored it, and Count is one.
func TestCountAfterRegisterIfAbsentFollowingRemove(t *testing.T) {
	r := NewRegistry()

	if got, stored := r.RegisterIfAbsent(1, 2, "a:1"); got != "a:1" || !stored {
		t.Fatalf("first RegisterIfAbsent = (%q, %v), want (\"a:1\", true)", got, stored)
	}
	r.Remove(1, 2)
	if got := r.Count(); got != 0 {
		t.Fatalf("Count() after Remove = %d, want 0", got)
	}

	got, stored := r.RegisterIfAbsent(1, 2, "b:2")
	if got != "b:2" || !stored {
		t.Fatalf("RegisterIfAbsent after Remove = (%q, %v), want (\"b:2\", true)", got, stored)
	}
	if n := r.Count(); n != 1 {
		t.Fatalf("Count() after RegisterIfAbsent following Remove = %d, want 1", n)
	}
	addr, err := r.Resolve(1, 2)
	if err != nil || addr != "b:2" {
		t.Fatalf("Resolve = (%q, %v), want (\"b:2\", nil)", addr, err)
	}
}

// TestCountAfterResolveErrorWrapped checks that a Resolve miss still matches
// ErrNodeNotFound once wrapped, and that an unrelated error does not.
func TestCountAfterResolveErrorWrapped(t *testing.T) {
	r := NewRegistry()
	_, resolveErr := r.Resolve(1, 2)
	if resolveErr == nil {
		t.Fatal("Resolve on a fresh registry returned nil error")
	}
	if got := r.Count(); got != 0 {
		t.Fatalf("Count() after Resolve miss = %d, want 0", got)
	}

	if !errors.Is(fmt.Errorf("w: %w", resolveErr), ErrNodeNotFound) {
		t.Fatal("wrapped Resolve error does not match ErrNodeNotFound")
	}
	if errors.Is(errors.New("x"), ErrNodeNotFound) {
		t.Fatal("unrelated error matches ErrNodeNotFound")
	}
}
