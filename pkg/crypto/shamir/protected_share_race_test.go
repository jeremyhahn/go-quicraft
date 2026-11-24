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

package shamir

import (
	"bytes"
	"encoding/json"
	"errors"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
)

// Read/Destroy race tests for ProtectedShare; meaningful under -race.

// protectedTestShare builds one share to exercise.
func protectedTestShare(t *testing.T) *ProtectedShare {
	t.Helper()
	shares, err := SplitProtected([]byte("a-secret-worth-splitting-32-byte"), 2, 3)
	if err != nil {
		t.Fatalf("SplitProtected: %v", err)
	}
	// Destroy the ones this test does not drive, so the enclave count stays
	// bounded — locked memory is an RLIMIT_MEMLOCK resource.
	for _, s := range shares[1:] {
		s.Destroy()
	}
	return shares[0]
}

// TestProtectedShare_WithValueExcludesDestroy asserts no callback runs after Destroy returns.
func TestProtectedShare_WithValueExcludesDestroy(t *testing.T) {
	const iterations = 100

	for i := 0; i < iterations; i++ {
		share := protectedTestShare(t)
		want := valueCopy(share)
		if len(want) == 0 {
			t.Fatal("share value is empty")
		}

		var (
			destroyReturned atomic.Bool
			violations      atomic.Int64
			wg              sync.WaitGroup
		)
		start := make(chan struct{})

		for r := 0; r < 4; r++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				<-start
				for k := 0; k < 25; k++ {
					_ = share.WithValue(func(v []byte) error {
						if !bytes.Equal(v, want) {
							t.Errorf("WithValue saw %q, want %q", v, want)
						}
						for spin := 0; spin < 64; spin++ {
							_ = len(v)
							runtime.Gosched()
						}
						if destroyReturned.Load() {
							violations.Add(1)
						}
						return nil
					})
				}
			}()
		}

		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			share.Destroy()
			destroyReturned.Store(true)
		}()

		close(start)
		wg.Wait()

		if got := violations.Load(); got != 0 {
			t.Fatalf("callback ran %d time(s) after Destroy had already returned; "+
				"the share was torn down while a reader held its value", got)
		}

		share.Destroy()
		ran := false
		if err := share.WithValue(func([]byte) error {
			ran = true
			return nil
		}); err == nil || ran {
			t.Fatalf("WithValue after Destroy: err = %v, callback ran = %v; want a refusal without a call", err, ran)
		}
	}
}

// TestProtectedShare_WithValueNilReceiver keeps WithValue consistent with
// Destroy, which is nil-safe.
func TestProtectedShare_WithValueNilReceiver(t *testing.T) {
	var share *ProtectedShare

	called := false
	err := share.WithValue(func([]byte) error {
		called = true
		return nil
	})
	if called {
		t.Fatal("callback ran on a nil share")
	}
	if err == nil {
		t.Fatal("WithValue on a nil share returned nil error")
	}
}

// TestCombineProtected_RacesShareDestroy runs CombineProtected against concurrent share Destroy under -race.
func TestCombineProtected_RacesShareDestroy(t *testing.T) {
	const iterations = 200

	secret := []byte("a-secret-worth-splitting-32-byte")

	for i := 0; i < iterations; i++ {
		shares, err := SplitProtected(secret, 2, 3)
		if err != nil {
			t.Fatalf("SplitProtected: %v", err)
		}

		var wg sync.WaitGroup

		wg.Add(1)
		go func() {
			defer wg.Done()
			out, err := CombineProtected(shares[:2])
			if err == nil {
				// When reconstruction wins, it must produce the real secret —
				// a torn read that still returned nil error would be the worst
				// outcome, so check rather than discard.
				if got := string(encCopy(out)); got != string(secret) {
					t.Errorf("reconstructed %q, want %q", got, string(secret))
				}
				out.Destroy()
			}
		}()

		// Tear one share down underneath the reconstruction.
		wg.Add(1)
		go func() {
			defer wg.Done()
			shares[1].Destroy()
		}()

		wg.Wait()
		for _, s := range shares {
			s.Destroy()
		}
	}
}

// MarshalJSON alias lifetime.

// TestProtectedShare_MarshalJSONRacesDestroy races MarshalJSON against Destroy with a large share; probabilistic and only meaningful under -race.
func TestProtectedShare_MarshalJSONRacesDestroy(t *testing.T) {
	const (
		iterations = 200
		// Large enough to make the append the dominant cost of MarshalJSON,
		// small enough that a 3-share set stays far inside MaxShareSetBytes.
		secretBytes = 32 << 10
	)

	secret := make([]byte, secretBytes)
	for i := range secret {
		secret[i] = byte(i)
	}

	for i := 0; i < iterations; i++ {
		shares, err := SplitProtected(secret, 2, 3)
		if err != nil {
			t.Fatalf("SplitProtected: %v", err)
		}
		for _, s := range shares[1:] {
			s.Destroy()
		}
		share := shares[0]
		want := valueCopy(share)

		var (
			wg       sync.WaitGroup
			out      []byte
			marshErr error
		)
		start := make(chan struct{})

		wg.Add(2)
		go func() {
			defer wg.Done()
			<-start
			out, marshErr = share.MarshalJSON()
		}()
		go func() {
			defer wg.Done()
			<-start
			share.Destroy()
		}()
		close(start)
		wg.Wait()

		if marshErr != nil {
			// Losing outright is legitimate, but only with this exact reason.
			var validationErr *ValidationError
			if !errors.As(marshErr, &validationErr) || validationErr.Reason != "share has been destroyed" {
				t.Fatalf("iteration %d: MarshalJSON failed with %v, want the destroyed-share ValidationError", i, marshErr)
			}
			continue
		}

		// It succeeded, so it must have produced the complete, correct document
		// — the value written verbatim into the JSON string literal.
		var decoded ProtectedShare
		if uerr := json.Unmarshal(out, &decoded); uerr != nil {
			t.Fatalf("iteration %d: MarshalJSON produced undecodable JSON (%d bytes): %v", i, len(out), uerr)
		}
		got := valueCopy(&decoded)
		decoded.Destroy()
		if !bytes.Equal(got, want) {
			t.Fatalf("iteration %d: marshalled value differs from the share value — torn read of the protected buffer", i)
		}
	}
}
