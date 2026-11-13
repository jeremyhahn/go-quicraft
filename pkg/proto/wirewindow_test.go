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

package proto

import (
	"errors"
	"math"
	"testing"
)

// TestWireWindowAcceptsOnlyItsTwoVersions pins Accepts to the closed set
// {Current, Previous}: it never widens to a range or admits zero.
func TestWireWindowAcceptsOnlyItsTwoVersions(t *testing.T) {
	windows := map[string]WireWindow{
		"default":     DefaultWireWindow(),
		"two-version": {Current: 3, Previous: 2},
	}
	for name, w := range windows {
		t.Run(name, func(t *testing.T) {
			for _, v := range []uint64{w.Current, w.Previous} {
				if !w.Accepts(v) {
					t.Errorf("Accepts(%d) = false on %+v, want true", v, w)
				}
			}
			for _, v := range []uint64{0, w.Current + 1, math.MaxUint64} {
				if w.Accepts(v) {
					t.Errorf("Accepts(%d) = true on %+v, want false", v, w)
				}
			}
		})
	}
}

// TestEffectiveWireVersion pins the unswitched value (zero) to
// PreviousWireVersion and every other value to itself.
func TestEffectiveWireVersion(t *testing.T) {
	if got := EffectiveWireVersion(0); got != PreviousWireVersion {
		t.Errorf("EffectiveWireVersion(0) = %d, want PreviousWireVersion %d",
			got, PreviousWireVersion)
	}
	for _, v := range []uint64{1, 2, 3, WireVersion, math.MaxUint64} {
		if got := EffectiveWireVersion(v); got != v {
			t.Errorf("EffectiveWireVersion(%d) = %d, want %d", v, got, v)
		}
	}
}

// TestWireWindowCheckCommitNeverReverts pins CheckCommit on a two-version
// window: an in-window forward or same-version commit passes; a revert and
// any out-of-window version are ErrUnsupportedVersion.
func TestWireWindowCheckCommitNeverReverts(t *testing.T) {
	w := WireWindow{Current: 3, Previous: 2}
	tests := []struct {
		cur, v uint64
		want   error
	}{
		{0, 2, nil},
		{0, 3, nil},
		{2, 3, nil},
		{3, 3, nil},
		{3, 2, ErrUnsupportedVersion}, // revert
		{0, 4, ErrUnsupportedVersion}, // above the window
		{0, 1, ErrUnsupportedVersion}, // below the window
	}
	for _, tt := range tests {
		err := w.CheckCommit(tt.cur, tt.v)
		if tt.want == nil {
			if err != nil {
				t.Errorf("CheckCommit(%d, %d) = %v, want nil", tt.cur, tt.v, err)
			}
			continue
		}
		if !errors.Is(err, tt.want) {
			t.Errorf("CheckCommit(%d, %d) = %v, want %v", tt.cur, tt.v, err, tt.want)
		}
	}
}

// TestDefaultWireWindowAndAcceptsWireVersion pins the build's window to the
// two constants and AcceptsWireVersion to that window.
func TestDefaultWireWindowAndAcceptsWireVersion(t *testing.T) {
	w := DefaultWireWindow()
	want := WireWindow{Current: WireVersion, Previous: PreviousWireVersion}
	if w != want {
		t.Fatalf("DefaultWireWindow() = %+v, want %+v", w, want)
	}
	for _, v := range []uint64{0, 1, 2, 3, WireVersion, PreviousWireVersion,
		WireVersion + 1, math.MaxUint64} {
		if got, exp := AcceptsWireVersion(v), w.Accepts(v); got != exp {
			t.Errorf("AcceptsWireVersion(%d) = %v, DefaultWireWindow().Accepts = %v",
				v, got, exp)
		}
	}
}

// TestEntryIsInitialMembership pins IsInitialMembership to the type bits,
// ignoring the compression flag.
func TestEntryIsInitialMembership(t *testing.T) {
	tests := []struct {
		name string
		typ  uint64
		want bool
	}{
		{"initial membership", EntryInitialMembership, true},
		{"initial membership compressed", EntryInitialMembership | EntryCompressFlag, true},
		{"normal", EntryNormal, false},
		{"normal compressed", EntryNormal | EntryCompressFlag, false},
		{"config change", EntryConfigChange, false},
		{"metadata", EntryMetadata, false},
	}
	for _, tt := range tests {
		e := Entry{Type: tt.typ}
		if got := e.IsInitialMembership(); got != tt.want {
			t.Errorf("%s: IsInitialMembership() = %v, want %v", tt.name, got, tt.want)
		}
	}
}
