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

// Package golden checks encoded bytes against the golden format fixtures
// checked in under a package's testdata/golden directory.
//
// A fixture is the exact byte string a pinned layout encodes from a fixed
// input. Every package that owns a pinned layout tests both directions against
// its fixtures: decoding a fixture yields the input field for field, and
// encoding the input reproduces the fixture byte for byte. Because every
// format version is pinned until v1.0.0, these fixtures are what makes a
// layout change visible: it cannot land without a fixture diff in the same
// commit.
//
// Running a package's tests with -update-golden rewrites its fixtures from the
// current encoders. CI never passes the flag; a fixture is regenerated only
// for a deliberate layout change, and the diff is reviewed with it.
package golden

import (
	"bytes"
	"flag"
	"os"
	"path/filepath"
	"testing"
)

// Dir is the directory, relative to a package's test working directory, that
// holds its fixtures.
const Dir = "testdata/golden"

var update = flag.Bool("update-golden", false,
	"rewrite the golden format fixtures under testdata/golden from the current encoders")

// Updating reports whether this test run regenerates fixtures.
func Updating() bool { return *update }

// Path returns the path of the named fixture.
func Path(name string) string { return filepath.Join(Dir, name) }

// Load returns the named fixture's bytes, failing tb if it cannot be read.
func Load(tb testing.TB, name string) []byte {
	tb.Helper()
	b, err := os.ReadFile(Path(name))
	if err != nil {
		tb.Fatalf("golden fixture %s: %v (regenerate with -update-golden only for a deliberate layout change)", name, err)
	}
	return b
}

// Check compares got with the named fixture and fails tb on any difference.
// With -update-golden it writes got as the fixture instead.
func Check(tb testing.TB, name string, got []byte) {
	tb.Helper()
	if *update {
		if err := os.MkdirAll(Dir, 0o755); err != nil {
			tb.Fatalf("golden fixture %s: %v", name, err)
		}
		if err := os.WriteFile(Path(name), got, 0o644); err != nil {
			tb.Fatalf("golden fixture %s: %v", name, err)
		}
		return
	}
	want := Load(tb, name)
	if !bytes.Equal(got, want) {
		tb.Fatalf("golden fixture %s: the encoder no longer reproduces the pinned layout\n"+
			" at offset %d (got %d bytes, want %d)\n got  %x\n want %x\n"+
			"A layout change takes a new payload-type byte or record-level "+
			"discriminator and regenerates this fixture in the same commit.",
			name, firstDiff(got, want), len(got), len(want), got, want)
	}
}

// firstDiff returns the offset of the first byte at which a and b differ.
func firstDiff(a, b []byte) int {
	n := min(len(a), len(b))
	for i := range n {
		if a[i] != b[i] {
			return i
		}
	}
	return n
}
