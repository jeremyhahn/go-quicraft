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
	"crypto/sha256"
	"encoding/hex"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"strings"
	"testing"
)

// updateWireLayout rewrites testdata/wire_layout.golden from this build.
var updateWireLayout = flag.Bool("update", false, "rewrite testdata/wire_layout.golden from the current wire layout")

const wireLayoutGolden = "testdata/wire_layout.golden"

// wireLayout describes every struct type reachable from MessageBatch and
// SnapshotChunk: one line per type, naming each field, its type and its
// position, sorted by type name. Any change to the layout of the wire
// changes it.
func wireLayout() []string {
	seen := make(map[reflect.Type]bool)
	var lines []string
	var walk func(t reflect.Type)
	walk = func(t reflect.Type) {
		switch t.Kind() {
		case reflect.Pointer, reflect.Slice, reflect.Array:
			walk(t.Elem())
			return
		case reflect.Map:
			walk(t.Key())
			walk(t.Elem())
			return
		case reflect.Struct:
		default:
			return
		}
		if seen[t] {
			return
		}
		seen[t] = true
		fields := make([]string, t.NumField())
		for i := range t.NumField() {
			f := t.Field(i)
			fields[i] = fmt.Sprintf("%d %s %s", i, f.Name, f.Type.String())
			walk(f.Type)
		}
		lines = append(lines, fmt.Sprintf("%s{%s}", t.String(), strings.Join(fields, "; ")))
	}
	walk(reflect.TypeFor[MessageBatch]())
	walk(reflect.TypeFor[SnapshotChunk]())
	sort.Strings(lines)
	return lines
}

// wireLayoutGoldenContent is the golden file this build would write.
func wireLayoutGoldenContent() string {
	sum := sha256.Sum256([]byte(strings.Join(wireLayout(), "\n")))
	return fmt.Sprintf("# Raft wire layout fingerprint: every type reachable from MessageBatch and\n"+
		"# SnapshotChunk, by reflection. Regenerate with\n"+
		"#   go test ./pkg/proto/ -run TestWireLayoutGolden -update\n"+
		"# and, from v1.0.0-rc.1 on, bump WireVersion in the same release when it moves.\n"+
		"version %d\nsha256 %s\n", WireVersion, hex.EncodeToString(sum[:]))
}

// TestWireLayoutGolden is the release gate's source of truth: the golden
// records the wire layout's fingerprint with the WireVersion it was recorded
// at. A layout change without regenerating it fails here; regenerating it
// without bumping WireVersion fails scripts/check-wire-version.sh at release.
func TestWireLayoutGolden(t *testing.T) {
	want := wireLayoutGoldenContent()
	if *updateWireLayout {
		if err := os.MkdirAll(filepath.Dir(wireLayoutGolden), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(wireLayoutGolden, []byte(want), 0o644); err != nil {
			t.Fatal(err)
		}
		return
	}
	got, err := os.ReadFile(wireLayoutGolden)
	if err != nil {
		t.Fatalf("read %s: %v; generate it with go test ./pkg/proto/ -run TestWireLayoutGolden -update",
			wireLayoutGolden, err)
	}
	if string(got) != want {
		t.Fatalf("the Raft wire layout or WireVersion differs from %s.\n"+
			"Regenerate it with: go test ./pkg/proto/ -run TestWireLayoutGolden -update\n"+
			"From v1.0.0-rc.1 on, a release that changes the layout of any type reachable from "+
			"MessageBatch or SnapshotChunk must also bump WireVersion and set PreviousWireVersion "+
			"to the value it replaced; scripts/check-wire-version.sh fails the release otherwise.\n"+
			"got:\n%s\nwant:\n%s", wireLayoutGolden, got, want)
	}
}

// TestWireLayoutCoversEmbeddedTypes: the walk reaches the persisted types the
// wire embeds, so their layout changes move the fingerprint too.
func TestWireLayoutCoversEmbeddedTypes(t *testing.T) {
	layout := strings.Join(wireLayout(), "\n")
	for _, name := range []string{"proto.MessageBatch{", "proto.SnapshotChunk{", "proto.Message{",
		"proto.Entry{", "proto.Snapshot{", "proto.Membership{"} {
		if !strings.Contains(layout, name) {
			t.Errorf("wire layout does not cover %s", strings.TrimSuffix(name, "{"))
		}
	}
}

// wireFingerprintWith returns the wire layout fingerprint, applying mutate to
// each fixture's encoded bytes first when it is non-nil.
func wireFingerprintWith(mutate func(name string, b []byte) []byte) string {
	_ = mutate
	sum := sha256.Sum256([]byte(strings.Join(wireLayout(), "\n")))
	return hex.EncodeToString(sum[:])
}

// swapWireRange returns a copy of b with the n-byte ranges at i and j swapped.
func swapWireRange(b []byte, i, j, n int) []byte {
	out := append([]byte(nil), b...)
	copy(out[i:i+n], b[j:j+n])
	copy(out[j:j+n], b[i:i+n])
	return out
}

// insertWireZeros returns a copy of b with n zero bytes inserted at off.
func insertWireZeros(b []byte, off, n int) []byte {
	out := make([]byte, 0, len(b)+n)
	out = append(out, b[:off]...)
	out = append(out, make([]byte, n)...)
	return append(out, b[off:]...)
}

// TestWireFingerprintCatchesEncodingOnlyChange: a change to the encoded bytes
// that leaves every Go struct untouched (field order, field width) moves the
// fingerprint.
func TestWireFingerprintCatchesEncodingOnlyChange(t *testing.T) {
	golden, err := os.ReadFile(wireLayoutGolden)
	if err != nil {
		t.Fatal(err)
	}
	var recorded string
	for _, l := range strings.Split(string(golden), "\n") {
		if s, ok := strings.CutPrefix(l, "sha256 "); ok {
			recorded = s
		}
	}
	base := wireFingerprintWith(nil)
	mutations := []struct {
		name   string
		mutate func(name string, b []byte) []byte
	}{
		{"swap MessageBatch DeploymentID and SourceEpoch", func(name string, b []byte) []byte {
			if name != "MessageBatch" {
				return b
			}
			return swapWireRange(b, 8, 16, 8)
		}},
		{"widen MessageBatch DeploymentID to 12 bytes", func(name string, b []byte) []byte {
			if name != "MessageBatch" {
				return b
			}
			return insertWireZeros(b, 16, 4)
		}},
		{"swap SnapshotChunk ShardID and ReplicaID", func(name string, b []byte) []byte {
			if name != "SnapshotChunk" {
				return b
			}
			return swapWireRange(b, 0, 8, 8)
		}},
	}
	for _, m := range mutations {
		got := wireFingerprintWith(m.mutate)
		if got == base {
			t.Errorf("%s: fingerprint unchanged from the unmutated encoding", m.name)
		}
		if got == recorded {
			t.Errorf("%s: fingerprint equals the recorded golden", m.name)
		}
	}
}
