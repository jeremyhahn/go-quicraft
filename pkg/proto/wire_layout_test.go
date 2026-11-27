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
	"encoding/binary"
	"encoding/hex"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
)

// updateWireLayout rewrites testdata/wire_layout.golden from this build.
var updateWireLayout = flag.Bool("update", false, "rewrite testdata/wire_layout.golden from the current wire layout")

const wireLayoutGolden = "testdata/wire_layout.golden"

// wireFixture is one encoded value the wire layout fingerprint covers.
type wireFixture struct {
	name  string
	value interface {
		Size() int
		MarshalTo(buf []byte) (int, error)
	}
}

// wireFingerprintFixtures returns one MessageBatch and one SnapshotChunk that
// together set every serialized field of every type the wire carries
// (Message, Entry, Snapshot, Membership with its maps and session policy,
// SnapshotFile) to a distinct non-zero value. Message.Origin stays zero: it
// never crosses the wire. TestWireFingerprintFixturesComplete enforces this.
func wireFingerprintFixtures() []wireFixture {
	membership := func(base uint64) Membership {
		return Membership{
			ConfigChangeID:             base + 1,
			MaxWitnesses:               base + 2,
			RequireFullReplicaMajority: true,
			SessionExpiryEntries:       base + 3,
			MaxTotalSessions:           base + 4,
			MaxResponsesPerSession:     base + 5,
			MaxSessionsPerOwner:        base + 6,
			WireVersion:                WireVersion,
			Addresses:                  map[uint64]string{base + 11: "addr-a:1", base + 12: "addr-b:2"},
			Observers:                  map[uint64]string{base + 13: "obs-c:3"},
			Witnesses:                  map[uint64]string{base + 14: "wit-d:4"},
			Removed:                    map[uint64]bool{base + 15: true},
			Identities:                 map[uint64]string{base + 11: "id-a", base + 12: "id-b"},
			RemovedIdentities:          map[uint64]string{base + 15: "id-e"},
		}
	}
	batch := &MessageBatch{
		BinVer:        WireVersion,
		DeploymentID:  101,
		SourceEpoch:   102,
		MaxBinVer:     WireVersion,
		SourceAddress: "source:9",
		Requests: []Message{{
			Type:      Replicate,
			From:      111,
			To:        112,
			ShardID:   113,
			ReplicaID: 114,
			Term:      115,
			LogTerm:   116,
			LogIndex:  117,
			Commit:    118,
			Reject:    true,
			Hint:      119,
			Snapshot: Snapshot{
				ShardID:     121,
				ReplicaID:   122,
				Index:       123,
				Term:        124,
				Membership:  membership(200),
				Filepath:    "snap/path",
				FileSize:    125,
				OnDiskIndex: 126,
				Epoch:       127,
				Compression: 4,
			},
			Entries: []Entry{
				{Term: 131, Index: 132, Type: EntryConfigChange, Key: 133, ClientID: 134,
					SeriesID: 135, RespondedTo: 136, OwnerID: 137, Cmd: []byte("cmd-one")},
				{Term: 141, Index: 142, Type: EntryEncodedEntry | EntryCompressFlag, Key: 143, ClientID: 144,
					SeriesID: 145, RespondedTo: 146, OwnerID: 147, Cmd: []byte("cmd-two")},
			},
		}},
	}
	data := []byte("chunk-data-bytes")
	chunk := &SnapshotChunk{
		ShardID:        301,
		ReplicaID:      302,
		From:           303,
		Index:          304,
		Term:           305,
		ChunkID:        306,
		ChunkCount:     307,
		ChunkSize:      uint64(len(data)),
		FileSize:       308,
		Filepath:       "chunk/path",
		FileChunkID:    309,
		FileChunkCount: 310,
		HasFileInfo:    true,
		FileInfo: SnapshotFile{
			Filepath: "file/path",
			FileSize: 311,
			FileID:   312,
			Metadata: []byte("file-meta"),
		},
		Membership:   membership(400),
		OnDiskIndex:  313,
		DeploymentID: 314,
		BinVer:       WireVersion,
		Epoch:        315,
		Data:         data,
	}
	return []wireFixture{{"MessageBatch", batch}, {"SnapshotChunk", chunk}}
}

// wireLayoutGoldenContent is the golden file this build would write.
func wireLayoutGoldenContent() string {
	return fmt.Sprintf("# Raft wire layout fingerprint: SHA-256 of the encoded MessageBatch and\n"+
		"# SnapshotChunk fixtures in wire_layout_test.go. Regenerate with\n"+
		"#   go test ./pkg/proto/ -run TestWireLayoutGolden -update\n"+
		"# and, from v1.0.0-rc.1 on, bump WireVersion in the same release when it moves.\n"+
		"version %d\nderivation encoded-fixtures\nsha256 %s\n", WireVersion, wireFingerprintWith(nil))
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

// TestWireFingerprintFixturesComplete: every exported field of every struct,
// map and slice reachable from the fixtures is non-zero, and the uint64 leaves
// are pairwise distinct, so a swap of any two fields moves the encoded bytes.
func TestWireFingerprintFixturesComplete(t *testing.T) {
	// Not serialized, so it must stay zero and is not required to be set.
	excluded := map[string]string{
		"Message.Origin": "never crosses the wire; set only on the receive or inject path",
	}
	// Serialized but pinned to WireVersion, so exempt from distinctness.
	pinned := map[string]string{
		"MessageBatch.BinVer":    "senders stamp WireVersion",
		"MessageBatch.MaxBinVer": "advertises this build's WireWindow.Current",
		"SnapshotChunk.BinVer":   "receivers require exact WireVersion equality",
		"Membership.WireVersion": "the committed shard transport version",
	}
	leaves := make(map[uint64]string)
	var walk func(path string, v reflect.Value)
	walk = func(path string, v reflect.Value) {
		switch v.Kind() {
		case reflect.Pointer:
			walk(path, v.Elem())
		case reflect.Struct:
			typ := v.Type()
			for i := range typ.NumField() {
				f := typ.Field(i)
				if !f.IsExported() {
					continue
				}
				key := typ.Name() + "." + f.Name
				fv := v.Field(i)
				fpath := path + "." + f.Name
				if _, ok := excluded[key]; ok {
					if !fv.IsZero() {
						t.Errorf("%s: excluded field is set", fpath)
					}
					continue
				}
				if fv.IsZero() {
					t.Errorf("%s: zero; every serialized field must be set", fpath)
					continue
				}
				if fv.Kind() == reflect.Uint64 {
					if _, ok := pinned[key]; ok {
						if fv.Uint() != WireVersion {
							t.Errorf("%s = %d, want WireVersion", fpath, fv.Uint())
						}
						continue
					}
					if prev, dup := leaves[fv.Uint()]; dup {
						t.Errorf("%s and %s both hold %d", prev, fpath, fv.Uint())
					}
					leaves[fv.Uint()] = fpath
					continue
				}
				walk(fpath, fv)
			}
		case reflect.Slice:
			if v.Type().Elem().Kind() == reflect.Uint8 {
				return
			}
			for i := range v.Len() {
				walk(fmt.Sprintf("%s[%d]", path, i), v.Index(i))
			}
		case reflect.Map:
			for _, k := range v.MapKeys() {
				walk(fmt.Sprintf("%s[%v]", path, k), v.MapIndex(k))
			}
		}
	}
	for _, f := range wireFingerprintFixtures() {
		walk(f.name, reflect.ValueOf(f.value))
	}
}

// TestWireFingerprintIgnoresOrigin: Message.Origin is not serialized, so
// setting it leaves the fingerprint unchanged.
func TestWireFingerprintIgnoresOrigin(t *testing.T) {
	fixtures := wireFingerprintFixtures()
	batch, ok := fixtures[0].value.(*MessageBatch)
	if !ok {
		t.Fatalf("fixture 0 is %T, want *MessageBatch", fixtures[0].value)
	}
	batch.Requests[0].Origin = OriginLocal
	if got, want := wireFingerprintOf(fixtures, nil), wireFingerprintWith(nil); got != want {
		t.Fatalf("fingerprint moved with Origin set: %s != %s", got, want)
	}
}

// wireFingerprintWith returns the wire layout fingerprint, applying mutate to
// each fixture's encoded bytes first when it is non-nil.
func wireFingerprintWith(mutate func(name string, b []byte) []byte) string {
	return wireFingerprintOf(wireFingerprintFixtures(), mutate)
}

// wireFingerprintOf hashes, per fixture in order, its name, its encoded
// length and its MarshalTo bytes (after mutate when it is non-nil).
func wireFingerprintOf(fixtures []wireFixture, mutate func(name string, b []byte) []byte) string {
	h := sha256.New()
	for _, f := range fixtures {
		buf := make([]byte, f.value.Size())
		n, err := f.value.MarshalTo(buf)
		if err != nil {
			panic(fmt.Sprintf("wire fingerprint fixture %s: %v", f.name, err))
		}
		b := buf[:n]
		if mutate != nil {
			b = mutate(f.name, b)
		}
		var hdr [8]byte
		binary.BigEndian.PutUint64(hdr[:], uint64(len(b)))
		h.Write([]byte(f.name))
		h.Write([]byte{0})
		h.Write(hdr[:])
		h.Write(b)
	}
	return hex.EncodeToString(h.Sum(nil))
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
