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

package engine

import (
	"bytes"
	"errors"
	"io"
	"os"
	"path/filepath"
	"testing"

	"github.com/cespare/xxhash/v2"
	"github.com/golang/snappy"

	"github.com/jeremyhahn/go-quicraft/pkg/config"
)

// snapshotReaderPayloads returns an empty payload, a small one and one
// larger than a snappy block (64 KiB) so the stream spans several chunks.
func snapshotReaderPayloads() map[string][]byte {
	large := make([]byte, 3*65536+17)
	for i := range large {
		large[i] = byte(i*31 + i/7)
	}
	return map[string][]byte{
		"empty": {},
		"small": []byte("snapshot reader payload"),
		"large": large,
	}
}

// writeCompressed writes data through the writer matching c.
func writeCompressed(t *testing.T, c config.CompressionType, data []byte) []byte {
	t.Helper()
	var buf bytes.Buffer
	switch c {
	case config.NoCompression:
		buf.Write(data)
	case config.Snappy:
		w := snappy.NewBufferedWriter(&buf)
		if _, err := w.Write(data); err != nil {
			t.Fatalf("snappy write: %v", err)
		}
		if err := w.Close(); err != nil {
			t.Fatalf("snappy close: %v", err)
		}
	default:
		t.Fatalf("no writer for compression %d", c)
	}
	return buf.Bytes()
}

func TestDecompressingSnapshotReaderRoundTrip(t *testing.T) {
	for _, c := range []config.CompressionType{config.NoCompression, config.Snappy} {
		for name, data := range snapshotReaderPayloads() {
			t.Run(c.String()+"/"+name, func(t *testing.T) {
				encoded := writeCompressed(t, c, data)
				r, err := DecompressingSnapshotReader(bytes.NewReader(encoded), c)
				if err != nil {
					t.Fatalf("DecompressingSnapshotReader: %v", err)
				}
				got, err := io.ReadAll(r)
				if err != nil {
					t.Fatalf("read: %v", err)
				}
				if !bytes.Equal(got, data) {
					t.Fatalf("read back %d bytes, want %d unchanged", len(got), len(data))
				}
			})
		}
	}
}

func TestDecompressingSnapshotReaderNoCompressionReturnsSameReader(t *testing.T) {
	in := bytes.NewReader([]byte("x"))
	r, err := DecompressingSnapshotReader(in, config.NoCompression)
	if err != nil {
		t.Fatalf("DecompressingSnapshotReader: %v", err)
	}
	if r != io.Reader(in) {
		t.Fatalf("NoCompression returned %T %p, want the input reader %p", r, r, in)
	}
}

func TestDecompressingSnapshotReaderUnknownType(t *testing.T) {
	for _, c := range []config.CompressionType{config.Snappy + 1, 42, 255} {
		r, err := DecompressingSnapshotReader(bytes.NewReader([]byte("plain")), c)
		if r != nil {
			t.Fatalf("compression %d: reader %T, want nil", c, r)
		}
		if !errors.Is(err, ErrUnsupportedSnapshotCompression) {
			t.Fatalf("compression %d: err %v, want ErrUnsupportedSnapshotCompression", c, err)
		}
	}
}

func TestDecompressingSnapshotReaderSnappyRejectsRawBytes(t *testing.T) {
	raw := []byte("these bytes are not a snappy framed stream at all")
	r, err := DecompressingSnapshotReader(bytes.NewReader(raw), config.Snappy)
	if err != nil {
		t.Fatalf("DecompressingSnapshotReader: %v", err)
	}
	got, err := io.ReadAll(r)
	if err == nil {
		t.Fatalf("reading non-snappy bytes succeeded with %d bytes, want an error", len(got))
	}
	if bytes.Contains(got, raw[:8]) {
		t.Fatalf("reader yielded the raw bytes %q", got)
	}
}

func TestHashSnapshotReaderMatchesFileHash(t *testing.T) {
	dir := t.TempDir()
	for name, data := range snapshotReaderPayloads() {
		t.Run(name, func(t *testing.T) {
			sum, size, err := HashSnapshotReader(bytes.NewReader(data))
			if err != nil {
				t.Fatalf("HashSnapshotReader: %v", err)
			}
			if sum != xxhash.Sum64(data) {
				t.Fatalf("sum %x, want xxhash64 %x", sum, xxhash.Sum64(data))
			}
			if size != uint64(len(data)) {
				t.Fatalf("size %d, want %d", size, len(data))
			}
			path := filepath.Join(dir, name+".dat")
			if err := os.WriteFile(path, data, 0o600); err != nil {
				t.Fatalf("write file: %v", err)
			}
			fsum, fsize, err := hashSnapshotData(path)
			if err != nil {
				t.Fatalf("hashSnapshotData: %v", err)
			}
			if fsum != sum || fsize != size {
				t.Fatalf("file hash (%x, %d) differs from reader hash (%x, %d)", fsum, fsize, sum, size)
			}
		})
	}
}

// failingSnapshotReader yields n bytes, then fails with err.
type failingSnapshotReader struct {
	n   int
	err error
}

func (f *failingSnapshotReader) Read(p []byte) (int, error) {
	if f.n == 0 {
		return 0, f.err
	}
	k := min(len(p), f.n)
	for i := range p[:k] {
		p[i] = 'a'
	}
	f.n -= k
	return k, nil
}

func TestHashSnapshotReaderReadFailure(t *testing.T) {
	want := errors.New("disk read failed")
	sum, size, err := HashSnapshotReader(&failingSnapshotReader{n: 100000, err: want})
	if sum != 0 || size != 0 {
		t.Fatalf("got (%x, %d), want (0, 0) on a read failure", sum, size)
	}
	if !errors.Is(err, want) {
		t.Fatalf("err %v, want %v", err, want)
	}
}
