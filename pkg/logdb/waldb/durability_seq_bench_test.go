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

package waldb

import (
	"io/fs"
	"sync/atomic"
	"testing"

	"github.com/jeremyhahn/go-quicraft/pkg/logdb"
)

// Benchmarks for the cost of the durability sequence on the WAL write path.
//
// The sequence adds four bytes to every chunk header and four bytes to every
// chunk's CRC input, both on the hot path, so the cost is measured rather than
// argued about. They are written to be run against two builds — one with the
// field and one without — and compared, which is why they cover the framing in
// isolation as well as end to end:
//
//   - BenchmarkEncodeRecord*: the framing alone, with no I/O to hide it in.
//   - BenchmarkSegmentWriteHardState: one 48-byte record into the block buffer,
//     the shape a HardState-only SaveNodeState produces.
//   - BenchmarkSaveNodeState / BenchmarkSaveStateBatch: end to end through the
//     shard, including the fdatasync the writer actually pays.
//
// The last two also report wbytes/op — the bytes handed to WriteAt per
// operation — because write amplification, not CPU, is what the flush-watermark
// work in this area was about, and a framing change is exactly the sort of thing
// that could quietly give it back.

// countingFS wraps an FS and totals the bytes every WriteAt puts on the medium.
type countingFS struct {
	FS
	written atomic.Int64
}

func newCountingFS() *countingFS { return &countingFS{FS: defaultFS()} }

func (c *countingFS) OpenFile(name string, flag int, perm fs.FileMode) (File, error) {
	f, err := c.FS.OpenFile(name, flag, perm)
	if err != nil {
		return nil, err
	}
	return &countingFile{File: f, fs: c}, nil
}

type countingFile struct {
	File
	fs *countingFS
}

func (f *countingFile) WriteAt(p []byte, off int64) (int, error) {
	n, err := f.File.WriteAt(p, off)
	f.fs.written.Add(int64(n))
	return n, err
}

func (f *countingFile) Write(p []byte) (int, error) {
	n, err := f.File.Write(p)
	f.fs.written.Add(int64(n))
	return n, err
}

// benchEncodeRecord measures the framing alone at one payload size.
func benchEncodeRecord(b *testing.B, payloadLen int) {
	b.ReportAllocs()
	payload := make([]byte, payloadLen)
	for i := range payload {
		payload[i] = byte(i % 251)
	}
	buf := make([]byte, recordHeaderSize+payloadLen)
	var seq uint32

	for b.Loop() {
		if _, err := encodeRecord(buf, RecordFull, seq, 0, payload); err != nil {
			b.Fatalf("encodeRecord: %v", err)
		}
		seq++
	}
	b.SetBytes(int64(recordHeaderSize + payloadLen))
}

// BenchmarkEncodeRecordHardState frames the smallest record the WAL writes: a
// HardState payload of 48 bytes, where a four-byte header field is the largest
// relative overhead the change can have.
func BenchmarkEncodeRecordHardState(b *testing.B) { benchEncodeRecord(b, 48) }

// BenchmarkEncodeRecordEntryBatch frames an entry-bearing batch record, the
// other end of the size range the write path sees.
func BenchmarkEncodeRecordEntryBatch(b *testing.B) { benchEncodeRecord(b, 1408) }

// discardFile accepts block flushes and drops them. The segment write path is
// being measured, not the medium: a benchmark that let the flushes land would
// write gigabytes and end up measuring the filesystem, and one that capped the
// segment size would stop on ErrSegmentFull part-way through.
type discardFile struct{ File }

func (discardFile) WriteAt(p []byte, _ int64) (int, error) { return len(p), nil }
func (discardFile) Sync() error                            { return nil }
func (discardFile) Truncate(int64) error                   { return nil }
func (discardFile) Close() error                           { return nil }

// BenchmarkSegmentWriteHardState writes 48-byte records into the block buffer
// with no I/O at all, isolating the framing and block arithmetic.
func BenchmarkSegmentWriteHardState(b *testing.B) {
	b.ReportAllocs()
	const blockSize = 32 * 1024
	seg := &segment{
		f:           discardFile{},
		id:          1,
		blockSize:   blockSize,
		blockBuf:    make([]byte, blockSize),
		maxFileSize: 1 << 40,
	}

	record := make([]byte, 48)
	for i := range record {
		record[i] = byte(i % 251)
	}

	for b.Loop() {
		if _, err := seg.Write(record); err != nil {
			b.Fatalf("Write: %v", err)
		}
	}
}

// BenchmarkSaveNodeState is the HardState-only write path end to end: one small
// record and one fdatasync per operation, which is what a Raft node does on
// every term or vote change and on every commit advance it persists alone.
func BenchmarkSaveNodeState(b *testing.B) {
	b.ReportAllocs()
	cfs := newCountingFS()
	dir := b.TempDir()
	db, err := Open(dir, WithFS(cfs), WithBlockSize(32*1024), WithMaxFileSize(1024*1024*1024))
	if err != nil {
		b.Fatalf("Open: %v", err)
	}
	b.Cleanup(func() { db.Close() })

	start := cfs.written.Load()
	ops := 0
	term := uint64(1)
	for b.Loop() {
		if err := db.SaveNodeState(1, 1, logdb.State{Term: term, Vote: 1, Commit: term}); err != nil {
			b.Fatalf("SaveNodeState: %v", err)
		}
		term++
		ops++
	}
	b.ReportMetric(float64(cfs.written.Load()-start)/float64(ops), "wbytes/op")
}

// BenchmarkSaveStateBatch is the entry-bearing batch path end to end: ten
// 128-byte entries plus a HardState in one batch, one fdatasync.
func BenchmarkSaveStateBatch(b *testing.B) {
	b.ReportAllocs()
	cfs := newCountingFS()
	dir := b.TempDir()
	db, err := Open(dir, WithFS(cfs), WithBlockSize(32*1024), WithMaxFileSize(1024*1024*1024))
	if err != nil {
		b.Fatalf("Open: %v", err)
	}
	b.Cleanup(func() { db.Close() })

	entries := makeBenchEntries(1, 10, 1, 128)
	updates := []logdb.Update{{
		ShardID:   1,
		ReplicaID: 1,
		Entries:   entries,
		State:     logdb.State{Term: 1, Vote: 1, Commit: 10},
	}}

	start := cfs.written.Load()
	ops := 0
	idx := uint64(11)
	for b.Loop() {
		for i := range updates[0].Entries {
			updates[0].Entries[i].Index = idx + uint64(i)
		}
		updates[0].State.Commit = idx + 9
		idx += 10
		if err := db.SaveState(updates); err != nil {
			b.Fatalf("SaveState: %v", err)
		}
		ops++
	}
	b.ReportMetric(float64(cfs.written.Load()-start)/float64(ops), "wbytes/op")
}
