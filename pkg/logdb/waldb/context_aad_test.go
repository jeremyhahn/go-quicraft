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
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/jeremyhahn/go-quicraft/pkg/logdb"
	"github.com/jeremyhahn/go-quicraft/pkg/seal"
)

// aadTestBlockSize/aadTestMaxFileSize give the context-AAD tests a small,
// predictable segment geometry: multi-block records and segment rotation are
// both reachable with a handful of entries.
const (
	aadTestBlockSize   = 1024
	aadTestMaxFileSize = int64(32 * 1024)
)

// openAADTestDB opens an encrypted WAL with the small test geometry. noSync is
// left ON (fsync enabled is the default) so the recovery paths under test see
// real durable bytes.
func openAADTestDB(t *testing.T, dir string, barrier WALBarrier) *DB {
	t.Helper()
	db, err := Open(dir,
		WithBlockSize(aadTestBlockSize),
		WithMaxFileSize(aadTestMaxFileSize),
		WithBarrier(barrier),
	)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	return db
}

// walShardDir returns the on-disk directory of the WAL shard that serves the
// given Raft shard ID.
func walShardDir(root string, shardID uint64) string {
	return filepath.Join(root, shardDirName(int(shardID%DefaultShardCount)))
}

// readSegmentRecords returns the raw (still encrypted) logical records of a
// segment file, in the order recovery would replay them.
func readSegmentRecords(t *testing.T, dir string, segID uint64) [][]byte {
	t.Helper()
	seg := openSegmentForTest(t, dir, segID, aadTestBlockSize)
	defer seg.Close()
	records, _, err := seg.ReadAll()
	if err != nil {
		t.Fatalf("ReadAll(%s/%s): %v", dir, segmentFilename(segID), err)
	}
	return records
}

// rewriteSegment replaces the segment file at dir/segID with exactly the given
// raw records, reframed by the normal chunk encoder. It is the on-disk attacker
// in these tests: the CRCs are all valid and the barrier envelope bytes are
// untouched, so ONLY the context-AAD can notice the records moved.
func rewriteSegment(t *testing.T, dir string, segID uint64, records [][]byte) {
	t.Helper()
	if err := os.MkdirAll(dir, 0o700); err != nil {
		t.Fatalf("MkdirAll(%s): %v", dir, err)
	}
	seg, _, err := createSegment(defaultFS(), dir, segID, aadTestBlockSize, aadTestMaxFileSize)
	if err != nil {
		t.Fatalf("createSegment: %v", err)
	}
	for i, rec := range records {
		if _, err := seg.Write(rec); err != nil {
			t.Fatalf("rewrite record %d: %v", i, err)
		}
	}
	if err := seg.Sync(); err != nil {
		t.Fatalf("rewrite Sync: %v", err)
	}
	if err := seg.Close(); err != nil {
		t.Fatalf("rewrite Close: %v", err)
	}
}

// requireReopenRejects asserts that reopening the WAL at dir fails with a
// decryption error, i.e. the tampering under test was caught by the AEAD.
func requireReopenRejects(t *testing.T, dir string, barrier WALBarrier, what string) {
	t.Helper()
	db, err := Open(dir,
		WithBlockSize(aadTestBlockSize),
		WithMaxFileSize(aadTestMaxFileSize),
		WithBarrier(barrier),
	)
	if err == nil {
		_ = db.Close()
		t.Fatalf("%s: Open succeeded, want a decryption failure", what)
	}
	var decErr *WALDecryptError
	if !errors.As(err, &decErr) {
		t.Fatalf("%s: Open error = %v, want *WALDecryptError", what, err)
	}
}

// --- context layout ---

// TestWALRecordContextLayout pins the exact on-the-wire context bytes. The
// layout is a security contract shared by the encrypt and decrypt sides and by
// any future reader, so it is asserted literally rather than by round-trip.
func TestWALRecordContextLayout(t *testing.T) {
	var buf [walContextSize]byte
	got := walRecordContext(buf[:], 0x0102030405060708, 0x1112131415161718, 0x2122232425262728)

	if len(got) != 30 {
		t.Fatalf("context length = %d, want 30", len(got))
	}
	want := []byte{
		'Q', 'R', 'W', 'A', 'L', '1',
		0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08,
		0x11, 0x12, 0x13, 0x14, 0x15, 0x16, 0x17, 0x18,
		0x21, 0x22, 0x23, 0x24, 0x25, 0x26, 0x27, 0x28,
	}
	if !bytes.Equal(got, want) {
		t.Fatalf("context = % x\nwant       % x", got, want)
	}

	// Fixed-width big-endian with no delimiters: distinct positions must never
	// collide, including the adjacent-field shifts a delimited encoding would
	// alias.
	var a, b [walContextSize]byte
	if bytes.Equal(
		walRecordContext(a[:], 1, 0, 0),
		walRecordContext(b[:], 0, 1, 0),
	) {
		t.Fatal("shard 1/segment 0 collided with shard 0/segment 1")
	}
	if bytes.Equal(
		walRecordContext(a[:], 0, 1, 0),
		walRecordContext(b[:], 0, 0, 1),
	) {
		t.Fatal("segment 1/seq 0 collided with segment 0/seq 1")
	}
}

// TestWALRecordContextDoesNotAllocate guards the WAL write hot path: the AAD
// helper must reuse the caller's scratch buffer.
func TestWALRecordContextDoesNotAllocate(t *testing.T) {
	var buf [walContextSize]byte
	allocs := testing.AllocsPerRun(1000, func() {
		walRecordContext(buf[:], 3, 7, 11)
	})
	if allocs != 0 {
		t.Fatalf("walRecordContext allocs/op = %v, want 0", allocs)
	}
}

// TestCiphertextExpansionWithinBound pins maxCiphertextExpansion against a real
// seal.Barrier. writeRecord picks a record's landing segment from
// plaintext+maxCiphertextExpansion BEFORE sealing it, so an under-estimate
// would let a sealed record overflow the segment it is bound to.
func TestCiphertextExpansionWithinBound(t *testing.T) {
	barrier := newTestBarrier(t)
	for _, n := range []int{0, 1, 64, 4096, 70000} {
		ct, err := barrier.EncryptWithContext(nil, make([]byte, n), nil)
		if err != nil {
			t.Fatalf("EncryptWithContext(%d): %v", n, err)
		}
		expansion := len(ct) - n
		if expansion > maxCiphertextExpansion {
			t.Fatalf("plaintext %d: expansion %d exceeds maxCiphertextExpansion %d",
				n, expansion, maxCiphertextExpansion)
		}
		if expansion <= 0 {
			t.Fatalf("plaintext %d: expansion %d, want positive", n, expansion)
		}
	}
}

// --- round trips ---

// TestContextAADMultiChunkRecordRoundTrips proves the ordinal counts LOGICAL
// records, not physical chunks: a record far larger than one block is split
// into First/Middle/Last chunks on the way out and reassembled into a single
// entry by ReadAll, so its ordinal must be the same on both sides. Records
// written after it must therefore still decrypt.
func TestContextAADMultiChunkRecordRoundTrips(t *testing.T) {
	barrier := newTestBarrier(t)
	dir := t.TempDir()

	db := openAADTestDB(t, dir, barrier)
	// One entry whose Cmd spans many blocks (block size is 1 KiB), written
	// between ordinary single-chunk records.
	small := makeBenchEntries(1, 2, 1, 32)
	if err := db.SaveEntries(1, 1, small); err != nil {
		t.Fatalf("SaveEntries(small): %v", err)
	}
	big := makeBenchEntries(3, 1, 1, 5000)
	if err := db.SaveEntries(1, 1, big); err != nil {
		t.Fatalf("SaveEntries(big): %v", err)
	}
	tail := makeBenchEntries(4, 3, 1, 48)
	if err := db.SaveEntries(1, 1, tail); err != nil {
		t.Fatalf("SaveEntries(tail): %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	reopened := openAADTestDB(t, dir, barrier)
	defer reopened.Close()

	got, err := reopened.GetEntries(1, 1, 1, 7, 0)
	if err != nil {
		t.Fatalf("GetEntries after reopen: %v", err)
	}
	if len(got) != 6 {
		t.Fatalf("GetEntries after reopen: got %d entries, want 6", len(got))
	}
	if len(got[2].Cmd) != 5000 {
		t.Fatalf("multi-chunk entry Cmd length = %d, want 5000", len(got[2].Cmd))
	}
	if !bytes.Equal(got[2].Cmd, big[0].Cmd) {
		t.Fatal("multi-chunk entry Cmd corrupted across the round trip")
	}
}

// --- tamper detection ---

// TestContextAADRejectsReorderedRecord is the core security property: moving a
// record's ciphertext to a different ordinal within its own segment — with a
// perfectly valid CRC and an untouched barrier envelope — must fail to decrypt.
// Before positional binding, the two records authenticated identically and the
// swap was invisible.
func TestContextAADRejectsReorderedRecord(t *testing.T) {
	barrier := newTestBarrier(t)
	dir := t.TempDir()

	db := openAADTestDB(t, dir, barrier)
	// One SaveEntries call == one WAL record, so write four separate calls to
	// get four independently sealed records in one segment.
	for i := uint64(0); i < 4; i++ {
		if err := db.SaveEntries(1, 1, makeBenchEntries(i+1, 1, 1, 64)); err != nil {
			t.Fatalf("SaveEntries(%d): %v", i, err)
		}
	}
	if err := db.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	shardDir := walShardDir(dir, 1)
	records := readSegmentRecords(t, shardDir, 0)
	if len(records) < 2 {
		t.Fatalf("got %d records, need at least 2 to swap", len(records))
	}
	records[0], records[1] = records[1], records[0]
	rewriteSegment(t, shardDir, 0, records)

	requireReopenRejects(t, dir, barrier, "reordered record")
}

// TestContextAADRejectsTruncatedPrefix proves the binding also catches a
// DELETED record: dropping the first record shifts every later ordinal down by
// one, so nothing after the deletion authenticates.
func TestContextAADRejectsTruncatedPrefix(t *testing.T) {
	barrier := newTestBarrier(t)
	dir := t.TempDir()

	db := openAADTestDB(t, dir, barrier)
	// One SaveEntries call == one WAL record, so write four separate calls to
	// get four independently sealed records in one segment.
	for i := uint64(0); i < 4; i++ {
		if err := db.SaveEntries(1, 1, makeBenchEntries(i+1, 1, 1, 64)); err != nil {
			t.Fatalf("SaveEntries(%d): %v", i, err)
		}
	}
	if err := db.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	shardDir := walShardDir(dir, 1)
	records := readSegmentRecords(t, shardDir, 0)
	if len(records) < 2 {
		t.Fatalf("got %d records, need at least 2", len(records))
	}
	rewriteSegment(t, shardDir, 0, records[1:])

	requireReopenRejects(t, dir, barrier, "dropped leading record")
}

// TestContextAADRejectsRecordCopiedToAnotherSegment proves the SegmentID field
// carries its weight: the same records, at the same ordinals, in a different
// segment file must not authenticate.
func TestContextAADRejectsRecordCopiedToAnotherSegment(t *testing.T) {
	barrier := newTestBarrier(t)
	dir := t.TempDir()

	db := openAADTestDB(t, dir, barrier)
	// One SaveEntries call == one WAL record, so write four separate calls to
	// get four independently sealed records in one segment.
	for i := uint64(0); i < 4; i++ {
		if err := db.SaveEntries(1, 1, makeBenchEntries(i+1, 1, 1, 64)); err != nil {
			t.Fatalf("SaveEntries(%d): %v", i, err)
		}
	}
	if err := db.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	shardDir := walShardDir(dir, 1)
	records := readSegmentRecords(t, shardDir, 0)

	// Republish segment 0's records, in their original order, as segment 1 and
	// remove the original. Ordinals are identical; only the segment ID moved.
	rewriteSegment(t, shardDir, 1, records)
	if err := os.Remove(filepath.Join(shardDir, segmentFilename(0))); err != nil {
		t.Fatalf("Remove segment 0: %v", err)
	}

	requireReopenRejects(t, dir, barrier, "records copied to another segment")
}

// TestContextAADRejectsRecordCopiedToAnotherShard proves the WALShardIdx field
// carries its weight. All DefaultShardCount WAL shards share ONE barrier and
// each has its own independent %06d.log ID space, so without the shard index a
// segment could be swapped wholesale between shard directories undetected.
func TestContextAADRejectsRecordCopiedToAnotherShard(t *testing.T) {
	barrier := newTestBarrier(t)
	dir := t.TempDir()

	db := openAADTestDB(t, dir, barrier)
	// One SaveEntries call == one WAL record, so write four separate calls to
	// get four independently sealed records in one segment.
	for i := uint64(0); i < 4; i++ {
		if err := db.SaveEntries(1, 1, makeBenchEntries(i+1, 1, 1, 64)); err != nil {
			t.Fatalf("SaveEntries(%d): %v", i, err)
		}
	}
	if err := db.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	srcDir := walShardDir(dir, 1)
	records := readSegmentRecords(t, srcDir, 0)

	// Same segment ID, same ordinals, different WAL shard directory.
	dstDir := walShardDir(dir, 2)
	rewriteSegment(t, dstDir, 0, records)
	if err := os.RemoveAll(srcDir); err != nil {
		t.Fatalf("RemoveAll(%s): %v", srcDir, err)
	}

	requireReopenRejects(t, dir, barrier, "segment moved to another WAL shard")
}

// --- BLOCKER-2 regression: rollback-aware ordinal ---

// TestContextAADRecoversAfterBatchRollback is the DiscardPending regression
// test. A failed batch is rolled back, rewinding the segment's write offsets;
// the record ordinal MUST rewind with them. If it does not, every record
// written after the rollback is sealed at an ordinal one or more ahead of the
// index recovery will read it at, and the entire segment tail becomes
// permanently undecryptable at the next start.
func TestContextAADRecoversAfterBatchRollback(t *testing.T) {
	barrier := newTestBarrier(t)
	dir := t.TempDir()

	db := openAADTestDB(t, dir, barrier)

	// Durable, committed prefix.
	if err := db.SaveState([]logdb.Update{{
		ShardID: 1, ReplicaID: 1,
		Entries: makeBenchEntries(1, 3, 1, 64),
		State:   logdb.State{Term: 1, Vote: 1, Commit: 3},
	}}); err != nil {
		t.Fatalf("SaveState(prefix): %v", err)
	}

	// A batch whose FIRST update writes records and whose SECOND update is
	// rejected, forcing rollbackShard -> DiscardPending on a segment that
	// already advanced. Raft shards 1 and 17 both hash to WAL shard 1, so both
	// updates share one segment and one checkpoint.
	oversized := makeBenchEntries(1, 1, 1, 8)
	oversized[0].Cmd = make([]byte, maxCmdLen+1)
	err := db.SaveState([]logdb.Update{
		{
			ShardID: 1, ReplicaID: 1,
			Entries: makeBenchEntries(4, 3, 1, 64),
			State:   logdb.State{Term: 2, Vote: 1, Commit: 6},
		},
		{
			ShardID: 17, ReplicaID: 1,
			Entries: oversized,
		},
	})
	if err == nil {
		t.Fatal("SaveState with an oversized entry succeeded, want failure")
	}

	// The rolled-back records must be gone, and further writes must land at the
	// rewound ordinals.
	if err := db.SaveState([]logdb.Update{{
		ShardID: 1, ReplicaID: 1,
		Entries: makeBenchEntries(4, 4, 2, 64),
		State:   logdb.State{Term: 2, Vote: 1, Commit: 7},
	}}); err != nil {
		t.Fatalf("SaveState(after rollback): %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	// Recovery must reproduce every ordinal. A stale (un-rewound) counter shows
	// up here as a WALDecryptError.
	reopened := openAADTestDB(t, dir, barrier)
	defer reopened.Close()

	got, err := reopened.GetEntries(1, 1, 1, 8, 0)
	if err != nil {
		t.Fatalf("GetEntries after rollback+reopen: %v", err)
	}
	if len(got) != 7 {
		t.Fatalf("GetEntries after rollback+reopen: got %d entries, want 7", len(got))
	}
	for i := 4; i <= 7; i++ {
		if got[i-1].Term != 2 {
			t.Fatalf("entry %d term = %d, want 2 (post-rollback write)", i, got[i-1].Term)
		}
	}
	st, _, err := reopened.ReadState(1, 1)
	if err != nil {
		t.Fatalf("ReadState: %v", err)
	}
	if st.Commit != 7 {
		t.Fatalf("ReadState.Commit = %d, want 7", st.Commit)
	}
}

// --- fail-closed barrier capability ---

// noContextBarrier satisfies WALBarrier but not walContextBarrier: it cannot
// bind a caller context into the AEAD AAD.
type noContextBarrier struct{}

func (noContextBarrier) Encrypt(dst, plaintext []byte) ([]byte, error) {
	out := make([]byte, ciphertextEpochSize+len(plaintext))
	binary.BigEndian.PutUint64(out[:ciphertextEpochSize], 1)
	copy(out[ciphertextEpochSize:], plaintext)
	return out, nil
}

func (noContextBarrier) Decrypt(dst, ciphertext []byte) ([]byte, error) {
	return ciphertext[ciphertextEpochSize:], nil
}

func (b noContextBarrier) EncryptWithEpoch(dst, plaintext []byte) ([]byte, uint64, error) {
	ct, err := b.Encrypt(dst, plaintext)
	return ct, 1, err
}

func (b noContextBarrier) DecryptForEpoch(dst, ciphertext []byte, epoch uint64) ([]byte, error) {
	return b.Decrypt(dst, ciphertext)
}

func (noContextBarrier) CurrentEpoch() uint64 { return 1 }

// TestOpenRejectsBarrierWithoutContextSupport pins the fail-closed rule: a
// barrier that cannot carry context-AAD is refused at Open. Accepting it and
// writing epoch-only records would be the silent downgrade the whole scheme
// exists to prevent.
func TestOpenRejectsBarrierWithoutContextSupport(t *testing.T) {
	db, err := Open(t.TempDir(),
		WithBlockSize(aadTestBlockSize),
		WithMaxFileSize(aadTestMaxFileSize),
		WithBarrier(noContextBarrier{}),
	)
	if err == nil {
		_ = db.Close()
		t.Fatal("Open with a context-incapable barrier succeeded, want ErrBarrierContextUnsupported")
	}
	if !errors.Is(err, ErrBarrierContextUnsupported) {
		t.Fatalf("Open error = %v, want ErrBarrierContextUnsupported", err)
	}
}

// newBenchBarrier is newTestBarrier for a benchmark: a real seal.Barrier with
// cheap KDF parameters.
func newBenchBarrier(tb testing.TB) *seal.Barrier {
	tb.Helper()
	_ = seal.AllowWeakKDFParams()
	strategy, err := newTestSealStrategy([]byte("test-passphrase"))
	if err != nil {
		tb.Fatalf("newTestSealStrategy: %v", err)
	}
	tb.Cleanup(func() { strategy.Close() })

	barrier := seal.MustNewBarrier(testBarrierConfig(1))
	if err := barrier.Initialize(context.Background(), strategy, seal.Credentials{}); err != nil {
		tb.Fatalf("barrier.Initialize: %v", err)
	}
	return barrier
}

// BenchmarkDBSaveStateEncrypted measures the encrypted WAL write hot path
// (marshal -> seal -> segment append) with fsync disabled so the AEAD and
// framing costs are not buried under a 300us fdatasync.
func BenchmarkDBSaveStateEncrypted(b *testing.B) {
	b.ReportAllocs()
	barrier := newBenchBarrier(b)
	db, err := Open(b.TempDir(),
		WithBlockSize(4096),
		WithMaxFileSize(64*1024*1024),
		WithNoSync(true),
		WithBarrier(barrier),
	)
	if err != nil {
		b.Fatalf("Open: %v", err)
	}
	b.Cleanup(func() { _ = db.Close() })

	entries := makeBenchEntries(1, 10, 1, 128)
	updates := []logdb.Update{
		{
			ShardID:   1,
			ReplicaID: 1,
			Entries:   entries,
			State:     logdb.State{Term: 1, Vote: 1, Commit: 10},
		},
	}

	idx := uint64(11)
	b.ResetTimer()
	for b.Loop() {
		for i := range updates[0].Entries {
			updates[0].Entries[i].Index = idx + uint64(i)
		}
		updates[0].State.Commit = idx + 9
		idx += 10

		if err := db.SaveState(updates); err != nil {
			b.Fatalf("SaveState: %v", err)
		}
	}
}

// newSealableTestBarrier returns an initialized software barrier together with
// the strategy it was initialized under, so the caller can seal and unseal it.
// newTestBarrier closes its strategy at cleanup, which makes it unusable for a
// later Unseal.
func newSealableTestBarrier(t *testing.T) (*seal.Barrier, seal.SealingStrategy) {
	t.Helper()
	_ = seal.AllowWeakKDFParams()
	strategy, err := newTestSealStrategy([]byte("test-passphrase"))
	if err != nil {
		t.Fatalf("newTestSealStrategy: %v", err)
	}
	barrier := seal.MustNewBarrier(testBarrierConfig(7))
	if err := barrier.Initialize(context.Background(), strategy, seal.Credentials{}); err != nil {
		t.Fatalf("barrier.Initialize: %v", err)
	}
	t.Cleanup(func() { strategy.Close() })
	return barrier, strategy
}

// TestOpenRefusesSealedBarrierRatherThanLatchingNoBinding is the regression
// test for the silent context-AAD downgrade on a sealed start.
//
// A sealed barrier has no encryptor, so it reports no context-binding
// capability — the ABSENCE of an answer, not an answer of "no". The WAL
// resolves that capability exactly once, at open, and writes every record of
// the process against it. Latching the sealed barrier's "false" would make a
// node that starts sealed (the Vault-style start-then-unseal flow this stack
// targets) write epoch-only records FOREVER, including after the operator
// unseals it with a fully AAD-capable barrier: no error, no log line, and every
// record spliceable and reorderable at rest.
//
// waldb.Open therefore fails closed with ErrBarrierContextUnresolved.
//
// The ASSERTION IS ON OPEN, and that is the whole point of the test. Asserting
// only that a later write fails would be satisfied by a WAL that latched the
// sealed barrier's "false" too, since the writes would then be rejected by
// EncryptWithContext anyway while the barrier was still sealed. Open
// returning ErrBarrierContextUnresolved is the one observable that distinguishes
// failing closed from latching a downgrade, so it is the one asserted here.
// TestUnsealedBarrierBindsRecordsPositionally carries the positive half.
func TestOpenRefusesSealedBarrierRatherThanLatchingNoBinding(t *testing.T) {
	barrier, _ := newSealableTestBarrier(t)
	dir := t.TempDir()

	if err := barrier.Seal(); err != nil {
		t.Fatalf("barrier.Seal: %v", err)
	}
	if barrier.SupportsContextAAD() {
		t.Fatal("fixture: a sealed barrier must not report context-AAD support")
	}
	if barrier.ContextBindingResolved() {
		t.Fatal("fixture: a sealed barrier's binding must be unresolved")
	}

	db, err := Open(dir,
		WithBlockSize(aadTestBlockSize),
		WithMaxFileSize(aadTestMaxFileSize),
		WithBarrier(barrier),
	)
	if err == nil {
		_ = db.Close()
		t.Fatal("Open accepted a sealed barrier: the WAL just latched 'cannot bind " +
			"context' from a barrier that had not been unsealed yet, so every record " +
			"this node ever writes is epoch-only bound even after the operator " +
			"unseals an AAD-capable barrier")
	}
	if !errors.Is(err, ErrBarrierContextUnresolved) {
		t.Fatalf("Open error = %v, want ErrBarrierContextUnresolved", err)
	}

	// Nothing was created that could hold a latched capability.
	if _, statErr := os.Stat(filepath.Join(dir, shardDirName(1))); statErr == nil {
		t.Fatal("Open built shard state before refusing the sealed barrier; the " +
			"capability must be resolved while there is still nothing to latch it into")
	}
}

// TestUnsealedBarrierBindsRecordsPositionally is the positive half: once the
// operator unseals, the WAL must actually use the capability the barrier now
// reports, and the bytes on disk must carry the binding.
//
// Split out from the refusal test deliberately. Bundled together, a failure
// here reads as a failure of the fail-closed guard, and — worse — the pair
// could be satisfied by a WAL that refused the sealed barrier for the wrong
// reason and then wrote unbound records anyway.
func TestUnsealedBarrierBindsRecordsPositionally(t *testing.T) {
	barrier, strategy := newSealableTestBarrier(t)
	dir := t.TempDir()

	if err := barrier.Seal(); err != nil {
		t.Fatalf("barrier.Seal: %v", err)
	}
	// Unsealed, the same barrier binds context, and the WAL must use it.
	if err := barrier.Unseal(context.Background(), strategy, seal.Credentials{}); err != nil {
		t.Fatalf("barrier.Unseal: %v", err)
	}
	if !barrier.SupportsContextAAD() {
		t.Fatal("fixture: the unsealed software barrier must support context AAD")
	}

	db := openAADTestDB(t, dir, barrier)
	const shardID, replicaID = uint64(1), uint64(1)
	if err := db.SaveEntries(shardID, replicaID, makeEntries(1, 2, 1)); err != nil {
		t.Fatalf("SaveEntries: %v", err)
	}
	shard := db.getShard(shardID)
	if shard.noContextBinding {
		t.Fatal("the WAL suppressed the positional context against an AAD-capable " +
			"barrier: the capability was latched from the sealed barrier")
	}
	if err := db.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	// The bytes on disk carry the binding: the record opens under its own
	// (shard, segment, ordinal) context and under no other.
	records := readSegmentRecords(t, walShardDir(dir, shardID), 0)
	if len(records) == 0 {
		t.Fatal("no records written")
	}
	shardIdx := shardID % DefaultShardCount
	var ctxBuf [walContextSize]byte
	for i, rec := range records {
		want := walRecordContext(ctxBuf[:], shardIdx, 0, uint64(i))
		if _, err := barrier.DecryptWithContext(nil, rec, want); err != nil {
			t.Fatalf("record %d does not open under its own position: %v", i, err)
		}
		if _, err := barrier.DecryptWithContext(nil, rec, nil); err == nil {
			t.Fatalf("record %d opens with NO context: it was written unbound, so an "+
				"at-rest attacker can reorder or splice it undetected", i)
		}
	}
}

// TestWriteRefusesBindingCapabilityGainedAfterOpen covers the other half of the
// latch: the capability is only valid while the barrier stays unsealed, and
// Host.EmergencySeal plus a later unseal can change it underneath a running
// WAL. A shard that resolved "cannot bind" at open must not keep writing
// unbound records once the barrier reports that it can.
func TestWriteRefusesBindingCapabilityGainedAfterOpen(t *testing.T) {
	barrier := newTestBarrier(t)
	dir := t.TempDir()
	db := openAADTestDB(t, dir, barrier)
	defer db.Close()

	const shardID, replicaID = uint64(1), uint64(1)
	if err := db.SaveEntries(shardID, replicaID, makeEntries(1, 1, 1)); err != nil {
		t.Fatalf("SaveEntries: %v", err)
	}

	// Simulate the drift: the shard behaves as if it had resolved "cannot bind"
	// while the barrier reports that it can.
	shard := db.getShard(shardID)
	shard.mu.Lock()
	shard.noContextBinding = true
	shard.mu.Unlock()

	err := db.SaveEntries(shardID, replicaID, makeEntries(2, 1, 1))
	if !errors.Is(err, ErrBarrierContextBindingChanged) {
		t.Fatalf("SaveEntries after the capability changed = %v, want "+
			"ErrBarrierContextBindingChanged; the WAL kept writing unbound records "+
			"against a barrier that would have authenticated their position", err)
	}
}
