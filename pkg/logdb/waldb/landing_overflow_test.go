package waldb

import (
	"bytes"
	"errors"
	"testing"

	"github.com/jeremyhahn/go-quicraft/pkg/logdb"
	"github.com/jeremyhahn/go-quicraft/pkg/seal"
)

// oversizeEnvelopeTail is the number of bytes oversizeEnvelopeBarrier adds to
// every sealed record on top of the real envelope. It is far above
// maxCiphertextExpansion, so a record pre-fitted near the end of a segment by
// reserveLanding no longer fits once sealed.
const oversizeEnvelopeTail = 2048

// oversizeEnvelopeBarrier models a delegated encryptor whose envelope exceeds
// maxCiphertextExpansion. It binds the caller's context exactly as the
// embedded seal.Barrier does, then appends a fixed tail that DecryptWithContext
// strips before opening, so every record it seals is readable but longer
// than reserveLanding assumed.
type oversizeEnvelopeBarrier struct {
	*seal.Barrier
}

func (b oversizeEnvelopeBarrier) EncryptWithContext(dst, plaintext, contextAAD []byte) ([]byte, error) {
	ct, err := b.Barrier.EncryptWithContext(dst, plaintext, contextAAD)
	if err != nil {
		return nil, err
	}
	return append(ct, make([]byte, oversizeEnvelopeTail)...), nil
}

func (b oversizeEnvelopeBarrier) DecryptWithContext(dst, ciphertext, contextAAD []byte) ([]byte, error) {
	if len(ciphertext) < oversizeEnvelopeTail {
		return nil, errors.New("oversizeEnvelopeBarrier: ciphertext shorter than its tail")
	}
	return b.Barrier.DecryptWithContext(dst, ciphertext[:len(ciphertext)-oversizeEnvelopeTail], contextAAD)
}

// landingOverflowCmd is the deterministic payload of entry index i.
func landingOverflowCmd(i uint64) []byte {
	return bytes.Repeat([]byte{byte(i)}, 100)
}

func openLandingOverflowDB(t *testing.T, dir string, barrier WALBarrier) *DB {
	t.Helper()
	db, err := Open(dir,
		WithNoSync(true),
		WithBlockSize(1024),
		WithMaxFileSize(64*1024),
		WithBarrier(barrier),
	)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	return db
}

// landingOverflowMaxSaves bounds the save loop: at ~2.3 KiB per sealed record
// a 64 KiB segment fills in under 30 saves, so the oversize barrier must be
// refused well before this; at ~200 bytes per record the plain barrier writes
// about three segments' worth within it.
const landingOverflowMaxSaves = 1000

func saveLandingOverflowEntry(db *DB, i uint64) error {
	return db.SaveEntries(1, 1, []logdb.Entry{{Index: i, Term: 1, Cmd: landingOverflowCmd(i)}})
}

// TestWriteRecordOverflowingLandingFailsClosed pins the behaviour documented
// at maxCiphertextExpansion: a context-binding barrier whose envelope exceeds
// that bound is refused with ErrLandingSegmentOverflow at a segment boundary,
// and nothing already acknowledged is corrupted. The record is never moved to
// a fresh segment, where its context-AAD would name the wrong position.
func TestWriteRecordOverflowingLandingFailsClosed(t *testing.T) {
	t.Run("OversizeEnvelopeRefusedAtBoundary", func(t *testing.T) {
		barrier := oversizeEnvelopeBarrier{Barrier: newTestBarrier(t)}
		dir := t.TempDir()
		db := openLandingOverflowDB(t, dir, barrier)

		var acked uint64
		var saveErr error
		for i := uint64(1); i <= landingOverflowMaxSaves; i++ {
			if saveErr = saveLandingOverflowEntry(db, i); saveErr != nil {
				break
			}
			acked = i
		}
		if saveErr == nil {
			db.Close()
			t.Fatalf("%d saves all succeeded; want ErrLandingSegmentOverflow at the first segment boundary", landingOverflowMaxSaves)
		}
		if !errors.Is(saveErr, ErrLandingSegmentOverflow) {
			db.Close()
			t.Fatalf("save %d: got %v, want ErrLandingSegmentOverflow", acked+1, saveErr)
		}
		if acked == 0 {
			db.Close()
			t.Fatal("the very first save was refused; want at least one acknowledged save before the boundary")
		}
		refused := acked + 1

		// The refusal is rolled back, not a fail-stop: the DB is not poisoned
		// and still serves what it acknowledged. The record is never moved, so
		// a retry lands at the same position and is refused the same way.
		if db.poisoned.Load() {
			db.Close()
			t.Fatal("DB poisoned by ErrLandingSegmentOverflow; want the refused batch rolled back only")
		}
		if err := saveLandingOverflowEntry(db, refused); !errors.Is(err, ErrLandingSegmentOverflow) {
			db.Close()
			t.Fatalf("retry of entry %d: got %v, want ErrLandingSegmentOverflow again", refused, err)
		}
		if got, err := db.GetEntries(1, 1, 1, refused+1, 0); err != nil || uint64(len(got)) != acked {
			db.Close()
			t.Fatalf("before reopen: GetEntries = %d entries, %v; want the %d acknowledged ones", len(got), err, acked)
		}

		if err := db.Close(); err != nil {
			t.Fatalf("Close: %v", err)
		}

		db2 := openLandingOverflowDB(t, dir, barrier)
		defer db2.Close()
		got, err := db2.GetEntries(1, 1, 1, refused+1, 0)
		if err != nil {
			t.Fatalf("GetEntries after reopen: %v", err)
		}
		if uint64(len(got)) != acked {
			t.Fatalf("after reopen: got %d entries, want the %d acknowledged ones (refused entry %d must be absent)", len(got), acked, refused)
		}
		for k, e := range got {
			want := uint64(k) + 1
			if e.Index != want || e.Term != 1 || !bytes.Equal(e.Cmd, landingOverflowCmd(want)) {
				t.Fatalf("after reopen: entry[%d] = {Index:%d Term:%d len(Cmd):%d}, want index %d intact", k, e.Index, e.Term, len(e.Cmd), want)
			}
		}
	})

	t.Run("PlainBarrierRotatesAcrossBoundary", func(t *testing.T) {
		db := openLandingOverflowDB(t, t.TempDir(), newTestBarrier(t))
		defer db.Close()
		first := uint64(0)
		for i := uint64(1); i <= landingOverflowMaxSaves; i++ {
			if err := saveLandingOverflowEntry(db, i); err != nil {
				t.Fatalf("save %d with the plain barrier: %v", i, err)
			}
			if i == 1 {
				first = landingOverflowActiveSegment(db)
			}
		}
		// The control is only meaningful if it crossed the boundary the
		// oversize barrier was refused at.
		if last := landingOverflowActiveSegment(db); last == first {
			t.Fatalf("plain barrier never rotated past segment %d; the control did not cross a segment boundary", first)
		}
	})
}

// landingOverflowActiveSegment reads the active segment ID of the WAL shard
// that Raft shard 1 maps to, under the shard's write lock.
func landingOverflowActiveSegment(db *DB) uint64 {
	s := db.shards[1%DefaultShardCount]
	s.mu.Lock()
	defer s.mu.Unlock()
	return uint64(s.activeSegmentID)
}
