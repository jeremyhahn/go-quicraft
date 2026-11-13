package waldb

import (
	"errors"
	"testing"
)

// openRefusalBlockSize is a valid (power-of-two) block size for the refusal
// tests below.
const openRefusalBlockSize = 1024

// TestOpenRefusesSegmentSmallerThanTwoBlocks pins the documented refusal of a
// MaxFileSize that spans fewer than two blocks: Open fails with
// ErrSegmentTooSmallForEntry and returns no DB, while exactly two blocks opens.
func TestOpenRefusesSegmentSmallerThanTwoBlocks(t *testing.T) {
	bs := openRefusalBlockSize
	db, err := Open(t.TempDir(),
		WithBlockSize(bs),
		WithMaxFileSize(int64(2*bs-1)),
	)
	if err == nil {
		_ = db.Close()
		t.Fatal("Open with MaxFileSize < 2 blocks succeeded, want ErrSegmentTooSmallForEntry")
	}
	if !errors.Is(err, ErrSegmentTooSmallForEntry) {
		t.Fatalf("Open with MaxFileSize < 2 blocks: err = %v, want ErrSegmentTooSmallForEntry", err)
	}
	if db != nil {
		t.Fatalf("Open with MaxFileSize < 2 blocks returned a non-nil *DB alongside %v", err)
	}

	db, err = Open(t.TempDir(),
		WithBlockSize(bs),
		WithMaxFileSize(int64(2*bs)),
	)
	if err != nil {
		t.Fatalf("Open with MaxFileSize == 2 blocks: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
}

// TestOpenRefusesSegmentTooSmallOnlyAtOpen pins the documented split: the
// two-block minimum is enforced by Open, not by Options.Validate.
func TestOpenRefusesSegmentTooSmallOnlyAtOpen(t *testing.T) {
	bs := openRefusalBlockSize
	o := Options{
		WALDir:      t.TempDir(),
		BlockSize:   bs,
		MaxFileSize: int64(2*bs - 1),
	}
	if err := o.Validate(); err != nil {
		t.Fatalf("Options.Validate with MaxFileSize < 2 blocks: err = %v, want nil", err)
	}
}

// contextNoResolveBarrier implements every context method the WAL needs
// except ContextBindingResolved.
type contextNoResolveBarrier struct {
	noContextBarrier
}

func (b contextNoResolveBarrier) EncryptWithContext(dst, plaintext, _ []byte) ([]byte, error) {
	return b.Encrypt(dst, plaintext)
}

func (b contextNoResolveBarrier) DecryptWithContext(dst, ciphertext, _ []byte) ([]byte, error) {
	return b.Decrypt(dst, ciphertext)
}

func (contextNoResolveBarrier) IsHardwareMode() bool { return false }

func (contextNoResolveBarrier) SupportsContextAAD() bool { return true }

// TestOpenRefusesBarrierMissingContextBindingResolved pins the named error
// for a barrier that binds context but cannot report whether that binding is
// resolved: ErrBarrierContextBindingUnreportable, not the generic
// ErrBarrierContextUnsupported.
func TestOpenRefusesBarrierMissingContextBindingResolved(t *testing.T) {
	var b WALBarrier = contextNoResolveBarrier{}
	if _, ok := b.(walContextBarrier); ok {
		t.Fatal("fixture: barrier must not implement ContextBindingResolved")
	}
	db, err := Open(t.TempDir(),
		WithBlockSize(openRefusalBlockSize),
		WithMaxFileSize(64*1024),
		WithBarrier(b),
	)
	if err == nil {
		_ = db.Close()
		t.Fatal("Open with a barrier missing ContextBindingResolved succeeded, " +
			"want ErrBarrierContextBindingUnreportable")
	}
	if !errors.Is(err, ErrBarrierContextBindingUnreportable) {
		t.Fatalf("err = %v, want ErrBarrierContextBindingUnreportable", err)
	}
	if errors.Is(err, ErrBarrierContextUnsupported) {
		t.Fatalf("err = %v also matches ErrBarrierContextUnsupported, want only "+
			"ErrBarrierContextBindingUnreportable", err)
	}
	if db != nil {
		t.Fatalf("Open returned a non-nil *DB alongside %v", err)
	}
}

// TestOpenRefusesInvalidOptions pins the option errors Open returns.
func TestOpenRefusesInvalidOptions(t *testing.T) {
	cases := []struct {
		name string
		dir  func(t *testing.T) string
		opts []Option
		want error
	}{
		{
			name: "empty dir",
			dir:  func(*testing.T) string { return "" },
			want: ErrEmptyWALDir,
		},
		{
			name: "max file size above segment limit",
			dir:  func(t *testing.T) string { return t.TempDir() },
			opts: []Option{WithMaxFileSize(maxSegmentFileSize + 1)},
			want: ErrInvalidMaxFileSize,
		},
		{
			name: "block size not a power of two",
			dir:  func(t *testing.T) string { return t.TempDir() },
			opts: []Option{WithBlockSize(3000)},
			want: ErrInvalidBlockSize,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			db, err := Open(tc.dir(t), tc.opts...)
			if err == nil {
				_ = db.Close()
				t.Fatalf("Open succeeded, want %v", tc.want)
			}
			if !errors.Is(err, tc.want) {
				t.Fatalf("err = %v, want %v", err, tc.want)
			}
			if db != nil {
				t.Fatalf("Open returned a non-nil *DB alongside %v", err)
			}
		})
	}
}
