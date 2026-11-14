package quicraft

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"testing"
)

const wantRelativeExportPathMsg = "quicraft: invalid snapshot option ExportPath: must be an absolute path"

// requestSnapshotOptionValidate calls RequestSnapshot on shard 99 and checks
// that no RequestState is returned with the error.
func requestSnapshotOptionValidate(ctx context.Context, t *testing.T, h *Host, opt SnapshotOption) error {
	t.Helper()
	rs, err := h.RequestSnapshot(ctx, 99, opt)
	if rs != nil {
		t.Errorf("RequestSnapshot(%+v) returned a non-nil RequestState with err %v", opt, err)
	}
	return err
}

func TestSnapshotOptionValidate_AcceptsZeroDefaultAndBareExported(t *testing.T) {
	zero := SnapshotOption{}
	if err := zero.Validate(); err != nil {
		t.Errorf("SnapshotOption{}.Validate() = %v, want nil", err)
	}
	if err := DefaultSnapshotOption.Validate(); err != nil {
		t.Errorf("DefaultSnapshotOption.Validate() = %v, want nil", err)
	}
	bare := SnapshotOption{Exported: true}
	if err := bare.Validate(); err != nil {
		t.Errorf("SnapshotOption{Exported: true}.Validate() = %v, want nil", err)
	}
	if DefaultSnapshotOption != (SnapshotOption{}) {
		t.Errorf("DefaultSnapshotOption = %+v, want SnapshotOption{}", DefaultSnapshotOption)
	}
	if DefaultSnapshotOption.IsExport() {
		t.Error("DefaultSnapshotOption.IsExport() = true, want false")
	}
}

func TestSnapshotOptionValidate_AbsoluteMissingDirIsAccepted(t *testing.T) {
	opt := SnapshotOption{ExportPath: filepath.Join(t.TempDir(), "missing")}
	if err := opt.Validate(); err != nil {
		t.Errorf("Validate(%q) = %v, want nil (Validate must not touch the filesystem)", opt.ExportPath, err)
	}
}

func TestSnapshotOptionValidate_RelativeExportPathExactError(t *testing.T) {
	for _, p := range []string{"snaps/out", "./x", ".."} {
		opt := SnapshotOption{ExportPath: p}
		err := opt.Validate()
		if err == nil {
			t.Errorf("Validate(%q) = nil, want error", p)
			continue
		}
		if got := err.Error(); got != wantRelativeExportPathMsg {
			t.Errorf("Validate(%q).Error() = %q, want %q", p, got, wantRelativeExportPathMsg)
		}
		var optErr *SnapshotOptionError
		if !errors.As(err, &optErr) {
			t.Errorf("Validate(%q) error %T is not *SnapshotOptionError", p, err)
			continue
		}
		if optErr.Field != "ExportPath" {
			t.Errorf("Validate(%q) Field = %q, want %q", p, optErr.Field, "ExportPath")
		}
	}
}

func TestSnapshotOptionValidate_ErrorsIsMatching(t *testing.T) {
	opt := SnapshotOption{ExportPath: "rel"}
	err := opt.Validate()
	if err == nil {
		t.Fatal("Validate(\"rel\") = nil, want error")
	}
	if !errors.Is(fmt.Errorf("wrap: %w", err), ErrInvalidSnapshotOption) {
		t.Error("wrapped option error does not match ErrInvalidSnapshotOption")
	}
	if !errors.Is(&SnapshotOptionError{Field: "Other"}, ErrInvalidSnapshotOption) {
		t.Error("SnapshotOptionError{Field: \"Other\"} does not match ErrInvalidSnapshotOption")
	}
	if errors.Is(err, ErrShardNotFound) {
		t.Error("option error matches ErrShardNotFound")
	}
	if errors.Is(errors.New("x"), ErrInvalidSnapshotOption) {
		t.Error("plain error matches ErrInvalidSnapshotOption")
	}
}

func TestHost_RequestSnapshot_OptionValidateBeforeShardLookup(t *testing.T) {
	h := newTestHost(t)
	ctx := context.Background()

	err := requestSnapshotOptionValidate(ctx, t, h, SnapshotOption{ExportPath: "rel"})
	if !errors.Is(err, ErrInvalidSnapshotOption) {
		t.Errorf("relative path on missing shard: err = %v, want ErrInvalidSnapshotOption", err)
	}
	if errors.Is(err, ErrShardNotFound) {
		t.Errorf("relative path on missing shard: err = %v matches ErrShardNotFound", err)
	}

	abs := filepath.Join(t.TempDir(), "out")
	err = requestSnapshotOptionValidate(ctx, t, h, SnapshotOption{ExportPath: abs})
	if !errors.Is(err, ErrShardNotFound) {
		t.Errorf("absolute path on missing shard: err = %v, want ErrShardNotFound", err)
	}
}

func TestHost_RequestSnapshot_OptionValidateAfterClosedAndContext(t *testing.T) {
	t.Run("closed", func(t *testing.T) {
		h := newTestHost(t)
		h.Close()
		err := requestSnapshotOptionValidate(context.Background(), t, h, SnapshotOption{ExportPath: "rel"})
		if !errors.Is(err, ErrClosed) {
			t.Errorf("closed host: err = %v, want ErrClosed", err)
		}
		if errors.Is(err, ErrInvalidSnapshotOption) {
			t.Errorf("closed host: err = %v matches ErrInvalidSnapshotOption", err)
		}
	})
	t.Run("canceled", func(t *testing.T) {
		h := newTestHost(t)
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		err := requestSnapshotOptionValidate(ctx, t, h, SnapshotOption{ExportPath: "rel"})
		if !errors.Is(err, ErrCanceled) {
			t.Errorf("cancelled context: err = %v, want ErrCanceled", err)
		}
		if errors.Is(err, ErrInvalidSnapshotOption) {
			t.Errorf("cancelled context: err = %v matches ErrInvalidSnapshotOption", err)
		}
	})
}
