package dragonboat

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	dbConfig "github.com/jeremyhahn/go-quicraft/contrib/dragonboat/config"
)

// exportPathCases returns a missing path and a regular file, neither of which
// is a usable export directory.
func exportPathCases(ctx context.Context, t *testing.T) map[string]string {
	t.Helper()
	if err := ctx.Err(); err != nil {
		t.Fatalf("context: %v", err)
	}
	dir := t.TempDir()
	file := filepath.Join(dir, "regular-file")
	if err := os.WriteFile(file, []byte("x"), 0o600); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}
	return map[string]string{
		"missing": filepath.Join(dir, "does-not-exist"),
		"file":    file,
	}
}

func TestCheckExportPath_AcceptsEmptyAndExistingDir(t *testing.T) {
	if err := checkExportPath(SnapshotOption{}); err != nil {
		t.Fatalf("empty ExportPath: got %v, want nil", err)
	}
	if err := checkExportPath(SnapshotOption{Exported: true, ExportPath: t.TempDir()}); err != nil {
		t.Fatalf("existing dir: got %v, want nil", err)
	}
}

func TestCheckExportPath_RefusesMissingAndFile(t *testing.T) {
	for name, p := range exportPathCases(context.Background(), t) {
		t.Run(name, func(t *testing.T) {
			err := checkExportPath(SnapshotOption{Exported: true, ExportPath: p})
			if !errors.Is(err, ErrDirNotExist) {
				t.Fatalf("got %v, want ErrDirNotExist", err)
			}
		})
	}
}

func TestSnapshotRequests_ExportPathRefusedWithErrDirNotExist(t *testing.T) {
	// No host is running: the export path check precedes any host use.
	nh := &NodeHost{config: dbConfig.NodeHostConfig{RTTMillisecond: 100}}
	for name, p := range exportPathCases(context.Background(), t) {
		opt := SnapshotOption{Exported: true, ExportPath: p}
		t.Run("Sync/"+name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			idx, err := nh.SyncRequestSnapshot(ctx, 1, opt)
			if !errors.Is(err, ErrDirNotExist) {
				t.Fatalf("got %v, want ErrDirNotExist", err)
			}
			if idx != 0 {
				t.Fatalf("index: got %d, want 0", idx)
			}
		})
		t.Run("Async/"+name, func(t *testing.T) {
			rs, err := nh.RequestSnapshot(1, opt, 5*time.Second)
			if !errors.Is(err, ErrDirNotExist) {
				t.Fatalf("got %v, want ErrDirNotExist", err)
			}
			if rs != nil {
				t.Fatalf("RequestState: got %v, want nil", rs)
			}
		})
	}
}

func TestTranslateError_ExportPathErrDirNotExistUnchanged(t *testing.T) {
	if got := translateError(ErrDirNotExist); got != ErrDirNotExist {
		t.Fatalf("got %v, want ErrDirNotExist unchanged", got)
	}
}
