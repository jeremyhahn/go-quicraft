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

const refusalShardID = uint64(1)

// refusalPaths returns an ExportPath that does not exist and one that is a
// regular file, together with the regular file's contents.
func refusalPaths(_ context.Context, t *testing.T) (missing, file string, content []byte) {
	t.Helper()
	dir := t.TempDir()
	missing = filepath.Join(dir, "missing")
	file = filepath.Join(dir, "file")
	content = []byte("not a directory")
	if err := os.WriteFile(file, content, 0o600); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}
	return missing, file, content
}

// assertNothingExported checks the missing path is still absent and the
// regular file still holds exactly its original contents.
func assertNothingExported(_ context.Context, t *testing.T, missing, file string, content []byte) {
	t.Helper()
	if _, err := os.Stat(missing); !errors.Is(err, os.ErrNotExist) {
		t.Errorf("missing ExportPath %s exists after refusal (stat err %v)", missing, err)
	}
	got, err := os.ReadFile(file)
	if err != nil {
		t.Fatalf("ReadFile: %v", err)
	}
	if string(got) != string(content) {
		t.Errorf("regular-file ExportPath changed: got %q, want %q", got, content)
	}
}

// startRefusalShard starts a single-replica shard and proposes one entry so
// a snapshot has something to capture.
func startRefusalShard(ctx context.Context, t *testing.T) *NodeHost {
	t.Helper()
	nh := newTestNodeHost(t)
	startTestShard(t, nh, refusalShardID, 1)
	if _, err := nh.SyncPropose(ctx, nh.GetNoOPSession(refusalShardID), []byte("export-data")); err != nil {
		t.Fatalf("SyncPropose: %v", err)
	}
	return nh
}

func TestExportPath_SyncRequestSnapshot_Refused(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	nh := startRefusalShard(ctx, t)
	missing, file, content := refusalPaths(ctx, t)

	for name, path := range map[string]string{"missing": missing, "file": file} {
		idx, err := nh.SyncRequestSnapshot(ctx, refusalShardID, SnapshotOption{ExportPath: path})
		if !errors.Is(err, ErrDirNotExist) {
			t.Errorf("%s: SyncRequestSnapshot err = %v, want ErrDirNotExist", name, err)
		}
		if idx != 0 {
			t.Errorf("%s: SyncRequestSnapshot index = %d, want 0", name, idx)
		}
	}
	assertNothingExported(ctx, t, missing, file, content)
}

func TestExportPath_RequestSnapshot_Refused(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	nh := startRefusalShard(ctx, t)
	missing, file, content := refusalPaths(ctx, t)

	for name, path := range map[string]string{"missing": missing, "file": file} {
		rs, err := nh.RequestSnapshot(refusalShardID, SnapshotOption{ExportPath: path}, 5*time.Second)
		if !errors.Is(err, ErrDirNotExist) {
			t.Errorf("%s: RequestSnapshot err = %v, want ErrDirNotExist", name, err)
		}
		if rs != nil {
			rs.Release()
			t.Errorf("%s: RequestSnapshot returned a RequestState on refusal", name)
		}
	}
	assertNothingExported(ctx, t, missing, file, content)
}

func TestExportPath_ExistingDir_Exports(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	nh := startRefusalShard(ctx, t)

	syncDir := t.TempDir()
	idx, err := nh.SyncRequestSnapshot(ctx, refusalShardID, SnapshotOption{ExportPath: syncDir})
	if err != nil {
		t.Fatalf("SyncRequestSnapshot to an existing directory: %v", err)
	}
	if idx == 0 {
		t.Error("SyncRequestSnapshot index = 0, want non-zero")
	}
	assertExported(ctx, t, syncDir)

	if _, err := nh.SyncPropose(ctx, nh.GetNoOPSession(refusalShardID), []byte("more")); err != nil {
		t.Fatalf("SyncPropose: %v", err)
	}
	asyncDir := t.TempDir()
	rs, err := nh.RequestSnapshot(refusalShardID, SnapshotOption{ExportPath: asyncDir}, 5*time.Second)
	if err != nil {
		t.Fatalf("RequestSnapshot to an existing directory: %v", err)
	}
	defer rs.Release()
	select {
	case result := <-rs.ResultC():
		if !result.Completed() {
			t.Fatalf("RequestSnapshot result code %d, want completed", result.code)
		}
	case <-ctx.Done():
		t.Fatal("timed out waiting for RequestSnapshot result")
	}
	assertExported(ctx, t, asyncDir)
}

// assertExported checks dir holds at least one entry after an export.
func assertExported(_ context.Context, t *testing.T, dir string) {
	t.Helper()
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatalf("ReadDir(%s): %v", dir, err)
	}
	if len(entries) == 0 {
		t.Errorf("export directory %s is empty, want a snapshot", dir)
	}
}

func TestShardSettings_InvalidConfig_Refused(t *testing.T) {
	nh := newTestNodeHost(t)
	cases := map[string]dbConfig.Config{
		"zero ReplicaID": {
			ShardID: 7, ReplicaID: 0, ElectionRTT: 10, HeartbeatRTT: 1,
		},
		"ElectionRTT not above HeartbeatRTT": {
			ShardID: 8, ReplicaID: 1, ElectionRTT: 2, HeartbeatRTT: 2,
		},
	}
	for name, cfg := range cases {
		t.Run(name, func(t *testing.T) {
			validateErr := cfg.Validate()
			if validateErr == nil {
				t.Fatalf("config %+v passes Validate; the case is not invalid", cfg)
			}
			members := map[uint64]Target{1: nh.RaftAddress()}
			err := nh.StartReplica(members, false, testSMFactory(), cfg)
			var sse *ShardSettingsError
			if !errors.As(err, &sse) {
				t.Fatalf("StartReplica err = %v (%T), want *ShardSettingsError", err, err)
			}
			if sse.Err == nil || sse.Err.Error() != validateErr.Error() {
				t.Errorf("ShardSettingsError.Err = %v, want the Validate error %v", sse.Err, validateErr)
			}
			if errors.Unwrap(err) != sse.Err {
				t.Errorf("errors.Unwrap = %v, want the wrapped validation error", errors.Unwrap(err))
			}
			if !errors.Is(err, ErrInvalidShardSettings) {
				t.Errorf("errors.Is(err, ErrInvalidShardSettings) = false for %v", err)
			}
			if _, _, _, lerr := nh.GetLeaderID(cfg.ShardID); lerr == nil {
				t.Errorf("shard %d is running after a refused StartReplica", cfg.ShardID)
			}
			if info := nh.GetNodeHostInfo(DefaultNodeHostInfoOption); info != nil {
				for _, si := range info.ShardInfoList {
					if si.ShardID == cfg.ShardID {
						t.Errorf("shard %d listed in NodeHostInfo after refusal", cfg.ShardID)
					}
				}
			}
		})
	}
}
