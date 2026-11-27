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
	"errors"
	"os"
	"testing"

	"github.com/jeremyhahn/go-quicraft/pkg/logdb"
)

// TestSnapshotPool_ChooseRecoverySnapshot_FallbackMatchesRecordIndex pins the
// discovery branch of chooseRecoverySnapshot: a LogDB record without a
// Filepath resolves to the snapshot directory at the RECORDED index, never to
// the newest (or oldest) directory on disk. Recovery stamps the apply frontier
// with the record's index, so reading any other directory would double-apply
// or skip the entries between the two indexes.
func TestSnapshotPool_ChooseRecoverySnapshot_FallbackMatchesRecordIndex(t *testing.T) {
	tests := []struct {
		name      string
		dirs      []uint64
		filepath  bool
		wantIndex uint64
		wantDir   uint64
		wantErr   bool
	}{
		{name: "record index among newer and older dirs", dirs: []uint64{100, 200, 300}, wantIndex: 200, wantDir: 200},
		{name: "only a newer dir on disk", dirs: []uint64{300}, wantErr: true},
		{name: "only an older dir on disk", dirs: []uint64{100}, wantErr: true},
		{name: "recorded filepath is used unchanged", dirs: []uint64{300}, filepath: true, wantIndex: 200},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			stopC := make(chan struct{})
			defer close(stopC)
			ldb := newSnapshotTestLogDB()
			p := newSnapshotPool(2, ldb, nil, nil, nil, stopC, nil)
			node, _ := newRecoveryTestNode(t, 1, 1, newSnapshotTestSnapshotter(0))
			for _, idx := range tt.dirs {
				if err := os.MkdirAll(snapshotSaveDir(node.SnapshotDir(), 1, 1, idx), 0o750); err != nil {
					t.Fatalf("mkdir snapshot dir %d: %v", idx, err)
				}
			}
			rec := logdb.Snapshot{Index: 200, Term: 2}
			if tt.filepath {
				rec.Filepath = t.TempDir()
			}
			if err := ldb.SaveSnapshot(1, 1, rec); err != nil {
				t.Fatalf("SaveSnapshot: %v", err)
			}

			_, dir, idx, err := p.chooseRecoverySnapshot(snapshotRequest{shardID: 1, replicaID: 1, node: node})
			if tt.wantErr {
				if !errors.Is(err, ErrSnapshotNotFound) {
					t.Fatalf("chooseRecoverySnapshot error = %v (dir %q, index %d), want ErrSnapshotNotFound", err, dir, idx)
				}
				return
			}
			if err != nil {
				t.Fatalf("chooseRecoverySnapshot: %v", err)
			}
			wantPath := rec.Filepath
			if !tt.filepath {
				wantPath = snapshotSaveDir(node.SnapshotDir(), 1, 1, tt.wantDir)
			}
			if dir != wantPath {
				t.Errorf("dir = %q, want %q", dir, wantPath)
			}
			if idx != tt.wantIndex {
				t.Errorf("artifact index = %d, want %d", idx, tt.wantIndex)
			}
		})
	}

	t.Run("recoverSnapshot fails closed without the recorded dir", func(t *testing.T) {
		stopC := make(chan struct{})
		defer close(stopC)
		ldb := newSnapshotTestLogDB()
		p := newSnapshotPool(2, ldb, nil, nil, nil, stopC, nil)
		node, _ := newRecoveryTestNode(t, 1, 1, newSnapshotTestSnapshotter(0))
		if err := os.MkdirAll(snapshotSaveDir(node.SnapshotDir(), 1, 1, 300), 0o750); err != nil {
			t.Fatalf("mkdir snapshot dir: %v", err)
		}
		if err := ldb.SaveSnapshot(1, 1, logdb.Snapshot{Index: 200, Term: 2}); err != nil {
			t.Fatalf("SaveSnapshot: %v", err)
		}

		err := p.recoverSnapshot(snapshotRequest{shardID: 1, replicaID: 1, node: node})
		if !errors.Is(err, ErrSnapshotNotFound) {
			t.Fatalf("recoverSnapshot error = %v, want ErrSnapshotNotFound", err)
		}
		if got := node.LastApplied(); got != 0 {
			t.Errorf("node.LastApplied = %d, want 0", got)
		}
		if got := node.lastSnapshotIndex.Load(); got != 0 {
			t.Errorf("node.lastSnapshotIndex = %d, want 0", got)
		}
	})
}
