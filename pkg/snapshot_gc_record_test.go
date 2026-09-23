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

package quicraft

import (
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"testing"

	"github.com/jeremyhahn/go-quicraft/pkg/logdb"
	"github.com/jeremyhahn/go-quicraft/pkg/logdb/memdb"
)

// unreadableRecordLogDB fails every snapshot-record read.
type unreadableRecordLogDB struct {
	logdb.LogDB
}

func (unreadableRecordLogDB) GetSnapshot(uint64, uint64) (logdb.Snapshot, error) {
	return logdb.Snapshot{}, errors.New("injected: snapshot record unreadable")
}

// TestSnapshotRecordGuards_FailClosedOnAnUnreadableRecord: the snapshot GC and
// a receive's publish spare the directory the LogDB record names, and both
// used to read an UNREADABLE record as "no record" — so one transient read
// error let GC, under retention pressure, delete the recorded snapshot whose
// log below it was already compacted. An unreadable record now makes GC
// reclaim nothing, and makes any directory count as possibly recorded.
func TestSnapshotRecordGuards_FailClosedOnAnUnreadableRecord(t *testing.T) {
	const shardID, replicaID = uint64(9), uint64(1)
	base := t.TempDir()
	h := &Host{logdb: unreadableRecordLogDB{memdb.New()}, logger: slog.Default()}

	replicaDir := filepath.Join(base, fmt.Sprintf("shard-%d", shardID), fmt.Sprintf("replica-%d", replicaID))
	var dirs []string
	for i := 1; i <= 5; i++ { // more than the GC retains
		dir := filepath.Join(replicaDir, fmt.Sprintf("snapshot-%020d", i*100))
		if err := os.MkdirAll(dir, 0o750); err != nil {
			t.Fatalf("mkdir: %v", err)
		}
		dirs = append(dirs, dir)
	}

	h.gcShardSnapshots(base, shardID, replicaID)
	for _, dir := range dirs {
		if _, err := os.Stat(dir); err != nil {
			t.Fatalf("GC reclaimed %s although the record naming what to spare was unreadable: %v", dir, err)
		}
	}
	if !h.snapshotRecordMayName(shardID, replicaID, dirs[0]) {
		t.Fatal("an unreadable record was taken to name nothing: a publish or discard would delete what it names")
	}
}
