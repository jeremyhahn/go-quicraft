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
	"log/slog"
	"sort"

	"github.com/jeremyhahn/go-quicraft/pkg/logdb"
)

// Compile-time proof that a WAL DB can be discovered as a residual reporter.
var _ logdb.RecoveryResidualReporter = (*DB)(nil)

// noteGenerationBoundaryResidual records that recovery of one segment
// truncated across a completed fsync, i.e. that acknowledged records may have
// been destroyed with the damage. See logdb.RecoveryResidual for why recovery
// does this rather than refusing to open.
//
// Only the facts recovery knows at THIS point are captured. The per-replica
// index ranges are filled in later by finalizeResiduals, once every segment
// has been replayed: a range read now would name whatever the replay had
// reached mid-way through, which is not the range the node actually came up
// with.
//
// Called from replaySegment, which runs inside recover() before the shard is
// reachable from any DB, so no lock is taken here for the same reason the rest
// of recovery takes none.
func (s *walShard) noteGenerationBoundaryResidual(segID uint64, cleanEnd int64, prefixGen, survivingGen uint32) {
	s.residuals = append(s.residuals, logdb.RecoveryResidual{
		StorageShardIndex:   int(s.shardIdx),
		SegmentID:           segID,
		CleanEnd:            cleanEnd,
		PrefixGeneration:    prefixGen,
		SurvivingGeneration: survivingGen,
	})
}

// finalizeResiduals attaches the surviving per-replica index ranges to every
// residual this shard raised, and logs the whole finding at Error.
//
// Every replica whose log lives in this WAL shard is listed, not a guess at
// which of them the damage hit. The destroyed bytes are the only thing that
// could say whose records they were, so narrowing the list would be inventing
// precision: an operator reconciling against peers has to check all of them.
//
// It runs at the end of recover(), on the same goroutine, before the shard is
// published.
func (s *walShard) finalizeResiduals() {
	if len(s.residuals) == 0 {
		return
	}

	nodes := make([]logdb.ResidualNodeRange, 0, len(s.indexes))
	for key, idx := range s.indexes {
		rng := logdb.ResidualNodeRange{ShardID: key.ShardID, ReplicaID: key.ReplicaID}
		if first, last, count := idx.Bounds(); count > 0 {
			rng.FirstIndex = first.Index
			rng.LastIndex = last.Index
		}
		nodes = append(nodes, rng)
	}
	// Map iteration order is random; a diagnostic an operator compares across
	// two boots must not reshuffle itself.
	sort.Slice(nodes, func(i, j int) bool {
		if nodes[i].ShardID != nodes[j].ShardID {
			return nodes[i].ShardID < nodes[j].ShardID
		}
		return nodes[i].ReplicaID < nodes[j].ReplicaID
	})

	for i := range s.residuals {
		s.residuals[i].Nodes = nodes
		r := &s.residuals[i]
		slog.Error("waldb: recovery completed with a log that may be SHORTER than "+
			"the log this replica acknowledged; an fsync completed inside the "+
			"destroyed region, so records it made durable were lost with it — "+
			"reconcile this replica against its peers or rebuild it from a snapshot",
			"dir", s.dir,
			"wal_shard_index", r.StorageShardIndex,
			"segment_id", r.SegmentID,
			"clean_end", r.CleanEnd,
			"prefix_generation", r.PrefixGeneration,
			"surviving_generation", r.SurvivingGeneration,
			"affected_replicas", len(r.Nodes),
		)
		for _, n := range r.Nodes {
			slog.Error("waldb: replica surviving log range after a shortened recovery; "+
				"entries above last_index may have been acknowledged and lost",
				"dir", s.dir,
				"shard_id", n.ShardID,
				"replica_id", n.ReplicaID,
				"first_index", n.FirstIndex,
				"last_index", n.LastIndex,
			)
		}
	}
}

// recoveryResiduals returns this shard's residuals under the read lock.
func (s *walShard) recoveryResiduals() []logdb.RecoveryResidual {
	s.dataMu.RLock()
	defer s.dataMu.RUnlock()
	return s.residuals
}

// RecoveryResiduals implements logdb.RecoveryResidualReporter.
//
// It returns every generation-boundary residual raised while this DB was
// opened, ordered by WAL shard index and then segment ID, or nil when recovery
// was clean. A non-empty result means the node is running on a log that may be
// missing entries it already acknowledged to peers; see logdb.RecoveryResidual
// for the full contract.
//
// The value is fixed at Open. No later operation — including Close — clears or
// adds to it, so a caller may poll it at any point in the DB's life and get the
// same answer. It is not persisted: the damage is repaired on disk during the
// open that detects it, so a subsequent open of the same directory reports
// nothing, and a caller that needs the fact to outlive the process must record
// it itself.
//
// The returned slices are fresh; the caller may retain and modify them.
func (db *DB) RecoveryResiduals() []logdb.RecoveryResidual {
	var out []logdb.RecoveryResidual
	for _, s := range db.shards {
		if s == nil {
			continue
		}
		for _, r := range s.recoveryResiduals() {
			r.Nodes = append([]logdb.ResidualNodeRange(nil), r.Nodes...)
			out = append(out, r)
		}
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].StorageShardIndex != out[j].StorageShardIndex {
			return out[i].StorageShardIndex < out[j].StorageShardIndex
		}
		return out[i].SegmentID < out[j].SegmentID
	})
	return out
}
