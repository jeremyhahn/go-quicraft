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

package logdb

// RecoveryResidual reports the one outcome of a successful storage recovery
// that a node cannot simply carry on from: the store came back holding a log
// that may be STRICTLY SHORTER than the log this replica already acknowledged
// to its peers.
//
// The WAL backend produces it in a single, precisely characterised situation.
// Recovery classifies damage by the durability generations of the records that
// survive above it: records carrying the store's newest generation have no
// completed fsync behind them and were never acknowledged, so cutting them off
// is a repair. When the survivors' generation is strictly HIGHER than any
// generation in the clean prefix below the damage, a completed fsync fell
// inside the destroyed bytes — which means whatever older-generation records
// the damage swallowed HAD been acknowledged. Recovery still truncates, and
// deliberately so: refusing here would refuse the ordinary interrupted write
// whose lost page begins exactly at a batch boundary, and that is the common
// case the whole scheme exists to repair. What it must not do is stay silent.
//
// A replica in this state is not corrupt and not degraded in any way it can
// detect at runtime. It will serve reads and accept writes while missing
// entries it previously promised, so the condition is only ever resolved from
// the OUTSIDE: reconcile the replica against its peers, or rebuild it from a
// snapshot. Treat a non-empty report as an operational fault requiring
// intervention, not as a warning.
//
// A backend that cannot produce the condition reports nothing; see
// RecoveryResidualReporter.
type RecoveryResidual struct {
	// StorageShardIndex identifies the backend's internal storage shard the
	// damage was found in — for waldb, the WAL directory index in
	// [0, DefaultShardCount). It is NOT a Raft ShardID; the Raft shards
	// affected are listed in Nodes.
	StorageShardIndex int
	// SegmentID identifies the log segment file within that storage shard.
	SegmentID uint64
	// CleanEnd is the byte offset the segment was truncated to: the end of
	// the last record recovery could prove intact.
	CleanEnd int64
	// PrefixGeneration is the highest durability generation observed BELOW
	// the damage, and SurvivingGeneration the lowest observed ABOVE it.
	// SurvivingGeneration is always strictly greater than PrefixGeneration;
	// that gap is exactly the evidence that an fsync completed inside the
	// destroyed region.
	PrefixGeneration    uint32
	SurvivingGeneration uint32
	// Nodes lists every replica whose log lives in the affected storage shard,
	// with the index range that SURVIVED recovery. Entries above LastIndex are
	// the ones at risk: some of them may have been acknowledged and lost. It
	// is a superset of the truly affected replicas — the destroyed bytes
	// cannot say which replicas they belonged to, so every replica sharing the
	// storage shard is reported rather than guessing.
	Nodes []ResidualNodeRange
}

// ResidualNodeRange is one replica's surviving log range at the moment a
// RecoveryResidual was raised.
type ResidualNodeRange struct {
	// ShardID and ReplicaID identify the replica.
	ShardID   uint64
	ReplicaID uint64
	// FirstIndex and LastIndex bound the entries that survived recovery.
	// Both are zero when the replica has no entries left at all.
	FirstIndex uint64
	LastIndex  uint64
}

// RecoveryResidualReporter is the optional capability a LogDB implements when
// it can detect that recovery left the replica shorter than the log it had
// acknowledged. It is deliberately NOT part of LogDB: an in-memory store has no
// recovery and therefore nothing to report, and a backend that cannot produce
// the condition should not be made to carry a method that always returns nil.
// Callers discover it with a type assertion:
//
//	if r, ok := db.(logdb.RecoveryResidualReporter); ok {
//		for _, res := range r.RecoveryResiduals() { ... }
//	}
type RecoveryResidualReporter interface {
	// RecoveryResiduals returns every residual detected while the store was
	// opened, or nil when recovery was clean.
	//
	// The result is a property of the OPEN, fixed once recovery completed:
	// nothing a caller subsequently does clears it, because the whole point is
	// that an operator may look hours later. It does not, however, survive the
	// process — recovery repaired the damage on disk, so a later open of the
	// same store reports nothing. A caller that needs the fact to outlive the
	// process must record it when it observes it.
	RecoveryResiduals() []RecoveryResidual
}
