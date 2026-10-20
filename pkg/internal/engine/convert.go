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
	"github.com/jeremyhahn/go-quicraft/pkg/config"
	"github.com/jeremyhahn/go-quicraft/pkg/logdb"
	"github.com/jeremyhahn/go-quicraft/pkg/proto"
	"path/filepath"
)

// protoEntryToLogDB converts a proto.Entry to a logdb.Entry. All fields
// are copied, including the session fields (Key, ClientID, SeriesID,
// RespondedTo, OwnerID): they must survive the WAL round-trip or session dedup
// (at-most-once delivery) silently breaks across crash recovery.
//
// This function is field-by-field and therefore does NOT fail to compile when a
// field is added to either type — a new field simply stops being written, and
// the omission surfaces only as a behaviour change after a restart. Every
// addition to proto.Entry must be added here in the same change.
func protoEntryToLogDB(e *proto.Entry) logdb.Entry {
	return logdb.Entry{
		Index:       e.Index,
		Term:        e.Term,
		Type:        e.Type,
		Key:         e.Key,
		ClientID:    e.ClientID,
		SeriesID:    e.SeriesID,
		RespondedTo: e.RespondedTo,
		OwnerID:     e.OwnerID,
		Cmd:         e.Cmd,
	}
}

// protoStateToLogDB converts a proto.State to a logdb.State.
func protoStateToLogDB(s *proto.State) logdb.State {
	return logdb.State{
		Term:       s.Term,
		Vote:       s.Vote,
		Commit:     s.Commit,
		VoteWindow: s.VoteWindow,
	}
}

// protoMembershipToLogDB converts a proto.Membership to a logdb.Membership.
func protoMembershipToLogDB(m *proto.Membership) logdb.Membership {
	// MaxWitnesses and RequireFullReplicaMajority are replicated membership
	// state, not local config: dropping them here would persist every
	// snapshot with a zeroed witness policy, so a restarted replica would
	// compute a different witness quorum than its peers.
	//
	// The session policy is replicated state for the same reason and is
	// dropped just as silently. A snapshot persisted without it comes back
	// unset, the recovering replica falls back to its own Config, and it
	// expires sessions at log indexes its peers do not — a state-machine
	// divergence, not a recoverable error. See proto.SessionPolicy.
	result := logdb.Membership{
		ConfigChangeID:             m.ConfigChangeID,
		MaxWitnesses:               m.MaxWitnesses,
		RequireFullReplicaMajority: m.RequireFullReplicaMajority,
		SessionExpiryEntries:       m.SessionExpiryEntries,
		MaxTotalSessions:           m.MaxTotalSessions,
		MaxResponsesPerSession:     m.MaxResponsesPerSession,
		MaxSessionsPerOwner:        m.MaxSessionsPerOwner,
	}
	if len(m.Addresses) > 0 {
		result.Addresses = make(map[uint64]string, len(m.Addresses))
		for k, v := range m.Addresses {
			result.Addresses[k] = v
		}
	}
	if len(m.Observers) > 0 {
		result.Observers = make(map[uint64]string, len(m.Observers))
		for k, v := range m.Observers {
			result.Observers[k] = v
		}
	}
	if len(m.Witnesses) > 0 {
		result.Witnesses = make(map[uint64]string, len(m.Witnesses))
		for k, v := range m.Witnesses {
			result.Witnesses[k] = v
		}
	}
	if len(m.Removed) > 0 {
		result.Removed = make(map[uint64]bool, len(m.Removed))
		for k, v := range m.Removed {
			result.Removed[k] = v
		}
	}
	// Identities is the WRITE side of the snapshot-metadata round-trip: this
	// is the conversion a replica's own snapshot save goes through, so
	// dropping it here leaves nothing for recovery to read back and the
	// reloaded configuration silently authorizes by address alone. See
	// logdb.Membership.Identities.
	if len(m.Identities) > 0 {
		result.Identities = make(map[uint64]string, len(m.Identities))
		for k, v := range m.Identities {
			result.Identities[k] = v
		}
	}
	// RemovedIdentities is the identity half of the removal deny-list. Dropping
	// it here would revoke a revocation: the snapshot this save produces is what
	// a restart reads back, and a replica that comes up without its tombstone
	// identities can be evaded by a removed node claiming another replica ID.
	if len(m.RemovedIdentities) > 0 {
		result.RemovedIdentities = make(map[uint64]string, len(m.RemovedIdentities))
		for k, v := range m.RemovedIdentities {
			result.RemovedIdentities[k] = v
		}
	}
	return result
}

// snapshotRecordPath converts a proto snapshot's Filepath, which names the
// artifact's DATA FILE (<dir>/snapshot.dat, the file the send path opens), to
// the DIRECTORY a LogDB snapshot record names; every reader of a record joins
// snapshot.dat back on. The receive path records the directory itself, and
// before the receive became two-phase its record was always written first, so
// the one this conversion produced from raft's Update was discarded as out of
// date. Now it can be written first, and a record naming the file makes every
// reader open <dir>/snapshot.dat/snapshot.dat. A path that does not name the
// data file is already a directory and is kept.
func snapshotRecordPath(protoPath string) string {
	if filepath.Base(protoPath) == snapshotDataFile {
		return filepath.Dir(protoPath)
	}
	return protoPath
}

// protoSnapshotToLogDB converts a proto.Snapshot to a logdb.Snapshot.
//
// Compression is carried, and it could not be until proto.Snapshot grew the
// field: every record this conversion produced therefore said NoCompression
// whatever the artifact actually was. That is the one field here whose loss is
// not merely descriptive — recovery builds its reader chain from it, so a
// snappy artifact recorded as uncompressed is handed to the state machine as
// garbage. The direct receive path and the import path set it correctly, which
// is what kept the gap from showing.
func protoSnapshotToLogDB(s *proto.Snapshot) logdb.Snapshot {
	return logdb.Snapshot{
		Index:       s.Index,
		Term:        s.Term,
		Membership:  protoMembershipToLogDB(&s.Membership),
		Filepath:    snapshotRecordPath(s.Filepath),
		FileSize:    s.FileSize,
		OnDiskIndex: s.OnDiskIndex,
		Epoch:       s.Epoch,
		Compression: config.CompressionType(s.Compression),
	}
}

// protoUpdatesToLogDBReuse converts proto updates reusing both the
// destination update slice and a single flat entry buffer. Instead of
// maintaining per-update entry slices (which require N slice headers
// and N capacity checks), all entries across the batch are written into
// one contiguous []logdb.Entry and each logdb.Update receives a
// sub-slice. This reduces allocation pressure from O(N) to O(1) in
// steady state, improves cache locality for sequential LogDB writes,
// and eliminates per-update capacity bookkeeping.
//
// The flat buffer grows on demand and is returned for reuse by the
// caller. The returned dst and flatBuf should be passed back on the
// next call.
func protoUpdatesToLogDBReuse(
	dst []logdb.Update,
	flatBuf []logdb.Entry,
	src []proto.Update,
) ([]logdb.Update, []logdb.Entry) {
	// Size the destination update slice.
	if cap(dst) >= len(src) {
		dst = dst[:len(src)]
	} else {
		dst = make([]logdb.Update, len(src))
	}

	// Count total entries across all updates for flat buffer sizing.
	totalEntries := 0
	for i := range src {
		totalEntries += len(src[i].EntriesToSave)
	}

	// Size the flat entry buffer. Reuse when capacity permits.
	if cap(flatBuf) >= totalEntries {
		flatBuf = flatBuf[:totalEntries]
	} else {
		flatBuf = make([]logdb.Entry, totalEntries)
	}

	// Convert all entries into the flat buffer and assign sub-slices
	// to each logdb.Update.
	offset := 0
	for i := range src {
		u := &src[i]
		n := len(u.EntriesToSave)
		sub := flatBuf[offset : offset+n]
		for j := range u.EntriesToSave {
			sub[j] = protoEntryToLogDB(&u.EntriesToSave[j])
		}
		dst[i] = logdb.Update{
			ShardID:   u.ShardID,
			ReplicaID: u.ReplicaID,
			State:     protoStateToLogDB(&u.State),
			Entries:   sub,
			Snapshot:  protoSnapshotToLogDB(&u.Snapshot),
		}
		offset += n
	}
	return dst, flatBuf
}
