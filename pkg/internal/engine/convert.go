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

// protoEntryToLogDB copies every proto.Entry field, session fields included; add new fields here in the same change.
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
	// Witness and session policy are replicated state and must survive the snapshot round-trip.
	result := logdb.Membership{
		ConfigChangeID:             m.ConfigChangeID,
		MaxWitnesses:               m.MaxWitnesses,
		RequireFullReplicaMajority: m.RequireFullReplicaMajority,
		SessionExpiryEntries:       m.SessionExpiryEntries,
		MaxTotalSessions:           m.MaxTotalSessions,
		MaxResponsesPerSession:     m.MaxResponsesPerSession,
		MaxSessionsPerOwner:        m.MaxSessionsPerOwner,
		// The committed wire version is replicated state too: a snapshot
		// that dropped it would restart a switched replica on the previous
		// version, and a lower committed value is never accepted again.
		WireVersion: m.WireVersion,
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
	// Identities must be persisted, or a reload authorizes by address alone.
	if len(m.Identities) > 0 {
		result.Identities = make(map[uint64]string, len(m.Identities))
		for k, v := range m.Identities {
			result.Identities[k] = v
		}
	}
	// RemovedIdentities must be persisted so a removed node cannot rejoin under another replica ID.
	if len(m.RemovedIdentities) > 0 {
		result.RemovedIdentities = make(map[uint64]string, len(m.RemovedIdentities))
		for k, v := range m.RemovedIdentities {
			result.RemovedIdentities[k] = v
		}
	}
	return result
}

// snapshotRecordPath converts a snapshot's data-file path to the directory a LogDB record names.
func snapshotRecordPath(protoPath string) string {
	if filepath.Base(protoPath) == snapshotDataFile {
		return filepath.Dir(protoPath)
	}
	return protoPath
}

// protoSnapshotToLogDB converts a proto.Snapshot to a logdb.Snapshot, including its compression.
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

// protoUpdatesToLogDBReuse converts updates into dst and one flat entry buffer, both returned for reuse.
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
