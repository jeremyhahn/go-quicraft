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
	"github.com/jeremyhahn/go-quicraft/pkg/logdb"
	"github.com/jeremyhahn/go-quicraft/pkg/proto"
)

// protoEntryToLogDB converts a proto.Entry to a logdb.Entry. Only the
// fields required for persistence are copied; session-specific fields
// (Key, ClientID, SeriesID, RespondedTo) are not stored in LogDB.
func protoEntryToLogDB(e *proto.Entry) logdb.Entry {
	return logdb.Entry{
		Index: e.Index,
		Term:  e.Term,
		Type:  e.Type,
		Cmd:   e.Cmd,
	}
}

// protoStateToLogDB converts a proto.State to a logdb.State.
func protoStateToLogDB(s *proto.State) logdb.State {
	return logdb.State{
		Term:   s.Term,
		Vote:   s.Vote,
		Commit: s.Commit,
	}
}

// protoMembershipToLogDB converts a proto.Membership to a logdb.Membership.
func protoMembershipToLogDB(m *proto.Membership) logdb.Membership {
	result := logdb.Membership{
		ConfigChangeID: m.ConfigChangeID,
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
	return result
}

// protoSnapshotToLogDB converts a proto.Snapshot to a logdb.Snapshot.
func protoSnapshotToLogDB(s *proto.Snapshot) logdb.Snapshot {
	return logdb.Snapshot{
		Index:       s.Index,
		Term:        s.Term,
		Membership:  protoMembershipToLogDB(&s.Membership),
		Filepath:    s.Filepath,
		FileSize:    s.FileSize,
		OnDiskIndex: s.OnDiskIndex,
		Epoch:       s.Epoch,
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
