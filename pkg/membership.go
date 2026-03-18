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

// Membership describes the current cluster membership for a shard.
// It is returned by Host.SyncGetShardMembership and provides a
// point-in-time view of the committed membership configuration.
type Membership struct {
	// ConfigChangeID is the Raft log index of the configuration change
	// that produced this membership. Used to detect stale membership
	// data and to serialize concurrent membership changes.
	ConfigChangeID uint64

	// Nodes maps replicaID to network address for full voting members.
	Nodes map[uint64]string

	// Observers maps replicaID to network address for non-voting
	// observer replicas that receive replicated data but do not
	// participate in elections or quorum.
	Observers map[uint64]string

	// Witnesses maps replicaID to network address for witness replicas
	// that participate in elections and quorum but do not maintain a
	// state machine.
	Witnesses map[uint64]string

	// Removed tracks replicaIDs that have been removed from the cluster.
	// The struct{} value type avoids memory waste for large removed sets.
	Removed map[uint64]struct{}
}

// ConfigChangeIndex returns the ConfigChangeID. This alias exists for
// backward compatibility with code that references ConfigChangeIndex
// instead of ConfigChangeID. Both names refer to the same value: the
// Raft log index of the configuration change that produced this membership.
func (m *Membership) ConfigChangeIndex() uint64 {
	return m.ConfigChangeID
}

// NewMembership creates a Membership with all maps initialized to empty.
// This prevents nil map dereference when adding entries.
func NewMembership() *Membership {
	return &Membership{
		Nodes:     make(map[uint64]string),
		Observers: make(map[uint64]string),
		Witnesses: make(map[uint64]string),
		Removed:   make(map[uint64]struct{}),
	}
}
