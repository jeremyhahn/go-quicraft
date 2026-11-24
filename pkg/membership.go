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

	// MaxWitnesses is the shard's replicated witness cap (0 = no explicit
	// cap). Changed via Host.RequestUpdateWitnessPolicy.
	MaxWitnesses uint64

	// RequireFullReplicaMajority is the shard's replicated
	// full-replica-majority witness policy. Changed via
	// Host.RequestUpdateWitnessPolicy.
	RequireFullReplicaMajority bool

	// SessionExpiryEntries, MaxTotalSessions, MaxResponsesPerSession and MaxSessionsPerOwner are the shard's replicated session policy, seeded at bootstrap.
	SessionExpiryEntries uint64
	// MaxTotalSessions is the replicated cap on concurrently registered
	// sessions. Zero means the shard committed no session policy at all.
	MaxTotalSessions uint64
	// MaxResponsesPerSession is how many cached results one session retains for duplicate detection.
	MaxResponsesPerSession uint64
	// MaxSessionsPerOwner caps concurrent sessions per owner; zero means no per-owner cap.
	MaxSessionsPerOwner uint64

	// WireVersion is the shard's committed wire version; zero reads as the previous version.
	WireVersion uint64

	// Identities maps replicaID to the node identity the shard committed for it, the key peers authorize its messages by.
	Identities map[uint64]string

	// RemovedIdentities maps each removed replicaID to its committed identity, which stays banned from the shard.
	RemovedIdentities map[uint64]string
}

// ConfigChangeIndex returns ConfigChangeID, the configuration version.
func (m *Membership) ConfigChangeIndex() uint64 {
	return m.ConfigChangeID
}

// NewMembership creates a Membership with all maps initialized to empty.
// This prevents nil map dereference when adding entries.
func NewMembership() *Membership {
	return &Membership{
		Nodes:             make(map[uint64]string),
		Observers:         make(map[uint64]string),
		Witnesses:         make(map[uint64]string),
		Removed:           make(map[uint64]struct{}),
		Identities:        make(map[uint64]string),
		RemovedIdentities: make(map[uint64]string),
	}
}
