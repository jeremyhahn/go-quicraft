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

package dragonboat

import quicraft "github.com/jeremyhahn/go-quicraft/pkg"

// Membership represents cluster membership in dragonboat's format.
// Dragonboat uses "NonVotings" for what quicraft calls "Observers".
type Membership struct {
	// ConfigChangeID is the Raft log index of the configuration change
	// that produced this membership.
	ConfigChangeID uint64

	// Nodes maps replicaID to network address for full voting members.
	Nodes map[uint64]Target

	// NonVotings maps replicaID to network address for non-voting
	// observer replicas. Dragonboat uses "NonVotings" while quicraft
	// uses "Observers".
	NonVotings map[uint64]Target

	// Witnesses maps replicaID to network address for witness replicas
	// that participate in elections but do not maintain a state machine.
	Witnesses map[uint64]Target

	// Removed tracks replicaIDs that have been removed from the cluster.
	Removed map[uint64]struct{}
}

// fromQuicraftMembership converts a quicraft Membership to the
// dragonboat-compatible format. Returns nil if the input is nil.
func fromQuicraftMembership(m *quicraft.Membership) *Membership {
	if m == nil {
		return nil
	}

	dm := &Membership{
		ConfigChangeID: m.ConfigChangeID,
		Nodes:          make(map[uint64]Target, len(m.Nodes)),
		NonVotings:     make(map[uint64]Target, len(m.Observers)),
		Witnesses:      make(map[uint64]Target, len(m.Witnesses)),
		Removed:        make(map[uint64]struct{}, len(m.Removed)),
	}

	for id, addr := range m.Nodes {
		dm.Nodes[id] = addr
	}
	for id, addr := range m.Observers {
		dm.NonVotings[id] = addr
	}
	for id, addr := range m.Witnesses {
		dm.Witnesses[id] = addr
	}
	for id, v := range m.Removed {
		dm.Removed[id] = v
	}

	return dm
}
