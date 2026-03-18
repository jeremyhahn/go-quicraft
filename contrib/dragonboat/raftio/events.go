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

// Package raftio provides dragonboat-compatible Raft I/O event listener
// interfaces and info types that map to quicraft's event system.
package raftio

import qcConfig "github.com/jeremyhahn/go-quicraft/pkg/config"

// NoLeader is the special leader ID indicating no leader is known.
const NoLeader uint64 = 0

// LeaderInfo contains information about a leader change event.
type LeaderInfo struct {
	ShardID   uint64
	ReplicaID uint64
	Term      uint64
	LeaderID  uint64
}

// NodeInfo identifies a shard/replica pair for event notifications.
type NodeInfo struct {
	ShardID   uint64
	ReplicaID uint64
}

// ConnectionInfo contains connection event details.
type ConnectionInfo struct {
	Address string
	// SnapshotConnection indicates whether the connection is used for
	// snapshot transfers. Known limitation: this field is always false
	// in the QuicRaft adapter because QuicRaft multiplexes all traffic
	// over a single QUIC connection and does not expose a per-connection
	// snapshot indicator. Dragonboat's separate TCP snapshot connections
	// have no direct equivalent in QuicRaft's transport model.
	SnapshotConnection bool
}

// SnapshotInfo contains snapshot event details.
type SnapshotInfo struct {
	ShardID   uint64
	ReplicaID uint64
	From      uint64
	Index     uint64
}

// EntryInfo contains log entry event details for compaction notifications.
type EntryInfo struct {
	ShardID   uint64
	ReplicaID uint64
	Index     uint64
}

// GetNodeInfo constructs a NodeInfo from shard and replica identifiers.
func GetNodeInfo(shardID, replicaID uint64) NodeInfo {
	return NodeInfo{ShardID: shardID, ReplicaID: replicaID}
}

// IRaftEventListener receives Raft protocol event notifications such as
// leader changes.
type IRaftEventListener interface {
	LeaderUpdated(info LeaderInfo)
}

// ISystemEventListener receives system-level event notifications such as
// node lifecycle changes, connection events, and snapshot operations.
type ISystemEventListener interface {
	NodeHostShuttingDown()
	NodeUnloaded(info NodeInfo)
	NodeDeleted(info NodeInfo)
	NodeReady(info NodeInfo)
	MembershipChanged(info NodeInfo)
	ConnectionEstablished(info ConnectionInfo)
	ConnectionFailed(info ConnectionInfo)
	SendSnapshotStarted(info SnapshotInfo)
	SendSnapshotCompleted(info SnapshotInfo)
	SendSnapshotAborted(info SnapshotInfo)
	SnapshotReceived(info SnapshotInfo)
	SnapshotRecovered(info SnapshotInfo)
	SnapshotCreated(info SnapshotInfo)
	SnapshotCompacted(info SnapshotInfo)
	LogCompacted(info EntryInfo)
	LogDBCompacted(info EntryInfo)
}

// ConvertEventListeners converts dragonboat event listener interfaces to
// quicraft's EventListener struct. Each interface method is wired to the
// corresponding function field on the EventListener, converting info types
// between the two type systems. Nil listeners are handled gracefully by
// omitting their callbacks.
func ConvertEventListeners(raft IRaftEventListener, sys ISystemEventListener) *qcConfig.EventListener {
	el := &qcConfig.EventListener{}

	if raft != nil {
		el.OnLeaderUpdated = func(info qcConfig.LeaderInfo) {
			raft.LeaderUpdated(LeaderInfo{
				ShardID:   info.ShardID,
				ReplicaID: info.ReplicaID,
				Term:      info.Term,
				LeaderID:  info.LeaderID,
			})
		}
	}

	if sys != nil {
		el.OnNodeHostShuttingDown = func() {
			sys.NodeHostShuttingDown()
		}

		el.OnNodeReady = func(info qcConfig.EventShardInfo) {
			sys.NodeReady(NodeInfo{
				ShardID:   info.ShardID,
				ReplicaID: info.ReplicaID,
			})
		}

		el.OnNodeUnloaded = func(info qcConfig.EventShardInfo) {
			sys.NodeUnloaded(NodeInfo{
				ShardID:   info.ShardID,
				ReplicaID: info.ReplicaID,
			})
		}

		el.OnNodeDeleted = func(info qcConfig.EventShardInfo) {
			sys.NodeDeleted(NodeInfo{
				ShardID:   info.ShardID,
				ReplicaID: info.ReplicaID,
			})
		}

		el.OnMembershipChanged = func(info qcConfig.EventShardInfo) {
			sys.MembershipChanged(NodeInfo{
				ShardID:   info.ShardID,
				ReplicaID: info.ReplicaID,
			})
		}

		el.OnConnectionEstablished = func(info qcConfig.ConnectionInfo) {
			sys.ConnectionEstablished(ConnectionInfo{
				Address: info.Address,
				// Known limitation: QuicRaft multiplexes all traffic
				// (Raft messages and snapshots) over a single QUIC
				// connection, so qcConfig.ConnectionInfo does not
				// distinguish snapshot connections from regular ones.
				// SnapshotConnection is always false in this adapter.
				// Dragonboat users who rely on this field to filter
				// connection events should be aware that snapshot
				// connection events will appear as regular connection
				// events. This does not affect correctness since
				// snapshots are still transferred; only the event
				// metadata differs.
				SnapshotConnection: false,
			})
		}

		el.OnConnectionFailed = func(info qcConfig.ConnectionInfo) {
			sys.ConnectionFailed(ConnectionInfo{
				Address: info.Address,
				// Known limitation: see OnConnectionEstablished above.
				// SnapshotConnection is always false because QuicRaft
				// does not distinguish snapshot connections from regular
				// Raft connections at the transport level.
				SnapshotConnection: false,
			})
		}

		el.OnSnapshotSendStarted = func(info qcConfig.SnapshotInfo) {
			sys.SendSnapshotStarted(SnapshotInfo{
				ShardID:   info.ShardID,
				ReplicaID: info.ReplicaID,
				From:      info.From,
				Index:     info.Index,
			})
		}

		el.OnSnapshotSent = func(info qcConfig.SnapshotInfo) {
			sys.SendSnapshotCompleted(SnapshotInfo{
				ShardID:   info.ShardID,
				ReplicaID: info.ReplicaID,
				From:      info.From,
				Index:     info.Index,
			})
		}

		el.OnSnapshotSendAborted = func(info qcConfig.SnapshotInfo) {
			sys.SendSnapshotAborted(SnapshotInfo{
				ShardID:   info.ShardID,
				ReplicaID: info.ReplicaID,
				From:      info.From,
				Index:     info.Index,
			})
		}

		el.OnSnapshotReceived = func(info qcConfig.SnapshotInfo) {
			sys.SnapshotReceived(SnapshotInfo{
				ShardID:   info.ShardID,
				ReplicaID: info.ReplicaID,
				From:      info.From,
				Index:     info.Index,
			})
		}

		el.OnSnapshotRecovered = func(info qcConfig.SnapshotInfo) {
			sys.SnapshotRecovered(SnapshotInfo{
				ShardID:   info.ShardID,
				ReplicaID: info.ReplicaID,
				From:      info.From,
				Index:     info.Index,
			})
		}

		el.OnSnapshotCreated = func(info qcConfig.SnapshotInfo) {
			sys.SnapshotCreated(SnapshotInfo{
				ShardID:   info.ShardID,
				ReplicaID: info.ReplicaID,
				From:      info.From,
				Index:     info.Index,
			})
		}

		el.OnSnapshotCompacted = func(info qcConfig.SnapshotInfo) {
			sys.SnapshotCompacted(SnapshotInfo{
				ShardID:   info.ShardID,
				ReplicaID: info.ReplicaID,
				From:      info.From,
				Index:     info.Index,
			})
		}

		el.OnLogCompacted = func(info qcConfig.EventShardInfo) {
			sys.LogCompacted(EntryInfo{
				ShardID:   info.ShardID,
				ReplicaID: info.ReplicaID,
				// TODO: QuicRaft's EventShardInfo does not carry a compaction
				// index. EntryInfo.Index is always 0 until QuicRaft adds an
				// Index field to EventShardInfo for compaction events.
			})
		}

		el.OnLogDBCompacted = func(info qcConfig.EventShardInfo) {
			sys.LogDBCompacted(EntryInfo{
				ShardID:   info.ShardID,
				ReplicaID: info.ReplicaID,
				// TODO: QuicRaft's EventShardInfo does not carry a compaction
				// index. EntryInfo.Index is always 0 until QuicRaft adds an
				// Index field to EventShardInfo for compaction events.
			})
		}
	}

	return el
}
