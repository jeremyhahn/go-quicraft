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

package raftio

import (
	"context"
	"errors"
)

// LogDBBinVersion is the binary format version for LogDB data.
const LogDBBinVersion uint32 = 210

// PlainLogDBBinVersion is the binary format version for plain LogDB data.
const PlainLogDBBinVersion uint32 = 100

// TransportBinVersion is the binary format version for transport data.
const TransportBinVersion uint32 = 210

// ErrNoSavedLog indicates no saved log exists for the shard/replica.
var ErrNoSavedLog = errors.New("no saved log")

// ErrNoBootstrapInfo indicates no bootstrap info exists for the shard/replica.
var ErrNoBootstrapInfo = errors.New("no bootstrap info")

// Metrics reports I/O subsystem load.
type Metrics struct {
	// Busy indicates whether the subsystem is under heavy load.
	Busy bool
}

// RaftState holds the persisted Raft state read from LogDB.
type RaftState struct {
	// FirstIndex is the first available log index.
	FirstIndex uint64

	// EntryCount is the number of entries in the log.
	EntryCount uint64
}

// MessageHandler processes inbound Raft message batches.
type MessageHandler func(msg interface{})

// ChunkHandler processes inbound snapshot chunks. Returns true to
// indicate the chunk was accepted.
type ChunkHandler func(chunk interface{}) bool

// ILogDB is the interface for the Raft log database. In dragonboat,
// this is implemented by pebble-based and custom backends. QuicRaft
// provides this interface for API compatibility with factory types.
type ILogDB interface {
	// Name returns the human-readable name of the LogDB implementation.
	Name() string

	// Close releases all resources held by the LogDB.
	Close() error

	// BinaryFormat returns the binary format version of the data stored.
	BinaryFormat() uint32

	// ListNodeInfo returns all shard/replica pairs known to the LogDB.
	ListNodeInfo() ([]NodeInfo, error)

	// SaveBootstrapInfo persists the bootstrap configuration for a
	// shard/replica pair.
	SaveBootstrapInfo(shardID uint64, replicaID uint64, bootstrap interface{}) error

	// GetBootstrapInfo retrieves the bootstrap configuration for a
	// shard/replica pair.
	GetBootstrapInfo(shardID uint64, replicaID uint64) (interface{}, error)

	// SaveRaftState persists Raft state updates for a shard.
	SaveRaftState(updates []interface{}, shardID uint64) error

	// IterateEntries returns log entries in [low, high) up to maxSize bytes.
	IterateEntries(ents []interface{}, size uint64,
		shardID uint64, replicaID uint64,
		low uint64, high uint64, maxSize uint64) ([]interface{}, uint64, error)

	// ReadRaftState reads the persisted Raft state.
	ReadRaftState(shardID uint64, replicaID uint64,
		lastIndex uint64) (RaftState, error)

	// RemoveEntriesTo removes log entries up to the specified index.
	RemoveEntriesTo(shardID uint64, replicaID uint64, index uint64) error

	// CompactEntriesTo compacts log entries up to the specified index.
	// Returns a channel that is closed when compaction completes.
	CompactEntriesTo(shardID uint64, replicaID uint64,
		index uint64) (<-chan struct{}, error)

	// SaveSnapshots persists snapshot metadata.
	SaveSnapshots(updates []interface{}) error

	// GetSnapshot retrieves the latest snapshot metadata.
	GetSnapshot(shardID uint64, replicaID uint64) (interface{}, error)

	// RemoveNodeData removes all data for a shard/replica pair.
	RemoveNodeData(shardID uint64, replicaID uint64) error

	// ImportSnapshot imports a snapshot for a replica.
	ImportSnapshot(snapshot interface{}, replicaID uint64) error
}

// ITransport is the interface for Raft network transport.
type ITransport interface {
	// Name returns the human-readable name of the transport.
	Name() string

	// Start starts the transport.
	Start() error

	// Close stops the transport and releases resources.
	Close() error

	// GetConnection returns a connection to the target address for
	// sending Raft messages.
	GetConnection(ctx context.Context, target string) (IConnection, error)

	// GetSnapshotConnection returns a connection to the target address
	// for sending snapshot chunks.
	GetSnapshotConnection(ctx context.Context, target string) (ISnapshotConnection, error)
}

// IConnection is a connection for sending Raft messages.
type IConnection interface {
	// Close closes the connection.
	Close()

	// SendMessageBatch sends a batch of Raft messages.
	SendMessageBatch(batch interface{}) error
}

// ISnapshotConnection is a connection for sending snapshot chunks.
type ISnapshotConnection interface {
	// Close closes the connection.
	Close()

	// SendChunk sends a snapshot chunk.
	SendChunk(chunk interface{}) error
}

// INodeRegistry provides node address resolution for Raft transport.
type INodeRegistry interface {
	// Close releases resources held by the registry.
	Close() error

	// Add registers a shard/replica pair with the given address.
	Add(shardID uint64, replicaID uint64, url string)

	// Remove unregisters a shard/replica pair.
	Remove(shardID uint64, replicaID uint64)

	// RemoveShard unregisters all replicas for a shard.
	RemoveShard(shardID uint64)

	// Resolve returns the address for a shard/replica pair. Returns
	// the address, an alternative key, and any error.
	Resolve(shardID uint64, replicaID uint64) (string, string, error)
}
