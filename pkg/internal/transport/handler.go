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

package transport

import "github.com/jeremyhahn/go-quicraft/pkg/proto"

// PeerAuth binds a message's in-band sender claims to the connection; valid only during the call. Identity binds the sender; AuthorizesAddress gates only address-scoped decisions.
type PeerAuth struct {
	// Identity is the peer's verified node identity, or "" when it cannot be identified.
	Identity string

	// AuthorizesAddress reports whether the verified TLS certificate of the
	// connection this call arrived on covers the given network address. A nil
	// func must be treated as unauthorized for every address.
	AuthorizesAddress func(address string) bool
}

// Authorizes reports whether the peer's certificate covers address, treating a
// nil callback as unauthorized. It exists so every caller fails closed the same
// way instead of each remembering to nil-check.
func (p PeerAuth) Authorizes(address string) bool {
	if p.AuthorizesAddress == nil {
		return false
	}
	return p.AuthorizesAddress(address)
}

// MessageHandler defines the interface for handling inbound Raft messages
// and snapshots. The engine implements this interface to receive messages
// from the transport layer.
type MessageHandler interface {
	// HandleMessage processes an inbound batch, binding msg.From and SourceAddress to peer first.
	HandleMessage(batch proto.MessageBatch, peer PeerAuth) error

	// HandleSnapshot processes inbound snapshot chunks, binding chunk.From to peer before persisting.
	HandleSnapshot(chunks []proto.SnapshotChunk, peer PeerAuth) error

	// AuthorizeSnapshot decides from the header, before any slot or memory is taken, whether replica from may send a snapshot to replicaID of shardID.
	AuthorizeSnapshot(shardID, replicaID, from, index uint64, peer PeerAuth) error
}
