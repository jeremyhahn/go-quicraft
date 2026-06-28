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

// MessageHandler defines the interface for handling inbound Raft messages
// and snapshots. The engine implements this interface to receive messages
// from the transport layer.
type MessageHandler interface {
	// HandleMessage processes an inbound batch of Raft messages.
	//
	// senderAuthorized reports whether the verified TLS certificate of the
	// connection the batch arrived on authorizes the given network address.
	// The handler MUST use it to bind the in-band batch SourceAddress to the
	// sending peer's certificate before learning it as a (shard, replica)
	// return-path address: SourceAddress is not authenticated by mTLS, so
	// without this binding a peer could teach the registry an address its
	// certificate does not cover and hijack another replica's return path. A
	// nil senderAuthorized must be treated as unauthorized.
	HandleMessage(batch proto.MessageBatch, senderAuthorized func(address string) bool) error

	// HandleSnapshot processes inbound snapshot chunks.
	//
	// senderAuthorized reports whether the verified TLS certificate of the
	// connection the snapshot arrived on authorizes the given network
	// address. The handler MUST use it to bind the in-band chunk.From
	// replica identity (via its registered address) to the sending peer's
	// certificate before persisting anything: chunk fields are NOT
	// authenticated by mTLS, so without this binding any authenticated
	// peer could forge another replica's identity and inject snapshot
	// state. A nil senderAuthorized must be treated as unauthorized.
	HandleSnapshot(chunks []proto.SnapshotChunk, senderAuthorized func(address string) bool) error
}
