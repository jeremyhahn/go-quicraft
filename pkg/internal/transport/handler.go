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

// PeerAuth is everything the handler may use to bind a message's IN-BAND
// claims — msg.From, the batch SourceAddress, a snapshot chunk's From — to the
// peer that actually sent them.
//
// It carries two independently sourced facts, because they answer two different
// questions and only one of them survives a promiscuous certificate:
//
//   - Identity is WHO the peer is: derived from the leaf's public key, so it
//     names exactly one keypair and cannot be presented by anyone else,
//     whatever SANs the certificate carries.
//   - AuthorizesAddress is WHERE the certificate says the peer may be: matched
//     against the leaf's SANs, which a wildcard or shared certificate makes
//     true for every member of the cluster at once.
//
// A handler binds the claimed sender by Identity and nothing else: the committed
// configuration records an identity for every member, and a member with none is
// refused. AuthorizesAddress never admits a message or binds a sender, because a
// wildcard or shared certificate satisfies it for every member. It still gates
// the address-scoped decisions: learning a route from a message's source
// address, and the transport's in-band address checks (SourceAddress clearing,
// peer-epoch changes, live-outbound eviction, snapshot-sender binding). That is
// why StartShard and every membership add refuse a certificate shared between
// members (ErrSharedPeerCertificate).
//
// The value is valid ONLY for the duration of the call. AuthorizesAddress
// captures the inbound *quic.Conn, so a handler that stores it (or hands it to
// a goroutine outliving the call) pins that connection and its buffers for as
// long as the reference lives — including after the peer has gone — and later
// consults a certificate that is no longer the peer's. Call it synchronously;
// copy the boolean result, never the closure. Identity is a plain string and
// may be copied freely.
type PeerAuth struct {
	// Identity is the peer's authenticated node identity
	// (NodeIdentityFromCertificate over the verified leaf), or "" when the
	// connection presents no verified leaf. Mandatory mTLS makes "" mean
	// "unit-test path with no connection" in practice; a handler must treat it
	// as "cannot be identified", never as a wildcard.
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
	// HandleMessage processes an inbound batch of Raft messages.
	//
	// The handler MUST use peer to bind the batch's in-band claims to the
	// sending peer before acting on them: neither msg.From nor SourceAddress is
	// authenticated by mTLS, so without this binding any host holding a valid
	// cluster certificate can assert another replica's identity and have forged
	// quorum responses counted, or teach the routing registry an address its
	// certificate does not cover.
	HandleMessage(batch proto.MessageBatch, peer PeerAuth) error

	// HandleSnapshot processes inbound snapshot chunks.
	//
	// The handler MUST use peer to bind the in-band chunk.From replica identity
	// to the sending peer before persisting anything. Chunk fields are NOT
	// authenticated by mTLS, and a snapshot replaces both the state machine and
	// the committed membership every other authorization decision reads, so
	// this is the single most consequential binding in the transport.
	HandleSnapshot(chunks []proto.SnapshotChunk, peer PeerAuth) error

	// AuthorizeSnapshot decides, from the stream header alone, whether a
	// snapshot transfer may proceed: the sending replica from, to replica
	// replicaID of shardID, over a connection whose verified identity is
	// peer. The transport calls it BEFORE taking a receive slot or any
	// memory, so a peer the shard does not admit — any holder of a cluster
	// certificate — cannot hold those for the length of a transfer and starve
	// the snapshots the shard needs. A non-nil error refuses the transfer.
	// It does not replace HandleSnapshot's own checks: every chunk is bound to
	// the header it authorized, and HandleSnapshot re-checks before it
	// persists anything.
	AuthorizeSnapshot(shardID, replicaID, from, index uint64, peer PeerAuth) error
}
