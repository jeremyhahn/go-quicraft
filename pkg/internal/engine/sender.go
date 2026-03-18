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

import "github.com/jeremyhahn/go-quicraft/pkg/proto"

// MessageSender is the interface for sending Raft protocol messages to
// remote peers. The engine calls Send after producing an Update in the
// step worker, pipelining message delivery before persistence.
//
// Implementations must be safe for concurrent use by multiple step
// workers. Send must not block; it buffers messages into the transport
// send queue. Messages that cannot be delivered are silently dropped
// (Raft handles retransmission via heartbeat/replicate cycles).
type MessageSender interface {
	// Send delivers a batch of Raft protocol messages to their
	// respective target replicas. The msgs slice must not be retained
	// after Send returns.
	Send(msgs []proto.Message)

	// SendSnapshot initiates an asynchronous snapshot transfer for an
	// InstallSnapshot message. The implementation reads the snapshot
	// data file referenced by msg.Snapshot.Filepath, splits it into
	// transport-level chunks, and streams them to the target replica.
	//
	// On completion (success or failure), the implementation must
	// deliver a SnapshotStatus message back to the sender's raft
	// layer via the engine so the leader can transition the remote
	// out of the snapshot state.
	//
	// SendSnapshot is called from the step worker goroutine. The
	// implementation must not block; it should launch the transfer
	// asynchronously and return immediately.
	SendSnapshot(msg proto.Message)
}
