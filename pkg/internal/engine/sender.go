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

// MessageSender sends Raft messages to peers; Send must be concurrency-safe and non-blocking, and undeliverable messages are dropped.
type MessageSender interface {
	// Send delivers a batch of Raft protocol messages to their
	// respective target replicas. The msgs slice must not be retained
	// after Send returns.
	Send(msgs []proto.Message)

	// SendSnapshot streams msg.Snapshot.Filepath asynchronously and must deliver a SnapshotStatus echoing msg.Hint verbatim (the transfer sequence, not the index).
	SendSnapshot(msg proto.Message)
}
