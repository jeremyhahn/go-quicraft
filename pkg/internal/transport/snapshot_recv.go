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

import (
	"errors"
	"io"
	"log/slog"
	"path/filepath"
	"strings"
	"sync/atomic"
	"time"

	"github.com/jeremyhahn/go-quicraft/pkg/config"
	"github.com/jeremyhahn/go-quicraft/pkg/proto"
	"github.com/quic-go/quic-go"
)

const (
	// maxSnapshotFilepathLen is the maximum filepath length for snapshot
	// chunks. Corresponds to POSIX PATH_MAX.
	maxSnapshotFilepathLen = 4096

	// maxSnapshotChunkDataLen is the maximum data payload length for a
	// single snapshot chunk. Matches the default MaxApplyEntrySize (64MB).
	maxSnapshotChunkDataLen = 64 * 1024 * 1024
)

// chunkReader abstracts the stream operations needed for receiving snapshot
// chunks: setting a read deadline and reading bytes. This enables testing
// with mock streams.
type chunkReader interface {
	io.Reader
	SetReadDeadline(t time.Time) error
}

// EpochTracker manages in-flight epoch reference counts to prevent
// premature purge of encryption keys while snapshot transfers are
// in progress. The concrete implementation is crypto.Barrier.
type EpochTracker interface {
	// RegisterInFlightEpoch increments the in-flight reference count.
	RegisterInFlightEpoch(epoch uint64)
	// DeregisterInFlightEpoch decrements the in-flight reference count.
	DeregisterInFlightEpoch(epoch uint64)
}

// snapshotReceiver manages concurrent snapshot receive operations with
// concurrency limits, memory budgets, and rate limiting.
type snapshotReceiver struct {
	// sem is a counting semaphore limiting concurrent snapshot receives.
	sem chan struct{}

	// memoryBudget tracks the remaining memory budget in bytes. Atomic
	// operations provide lock-free updates.
	memoryBudget atomic.Int64

	// maxMemory is the total memory budget configured at construction.
	maxMemory int64

	// bytesPerSec is the maximum receive rate for snapshot data.
	bytesPerSec int64

	// deploymentID is the expected deployment identifier. Snapshot chunks
	// from a different deployment are rejected to prevent cross-cluster
	// data corruption.
	deploymentID uint64

	// handler processes received snapshot chunks.
	handler MessageHandler

	// epochTracker manages in-flight epoch references to prevent key
	// purge during snapshot receive. Nil when encryption is disabled.
	epochTracker EpochTracker

	// snapshotTracker tracks in-flight snapshot receives so Stop() can
	// atomically reject new operations and wait for existing ones to
	// complete before closing connections. Nil when not set.
	snapshotTracker *shutdownTracker

	// listener holds optional event callbacks. May be nil.
	listener *config.EventListener

	// metrics collects transport-level metrics. Nil when disabled.
	metrics Metrics
}

// newSnapshotReceiver creates a snapshot receiver with the specified limits.
func newSnapshotReceiver(
	maxConcurrent int,
	maxMemory int64,
	bytesPerSec int64,
	deploymentID uint64,
	handler MessageHandler,
) *snapshotReceiver {
	sr := &snapshotReceiver{
		sem:          make(chan struct{}, maxConcurrent),
		maxMemory:    maxMemory,
		bytesPerSec:  bytesPerSec,
		deploymentID: deploymentID,
		handler:      handler,
	}
	sr.memoryBudget.Store(maxMemory)
	return sr
}

// receiveSnapshot processes a snapshot stream. It enforces the concurrency
// semaphore, registers the epoch for in-flight protection, and delegates
// to receiveChunks for the actual I/O. The epoch is derived from the
// snapshot header's Term field, which carries the barrier epoch when
// encryption is enabled. When the epoch is 0 (no encryption), in-flight
// tracking is skipped.
func (sr *snapshotReceiver) receiveSnapshot(stream *quic.Stream, hdr *SnapshotHeader, stopC <-chan struct{}) {
	// Atomically register this receive operation. TryAdd returns false
	// if Stop() has already initiated shutdown, preventing the race
	// between WaitGroup.Add(1) and WaitGroup.Wait().
	if sr.snapshotTracker != nil {
		if !sr.snapshotTracker.TryAdd() {
			return
		}
		defer sr.snapshotTracker.Done()
	}

	// Try to acquire concurrency semaphore.
	select {
	case sr.sem <- struct{}{}:
		defer func() { <-sr.sem }()
	case <-stopC:
		return
	default:
		// At concurrency limit, drop the snapshot.
		return
	}

	// Register in-flight epoch protection if an epoch tracker is available
	// and the snapshot has a non-zero epoch (barrier-encrypted snapshot).
	// This prevents PurgeEpochsBefore from removing the DEK while the
	// snapshot transfer is in progress.
	if sr.epochTracker != nil && hdr.Epoch > 0 {
		sr.epochTracker.RegisterInFlightEpoch(hdr.Epoch)
		defer sr.epochTracker.DeregisterInFlightEpoch(hdr.Epoch)
	}

	sr.receiveChunks(stream, hdr, stopC)
}

// receiveChunks reads snapshot chunks from the stream, enforces the memory
// budget, applies rate limiting, and dispatches chunks to the handler.
func (sr *snapshotReceiver) receiveChunks(stream chunkReader, hdr *SnapshotHeader, stopC <-chan struct{}) { //nolint:gocognit // metrics instrumentation
	recvStart := time.Now()
	// Cap initial allocation to prevent a malicious header from causing an
	// oversized pre-allocation. The slice grows dynamically beyond this cap.
	const maxInitialChunkCap = 1024
	initCap := hdr.ChunkCount
	if initCap > maxInitialChunkCap {
		initCap = maxInitialChunkCap
	}
	chunks := make([]proto.SnapshotChunk, 0, initCap)
	var totalBytes int64

	// Ensure all accumulated memory budget is restored on every exit path.
	// Without this defer, early returns leak the budget for previously
	// received chunks whose bytes were tracked in totalBytes but never
	// returned to the atomic budget counter.
	defer func() {
		if totalBytes > 0 {
			sr.memoryBudget.Add(totalBytes)
		}
	}()

	// Pre-allocate a stopped timer for rate limiting to avoid per-chunk
	// allocations that create GC pressure on large snapshots.
	rateLimitTimer := time.NewTimer(0)
	if !rateLimitTimer.Stop() {
		<-rateLimitTimer.C
	}
	defer rateLimitTimer.Stop()

	for i := uint64(0); i < hdr.ChunkCount; i++ {
		select {
		case <-stopC:
			return
		default:
		}

		if err := stream.SetReadDeadline(time.Now().Add(30 * time.Second)); err != nil {
			return
		}

		length, _, err := ReadFrameHeader(stream)
		if err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			return
		}

		if length == 0 {
			continue
		}

		// Check memory budget before allocating.
		remaining := sr.memoryBudget.Add(-int64(length))
		if remaining < 0 {
			// Over budget, restore and reject. This chunk was never
			// added to totalBytes so it must be restored inline.
			sr.memoryBudget.Add(int64(length))
			return
		}

		buf := make([]byte, length)
		if _, err := io.ReadFull(stream, buf); err != nil {
			// Read failed. This chunk was deducted from budget but
			// not yet added to totalBytes, so restore it inline.
			sr.memoryBudget.Add(int64(length))
			return
		}

		var chunk proto.SnapshotChunk
		if _, err := chunk.UnmarshalFrom(buf); err != nil {
			// Unmarshal failed. Same as above: restore inline since
			// this chunk is not yet tracked in totalBytes.
			sr.memoryBudget.Add(int64(length))
			return
		}

		// Validate chunk fields to prevent oversized allocations
		// and cross-cluster data corruption.
		if err := sr.validateChunk(&chunk); err != nil {
			sr.memoryBudget.Add(int64(length))
			slog.Warn("snapshot chunk validation failed",
				"shard", hdr.ShardID,
				"replica", hdr.ReplicaID,
				"chunkID", chunk.ChunkID,
				"error", err,
			)
			return
		}

		chunks = append(chunks, chunk)
		totalBytes += int64(length)

		// Guard against unbounded memory growth from a single snapshot
		// receive. The per-chunk atomic budget check above protects
		// against global overcommit, but this check ensures a single
		// receive does not consume the entire budget.
		if totalBytes > sr.maxMemory {
			slog.Warn("snapshot receive exceeded memory budget",
				"shard", hdr.ShardID,
				"replica", hdr.ReplicaID,
				"accumulated_bytes", totalBytes,
				"budget_bytes", sr.maxMemory,
			)
			return
		}

		// Apply rate limiting: sleep proportional to chunk size.
		if sr.bytesPerSec > 0 {
			chunkDuration := time.Duration(length) * time.Second / time.Duration(sr.bytesPerSec)
			if chunkDuration > time.Microsecond {
				rateLimitTimer.Reset(chunkDuration)
				select {
				case <-stopC:
					rateLimitTimer.Stop()
					return
				case <-rateLimitTimer.C:
				}
			}
		}
	}

	if len(chunks) > 0 {
		if err := sr.handler.HandleSnapshot(chunks); err != nil {
			slog.Warn("snapshot handler failed",
				"shard", hdr.ShardID,
				"replica", hdr.ReplicaID,
				"chunks", len(chunks),
				"error", err)
		} else {
			slog.Info("snapshot received",
				"shard", hdr.ShardID,
				"replica", hdr.ReplicaID,
				"index", hdr.Index,
				"chunks", len(chunks))
			if sr.listener != nil && sr.listener.OnSnapshotReceived != nil {
				sr.listener.OnSnapshotReceived(config.SnapshotInfo{
					ShardID:   hdr.ShardID,
					ReplicaID: hdr.ReplicaID,
					Index:     hdr.Index,
				})
			}
			if sr.metrics != nil {
				sr.metrics.IncSnapshotsReceived(hdr.ShardID, totalBytes, time.Since(recvStart))
			}
		}
	}
}

// SetEpochTracker sets the epoch tracker for in-flight epoch protection.
// Must be called before any snapshot receives begin. When set, the receiver
// will register/deregister epochs for all encrypted snapshot transfers to
// prevent premature DEK purge.
func (sr *snapshotReceiver) SetEpochTracker(tracker EpochTracker) {
	sr.epochTracker = tracker
}

// SetSnapshotTracker sets the shutdown tracker used to track in-flight
// snapshot receives. This allows Stop() to atomically reject new
// operations and wait for existing ones to complete before tearing down
// connections.
func (sr *snapshotReceiver) SetSnapshotTracker(tracker *shutdownTracker) {
	sr.snapshotTracker = tracker
}

// validateChunk checks bounds on deserialized snapshot chunk fields and
// verifies the DeploymentID matches this receiver's configured deployment.
func (sr *snapshotReceiver) validateChunk(chunk *proto.SnapshotChunk) error {
	if len(chunk.Filepath) > maxSnapshotFilepathLen {
		return &ChunkFilepathTooLongError{
			ShardID: chunk.ShardID,
			Length:  len(chunk.Filepath),
			Max:     maxSnapshotFilepathLen,
		}
	}
	if chunk.Filepath != "" {
		cleaned := filepath.Clean(chunk.Filepath)
		if strings.Contains(cleaned, "..") {
			return &ChunkFilepathTraversalError{
				ShardID:  chunk.ShardID,
				Filepath: chunk.Filepath,
			}
		}
	}
	if len(chunk.Data) > maxSnapshotChunkDataLen {
		return &ChunkDataTooLargeError{
			ShardID: chunk.ShardID,
			Length:  len(chunk.Data),
			Max:     maxSnapshotChunkDataLen,
		}
	}
	if sr.deploymentID != 0 && chunk.DeploymentID != sr.deploymentID {
		return &ChunkDeploymentIDMismatchError{
			ShardID:  chunk.ShardID,
			Expected: sr.deploymentID,
			Got:      chunk.DeploymentID,
		}
	}
	return nil
}

// AvailableMemory returns the remaining memory budget for snapshot receives.
// Used for testing and monitoring.
func (sr *snapshotReceiver) AvailableMemory() int64 {
	return sr.memoryBudget.Load()
}
