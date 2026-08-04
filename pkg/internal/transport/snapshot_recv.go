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
	"sync"
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

	// snapshotChunkFixedOverhead is the minimum number of bytes a SnapshotChunk
	// occupies for its non-Data fields (see SnapshotChunk.Size): 9 fixed uint64
	// fields (72) + filepath length prefix (4) + FileChunkID/FileChunkCount (16)
	// + HasFileInfo (8) + OnDiskIndex/DeploymentID/BinVer/Epoch (32) + Data
	// length prefix (4). Filepath content, FileInfo, and Membership only add to
	// this, so it is a safe lower bound on the per-chunk framing overhead.
	snapshotChunkFixedOverhead = 9*8 + 4 + 2*8 + 8 + 4*8 + 4 // 136 bytes

	// maxSnapshotChunkDataLen is the maximum Data payload length for a single
	// snapshot chunk. Every chunk travels inside one transport frame, whose
	// total payload is already capped at MaxFrameSize by ReadFrameHeader. Since
	// Data is length-prefixed *within* that frame alongside the fixed fields
	// above, the largest Data that can legitimately arrive is MaxFrameSize minus
	// the chunk framing overhead. Bounding it at the full MaxFrameSize (as a
	// prior version did) made the check unreachable; subtracting the fixed
	// overhead makes a malformed/oversized Data length prefix actually catchable.
	maxSnapshotChunkDataLen = int(MaxFrameSize) - snapshotChunkFixedOverhead
)

// maxRateLimitChunkSleep caps the per-chunk rate-limit sleep. A pathologically
// small MaxSnapshotReceiveRate (combined with a large chunk) could otherwise
// compute a multi-day sleep, holding the concurrency semaphore and memory
// budget for the entire duration and stalling availability. The ceiling bounds
// any single sleep to a few seconds; the configured rate is still honored on
// average across chunks. TransportConfig.Validate also enforces a sane
// minimum rate, so this ceiling is a defense-in-depth backstop.
const maxRateLimitChunkSleep = 5 * time.Second

// Whole-transfer liveness bounds for an inbound snapshot. The per-frame read
// deadline is reset on every iteration, so on its own it bounds nothing: a
// peer that emits one frame header just inside each deadline holds its
// concurrency slot indefinitely. These bound the transfer as a whole.
const (
	// maxSnapshotTransferDuration is the absolute ceiling on one inbound
	// transfer, regardless of progress.
	maxSnapshotTransferDuration = time.Hour

	// snapshotProgressGracePeriod is how long a transfer may run before the
	// throughput floor is enforced, absorbing a slow start (stream setup,
	// sender disk reads) without failing a legitimate transfer.
	snapshotProgressGracePeriod = 60 * time.Second

	// minSnapshotReceiveBytesPerSec is the average throughput a transfer must
	// sustain past the grace period. Far below any real link, it exists only to
	// evict a peer trickling bytes to occupy a concurrency slot.
	minSnapshotReceiveBytesPerSec = 1024

	// maxConsecutiveZeroLengthFrames bounds zero-length chunk frames in a row.
	// A legitimate sender emits none — SendSnapshot and SendSnapshotStreaming
	// always write a non-empty chunk — so any run of them is a peer burning
	// the declared ChunkCount to hold its slot.
	maxConsecutiveZeroLengthFrames = 8
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
	// sem is a counting semaphore limiting concurrent snapshot receives
	// host-wide, across every peer.
	sem chan struct{}

	// peerMu protects peerSlots.
	peerMu sync.Mutex

	// peerSlots counts in-flight receives per remote address. Without it the
	// host-global sem is holdable in full by one peer, after which every
	// legitimate snapshot to a recovering follower is dropped and the shard
	// cannot restore quorum.
	peerSlots map[string]int

	// maxPerPeer is the ceiling on concurrent receives from a single remote
	// address: ceil(maxConcurrent/2), so no peer can occupy the whole budget.
	maxPerPeer int

	// maxTransferDuration, progressGracePeriod, and minBytesPerSec are the
	// whole-transfer liveness bounds, seeded from the package constants.
	// Fields rather than direct constant references so tests can exercise the
	// bounds without waiting out production-scale timeouts.
	maxTransferDuration time.Duration
	progressGracePeriod time.Duration
	minBytesPerSec      int64

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

	// epochTrackerForShard, when non-nil, resolves the in-flight epoch
	// tracker per shard, used when each Raft shard is sealed under its own
	// barrier. It takes precedence over epochTracker. Nil for the
	// single-barrier case, preserving the original behavior.
	epochTrackerForShard func(shardID uint64) EpochTracker

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
	maxPerPeer := (maxConcurrent + 1) / 2
	if maxPerPeer < 1 {
		maxPerPeer = 1
	}
	sr := &snapshotReceiver{
		sem:                 make(chan struct{}, maxConcurrent),
		peerSlots:           make(map[string]int),
		maxPerPeer:          maxPerPeer,
		maxTransferDuration: maxSnapshotTransferDuration,
		progressGracePeriod: snapshotProgressGracePeriod,
		minBytesPerSec:      minSnapshotReceiveBytesPerSec,
		maxMemory:           maxMemory,
		bytesPerSec:         bytesPerSec,
		deploymentID:        deploymentID,
		handler:             handler,
	}
	sr.memoryBudget.Store(maxMemory)
	return sr
}

// acquirePeerSlot reserves one of a peer's concurrent-receive slots. An empty
// peer address (unit-test paths with no connection) is not accounted.
func (sr *snapshotReceiver) acquirePeerSlot(peer string) bool {
	if peer == "" {
		return true
	}
	sr.peerMu.Lock()
	defer sr.peerMu.Unlock()
	if sr.peerSlots[peer] >= sr.maxPerPeer {
		return false
	}
	sr.peerSlots[peer]++
	return true
}

// releasePeerSlot returns a peer's concurrent-receive slot, pruning the map
// entry at zero so it does not grow across peer churn.
func (sr *snapshotReceiver) releasePeerSlot(peer string) {
	if peer == "" {
		return
	}
	sr.peerMu.Lock()
	defer sr.peerMu.Unlock()
	n, ok := sr.peerSlots[peer]
	if !ok {
		return
	}
	if n <= 1 {
		delete(sr.peerSlots, peer)
		return
	}
	sr.peerSlots[peer] = n - 1
}

// cancelSnapshotReadOnStop arms a watcher that resets the read side of an
// inbound snapshot stream when the transport begins shutting down. receiveChunks
// only observes stopC at loop boundaries and during the rate-limit sleep; while
// it is blocked in ReadFrameHeader or io.ReadFull nothing else cancels it, so
// Stop()'s ShutdownAndWait would stall behind the 30s per-frame deadline. This
// mirrors cancelSnapshotWriteOnStop on the send side. The returned release func
// must be called when the transfer ends.
func cancelSnapshotReadOnStop(stream *quic.Stream, stopC <-chan struct{}) func() {
	if stream == nil {
		return func() {}
	}
	done := make(chan struct{})
	go func() {
		select {
		case <-stopC:
			stream.CancelRead(QUICStreamErrShutdown)
		case <-done:
		}
	}()
	return func() { close(done) }
}

// receiveSnapshot processes a snapshot stream. It enforces the concurrency
// semaphore, registers the epoch for in-flight protection, and delegates
// to receiveChunks for the actual I/O. The epoch is derived from the
// snapshot header's Term field, which carries the barrier epoch when
// encryption is enabled. When the epoch is 0 (no encryption), in-flight
// tracking is skipped.
func (sr *snapshotReceiver) receiveSnapshot(stream *quic.Stream, hdr *SnapshotHeader, peer string, senderAuthorized func(address string) bool, stopC <-chan struct{}) {
	// Atomically register this receive operation. TryAdd returns false
	// if Stop() has already initiated shutdown, preventing the race
	// between WaitGroup.Add(1) and WaitGroup.Wait().
	if sr.snapshotTracker != nil {
		if !sr.snapshotTracker.TryAdd() {
			sr.rejectSnapshotStream("transport shutting down", hdr, nil)
			return
		}
		defer sr.snapshotTracker.Done()
	}

	select {
	case <-stopC:
		sr.rejectSnapshotStream("transport shutting down", hdr, nil)
		return
	default:
	}

	// Per-peer fairness first: the host-global semaphore alone lets one peer
	// hold every slot, silently starving snapshot delivery to every other
	// recovering follower.
	if !sr.acquirePeerSlot(peer) {
		sr.rejectSnapshotStream("per-peer snapshot concurrency limit reached", hdr, nil)
		return
	}
	defer sr.releasePeerSlot(peer)

	// Try to acquire the host-global concurrency semaphore.
	select {
	case sr.sem <- struct{}{}:
		defer func() { <-sr.sem }()
	default:
		// At concurrency limit, drop the snapshot.
		sr.rejectSnapshotStream("global snapshot concurrency limit reached", hdr, nil)
		return
	}

	// Reset the stream's read side on shutdown so a transfer blocked in a
	// frame read cannot hold Stop() for the full per-frame deadline.
	defer cancelSnapshotReadOnStop(stream, stopC)()

	// Register in-flight epoch protection if an epoch tracker is available
	// and the snapshot has a non-zero epoch (barrier-encrypted snapshot).
	// This prevents PurgeEpochsBefore from removing the DEK while the
	// snapshot transfer is in progress. With per-shard barriers the tracker
	// is routed by the snapshot's shardID so the epoch is pinned on the
	// correct tenant barrier; otherwise the single host-wide tracker is used.
	if tracker := sr.trackerForShard(hdr.ShardID); tracker != nil && hdr.Epoch > 0 {
		tracker.RegisterInFlightEpoch(hdr.Epoch)
		defer tracker.DeregisterInFlightEpoch(hdr.Epoch)
	}

	sr.receiveChunks(stream, hdr, senderAuthorized, stopC)
}

// rateLimitChunkSleep computes the per-chunk rate-limit sleep for a chunk of
// length bytes at the given bytesPerSec rate, capped at maxRateLimitChunkSleep.
// The cap bounds the worst case so an absurdly low rate cannot produce a
// multi-day sleep that holds the snapshot concurrency semaphore and memory
// budget. The caller must ensure bytesPerSec > 0.
func rateLimitChunkSleep(length uint32, bytesPerSec int64) time.Duration {
	d := time.Duration(length) * time.Second / time.Duration(bytesPerSec)
	if d > maxRateLimitChunkSleep {
		return maxRateLimitChunkSleep
	}
	return d
}

// receiveChunks reads snapshot chunks from the stream, enforces the memory
// budget, applies rate limiting, and dispatches chunks to the handler.
func (sr *snapshotReceiver) receiveChunks(stream chunkReader, hdr *SnapshotHeader, senderAuthorized func(address string) bool, stopC <-chan struct{}) { //nolint:gocognit // metrics instrumentation
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

	// truncated tracks whether the transfer ended before all expected chunks
	// arrived (early EOF or stop). A truncated transfer must NOT be committed
	// as a snapshot: a partial snapshot persisted as the latest snapshot is
	// usable for recovery and would corrupt state.
	truncated := false

	// zeroFrames counts consecutive zero-length chunk frames, which no
	// legitimate sender produces.
	zeroFrames := 0

	for i := uint64(0); i < hdr.ChunkCount; i++ {
		// Bound the transfer as a whole. The per-frame deadline below is reset
		// on every iteration, so without these a peer can trickle one frame
		// header per deadline and hold its concurrency slot for years.
		elapsed := time.Since(recvStart)
		if sr.maxTransferDuration > 0 && elapsed > sr.maxTransferDuration {
			sr.rejectSnapshot(hdr, "transfer exceeded maximum duration", totalBytes,
				&SnapshotTransferTimeoutError{
					Elapsed:       elapsed,
					Limit:         sr.maxTransferDuration,
					ReceivedBytes: totalBytes,
				})
			return
		}
		if sr.minBytesPerSec > 0 && elapsed > sr.progressGracePeriod &&
			totalBytes < int64(elapsed/time.Second)*sr.minBytesPerSec {
			sr.rejectSnapshot(hdr, "transfer below minimum throughput", totalBytes,
				&SnapshotProgressStalledError{
					Elapsed:        elapsed,
					ReceivedBytes:  totalBytes,
					MinBytesPerSec: sr.minBytesPerSec,
				})
			return
		}

		select {
		case <-stopC:
			// Stop requested: mark truncated and exit the loop. (A bare
			// break here would only break the select, not the for loop,
			// so set the flag and break the loop explicitly.)
			truncated = true
		default:
		}
		if truncated {
			break
		}

		if err := stream.SetReadDeadline(time.Now().Add(30 * time.Second)); err != nil {
			truncated = true
			break
		}

		length, _, err := ReadFrameHeader(stream)
		if err != nil {
			if errors.Is(err, io.EOF) {
				truncated = true
				break
			}
			sr.rejectSnapshot(hdr, "chunk frame read failed", totalBytes, err)
			return
		}

		if length == 0 {
			zeroFrames++
			if zeroFrames > maxConsecutiveZeroLengthFrames {
				sr.rejectSnapshot(hdr, "zero-length chunk frame flood", totalBytes,
					&SnapshotZeroLengthFrameFloodError{
						Count: zeroFrames,
						Max:   maxConsecutiveZeroLengthFrames,
					})
				return
			}
			continue
		}
		zeroFrames = 0

		// Check memory budget before allocating.
		remaining := sr.memoryBudget.Add(-int64(length))
		if remaining < 0 {
			// Over budget, restore and reject. This chunk was never
			// added to totalBytes so it must be restored inline.
			sr.memoryBudget.Add(int64(length))
			sr.rejectSnapshot(hdr, "receive memory budget exhausted", totalBytes+int64(length),
				&SnapshotMemoryBudgetExceededError{
					Accumulated: totalBytes + int64(length),
					Budget:      sr.maxMemory,
				})
			return
		}

		buf := make([]byte, length)
		if _, err := io.ReadFull(stream, buf); err != nil {
			// Read failed. This chunk was deducted from budget but
			// not yet added to totalBytes, so restore it inline.
			sr.memoryBudget.Add(int64(length))
			sr.rejectSnapshot(hdr, "chunk body read failed", totalBytes, err)
			return
		}

		var chunk proto.SnapshotChunk
		if _, err := chunk.UnmarshalFrom(buf); err != nil {
			// Unmarshal failed. Same as above: restore inline since
			// this chunk is not yet tracked in totalBytes.
			sr.memoryBudget.Add(int64(length))
			sr.rejectSnapshot(hdr, "chunk unmarshal failed", totalBytes, err)
			return
		}

		// Validate chunk fields (including consistency with the stream
		// header used for epoch pinning) to prevent oversized allocations
		// and cross-cluster data corruption.
		if err := sr.validateChunk(&chunk, hdr); err != nil {
			sr.memoryBudget.Add(int64(length))
			sr.rejectSnapshot(hdr, "chunk validation failed", totalBytes, err)
			return
		}

		chunks = append(chunks, chunk)
		totalBytes += int64(length)

		// Guard against unbounded memory growth from a single snapshot
		// receive. The per-chunk atomic budget check above protects
		// against global overcommit, but this check ensures a single
		// receive does not consume the entire budget.
		if totalBytes > sr.maxMemory {
			sr.rejectSnapshot(hdr, "per-receive memory budget exceeded", totalBytes,
				&SnapshotMemoryBudgetExceededError{
					Accumulated: totalBytes,
					Budget:      sr.maxMemory,
				})
			return
		}

		// Apply rate limiting: sleep proportional to chunk size, capped so a
		// pathologically small configured rate cannot sleep for days while
		// holding the concurrency semaphore and memory budget.
		if sr.bytesPerSec > 0 {
			chunkDuration := rateLimitChunkSleep(length, sr.bytesPerSec)
			if chunkDuration > time.Microsecond {
				rateLimitTimer.Reset(chunkDuration)
				select {
				case <-stopC:
					// Shutdown during the rate-limit sleep: flow into the
					// truncated-transfer path below so the discard is logged
					// and reported like every other incomplete transfer.
					rateLimitTimer.Stop()
					truncated = true
				case <-rateLimitTimer.C:
				}
			}
		}
		if truncated {
			break
		}
	}

	// Only commit a snapshot when the transfer is COMPLETE: every expected
	// chunk arrived. A truncated transfer (peer disconnect / early EOF /
	// shutdown) leaves fewer chunks than hdr.ChunkCount; committing it would
	// persist a partial snapshot as the latest snapshot, which recovery would
	// then load as if whole — data corruption. Treat it as a failed receive:
	// log, fire the rejection listener, and do NOT call HandleSnapshot.
	//
	// hdr.ChunkCount == 0 is named explicitly: the count comparison alone
	// (0 != 0) is false, so an empty transfer would otherwise fall through to
	// a silent return with no log and no listener callback.
	if truncated || hdr.ChunkCount == 0 || uint64(len(chunks)) != hdr.ChunkCount {
		slog.Warn("snapshot receive incomplete, discarding partial transfer",
			"shard", hdr.ShardID,
			"replica", hdr.ReplicaID,
			"index", hdr.Index,
			"received_chunks", len(chunks),
			"expected_chunks", hdr.ChunkCount,
		)
		if sr.listener != nil && sr.listener.OnSnapshotRejected != nil {
			sr.listener.OnSnapshotRejected(config.SnapshotInfo{
				ShardID:   hdr.ShardID,
				ReplicaID: hdr.ReplicaID,
				Index:     hdr.Index,
			})
		}
		return
	}

	if len(chunks) > 0 {
		if err := sr.handler.HandleSnapshot(chunks, senderAuthorized); err != nil {
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

// rejectSnapshot records a discarded inbound snapshot transfer: a structured
// warning with the transfer's identity and size, plus the OnSnapshotRejected
// listener — the same failure signal the truncated-transfer path fires — so a
// dropped receive is observable instead of silently timing out the sender's
// ack wait.
func (sr *snapshotReceiver) rejectSnapshot(hdr *SnapshotHeader, reason string, receivedBytes int64, err error) {
	slog.Warn("snapshot receive rejected, discarding transfer",
		"reason", reason,
		"shard", hdr.ShardID,
		"replica", hdr.ReplicaID,
		"index", hdr.Index,
		"received_bytes", receivedBytes,
		"expected_chunks", hdr.ChunkCount,
		"error", err,
	)
	if sr.listener != nil && sr.listener.OnSnapshotRejected != nil {
		sr.listener.OnSnapshotRejected(config.SnapshotInfo{
			ShardID:   hdr.ShardID,
			ReplicaID: hdr.ReplicaID,
			Index:     hdr.Index,
		})
	}
}

// rejectSnapshotStream records a snapshot transfer discarded before any chunk
// was read: an admission failure (shutdown, global or per-peer concurrency
// limit) or a header-stage failure, where the transfer's identity may not be
// known yet. It fires the same OnSnapshotRejected listener and structured
// warning as rejectSnapshot so every discarded transfer is observable through
// one signal, rather than some paths returning silently and leaving the sender
// to time out its ack wait. hdr may be nil when the header never parsed.
func (sr *snapshotReceiver) rejectSnapshotStream(reason string, hdr *SnapshotHeader, err error) {
	var info config.SnapshotInfo
	if hdr != nil {
		info = config.SnapshotInfo{
			ShardID:   hdr.ShardID,
			ReplicaID: hdr.ReplicaID,
			Index:     hdr.Index,
		}
	}
	slog.Warn("snapshot stream rejected, discarding transfer",
		"reason", reason,
		"shard", info.ShardID,
		"replica", info.ReplicaID,
		"index", info.Index,
		"error", err,
	)
	if sr.listener != nil && sr.listener.OnSnapshotRejected != nil {
		sr.listener.OnSnapshotRejected(info)
	}
}

// SetEpochTracker sets the epoch tracker for in-flight epoch protection.
// Must be called before any snapshot receives begin. When set, the receiver
// will register/deregister epochs for all encrypted snapshot transfers to
// prevent premature DEK purge.
func (sr *snapshotReceiver) SetEpochTracker(tracker EpochTracker) {
	sr.epochTracker = tracker
}

// SetShardEpochTrackers sets a per-shard epoch tracker resolver. When non-nil
// it takes precedence over the single epochTracker, routing in-flight epoch
// protection to the barrier that owns the snapshot's shard. Must be called
// before any snapshot receives begin.
func (sr *snapshotReceiver) SetShardEpochTrackers(fn func(shardID uint64) EpochTracker) {
	sr.epochTrackerForShard = fn
}

// trackerForShard resolves the in-flight epoch tracker for a shard. A per-shard
// resolver takes precedence (falling through to the single tracker only when it
// returns nil); otherwise the single host-wide tracker is returned. Returns nil
// when no tracker is configured, in which case in-flight protection is skipped.
func (sr *snapshotReceiver) trackerForShard(shardID uint64) EpochTracker {
	if sr.epochTrackerForShard != nil {
		if t := sr.epochTrackerForShard(shardID); t != nil {
			return t
		}
	}
	return sr.epochTracker
}

// SetSnapshotTracker sets the shutdown tracker used to track in-flight
// snapshot receives. This allows Stop() to atomically reject new
// operations and wait for existing ones to complete before tearing down
// connections.
func (sr *snapshotReceiver) SetSnapshotTracker(tracker *shutdownTracker) {
	sr.snapshotTracker = tracker
}

// validateChunk checks bounds on deserialized snapshot chunk fields,
// verifies the DeploymentID matches this receiver's configured deployment,
// and binds the chunk to the stream header (hdr) that was used for epoch
// pinning: a chunk whose ShardID, Index, or Epoch diverges from the header
// could otherwise persist under a barrier epoch that was never pinned,
// letting a concurrent epoch purge strand an undecryptable snapshot.
func (sr *snapshotReceiver) validateChunk(chunk *proto.SnapshotChunk, hdr *SnapshotHeader) error {
	if len(chunk.Filepath) > maxSnapshotFilepathLen {
		return &ChunkFilepathTooLongError{
			ShardID: chunk.ShardID,
			Length:  len(chunk.Filepath),
			Max:     maxSnapshotFilepathLen,
		}
	}
	if chunk.Filepath != "" {
		// chunk.Filepath is INFORMATIONAL and attacker-controllable: the sender
		// sets it to the snapshot's on-disk directory (an ABSOLUTE path), and
		// the receiver ignores it when writing — the on-disk location is built
		// solely from shard/replica/snapshot-index. So absolute paths are
		// legitimate here and must NOT be rejected. We still reject relative
		// ".." traversal (which a clean legitimate path never contains) as
		// defense-in-depth. SECURITY INVARIANT: any future code that turns this
		// field into a write path MUST sanitize it (extract the base name, or
		// filepath.Join onto a fixed base then verify the result stays within
		// that base) — never use it directly.
		cleaned := filepath.Clean(chunk.Filepath)
		if cleaned == ".." ||
			strings.HasPrefix(cleaned, ".."+string(filepath.Separator)) ||
			strings.Contains(cleaned, string(filepath.Separator)+".."+string(filepath.Separator)) {
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
	if hdr != nil {
		if chunk.ShardID != hdr.ShardID {
			return &ChunkHeaderMismatchError{
				Field:       "shard_id",
				ChunkID:     chunk.ChunkID,
				HeaderValue: hdr.ShardID,
				ChunkValue:  chunk.ShardID,
			}
		}
		if chunk.Index != hdr.Index {
			return &ChunkHeaderMismatchError{
				Field:       "index",
				ChunkID:     chunk.ChunkID,
				HeaderValue: hdr.Index,
				ChunkValue:  chunk.Index,
			}
		}
		if chunk.Epoch != hdr.Epoch {
			return &ChunkHeaderMismatchError{
				Field:       "epoch",
				ChunkID:     chunk.ChunkID,
				HeaderValue: hdr.Epoch,
				ChunkValue:  chunk.Epoch,
			}
		}
	}
	return nil
}

// AvailableMemory returns the remaining memory budget for snapshot receives.
// Used for testing and monitoring.
func (sr *snapshotReceiver) AvailableMemory() int64 {
	return sr.memoryBudget.Load()
}
