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
	"unsafe"

	"github.com/jeremyhahn/go-quicraft/pkg/config"
	"github.com/jeremyhahn/go-quicraft/pkg/proto"
	"github.com/quic-go/quic-go"
)

const (
	// maxSnapshotFilepathLen is the maximum filepath length for snapshot
	// chunks. Corresponds to POSIX PATH_MAX.
	maxSnapshotFilepathLen = 4096

	// maxSnapshotChunkDataLen is the maximum Data payload length for a single
	// snapshot chunk. The limit lives in one place: the decoder
	// (proto.SnapshotChunk.UnmarshalFrom) already rejects a longer Data length
	// prefix with ErrChunkDataTooLong, so restating it as any other bound —
	// e.g. MaxFrameSize minus the framing overhead, which is larger — makes
	// validateChunk's check unreachable and lets the two limits drift.
	maxSnapshotChunkDataLen = int(proto.MaxSnapshotChunkDataLen)
)

var (
	// snapshotChunkStructBytes is the resident size of one retained
	// proto.SnapshotChunk header. receiveChunks accumulates every accepted
	// chunk in a slice for the whole transfer, so charging the memory budget
	// the frame length alone under-counts residency — by ~2.6x for
	// minimum-size chunks — and lets concurrent transfers hold multiples of
	// the configured budget.
	snapshotChunkStructBytes = int64(unsafe.Sizeof(proto.SnapshotChunk{}))

	// minSnapshotChunkResidentBytes is the least memory one admitted chunk can
	// occupy: the smallest frame a valid chunk can arrive in, plus its
	// retained struct header. It converts a memory budget into a chunk-count
	// ceiling at admission time.
	minSnapshotChunkResidentBytes = int64((&proto.SnapshotChunk{}).Size()) + snapshotChunkStructBytes
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
	// transfer, regardless of progress and regardless of how much work the
	// sender declared. It is the clamp on the derived per-transfer budget
	// (see transferBudget), never the budget itself.
	maxSnapshotTransferDuration = time.Hour

	// minSnapshotTransferDuration is the floor on the derived per-transfer
	// budget, so a small snapshot on a congested link is not failed by an
	// arithmetically tight deadline. It is also the ceiling a peer gets by
	// declaring the SMALLEST possible transfer — the cheapest way to occupy a
	// slot — which is why it is minutes rather than the flat hour that used to
	// apply to every transfer alike.
	minSnapshotTransferDuration = 5 * time.Minute

	// snapshotTransferRateMultiple is how many times the receiver's own
	// configured rate limit (MaxSnapshotReceiveRate) a transfer is allowed to
	// take. The rate limit is the operator's statement of the throughput this
	// deployment expects to move snapshots at; the multiple absorbs the gap
	// between that and what the link actually delivers under congestion, plus
	// the sender's disk reads. An operator on a genuinely slow link lowers the
	// rate limit and the budget widens in proportion.
	snapshotTransferRateMultiple = 16

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

	// peerSlots counts in-flight receives per peer IDENTITY — see
	// snapshotFairnessKey, which derives the key from the peer's authenticated
	// certificate rather than from anything the peer picks per connection.
	// Without it the host-global sem is holdable in full by one peer, after
	// which every legitimate snapshot to a recovering follower is dropped and
	// the shard cannot restore quorum.
	peerSlots map[string]int

	// maxPerPeer is the ceiling on concurrent receives from a single peer
	// identity: ceil(maxConcurrent/2), so no peer can occupy the whole budget.
	maxPerPeer int

	// maxTransferDuration, minTransferDuration, progressGracePeriod and
	// minBytesPerSec are the whole-transfer liveness bounds, seeded from the
	// package constants. Fields rather than direct constant references so
	// tests can exercise the bounds without waiting out production-scale
	// timeouts. maxTransferDuration and minTransferDuration clamp the
	// per-transfer budget derived by transferBudget; they are not the budget.
	maxTransferDuration time.Duration
	minTransferDuration time.Duration
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
		minTransferDuration: minSnapshotTransferDuration,
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

// acquirePeerSlot reserves one of a peer identity's concurrent-receive slots.
// peerKey comes from snapshotFairnessKey and must be an identity the peer
// cannot vary; keying this on anything a client chooses per connection (an
// ip:port, say) makes the whole limit a formality. An empty key (unit-test
// paths with no connection) is not accounted.
func (sr *snapshotReceiver) acquirePeerSlot(peerKey string) bool {
	if peerKey == "" {
		return true
	}
	sr.peerMu.Lock()
	defer sr.peerMu.Unlock()
	if sr.peerSlots[peerKey] >= sr.maxPerPeer {
		return false
	}
	sr.peerSlots[peerKey]++
	return true
}

// releasePeerSlot returns a peer identity's concurrent-receive slot, pruning
// the map entry at zero so it does not grow across peer churn.
func (sr *snapshotReceiver) releasePeerSlot(peerKey string) {
	if peerKey == "" {
		return
	}
	sr.peerMu.Lock()
	defer sr.peerMu.Unlock()
	n, ok := sr.peerSlots[peerKey]
	if !ok {
		return
	}
	if n <= 1 {
		delete(sr.peerSlots, peerKey)
		return
	}
	sr.peerSlots[peerKey] = n - 1
}

// transferBudget returns how long ONE inbound transfer of the declared chunk
// count may run before it is failed regardless of progress.
//
// A flat ceiling is the wrong shape for this bound. Combined with the
// throughput floor it fixed the work a single held slot represents at
// ceiling * minBytesPerSec — a bit over 3 MB for the hour-and-1 KB/s pair the
// constants used to give every transfer alike — while saying nothing about how
// much work the sender actually claimed to be doing. Deriving the budget from
// the DECLARED work ties the two together: a peer that wants a long slot has
// to declare a large transfer, and a large declaration is already bounded at
// the header by maxAdmissibleChunkCount, which the configured memory budget
// caps.
//
// The declared work is valued at its maximum: ChunkCount chunks of the largest
// payload a chunk may carry. Undershooting here would fail honest transfers, so
// the estimate is deliberately the generous end, and it is then divided by the
// receiver's own configured rate limit and multiplied by
// snapshotTransferRateMultiple. The progress grace period is added on top so
// the two liveness bounds do not contradict each other — a transfer must not
// hit its absolute deadline before the throughput floor has even begun to be
// enforced.
//
// The result is clamped to [minTransferDuration, maxTransferDuration]. A zero
// or negative maxTransferDuration disables the bound entirely, as before, and
// takes precedence over the floor so a test can still pin a tiny ceiling.
func (sr *snapshotReceiver) transferBudget(chunkCount uint64) time.Duration {
	if sr.maxTransferDuration <= 0 {
		return 0
	}
	if sr.bytesPerSec <= 0 {
		// No configured rate to derive from; fall back to the absolute ceiling.
		return sr.maxTransferDuration
	}

	// Saturate at the protocol ceiling. processSnapshotHeader already refuses a
	// larger declaration, but the arithmetic below must not depend on a caller
	// having done that: an unsaturated wire value overflows the product.
	if chunkCount > uint64(defaultMaxSnapshotChunkCount) {
		chunkCount = uint64(defaultMaxSnapshotChunkCount)
	}
	declaredBytes := int64(chunkCount) * int64(maxSnapshotChunkDataLen)

	// Work in SECONDS and clamp before converting to a Duration: at a low
	// configured rate the quotient is large enough that scaling it to
	// nanoseconds would overflow int64.
	seconds := declaredBytes / sr.bytesPerSec
	if seconds > int64(sr.maxTransferDuration/time.Second)/snapshotTransferRateMultiple {
		return sr.maxTransferDuration
	}
	budget := sr.progressGracePeriod +
		time.Duration(seconds*snapshotTransferRateMultiple)*time.Second

	if budget < sr.minTransferDuration {
		budget = sr.minTransferDuration
	}
	if budget > sr.maxTransferDuration {
		budget = sr.maxTransferDuration
	}
	return budget
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
func (sr *snapshotReceiver) receiveSnapshot(stream *quic.Stream, hdr *SnapshotHeader, peerKey string, peer PeerAuth, stopC <-chan struct{}) {
	// Atomically register this receive operation. TryAdd returns false
	// if Stop() has already initiated shutdown, preventing the race
	// between WaitGroup.Add(1) and WaitGroup.Wait().
	if sr.snapshotTracker != nil {
		if !sr.snapshotTracker.TryAdd() {
			sr.rejectSnapshotStream(snapshotRejectShutdown, "transport shutting down", hdr, nil)
			return
		}
		defer sr.snapshotTracker.Done()
	}

	select {
	case <-stopC:
		sr.rejectSnapshotStream(snapshotRejectShutdown, "transport shutting down", hdr, nil)
		return
	default:
	}

	// Per-peer fairness first: the host-global semaphore alone lets one peer
	// hold every slot, silently starving snapshot delivery to every other
	// recovering follower. peerKey is the peer's authenticated identity, not
	// its transport address — see snapshotFairnessKey.
	if !sr.acquirePeerSlot(peerKey) {
		sr.rejectSnapshotStream(snapshotRejectConcurrency, "per-peer snapshot concurrency limit reached", hdr, nil)
		return
	}
	defer sr.releasePeerSlot(peerKey)

	// Try to acquire the host-global concurrency semaphore.
	select {
	case sr.sem <- struct{}{}:
		defer func() { <-sr.sem }()
	default:
		// At concurrency limit, drop the snapshot.
		sr.rejectSnapshotStream(snapshotRejectConcurrency, "global snapshot concurrency limit reached", hdr, nil)
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

	sr.receiveChunks(stream, hdr, peer, stopC)
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
//
//nolint:gocyclo // chunk streaming interleaves budget reservation with four distinct release paths, liveness accounting and validation; the budget charge and its release must stay in one function, since a missed release is a permanent leak that eventually rejects every inbound snapshot host-wide
func (sr *snapshotReceiver) receiveChunks(stream chunkReader, hdr *SnapshotHeader, peer PeerAuth, stopC <-chan struct{}) { //nolint:gocognit // metrics instrumentation
	recvStart := time.Now()
	// Cap initial allocation to prevent a malicious header from causing an
	// oversized pre-allocation. The slice grows dynamically beyond this cap.
	const maxInitialChunkCap = 1024
	initCap := hdr.ChunkCount
	if initCap > maxInitialChunkCap {
		initCap = maxInitialChunkCap
	}
	chunks := make([]proto.SnapshotChunk, 0, initCap)
	// totalBytes counts WIRE bytes: it drives the throughput floor and the
	// bytes-received metric. charged counts what was reserved against the
	// memory budget, which is the wire bytes PLUS the retained struct header
	// of every accumulated chunk. Keeping them separate stops the accounting
	// correction from inflating the reported transfer size.
	var totalBytes, charged int64

	// Ensure all accumulated memory budget is restored on every exit path.
	// Without this defer, early returns leak the budget for previously
	// received chunks whose bytes were reserved but never returned to the
	// atomic budget counter.
	defer func() {
		if charged > 0 {
			sr.memoryBudget.Add(charged)
		}
	}()

	// Pre-allocate a stopped timer for rate limiting to avoid per-chunk
	// allocations that create GC pressure on large snapshots.
	rateLimitTimer := time.NewTimer(0)
	if !rateLimitTimer.Stop() {
		<-rateLimitTimer.C
	}
	defer rateLimitTimer.Stop()

	// The absolute deadline is derived from the work the sender DECLARED, so a
	// slot's lifetime is proportional to the transfer it was taken out for
	// rather than a flat ceiling every transfer inherits alike.
	transferBudget := sr.transferBudget(hdr.ChunkCount)

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
		if transferBudget > 0 && elapsed > transferBudget {
			sr.rejectSnapshot(hdr, snapshotRejectLiveness, "transfer exceeded maximum duration", totalBytes,
				&SnapshotTransferTimeoutError{
					Elapsed:       elapsed,
					Limit:         transferBudget,
					ReceivedBytes: totalBytes,
				})
			return
		}
		if sr.minBytesPerSec > 0 && elapsed > sr.progressGracePeriod &&
			totalBytes < int64(elapsed/time.Second)*sr.minBytesPerSec {
			sr.rejectSnapshot(hdr, snapshotRejectLiveness, "transfer below minimum throughput", totalBytes,
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
			sr.rejectSnapshot(hdr, snapshotRejectIO, "chunk frame read failed", totalBytes, err)
			return
		}

		if length == 0 {
			zeroFrames++
			if zeroFrames > maxConsecutiveZeroLengthFrames {
				sr.rejectSnapshot(hdr, snapshotRejectLiveness, "zero-length chunk frame flood", totalBytes,
					&SnapshotZeroLengthFrameFloodError{
						Count: zeroFrames,
						Max:   maxConsecutiveZeroLengthFrames,
					})
				return
			}
			continue
		}
		zeroFrames = 0

		// Check memory budget before allocating. The charge covers the frame
		// buffer AND the SnapshotChunk header that will be retained in chunks
		// until the transfer completes; charging the frame length alone lets a
		// flood of minimum-size chunks hold multiples of the accounted memory.
		chunkCharge := int64(length) + snapshotChunkStructBytes
		remaining := sr.memoryBudget.Add(-chunkCharge)
		if remaining < 0 {
			// Over budget, restore and reject. This chunk was never
			// added to charged so it must be restored inline.
			sr.memoryBudget.Add(chunkCharge)
			sr.rejectSnapshot(hdr, snapshotRejectMemory, "receive memory budget exhausted", totalBytes+int64(length),
				&SnapshotMemoryBudgetExceededError{
					Accumulated: charged + chunkCharge,
					Budget:      sr.maxMemory,
				})
			return
		}

		buf := make([]byte, length)
		if _, err := io.ReadFull(stream, buf); err != nil {
			// Read failed. This chunk was deducted from budget but
			// not yet added to charged, so restore it inline.
			sr.memoryBudget.Add(chunkCharge)
			sr.rejectSnapshot(hdr, snapshotRejectIO, "chunk body read failed", totalBytes, err)
			return
		}

		var chunk proto.SnapshotChunk
		if _, err := chunk.UnmarshalFrom(buf); err != nil {
			// Unmarshal failed. Same as above: restore inline since
			// this chunk is not yet tracked in charged.
			sr.memoryBudget.Add(chunkCharge)
			sr.rejectSnapshot(hdr, snapshotRejectMalformed, "chunk unmarshal failed", totalBytes, err)
			return
		}

		// Validate chunk fields (including consistency with the stream
		// header used for epoch pinning) to prevent oversized allocations
		// and cross-cluster data corruption.
		if err := sr.validateChunk(&chunk, hdr); err != nil {
			sr.memoryBudget.Add(chunkCharge)
			sr.rejectSnapshot(hdr, snapshotRejectMalformed, "chunk validation failed", totalBytes, err)
			return
		}

		chunks = append(chunks, chunk)
		totalBytes += int64(length)
		charged += chunkCharge

		// Guard against unbounded memory growth from a single snapshot
		// receive. The per-chunk atomic budget check above protects
		// against global overcommit, but this check ensures a single
		// receive does not consume the entire budget.
		if charged > sr.maxMemory {
			sr.rejectSnapshot(hdr, snapshotRejectMemory, "per-receive memory budget exceeded", totalBytes,
				&SnapshotMemoryBudgetExceededError{
					Accumulated: charged,
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
		if err := sr.handler.HandleSnapshot(chunks, peer); err != nil {
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

// rejectSnapshot records a discarded inbound snapshot transfer: the
// IncSnapshotsRejected counter bucketed by reason, a structured warning
// carrying the transfer's identity, size and free-form detail, plus the
// OnSnapshotRejected listener — the same failure signal the truncated-transfer
// path fires — so a dropped receive is observable instead of silently timing
// out the sender's ack wait.
func (sr *snapshotReceiver) rejectSnapshot(hdr *SnapshotHeader, reason snapshotRejectReason, detail string, receivedBytes int64, err error) {
	slog.Warn("snapshot receive rejected, discarding transfer",
		"reason", reason,
		"detail", detail,
		"shard", hdr.ShardID,
		"replica", hdr.ReplicaID,
		"index", hdr.Index,
		"received_bytes", receivedBytes,
		"expected_chunks", hdr.ChunkCount,
		"error", err,
	)
	if sr.metrics != nil {
		sr.metrics.IncSnapshotsRejected(hdr.ShardID, string(reason))
	}
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
// to time out its ack wait. hdr may be nil when the header never parsed, in
// which case the counter is attributed to shard 0.
func (sr *snapshotReceiver) rejectSnapshotStream(reason snapshotRejectReason, detail string, hdr *SnapshotHeader, err error) {
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
		"detail", detail,
		"shard", info.ShardID,
		"replica", info.ReplicaID,
		"index", info.Index,
		"error", err,
	)
	if sr.metrics != nil {
		sr.metrics.IncSnapshotsRejected(info.ShardID, string(reason))
	}
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

// maxAdmissibleChunkCount returns the largest declared ChunkCount this
// receiver can admit: the fixed protocol ceiling, lowered to what the
// configured memory budget could actually hold. The fixed ceiling alone is not
// a memory bound — a peer declaring it and sending minimum-size chunks holds
// ChunkCount * minSnapshotChunkResidentBytes, which for a small budget is
// orders of magnitude past it — so the transfer is refused at the header
// instead of after the memory has been committed.
func (sr *snapshotReceiver) maxAdmissibleChunkCount() uint64 {
	limit := uint64(defaultMaxSnapshotChunkCount)
	if sr.maxMemory > 0 {
		if budgeted := uint64(sr.maxMemory / minSnapshotChunkResidentBytes); budgeted < limit {
			limit = budgeted
		}
	}
	return limit
}

// AvailableMemory returns the remaining memory budget for snapshot receives.
// Used for testing and monitoring.
func (sr *snapshotReceiver) AvailableMemory() int64 {
	return sr.memoryBudget.Load()
}
