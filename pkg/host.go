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

package quicraft

import (
	"context"
	"crypto/rand"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"maps"
	"math"
	mathrand "math/rand/v2"
	"os"
	"path/filepath"
	"reflect"
	"runtime"
	"runtime/debug"
	"slices"
	"sync"
	"sync/atomic"
	"time"

	"github.com/jeremyhahn/go-quicraft/pkg/config"
	"github.com/jeremyhahn/go-quicraft/pkg/fsutil"
	"github.com/jeremyhahn/go-quicraft/pkg/internal/engine"
	"github.com/jeremyhahn/go-quicraft/pkg/internal/netaddr"
	"github.com/jeremyhahn/go-quicraft/pkg/internal/raft"
	"github.com/jeremyhahn/go-quicraft/pkg/internal/registry"
	"github.com/jeremyhahn/go-quicraft/pkg/internal/rsm"
	"github.com/jeremyhahn/go-quicraft/pkg/internal/supervise"
	"github.com/jeremyhahn/go-quicraft/pkg/internal/transport"
	"github.com/jeremyhahn/go-quicraft/pkg/internal/transport/revocation"
	"github.com/jeremyhahn/go-quicraft/pkg/logdb"
	"github.com/jeremyhahn/go-quicraft/pkg/logdb/memdb"
	"github.com/jeremyhahn/go-quicraft/pkg/logdb/waldb"
	"github.com/jeremyhahn/go-quicraft/pkg/proto"
	"github.com/jeremyhahn/go-quicraft/pkg/sm"
)

const (
	// hostIDFileName is the file that stores the host's unique identifier.
	hostIDFileName = "host.id"

	// hostIDLength is the expected length of the host ID hex string.
	hostIDLength = 32

	// hostIDFilePerms is the file permissions for the host ID file.
	hostIDFilePerms = 0o600

	// pendingSweepInterval is how often expired pending proposals, reads and snapshots are completed with ErrTimeout.
	pendingSweepInterval = 10 * time.Second

	// loopPanicRestartDelay is the pause before restarting a panicked background loop.
	loopPanicRestartDelay = 100 * time.Millisecond

	// defaultPendingRequestTimeout bounds a pending request whose context has no deadline, so the sweeper can always expire it.
	defaultPendingRequestTimeout = 5 * time.Minute

	// defaultPendingSnapshotTimeout bounds a pending snapshot request with no deadline; it matches the snapshot transfer ceiling.
	defaultPendingSnapshotTimeout = 1 * time.Hour
)

// Barrier is the at-rest encryption barrier interface, defined here to avoid an import cycle with quicraft/seal.
type Barrier interface {
	// IsSealed returns true if the barrier is in the sealed state.
	IsSealed() bool

	// Seal transitions the barrier to the sealed state, zeroing all
	// key material from memory.
	Seal() error

	// Encrypt encrypts plaintext using the current epoch's DEK.
	// dst may be provided as a pre-allocated buffer to reduce allocations.
	Encrypt(dst, plaintext []byte) ([]byte, error)

	// Decrypt decrypts ciphertext. The epoch is extracted from the
	// ciphertext prefix and the corresponding DEK is used.
	Decrypt(dst, ciphertext []byte) ([]byte, error)

	// DecryptWithContext decrypts ciphertext sealed with the same caller context in its AAD; there is no fallback.
	DecryptWithContext(dst, ciphertext, contextAAD []byte) ([]byte, error)

	// EncryptWithEpoch encrypts plaintext and returns both the ciphertext
	// and the epoch used for encryption.
	EncryptWithEpoch(dst, plaintext []byte) ([]byte, uint64, error)

	// EncryptWithEpochContext is EncryptWithEpoch with a caller-supplied
	// context folded into the AAD alongside the epoch. An empty context is
	// byte-identical to EncryptWithEpoch.
	EncryptWithEpochContext(dst, plaintext, contextAAD []byte) ([]byte, uint64, error)

	// EncryptWithEpochPinnedContext encrypts and registers the epoch as in-flight atomically; the caller must DeregisterInFlightEpoch it.
	EncryptWithEpochPinnedContext(dst, plaintext, contextAAD []byte) ([]byte, uint64, error)

	// DecryptForEpoch decrypts ciphertext using a specific epoch's DEK.
	DecryptForEpoch(dst, ciphertext []byte, epoch uint64) ([]byte, error)

	// IsHardwareMode reports whether the barrier delegates sealing to an
	// external encryptor. It says nothing about whether that encryptor can
	// authenticate caller context — use SupportsContextAAD for that.
	IsHardwareMode() bool

	// SupportsContextAAD reports whether the barrier authenticates the caller context snapshot frames are bound to.
	SupportsContextAAD() bool

	// Rotate increments the epoch, derives a new DEK, and installs it
	// as the current cipher. Returns the new epoch number. ctx bounds the
	// rotation; the host passes one cancelled when the host closes.
	Rotate(ctx context.Context) (uint64, error)

	// PurgeEpochsBefore removes DEKs for epochs strictly less than minEpoch.
	// Returns the number of epochs actually purged. ctx bounds the purge.
	PurgeEpochsBefore(ctx context.Context, minEpoch uint64) (int, error)

	// RegisterInFlightEpoch increments the in-flight reference count for
	// an epoch, preventing PurgeEpochsBefore from removing it.
	RegisterInFlightEpoch(epoch uint64)

	// DeregisterInFlightEpoch decrements the in-flight reference count
	// for an epoch.
	DeregisterInFlightEpoch(epoch uint64)

	// CurrentEpoch returns the current key rotation epoch.
	CurrentEpoch() uint64

	// ShouldRotateKey returns true when the AEAD invocation counter has
	// reached its rotation threshold percentage. Returns false if invocation
	// counting is disabled or the barrier is sealed.
	ShouldRotateKey() bool
}

// hostOptions holds optional dependencies injected via HostOption functions.
type hostOptions struct {
	logdb             logdb.LogDB
	barrier           Barrier
	shardBarriers     func(shardID uint64) Barrier
	metrics           MetricsCollector
	logger            *slog.Logger
	disableTransport  bool
	withoutEncryption bool
	fsys              snapshotFS
	// wire is the host's wire window; zero means transport.DefaultWireWindow.
	wire transport.WireWindow
	// wireSwitchFault, when set, is consulted before each wire-version switch
	// proposal and refuses it with the error it returns. Only tests set it.
	wireSwitchFault func() error
}

// HostOption configures optional Host dependencies.
type HostOption func(*hostOptions)

// WithLogDB provides a custom LogDB implementation. When not set, the
// default WAL-based LogDB is created from the HostConfig directories.
func WithLogDB(l logdb.LogDB) HostOption {
	return func(o *hostOptions) {
		o.logdb = l
	}
}

// WithBarrier enables at-rest encryption with the given barrier.
// The barrier must already be initialized and unsealed.
func WithBarrier(b Barrier) HostOption {
	return func(o *hostOptions) {
		o.barrier = b
	}
}

// WithShardBarriers encrypts each shard's snapshots, and with a per-barrier LogDB its WAL, under the barrier fn returns; nil falls back to the host-wide route.
func WithShardBarriers(fn func(shardID uint64) Barrier) HostOption {
	return func(o *hostOptions) {
		o.shardBarriers = fn
	}
}

// WithMetrics enables metrics collection with the given collector. The
// collector's sub-interfaces are threaded into the engine (RaftMetrics),
// transport (TransportMetrics), and WAL LogDB (LogDBMetrics) pipelines.
func WithMetrics(c MetricsCollector) HostOption {
	return func(o *hostOptions) {
		// Normalize a typed-nil collector to nil so metrics guards do not dereference it.
		if isNilMetricsCollector(c) {
			o.metrics = nil
			return
		}
		// Wrap the collector so its panics cannot escape into engine workers.
		o.metrics = newSafeMetricsCollector(c)
	}
}

// isNilMetricsCollector reports whether c is nil or a typed-nil interface
// wrapping a nil pointer/map/etc.
func isNilMetricsCollector(c MetricsCollector) bool {
	if c == nil {
		return true
	}
	rv := reflect.ValueOf(c)
	switch rv.Kind() {
	case reflect.Pointer, reflect.Map, reflect.Chan, reflect.Func, reflect.Slice, reflect.Interface:
		return rv.IsNil()
	default:
		return false
	}
}

// WithMemoryLogDB uses an in-memory LogDB instead of the default
// WAL-based storage. Suitable for benchmarks, CI/CD, and ephemeral
// nodes where durability is not required.
func WithMemoryLogDB() HostOption {
	return func(o *hostOptions) {
		o.logdb = memdb.New()
	}
}

// WithLogger provides a pre-configured *slog.Logger. When set, the
// Host uses this logger instead of configuring one from LogConfig.
// Useful for testing or when the caller needs a custom slog.Handler.
func WithLogger(l *slog.Logger) HostOption {
	return func(o *hostOptions) {
		o.logger = l
	}
}

// WithoutEncryption declares the host-wide route stores the Raft log and snapshots unencrypted; it conflicts with WithBarrier.
func WithoutEncryption() HostOption {
	return func(o *hostOptions) {
		o.withoutEncryption = true
	}
}

// WithoutTransport disables the QUIC transport layer. Messages are
// silently dropped via a no-op sender. Use for single-node testing
// only, where no remote message delivery is needed.
func WithoutTransport() HostOption {
	return func(o *hostOptions) {
		o.disableTransport = true
	}
}

// withWireWindow sets the wire versions the host accepts and applies; tests use it to emulate the next release.
func withWireWindow(w transport.WireWindow) HostOption {
	return func(o *hostOptions) {
		o.wire = w
	}
}

// noopSender drops messages when no transport is configured.
type noopSender struct{}

// Send discards all messages. Raft handles retransmission, so dropped
// messages do not affect correctness.
func (s *noopSender) Send(_ []proto.Message) {}

// SendSnapshot discards snapshot messages. In a single-node cluster
// there are no followers that need snapshot transfers.
func (s *noopSender) SendSnapshot(_ proto.Message) {}

// Compile-time assertion that noopSender satisfies engine.MessageSender.
var _ engine.MessageSender = (*noopSender)(nil)

// snapshotChunkSize is the byte size of each chunk when splitting a
// snapshot file for transport. Matches dragonboat's default of 2MB.
const snapshotChunkSize = 2 * 1024 * 1024

// maxSnapshotFrameLen bounds one encrypted snapshot frame: a 64 KiB chunk plus AEAD overhead.
const maxSnapshotFrameLen = 64*1024 + 256

// snapshotEncryptChunkSize matches engine.snapshotEncryptChunkSize so received snapshots re-encrypt in the save-time frame format.
const snapshotEncryptChunkSize = 64 * 1024

// transportSender adapts a QUICTransport to engine.MessageSender, including chunked snapshot streaming.
type transportSender struct {
	transport *transport.QUICTransport
	engine    *engine.Engine
	barrier   Barrier
	// shardBarriers, when non-nil, routes snapshot decrypt-for-send to a
	// per-shard barrier. When nil the single host-wide barrier is used.
	shardBarriers func(shardID uint64) Barrier
	logger        *slog.Logger
	snapshotWg    sync.WaitGroup // tracks in-flight snapshot sends
	// fsys routes snapshot decrypt-for-send I/O; nil means the real
	// filesystem. Test-only injection, mirroring Host.fsys.
	fsys snapshotFS
}

// barrierFor returns the barrier for shardID's snapshots, falling back to the host-wide barrier.
func (s *transportSender) barrierFor(shardID uint64) Barrier {
	if s.shardBarriers != nil {
		if b := s.shardBarriers(shardID); b != nil {
			return b
		}
	}
	return s.barrier
}

// givenBarriers records every barrier the resolver returned so Close can seal each; the zero value is ready to use.
type givenBarriers struct {
	mu sync.Mutex
	// identified holds each distinct pointer barrier once.
	identified []Barrier
	// lastIdentified is the pointer barrier last recorded for each shard. A
	// resolution that repeats it, the common case, is recorded without
	// scanning identified.
	lastIdentified map[uint64]Barrier
	// unidentified is the latest non-pointer barrier recorded for each shard.
	unidentified map[uint64]Barrier
}

// recording wraps resolve so every barrier any consumer receives is recorded for Close.
func (g *givenBarriers) recording(resolve func(shardID uint64) Barrier) func(shardID uint64) Barrier {
	return func(shardID uint64) Barrier {
		b := resolve(shardID)
		if b != nil {
			g.record(shardID, b)
		}
		return b
	}
}

// record adds b, resolved for shardID, to the set.
func (g *givenBarriers) record(shardID uint64, b Barrier) {
	g.mu.Lock()
	defer g.mu.Unlock()
	if !logdb.SameBarrier(b, b) {
		if g.unidentified == nil {
			g.unidentified = make(map[uint64]Barrier)
		}
		g.unidentified[shardID] = b
		return
	}
	if logdb.SameBarrier(g.lastIdentified[shardID], b) {
		return
	}
	if g.lastIdentified == nil {
		g.lastIdentified = make(map[uint64]Barrier)
	}
	g.lastIdentified[shardID] = b
	for _, known := range g.identified {
		if logdb.SameBarrier(known, b) {
			return
		}
	}
	g.identified = append(g.identified, b)
}

// each calls fn once for every recorded barrier, outside the set's lock. A nil
// set records nothing.
func (g *givenBarriers) each(fn func(Barrier)) {
	if g == nil {
		return
	}
	g.mu.Lock()
	recorded := make([]Barrier, 0, len(g.identified)+len(g.unidentified))
	recorded = append(recorded, g.identified...)
	for _, b := range g.unidentified {
		recorded = append(recorded, b)
	}
	g.mu.Unlock()
	for _, b := range recorded {
		fn(b)
	}
}

// logDBAtRestProtection reports a LogDB's declared at-rest guarantee; no declaration or a nil LogDB reads as unprotected.
func logDBAtRestProtection(db logdb.LogDB) logdb.AtRestProtection {
	p, ok := db.(logdb.AtRestProtectedLogDB)
	if !ok {
		return logdb.AtRestUnprotected
	}
	return p.AtRestProtection()
}

// requireEncryptedLogDB refuses a LogDB whose Raft log could be stored in plaintext the host never declared.
func requireEncryptedLogDB(db logdb.LogDB, hostBarrier Barrier, plaintextRoute bool, refusal error) error {
	if db == nil {
		// The default WAL is about to be created and no barrier was configured
		// for it, so it would write plaintext segments.
		return refusal
	}
	switch logDBAtRestProtection(db) {
	case logdb.AtRestBarrierEncrypted, logdb.AtRestNonDurable:
		return nil
	case logdb.AtRestHostRouted:
		if plaintextRoute || logDBWritesUnder(db, hostBarrier) {
			return nil
		}
		return refusal
	default:
		return refusal
	}
}

// logDBWritesUnder reports whether db stores records under barrier, by barrier identity.
func logDBWritesUnder(db logdb.LogDB, barrier Barrier) bool {
	if barrier == nil || !logdb.SameBarrier(barrier, barrier) {
		return false
	}
	perBarrier, ok := db.(logdb.PerBarrierEpochAwareLogDB)
	if !ok {
		return false
	}
	_, known, err := perBarrier.MinLiveEpochForBarrier(barrier)
	return err == nil && known
}

// barrierForShard resolves a shard's at-rest barrier, failing with ErrShardBarrierUnresolved unless a host-wide route exists.
func (h *Host) barrierForShard(shardID uint64) (Barrier, error) {
	if h.shardBarriers == nil {
		return h.barrier, nil
	}
	if b := h.shardBarriers(shardID); b != nil {
		return b, nil
	}
	if h.barrier == nil {
		if h.plaintextHostRoute {
			h.warnBarrierFallback(shardID)
			return nil, nil
		}
		return nil, ErrShardBarrierUnresolved
	}
	// Documented fallback: WithShardBarriers says a nil result uses the
	// host-wide barrier, which still encrypts. Warn once per shard so an
	// unintended resolver miss is observable rather than merely legal.
	h.warnBarrierFallback(shardID)
	return h.barrier, nil
}

// isSealedLogWrite reports whether a SaveState failure is only the barrier's sealed refusal, which the engine holds on instead of failing.
func isSealedLogWrite(err error) bool {
	return waldb.IsCleanRefusal(err, ErrBarrierSealed)
}

// unsealNotifier is the optional Barrier capability the engine's sealed-write
// hold waits on; seal.Barrier implements it.
type unsealNotifier interface {
	UnsealedC() <-chan struct{}
}

// logWriteUnsealedC resolves the barrier that seals shardID's log writes the
// way barrierForShard does, and returns its unseal notification — nil when it
// offers none, which makes the engine poll instead.
func logWriteUnsealedC(hostBarrier Barrier, shardBarriers func(uint64) Barrier) func(uint64) <-chan struct{} {
	return func(shardID uint64) <-chan struct{} {
		var b Barrier
		if shardBarriers != nil {
			b = shardBarriers(shardID)
		}
		if b == nil {
			b = hostBarrier
		}
		if n, ok := b.(unsealNotifier); ok {
			return n.UnsealedC()
		}
		return nil
	}
}

// warnBarrierFallback logs the host-wide-barrier fallback once per shard. The
// resolution runs on every snapshot save, recovery, send, receive and purge, so
// an unconditional warning would bury the first (and only informative) one.
func (h *Host) warnBarrierFallback(shardID uint64) {
	if _, warned := h.barrierFallbackWarned.LoadOrStore(shardID, struct{}{}); warned {
		return
	}
	if h.barrier == nil {
		h.logger.Warn("per-shard barrier resolver returned no barrier; "+
			"the shard uses the plaintext host-wide route WithoutEncryption declared",
			"shard_id", shardID)
		return
	}
	h.logger.Warn("per-shard barrier resolver returned no barrier; "+
		"falling back to the host-wide barrier",
		"shard_id", shardID)
}

// verifyShardLogDBBarrier refuses a shard whose Raft log the LogDB does not store under the shard's own barrier.
func (h *Host) verifyShardLogDBBarrier(shardID uint64, barrier Barrier) error {
	if h.shardBarriers == nil || h.logdb == nil || barrier == nil {
		return nil
	}
	protection := logDBAtRestProtection(h.logdb)
	checked := protection == logdb.AtRestHostRouted ||
		(protection == logdb.AtRestBarrierEncrypted && h.barrier == nil)
	if !checked {
		return nil
	}
	perBarrier, ok := h.logdb.(logdb.PerBarrierEpochAwareLogDB)
	if !ok {
		h.logger.Error("refusing to start shard: the supplied LogDB cannot "+
			"attribute its records to a barrier, so it cannot prove this shard's "+
			"log is stored under this shard's own key",
			"shard_id", shardID)
		return ErrShardBarrierLogDBMismatch
	}
	if !logdb.SameBarrier(barrier, barrier) {
		h.logger.Error("refusing to start shard: the barrier the resolver "+
			"returned for it is not a pointer, so it has no identity a LogDB "+
			"could attribute this shard's log to",
			"shard_id", shardID)
		return ErrShardBarrierLogDBMismatch
	}
	_, known, err := perBarrier.MinLiveEpochForBarrier(barrier)
	if err != nil {
		return err
	}
	if !known {
		h.logger.Error("refusing to start shard: the supplied LogDB does not "+
			"store this shard's log under the barrier the resolver returned for "+
			"it; the log would be encrypted under another tenant's key",
			"shard_id", shardID)
		return ErrShardBarrierLogDBMismatch
	}
	return nil
}

// snapshotBarrierForShard resolves the barrier for a snapshot write, failing with ErrShardBarrierUnresolved or ErrSnapshotBarrierSealed.
func (h *Host) snapshotBarrierForShard(shardID uint64) (Barrier, error) {
	b, err := h.barrierForShard(shardID)
	if err != nil {
		return nil, err
	}
	if b != nil && b.IsSealed() {
		return nil, ErrSnapshotBarrierSealed
	}
	return b, nil
}

// Send delegates regular message delivery to the transport.
func (s *transportSender) Send(msgs []proto.Message) {
	s.transport.Send(msgs)
}

// SendSnapshot streams a snapshot to a follower in the background and reports a SnapshotStatus to raft when done.
func (s *transportSender) SendSnapshot(msg proto.Message) {
	// Open the artifact and its meta now so a later GC cannot remove them; register the send with the shard's pause first.
	var node *engine.Node
	if s.engine != nil {
		node = s.engine.GetNode(msg.ShardID)
	}
	if node != nil && !node.BeginSnapshotTransfer() {
		s.safeNotifySnapshotRejected(msg.ShardID, msg.From, msg.To, msg.Hint)
		return
	}
	var src *verifiedSnapshotFile
	var openErr error
	if msg.Snapshot.Filepath != "" {
		src, openErr = openVerifiedSnapshotFile(s.snapshotFsys(), msg.Snapshot.Filepath)
	}
	s.snapshotWg.Add(1)
	go func() {
		defer s.snapshotWg.Done()
		if node != nil {
			defer node.EndSnapshotTransfer()
		}
		defer func() {
			if src == nil {
				return
			}
			if err := src.Close(); err != nil && !errors.Is(err, os.ErrClosed) {
				s.logger.Debug("snapshot send source close failed",
					"shard", msg.ShardID, "path", msg.Snapshot.Filepath, "error", err)
			}
		}()
		// normalReturn tells a runtime.Goexit in user code apart from a
		// normal return; both leave recover() nil.
		normalReturn := false
		defer func() {
			if r, abnormal := supervise.ClassifyExit(recover(), normalReturn); abnormal {
				// Tell raft the snapshot was rejected before logging, so the remote leaves snapshot state.
				s.safeNotifySnapshotRejected(msg.ShardID, msg.From, msg.To, msg.Hint)
				stack := debug.Stack()
				supervise.LogContained(func() {
					s.logger.Error("panic recovered in doSendSnapshot",
						"shard", msg.ShardID,
						"from", msg.From,
						"to", msg.To,
						"panic", fmt.Sprint(r),
						"stack", string(stack),
					)
				})
			}
		}()
		s.doSendSnapshot(msg, src, openErr)
		normalReturn = true
	}()
}

// safeNotifySnapshotRejected delivers a rejected SnapshotStatus, recovering from a panic if the engine is gone.
func (s *transportSender) safeNotifySnapshotRejected(shardID, from, to, transferSeq uint64) {
	defer func() {
		if r := recover(); r != nil {
			supervise.LogContained(func() {
				s.logger.Error("panic in snapshot rejection notification",
					"shard", shardID,
					"from", from,
					"to", to,
					"panic", fmt.Sprint(r),
				)
			})
		}
	}()
	s.notifySnapshotStatus(shardID, from, to, transferSeq, true)
}

// Wait blocks until all in-flight snapshot sends complete. Called by
// Host.Close() to drain pending snapshot transfers before shutting
// down the transport.
func (s *transportSender) Wait() {
	s.snapshotWg.Wait()
}

// doSendSnapshot sends the snapshot in chunks, decrypting at-rest data first and verifying every byte against snapshot.meta.
func (s *transportSender) doSendSnapshot(msg proto.Message, src *verifiedSnapshotFile, openErr error) {
	// resourceCleanup tracks resources that must be cleaned up on exit,
	// whether by normal return, error, or panic. Resources are cleaned
	// up in reverse order of registration.
	var resourceCleanup []func()
	cleanup := func() {
		for i := len(resourceCleanup) - 1; i >= 0; i-- {
			resourceCleanup[i]()
		}
	}
	defer cleanup()

	// Every read goes through src or the anonymous decrypt file, never a path, so GC cannot affect the send.
	if openErr != nil {
		s.logger.Warn("snapshot send refused: the artifact could not be opened",
			"shard", msg.ShardID,
			"to", msg.To,
			"path", msg.Snapshot.Filepath,
			"error", openErr,
		)
		s.notifySnapshotStatus(msg.ShardID, msg.From, msg.To, msg.Hint, true)
		return
	}
	var source snapshotFile
	if src != nil {
		source = src
	}

	// A snapshot with no data file (a witness's) ships as one empty chunk at epoch 0, whatever its recorded epoch.
	if msg.Snapshot.Filepath == "" {
		msg.Snapshot.Epoch = 0
	}

	// Decrypt an encrypted snapshot under the shard's barrier: the receiver's barrier holds a different key.
	sendBarrier := s.barrierFor(msg.ShardID)
	if msg.Snapshot.Epoch > 0 {
		// Without the sealing barrier, refuse the send and report it rejected so the leader retries after unseal.
		if sendBarrier == nil || sendBarrier.IsSealed() {
			s.logger.Warn("snapshot send refused: encrypted snapshot but barrier unavailable",
				"shard", msg.ShardID,
				"to", msg.To,
				"epoch", msg.Snapshot.Epoch,
				"barrier_nil", sendBarrier == nil,
			)
			s.notifySnapshotStatus(msg.ShardID, msg.From, msg.To, msg.Hint, true)
			return
		}
		decrypted, decryptedSize, err := s.decryptSnapshotForSend(src, msg, sendBarrier)
		if err != nil {
			s.logger.Warn("snapshot decrypt-for-send failed",
				"shard", msg.ShardID,
				"to", msg.To,
				"epoch", msg.Snapshot.Epoch,
				"error", err,
			)
			s.notifySnapshotStatus(msg.ShardID, msg.From, msg.To, msg.Hint, true)
			return
		}
		// Registered immediately, so the anonymous scratch file is closed —
		// and with that gone — even if a panic follows.
		resourceCleanup = append(resourceCleanup, func() {
			if err := decrypted.Close(); err != nil && !errors.Is(err, os.ErrClosed) {
				s.logger.Debug("decrypted snapshot scratch file close failed", "error", err)
			}
		})
		// The decrypt read the whole artifact, so check its recorded integrity before emitting any chunk.
		if err := src.Verify(); err != nil {
			s.logger.Warn("snapshot send refused: artifact fails its recorded integrity check",
				"shard", msg.ShardID,
				"to", msg.To,
				"path", msg.Snapshot.Filepath,
				"error", err,
			)
			s.notifySnapshotStatus(msg.ShardID, msg.From, msg.To, msg.Hint, true)
			return
		}
		source = decrypted
		msg.Snapshot.FileSize = decryptedSize
		msg.Snapshot.Epoch = 0
	}

	// Stream one chunk at a time from the open file, so at most one 2 MB chunk is in memory.
	snap := &msg.Snapshot
	fileSize := snap.FileSize
	var chunkCount uint64
	if snap.Filepath == "" || fileSize == 0 {
		chunkCount = 1
	} else {
		chunkCount = (fileSize-1)/snapshotChunkSize + 1
	}

	meta := transport.SnapshotHeader{
		ShardID:    msg.ShardID,
		ReplicaID:  msg.To,
		Index:      snap.Index,
		Term:       snap.Term,
		ChunkCount: chunkCount,
		Epoch:      snap.Epoch,
		From:       msg.From,
	}

	producer := func(emit func(proto.SnapshotChunk) error) error {
		// A snapshot with no data file is the one metadata-only transfer.
		if snap.Filepath == "" {
			return emit(emptySnapshotChunk(msg, snap))
		}
		// For a plaintext artifact source IS src, so the check runs over the
		// bytes being streamed. For a decrypted one it was already made, and
		// repeating it before the last chunk costs a comparison.
		return streamSnapshotChunks(source, msg, emit, src.Verify)
	}

	if err := s.transport.SendSnapshotStreaming(meta, producer); err != nil {
		s.logger.Warn("snapshot send failed",
			"shard", msg.ShardID,
			"to", msg.To,
			"error", err,
		)
		s.notifySnapshotStatus(msg.ShardID, msg.From, msg.To, msg.Hint, true)
		return
	}

	s.notifySnapshotStatus(msg.ShardID, msg.From, msg.To, msg.Hint, false)
}

// decryptSnapshotForSend decrypts src into an anonymous, already-unlinked scratch file in the replica's snapshot directory and returns it rewound with the plaintext size.
func (s *transportSender) decryptSnapshotForSend(src snapshotFile, msg proto.Message, barrier Barrier) (snapshotFile, uint64, error) {
	if src == nil {
		return nil, 0, &SnapshotSendError{ShardID: msg.ShardID, ReplicaID: msg.To, Op: "read", Err: os.ErrNotExist}
	}
	barrier.RegisterInFlightEpoch(msg.Snapshot.Epoch)
	defer barrier.DeregisterInFlightEpoch(msg.Snapshot.Epoch)

	// Use the replica's own snapshot directory, which is swept at start and recovery; a shard no longer loaded as this replica has nothing to send.
	node := s.engine.GetNode(msg.ShardID)
	if node == nil || node.ReplicaID() != msg.ReplicaID {
		return nil, 0, &SnapshotSendError{ShardID: msg.ShardID, ReplicaID: msg.To, Op: "read", Err: ErrShardNotFound}
	}
	tmp, err := engine.CreateAnonymousScratch(s.snapshotFsys(), node.ReplicaSnapshotDir(), sendDecryptTempPattern)
	if err != nil {
		return nil, 0, err
	}
	// Closed on every way out that does not hand it to the caller — a panic
	// in the decrypt included — so no failed send leaks the descriptor.
	handedOff := false
	defer func() {
		if handedOff {
			return
		}
		if closeErr := tmp.Close(); closeErr != nil && !errors.Is(closeErr, os.ErrClosed) {
			s.logger.Debug("decrypt-for-send scratch file close failed", "error", closeErr)
		}
	}()
	size, err := decryptSnapshotFrames(barrier, msg.ShardID, msg.ReplicaID, msg.Snapshot.Index, src, tmp)
	if err != nil {
		return nil, 0, err
	}
	if _, err := tmp.Seek(0, io.SeekStart); err != nil {
		return nil, 0, err
	}
	handedOff = true
	return tmp, size, nil
}

// Scratch file name patterns come from the engine's constants so SweepStaleDecryptTmp can reclaim any orphan.
const (
	sendDecryptTempPattern    = engine.SendDecryptTempPattern
	recoverDecryptTempPattern = engine.RecoverDecryptTempPattern
	recvEncryptTempPattern    = engine.RecvEncryptTempPattern
	reencryptTempPattern      = engine.ReencryptTempPattern
)

// shardSnapshotBaseDir returns the shard's base snapshot directory, rooted at dataDir when set; every snapshot path must resolve under it.
func shardSnapshotBaseDir(nodeHostDir, dataDir string, shardID uint64) string {
	if dataDir != "" {
		return filepath.Join(dataDir, fmt.Sprintf("shard-%d", shardID))
	}
	return filepath.Join(nodeHostDir, "snapshots", fmt.Sprintf("shard-%d", shardID))
}

// decryptSnapshotToAnonymousScratch decrypts srcPath into an anonymous scratch file in scratchDir and returns it rewound; srcPath is never modified.
func decryptSnapshotToAnonymousScratch(fsys snapshotFS, scratchDir, srcPath string,
	shardID, replicaID, index, epoch uint64, barrier Barrier) (snapshotFile, error) {
	barrier.RegisterInFlightEpoch(epoch)
	defer barrier.DeregisterInFlightEpoch(epoch)

	srcFile, err := fsys.Open(srcPath)
	if err != nil {
		return nil, err
	}
	// The source is ciphertext and stays in place, so a failed close costs a
	// descriptor and nothing at rest.
	defer func() {
		if closeErr := srcFile.Close(); closeErr != nil {
			slog.Debug("snapshot source file close failed",
				"path", srcPath,
				"error", closeErr)
		}
	}()

	tmpFile, err := engine.CreateAnonymousScratch(fsys, scratchDir, recoverDecryptTempPattern)
	if err != nil {
		return nil, err
	}
	// Closed on every way out that does not hand it to the caller, a panic in
	// the decrypt included.
	handedOff := false
	defer func() {
		if handedOff {
			return
		}
		if closeErr := tmpFile.Close(); closeErr != nil && !errors.Is(closeErr, os.ErrClosed) {
			slog.Debug("snapshot decrypt scratch file close failed",
				"error", closeErr)
		}
	}()

	if _, err := decryptSnapshotFrames(barrier, shardID, replicaID, index, srcFile, tmpFile); err != nil {
		return nil, err
	}
	// No fsync: the file has no name, so nothing after a crash could read it
	// back. The caller reads it through this handle.
	if _, err := tmpFile.Seek(0, io.SeekStart); err != nil {
		return nil, err
	}
	handedOff = true
	return tmpFile, nil
}

// encryptSnapshotFrames seals src into the snapshot frame format and returns the minimum epoch and every frame epoch, pinned in-flight for the caller to deregister.
func encryptSnapshotFrames(barrier Barrier, shardID, replicaID, index uint64,
	src io.Reader, dst io.Writer) (uint64, []uint64, error) {
	return engine.SealSnapshotFrames(barrier, shardID, replicaID, index, src, dst)
}

// decryptSnapshotFrames authenticates and decrypts a sealed artifact frame by frame; damage reports ErrSnapshotCorrupt.
func decryptSnapshotFrames(barrier Barrier, shardID, replicaID, index uint64,
	src io.Reader, dst io.Writer) (uint64, error) {
	n, err := engine.OpenSnapshotFrames(barrier, shardID, replicaID, index, src, dst)
	if errors.Is(err, engine.ErrSnapshotCorrupt) {
		return 0, ErrSnapshotCorrupt
	}
	return n, err
}

// notifySnapshotStatus tells the leader a transfer's outcome, echoing its transfer sequence in Hint and retrying delivery briefly.
func (s *transportSender) notifySnapshotStatus(shardID, localReplicaID, targetReplicaID, transferSeq uint64, rejected bool) {
	statusMsg := proto.Message{
		Type:    proto.SnapshotStatus,
		ShardID: shardID,
		From:    targetReplicaID,
		To:      localReplicaID,
		Hint:    transferSeq,
		Reject:  rejected,
	}
	if !deliverSnapshotControl(s.engine, shardID, statusMsg) {
		s.logger.Warn("failed to deliver SnapshotStatus",
			"shard", shardID,
			"target", targetReplicaID,
			"transfer_seq", transferSeq,
			"rejected", rejected,
		)
	}
}

// emptySnapshotChunk builds the single metadata-only chunk for a snapshot with no data.
func emptySnapshotChunk(msg proto.Message, snap *proto.Snapshot) proto.SnapshotChunk {
	return proto.SnapshotChunk{
		ShardID:        msg.ShardID,
		ReplicaID:      msg.To,
		From:           msg.From,
		Index:          snap.Index,
		Term:           snap.Term,
		ChunkID:        0,
		ChunkCount:     1,
		ChunkSize:      0,
		FileSize:       0,
		Filepath:       snap.Filepath,
		FileChunkID:    0,
		FileChunkCount: 1,
		Membership:     snap.Membership,
		OnDiskIndex:    snap.OnDiskIndex,
		BinVer:         proto.PreviousWireVersion, // the transport restamps it from the shard's committed WireVersion
		Epoch:          snap.Epoch,
	}
}

// forEachSnapshotChunk passes each chunk of msg's snapshot, read from its verified data file, to emit.
func forEachSnapshotChunk(fsys snapshotFS, msg proto.Message, emit func(proto.SnapshotChunk) error) error {
	snap := &msg.Snapshot
	filePath := snap.Filepath

	// Only a snapshot with no data file is metadata-only; a non-empty Filepath is always opened and verified.
	if filePath == "" {
		return emit(emptySnapshotChunk(msg, snap))
	}

	// fileClosed lets the file close early without a double close in the deferred cleanup.
	f, err := openVerifiedSnapshotFile(fsys, filePath)
	if err != nil {
		return &SnapshotSendError{
			ShardID:   msg.ShardID,
			ReplicaID: msg.To,
			Op:        "read",
			Err:       err,
		}
	}
	fileClosed := false
	defer func() {
		if !fileClosed {
			if closeErr := f.Close(); closeErr != nil && !errors.Is(closeErr, os.ErrClosed) {
				slog.Debug("snapshot file close failed",
					"shard", msg.ShardID,
					"replica", msg.To,
					"path", filePath,
					"error", closeErr)
			}
		}
	}()

	if err := streamSnapshotChunks(f, msg, emit, f.Verify); err != nil {
		return err
	}

	// Close the file early to release the handle immediately after reading
	// completes. The deferred cleanup will be a no-op due to fileClosed flag.
	if err := f.Close(); err != nil {
		return &SnapshotSendError{
			ShardID:   msg.ShardID,
			ReplicaID: msg.To,
			Op:        "read",
			Err:       err,
		}
	}
	fileClosed = true
	return nil
}

// streamSnapshotChunks streams the open file f in chunks after checking its declared size, calling verify before the last chunk.
func streamSnapshotChunks(f snapshotFile, msg proto.Message, emit func(proto.SnapshotChunk) error, verify func() error) error {
	snap := &msg.Snapshot
	filePath := snap.Filepath
	fileSize := snap.FileSize

	// Verify the actual file size matches the declared size.
	info, err := f.Stat()
	if err != nil {
		return &SnapshotSendError{
			ShardID:   msg.ShardID,
			ReplicaID: msg.To,
			Op:        "read",
			Err:       err,
		}
	}
	if uint64(info.Size()) != fileSize {
		return &SnapshotSendError{
			ShardID:   msg.ShardID,
			ReplicaID: msg.To,
			Op:        "read",
			Err:       ErrSnapshotSizeMismatch,
		}
	}

	// A genuinely empty data file: metadata agrees with disk, so there is
	// nothing to stream. Computing chunkCount here would underflow.
	if fileSize == 0 {
		if err := verifySnapshotSend(msg, verify); err != nil {
			return err
		}
		return emit(emptySnapshotChunk(msg, snap))
	}

	chunkCount := (fileSize-1)/snapshotChunkSize + 1

	for i := uint64(0); i < chunkCount; i++ {
		remaining := fileSize - i*snapshotChunkSize
		chunkLen := uint64(snapshotChunkSize)
		if remaining < chunkLen {
			chunkLen = remaining
		}
		chunkData := make([]byte, chunkLen)
		if _, err := io.ReadFull(f, chunkData); err != nil {
			return &SnapshotSendError{
				ShardID:   msg.ShardID,
				ReplicaID: msg.To,
				Op:        "read",
				Err:       err,
			}
		}
		if i == chunkCount-1 {
			if err := verifySnapshotSend(msg, verify); err != nil {
				return err
			}
		}

		if err := emit(proto.SnapshotChunk{
			ShardID:        msg.ShardID,
			ReplicaID:      msg.To,
			From:           msg.From,
			Index:          snap.Index,
			Term:           snap.Term,
			ChunkID:        i,
			ChunkCount:     chunkCount,
			ChunkSize:      chunkLen,
			FileSize:       fileSize,
			Filepath:       filePath,
			FileChunkID:    i,
			FileChunkCount: chunkCount,
			Membership:     snap.Membership,
			OnDiskIndex:    snap.OnDiskIndex,
			BinVer:         proto.PreviousWireVersion, // the transport restamps it from the shard's committed WireVersion
			Epoch:          snap.Epoch,
			Data:           chunkData,
		}); err != nil {
			return err
		}
	}
	return nil
}

// verifySnapshotSend runs a stream's integrity check and reports a failure as
// the SnapshotSendError every other refusal of the stream is.
func verifySnapshotSend(msg proto.Message, verify func() error) error {
	if err := verify(); err != nil {
		return &SnapshotSendError{
			ShardID:   msg.ShardID,
			ReplicaID: msg.To,
			Op:        "verify",
			Err:       err,
		}
	}
	return nil
}

// verifiedSnapshotFile feeds every Read to a verifier for the snapshot.meta integrity fields; do not Seek it.
type verifiedSnapshotFile struct {
	snapshotFile
	verifier *engine.SnapshotDataVerifier
}

// openVerifiedSnapshotFile opens the data file at dataPath and reads its snapshot.meta record at open.
func openVerifiedSnapshotFile(fsys snapshotFS, dataPath string) (*verifiedSnapshotFile, error) {
	f, err := fsys.Open(dataPath)
	if err != nil {
		return nil, err
	}
	return &verifiedSnapshotFile{
		snapshotFile: f,
		verifier:     engine.NewSnapshotDataVerifier(filepath.Dir(dataPath)),
	}, nil
}

// Read reads from the file and adds what it read to the running hash.
func (f *verifiedSnapshotFile) Read(p []byte) (int, error) {
	n, err := f.snapshotFile.Read(p)
	if n > 0 {
		_, _ = f.verifier.Write(p[:n]) // the verifier's Write never fails
	}
	return n, err
}

// Verify checks every byte read so far against the recorded length and hash,
// under the policy recovery applies. A mismatch is ErrSnapshotCorrupt.
func (f *verifiedSnapshotFile) Verify() error {
	err := f.verifier.Verify()
	if errors.Is(err, engine.ErrSnapshotCorrupt) {
		return ErrSnapshotCorrupt
	}
	return err
}

// Compile-time assertion that transportSender satisfies MessageSender.
var _ engine.MessageSender = (*transportSender)(nil)

// pendingRead tracks one ReadIndex request; a batch key's aliases resolve with the same readIndex.
type pendingRead struct {
	rs        *RequestState
	readIndex uint64   // set when OnReadyToRead confirms
	confirmed bool     // true once readIndex is known
	aliases   []uint64 // other readKeys that share this ReadIndex result
}

// nodeState is the host's per-shard state for local reads and routing.
type nodeState struct {
	shardID   uint64
	replicaID uint64
	cfg       config.Config
	createFn  sm.CreateFunc
	smType    uint64

	// barrier is the at-rest barrier resolved at StartShard, nil when the shard is not encrypted.
	barrier Barrier

	// paused is set by PauseShard and cleared by ResumeShard. The client
	// entry points load it once and refuse with ErrShardPaused; the engine
	// node carries its own flag, which gates the replica itself.
	paused atomic.Bool
	// pauseGate serializes PauseShard and ResumeShard on this shard.
	pauseGate pauseGate

	// peerWire is each member's advertised MaxBinVer; wireSwitchAt rate-limits the leader's wire-version switch.
	peerWireMu   sync.Mutex
	peerWire     map[uint64]uint64
	wireSwitchAt atomic.Int64

	leaderID          atomic.Uint64
	term              atomic.Uint64
	lastApplied       atomic.Uint64
	configChangeIndex atomic.Uint64
	isLeader          atomic.Bool

	// readReady mirrors the last OnLeaderReady: the (leaderID, term) pair
	// above is read-ready. Every updateNodeLeader clears it — each names a new
	// pair — and markLeaderReady sets it, both under leaderSeq.
	readReady atomic.Bool

	// leaderSeq is a seqlock making leaderID, term and isLeader readable as a consistent triple.
	leaderSeq atomic.Uint64

	// leaderWake is closed by the next change to the leadership view or the shard's stop.
	leaderWake atomic.Pointer[chan struct{}]

	// leaderlessSince is the monotonic time since the shard last had a known leader, or 0; HealthCheck uses it.
	leaderlessSince atomic.Int64

	health      atomic.Uint32 // stores ShardHealth as uint32
	nodes       map[uint64]string
	nodesMu     sync.RWMutex
	requestPool *sync.Pool
	stopped     atomic.Bool
	rsm         *rsm.StateMachine // for local reads (Lookup)

	// openStopper is closed to cancel an in-progress DiskStateMachine.Open()
	// call during Host.Close() or StopShard(). For in-memory state machines
	// it is closed immediately after the no-op Open returns.
	openStopper chan struct{}

	// openStopOnce ensures openStopper is closed at most once.
	openStopOnce sync.Once

	// pendingProposals maps proposal key to the *RequestState waiting for its entry to apply.
	pendingProposals sync.Map

	// pendingReadsMu protects pendingReads only.
	pendingReadsMu sync.Mutex

	// pendingReads maps read key to its pendingRead until lastApplied reaches its readIndex.
	pendingReads map[uint64]*pendingRead

	// pendingSnapshotsMu protects pendingSnapshots only.
	pendingSnapshotsMu sync.Mutex

	// pendingSnapshots maps snapshot key to the RequestState waiting for
	// the snapshot to complete. Entries are added by RequestSnapshot and
	// removed by OnSnapshotCompleted.
	pendingSnapshots map[uint64]*RequestState
}

// barrierSealed reports whether the shard's barrier is sealed; client entry points refuse with ErrBarrierSealed while it is.
func (ns *nodeState) barrierSealed() bool {
	return ns.barrier != nil && ns.barrier.IsSealed()
}

// pendingProposal bundles a RequestState with its deadline, captured
// at Store time. The sweep reads only pp.deadline (immutable after Store),
// never touching the RS fields that may be recycled by sync.Pool.
type pendingProposal struct {
	rs       *RequestState
	deadline time.Time

	// floor is the committed index at registration; an entry at or below it cannot be this proposal.
	floor uint64

	// onFailure runs once if the proposal completes with an error, never when it applies.
	onFailure func()

	// node and enqueuedAt tell a drain whether the proposal reached raft.
	node       *engine.Node
	enqueuedAt uint64
}

// proposalMayReachRaft reports whether pp has been or may still be handed to raft.
func proposalMayReachRaft(pp *pendingProposal) bool {
	if pp.node == nil {
		return true
	}
	taken, final := pp.node.ProposalTaken(&pp.enqueuedAt)
	return taken || !final
}

// closedQueueRefusal returns ErrShardFailed or ErrClosed for a closed proposal queue, or nil for an overflow.
func closedQueueRefusal(engNode *engine.Node) error {
	if engNode.IsFailed() {
		return ErrShardFailed
	}
	if engNode.IsStopped() {
		return ErrClosed
	}
	return nil
}

// failed runs the entry's failure hook. Safe when no hook is set.
func (pp *pendingProposal) failed() {
	if pp.onFailure != nil {
		pp.onFailure()
	}
}

// hostEngineCallback implements engine.Callback by routing
// applied entries and leader changes back to the Host for proposal
// completion and leader state updates.
type hostEngineCallback struct {
	h *Host
}

// owns reports whether entry can be the entry this proposal became: its index
// must lie above the floor recorded at registration. See pendingProposal.floor.
func (pp *pendingProposal) owns(entry proto.Entry) bool {
	return entry.Index > pp.floor
}

// deliverProposal completes pp's caller with the applied result, mapping ResultSessionExpired to an error, and returns the request's creation time.
func deliverProposal(h *Host, shardID uint64, entry proto.Entry, pp *pendingProposal, result sm.Result) time.Time {
	rs := pp.rs
	// createdAt is read before the release, after which rs may be recycled.
	createdAt := rs.createdAt
	if result.Value == sm.ResultSessionExpired {
		rs.complete(RequestResult{Err: &SessionExpiredError{
			ShardID:  shardID,
			ClientID: entry.ClientID,
		}})
	} else {
		rs.complete(RequestResult{Value: result.Value, Data: result.Data})
	}
	rs.releaseRef()
	h.completePendingOp()
	return createdAt
}

// observeProposalLatency records proposal latency after the caller has been completed.
func observeProposalLatency(h *Host, shardID uint64, createdAt time.Time) {
	if h.metrics != nil && !createdAt.IsZero() {
		h.metrics.ObserveProposalLatency(shardID, time.Since(createdAt))
	}
}

// OnApplied completes the pending proposals whose entries were applied; entries and results are parallel slices.
func (c *hostEngineCallback) OnApplied(shardID uint64, entries []proto.Entry, results []sm.Result) {
	v, exists := c.h.nodes.Load(shardID)
	if !exists {
		return
	}
	ns, ok := v.(*nodeState)
	if !ok {
		c.h.logger.Error("OnApplied type assertion failed",
			"shard_id", shardID,
			"error", &TypeAssertionError{
				Context:      "OnApplied nodeState lookup",
				ExpectedType: "*nodeState",
				ActualValue:  v,
			})
		return
	}

	// Detect membership changes outside the lock. Both an incremental config
	// change and the bootstrap membership entry update the routing table.
	hasMembershipChange := false
	for _, entry := range entries {
		t := entry.Type & proto.EntryTypeMask
		if t == proto.EntryConfigChange || t == proto.EntryInitialMembership {
			hasMembershipChange = true
			break
		}
	}

	// Clamp a short results slice and log it; unmatched proposals expire through the sweeper.
	matched := entries
	if len(results) < len(entries) {
		c.h.logger.Error("OnApplied: results slice is shorter than entries; "+
			"engine invariant violated, proposals beyond the results will "+
			"time out instead of completing",
			"shard_id", shardID,
			"entries", len(entries),
			"results", len(results),
		)
		matched = entries[:len(results)]
	}

	// Claim every pending proposal before any other work; complete them only after routing and the lastApplied store.
	type claimedProposal struct {
		i  int
		pp *pendingProposal
	}
	var claimedBuf [32]claimedProposal
	claimed := claimedBuf[:0]
	for i, entry := range matched {
		// Match by entry.Key (the proposal tracking key set by
		// Host.Propose) rather than entry.Index (assigned by the raft
		// state machine).
		v, ok := ns.pendingProposals.Load(entry.Key)
		if !ok {
			continue
		}
		pp, ppOK := v.(*pendingProposal)
		if !ppOK {
			ns.pendingProposals.CompareAndDelete(entry.Key, v)
			c.h.logger.Error("OnApplied pendingProposal type assertion failed",
				"shard_id", shardID,
				"entry_key", entry.Key,
				"error", &TypeAssertionError{
					Context:      "OnApplied pendingProposal lookup",
					ExpectedType: "*pendingProposal",
					ActualValue:  v,
				})
			continue
		}
		if !pp.owns(entry) {
			// An entry that predates this proposal under its key (see
			// pendingProposal.floor). The proposal stays pending for the
			// entry that IS its own; nobody is waiting on this one.
			continue
		}
		if !ns.pendingProposals.CompareAndDelete(entry.Key, v) {
			// Claimed by a concurrent failure or sweep between the load and
			// here; that site completed the caller.
			continue
		}
		// Staged for completion after the reads scan (and, for a
		// membership-changing batch, routing) below — see the claiming
		// comment above for why.
		claimed = append(claimed, claimedProposal{i: i, pp: pp})
	}

	// One recover covers the reads scan, routing and completion, all of which call user code.
	func() {
		// completedClaims counts the claimed proposals already completed.
		completedClaims := 0
		// normalReturn distinguishes a runtime.Goexit in user code (a
		// MetricsCollector, the logger) from a normal return: both leave
		// recover() nil, and the tail must be completed for either exit.
		normalReturn := false
		defer func() {
			if r, abnormal := supervise.ClassifyExit(recover(), normalReturn); abnormal {
				// No user code runs here yet.
				stack := debug.Stack()
				panicErr := &LoopPanicError{
					LoopName: "OnApplied(membership)",
					Value:    r,
					Stack:    stack,
				}

				// Complete the not-yet-completed claims as maybe-applied before logging; never roll back, the entries are committed.
				for _, cp := range claimed[completedClaims:] {
					cp.pp.rs.complete(RequestResult{Err: proposalOutcomeUnknown(panicErr)})
					cp.pp.rs.releaseRef()
					c.h.completePendingOp()
				}

				// Log last, off this goroutine, so a panicking formatter or handler cannot skip the re-panic.
				logger := c.h.logger
				supervise.LogContained(func() {
					logger.Error("panic recovered completing a membership-changing batch; "+
						"claimed proposals already completed and released",
						"shard_id", shardID,
						"panic", fmt.Sprint(r),
						"stack", string(stack),
					)
				})

				if r == supervise.ErrGoexit {
					// A runtime.Goexit keeps unwinding on its own; the apply
					// worker's recovery classifies it and fails the shard.
					return
				}
				panic(r)
			}
		}()

		// Store lastApplied and complete satisfied reads under pendingReadsMu, released by defer before any user code.
		if len(entries) > 0 {
			c.completeAppliedReads(ns, shardID, entries[len(entries)-1].Index)
		}

		if hasMembershipChange {
			// Update routing and configChangeIndex before completing proposals, so a woken caller sees this batch's ConfigChangeID.
			c.applyMembershipRouting(ns, shardID, entries)
		}

		for i, cp := range claimed {
			createdAt := deliverProposal(c.h, shardID, matched[cp.i], cp.pp, results[cp.i])
			completedClaims = i + 1
			observeProposalLatency(c.h, shardID, createdAt)
		}
		normalReturn = true
	}()

	// Fire OnMembershipChanged last, after every proposal is completed, so a panicking listener cannot fail applied changes.
	if hasMembershipChange {
		c.fireMembershipChanged(ns, shardID)
	}
}

// completeAppliedReads stores newLastApplied and completes the reads it satisfies under pendingReadsMu, observing latencies after.
func (c *hostEngineCallback) completeAppliedReads(ns *nodeState, shardID, newLastApplied uint64) {
	var latencyBuf [8]time.Duration
	latencies := func() []time.Duration {
		latencies := latencyBuf[:0]
		observe := c.h.metrics != nil
		ns.pendingReadsMu.Lock()
		defer ns.pendingReadsMu.Unlock()
		ns.lastApplied.Store(newLastApplied)
		for key, pr := range ns.pendingReads {
			if pr.confirmed && newLastApplied >= pr.readIndex {
				// Read before the release, after which rs may be recycled.
				createdAt := pr.rs.createdAt
				pr.rs.complete(RequestResult{Value: pr.readIndex})
				delete(ns.pendingReads, key)
				pr.rs.releaseRef()
				c.h.completePendingOp()
				if observe && !createdAt.IsZero() {
					latencies = append(latencies, time.Since(createdAt))
				}
			}
		}
		return latencies
	}()
	for _, d := range latencies {
		c.h.metrics.ObserveReadIndexLatency(shardID, d)
	}
}

// fireMembershipChanged calls OnMembershipChanged for a batch that changed membership.
func (c *hostEngineCallback) fireMembershipChanged(ns *nodeState, shardID uint64) {
	if el := c.h.cfg.EventListener; el != nil && el.OnMembershipChanged != nil {
		el.OnMembershipChanged(config.EventShardInfo{
			ShardID:   shardID,
			ReplicaID: ns.replicaID,
		})
	}
}

// applyMembershipRouting updates ns.nodes, the registry and configChangeIndex from committed membership entries; a decode failure faults the shard.
func (c *hostEngineCallback) applyMembershipRouting(ns *nodeState, shardID uint64, entries []proto.Entry) {
	for _, entry := range entries {
		// Register every bootstrap member's address so a replica that learned membership from the log can route to them.
		if entry.Type&proto.EntryTypeMask == proto.EntryInitialMembership {
			if !c.registerBootstrapAddresses(ns, shardID, entry) {
				return
			}
			continue
		}
		if entry.Type&proto.EntryTypeMask != proto.EntryConfigChange {
			continue
		}
		var cc proto.ConfigChange
		if _, err := cc.UnmarshalFrom(entry.Cmd); err != nil {
			c.failShardOnCorruptConfig(shardID, entry.Index, "config change", err)
			return
		}
		switch cc.Type {
		case proto.AddNode, proto.AddNonVoting, proto.AddWitness:
			ns.nodesMu.Lock()
			ns.nodes[cc.ReplicaID] = cc.Address
			ns.nodesMu.Unlock()
			c.h.publishCommittedAddress(shardID, cc.ReplicaID, cc.Address)
			// A committed binding this host does not present means peers will refuse it; log it loudly.
			if own := c.h.NodeIdentity(); cc.ReplicaID == ns.cfg.ReplicaID && own != "" &&
				cc.NodeIdentity != "" && cc.NodeIdentity != own {
				c.h.logger.Error("quicraft: the committed identity for this replica is not the one this host "+
					"presents; every peer will refuse its messages — restore the keypair, or remove and re-add the replica",
					"shard", shardID,
					"replica", cc.ReplicaID,
					"committed_identity", cc.NodeIdentity,
					"host_identity", own,
				)
			}
		case proto.RemoveNode:
			// Capture the removed member's address before its registry entry is dropped.
			removedAddr, addrErr := c.h.registry.Resolve(shardID, cc.ReplicaID)
			ns.nodesMu.Lock()
			delete(ns.nodes, cc.ReplicaID)
			ns.nodesMu.Unlock()
			c.h.registry.Remove(shardID, cc.ReplicaID)
			if addrErr == nil && removedAddr != "" {
				c.h.releaseRemovedMemberConn(shardID, cc.ReplicaID, removedAddr)
			}
		}
		// Monotonic: a snapshot install may already have set a higher index.
		advanceConfigChangeIndex(ns, entry.Index)
		c.h.logger.Info("membership changed",
			"shard", shardID,
			"replica", ns.replicaID,
			"type", proto.ConfigChangeTypeName(cc.Type),
			"target", cc.ReplicaID,
			"address", cc.Address,
		)
	}
}

// failShardOnCorruptConfig fails the shard for a committed config entry that does not decode; a nil node is a no-op.
func (c *hostEngineCallback) failShardOnCorruptConfig(shardID, index uint64, kind string, err error) {
	c.h.logger.Error("committed "+kind+" entry does not decode: failing the shard "+
		"rather than routing on a configuration this replica cannot read",
		"shard", shardID,
		"index", index,
		"error", err,
	)
	if node := c.h.engine.GetNode(shardID); node != nil {
		corrupt := &CorruptConfigEntryError{ShardID: shardID, Index: index, Kind: kind, Err: err}
		// The engine's one latch-and-report path: whichever failure wins the
		// latch fires both EventListener.OnShardFailed and this callback's
		// OnShardFailed, which drains the reads pending across the halt.
		c.h.engine.FailShard(node, corrupt)
	}
}

// registerBootstrapAddresses registers every EntryInitialMembership address and reports whether the entry decoded.
func (c *hostEngineCallback) registerBootstrapAddresses(ns *nodeState, shardID uint64, entry proto.Entry) bool {
	var ms proto.Membership
	if _, err := ms.UnmarshalFrom(entry.Cmd); err != nil {
		c.failShardOnCorruptConfig(shardID, entry.Index, "bootstrap membership", err)
		return false
	}
	groups := []map[uint64]string{ms.Addresses, ms.Observers, ms.Witnesses}
	ns.nodesMu.Lock()
	for _, group := range groups {
		for id, addr := range group {
			ns.nodes[id] = addr
		}
	}
	ns.nodesMu.Unlock()
	for _, group := range groups {
		for id, addr := range group {
			c.h.publishCommittedAddress(shardID, id, addr)
		}
	}
	return true
}

// OnReadyToRead records confirmed read indexes and completes reads, and their aliases, that lastApplied already covers.
func (c *hostEngineCallback) OnReadyToRead(shardID uint64, readyToRead []proto.ReadyToRead) {
	v, exists := c.h.nodes.Load(shardID)
	if !exists {
		return
	}
	ns, ok := v.(*nodeState)
	if !ok {
		c.h.logger.Error("OnReadyToRead: nodeState type assertion failed",
			"shard_id", shardID,
			"error", &TypeAssertionError{
				Context:      "OnReadyToRead nodeState lookup",
				ExpectedType: "*nodeState",
				ActualValue:  v,
			})
		return
	}
	// Observe latencies only after pendingReadsMu is released and each read is completed.
	var latencyBuf [8]time.Duration
	latencies := c.confirmReadyReads(ns, readyToRead, latencyBuf[:0])
	if c.h.metrics == nil {
		return
	}
	for _, d := range latencies {
		c.h.metrics.ObserveReadIndexLatency(shardID, d)
	}
}

// confirmReadyReads applies confirmations under pendingReadsMu and returns the completed reads' latencies.
func (c *hostEngineCallback) confirmReadyReads(ns *nodeState, readyToRead []proto.ReadyToRead, latencies []time.Duration) []time.Duration {
	ns.pendingReadsMu.Lock()
	defer ns.pendingReadsMu.Unlock()
	observe := c.h.metrics != nil
	complete := func(key uint64, pr *pendingRead, index uint64) {
		// Read before the release: the RequestState may be recycled after it.
		createdAt := pr.rs.createdAt
		pr.rs.complete(RequestResult{Value: index})
		delete(ns.pendingReads, key)
		pr.rs.releaseRef()
		c.h.completePendingOp()
		if observe && !createdAt.IsZero() {
			latencies = append(latencies, time.Since(createdAt))
		}
	}
	// Load lastApplied under pendingReadsMu to stay consistent with OnApplied's store.
	lastApplied := ns.lastApplied.Load()
	for _, rtr := range readyToRead {
		pr, ok := ns.pendingReads[rtr.Key]
		if !ok {
			continue
		}
		pr.readIndex = rtr.Index
		pr.confirmed = true
		if lastApplied >= rtr.Index {
			complete(rtr.Key, pr, rtr.Index)
			// Complete all alias keys that share this batch's ReadIndex.
			for _, aliasKey := range pr.aliases {
				if aliasPR, aliasOK := ns.pendingReads[aliasKey]; aliasOK {
					complete(aliasKey, aliasPR, rtr.Index)
				}
			}
			continue
		}
		// SM has not caught up yet. Mark all alias keys as confirmed
		// so OnApplied resolves them when lastApplied advances.
		for _, aliasKey := range pr.aliases {
			if aliasPR, aliasOK := ns.pendingReads[aliasKey]; aliasOK {
				aliasPR.readIndex = rtr.Index
				aliasPR.confirmed = true
			}
		}
	}
	return latencies
}

// OnReadIndexBatched registers aliasKeys to complete with batchKey's ReadIndex; the step worker calls it before submitting to raft.
func (c *hostEngineCallback) OnReadIndexBatched(shardID uint64, batchKey uint64, aliasKeys []uint64) {
	v, exists := c.h.nodes.Load(shardID)
	if !exists {
		return
	}
	ns, ok := v.(*nodeState)
	if !ok {
		c.h.logger.Error("OnReadIndexBatched: nodeState type assertion failed",
			"shard_id", shardID,
			"error", &TypeAssertionError{
				Context:      "OnReadIndexBatched nodeState lookup",
				ExpectedType: "*nodeState",
				ActualValue:  v,
			})
		return
	}
	ns.pendingReadsMu.Lock()
	batchPR, ok := ns.pendingReads[batchKey]
	if !ok {
		// The batch key is already gone, so its aliases can never complete: fail them now.
		for _, aliasKey := range aliasKeys {
			aliasPR, aliasOK := ns.pendingReads[aliasKey]
			if !aliasOK {
				continue
			}
			aliasPR.rs.complete(RequestResult{Err: ErrTimeout})
			delete(ns.pendingReads, aliasKey)
			aliasPR.rs.releaseRef()
			c.h.completePendingOp()
		}
		ns.pendingReadsMu.Unlock()
		return
	}
	// Append alias keys, allocating on the first batch for this key.
	if batchPR.aliases == nil {
		batchPR.aliases = make([]uint64, 0, len(aliasKeys))
	}
	batchPR.aliases = append(batchPR.aliases, aliasKeys...)
	ns.pendingReadsMu.Unlock()
}

// OnLeaderUpdated updates the Host's cached leader information for the
// shard. If the local replica lost leadership, all pending proposals
// are completed with a NotLeaderError so callers do not block forever.
func (c *hostEngineCallback) OnLeaderUpdated(shardID uint64, leaderID uint64, term uint64) {
	v, exists := c.h.nodes.Load(shardID)
	if !exists {
		c.h.updateNodeLeader(shardID, leaderID, term)
		return
	}
	ns, ok := v.(*nodeState)
	if !ok {
		c.h.logger.Error("OnLeaderUpdated: nodeState type assertion failed",
			"shard_id", shardID,
			"error", &TypeAssertionError{
				Context:      "OnLeaderUpdated nodeState lookup",
				ExpectedType: "*nodeState",
				ActualValue:  v,
			})
		return
	}

	// Capture previous leadership state and leader identity before updating.
	wasLeader := ns.isLeader.Load()
	prevLeaderID := ns.leaderID.Load()
	c.h.updateNodeLeader(shardID, leaderID, term)

	// Log leadership transitions.
	if leaderID == ns.replicaID && !wasLeader {
		c.h.logger.Info("became leader",
			"shard", shardID,
			"replica", ns.replicaID,
			"term", term,
		)
	}

	// Fire OnLeaderUpdated event listener.
	if el := c.h.cfg.EventListener; el != nil && el.OnLeaderUpdated != nil {
		el.OnLeaderUpdated(config.LeaderInfo{
			ShardID:   shardID,
			ReplicaID: ns.replicaID,
			Term:      term,
			LeaderID:  leaderID,
		})
	}

	// Fail pending proposals only if this replica lost leadership.
	if wasLeader && leaderID != ns.replicaID {
		if el := c.h.cfg.EventListener; el != nil && el.OnLeaderLost != nil {
			el.OnLeaderLost(config.EventShardInfo{
				ShardID:   shardID,
				ReplicaID: ns.replicaID,
				LeaderID:  leaderID,
				Term:      term,
			})
		}
		c.h.logger.Info("leader lost",
			"shard", shardID,
			"replica", ns.replicaID,
			"new_leader", leaderID,
			"term", term,
		)
		// Proposals handed to raft may still commit under the new leader, so they fail maybe-applied; an unsubmitted proposal gets the bare error.
		c.h.failLeaderChangeRequests(ns, &NotLeaderError{ShardID: shardID, LeaderID: leaderID})
	}

	// On a follower, fail proposals forwarded to a leader that has since changed.
	if !wasLeader && prevLeaderID != leaderID && leaderID != ns.replicaID {
		c.h.logger.Debug("follower detected leader change, failing forwarded proposals",
			"shard", shardID,
			"replica", ns.replicaID,
			"prev_leader", prevLeaderID,
			"new_leader", leaderID,
			"term", term,
		)
		// Proposals handed to raft may still commit under the new leader, so they fail maybe-applied; an unsubmitted proposal gets the bare error.
		c.h.failLeaderChangeRequests(ns, &NotLeaderError{ShardID: shardID, LeaderID: leaderID})
	}
}

// OnLeaderReady marks the shard's (leaderID, term) read-ready and fires the listener; witnesses are skipped.
func (c *hostEngineCallback) OnLeaderReady(shardID, replicaID, leaderID, term uint64) {
	v, exists := c.h.nodes.Load(shardID)
	if !exists {
		return
	}
	ns, ok := v.(*nodeState)
	if !ok {
		c.h.logger.Error("OnLeaderReady: nodeState type assertion failed",
			"shard_id", shardID,
			"error", &TypeAssertionError{
				Context:      "OnLeaderReady nodeState lookup",
				ExpectedType: "*nodeState",
				ActualValue:  v,
			})
		return
	}
	if ns.cfg.IsWitness || !ns.markLeaderReady(leaderID, term) {
		return
	}
	// A shard with no other member never receives an advertisement, so a new
	// leader retries the wire-version switch itself for the term. It proposes
	// off the dispatcher, which must not wait on its own shard's commit.
	if leaderID == replicaID && c.h.wireSwitchEnabled() && !c.h.closed.Load() {
		c.h.wireSwitchWG.Add(1)
		go c.h.retryWireSwitch(ns, term)
	}
	if el := c.h.cfg.EventListener; el != nil && el.OnLeaderReady != nil {
		el.OnLeaderReady(config.LeaderInfo{
			ShardID:   shardID,
			ReplicaID: replicaID,
			Term:      term,
			LeaderID:  leaderID,
		})
	}
}

// OnLogCompacted fires the log compaction events for a snapshot save that compacted, after OnSnapshotCompleted.
func (c *hostEngineCallback) OnLogCompacted(shardID, replicaID uint64, compaction engine.LogCompaction) {
	el := c.h.cfg.EventListener
	if el == nil || !compaction.Reader {
		return
	}
	info := config.EventShardInfo{ShardID: shardID, ReplicaID: replicaID, Index: compaction.Index}
	c.dispatchListener("OnLogCompacted", shardID, func() {
		if el.OnLogCompacted != nil {
			el.OnLogCompacted(info)
		}
		if compaction.LogDB && el.OnLogDBCompacted != nil {
			el.OnLogDBCompacted(info)
		}
	})
}

// dispatchListener runs user EventListener code on the engine's listener dispatcher, never inline on a snapshot worker.
func (c *hostEngineCallback) dispatchListener(event string, shardID uint64, fn func()) {
	if c.h.engine == nil {
		fn()
		return
	}
	if !c.h.engine.DispatchListener(fn) {
		// Dropped on a full queue; log which event a shard never delivered.
		c.h.logger.Error("listener notification dropped: the EventListener is not consuming",
			"event", event,
			"shard_id", shardID)
	}
}

// OnSnapshotCompleted reconciles membership after a recovery and fires the snapshot listeners.
func (c *hostEngineCallback) OnSnapshotCompleted(shardID uint64, index uint64, op engine.SnapshotOperation, err error) {
	v, exists := c.h.nodes.Load(shardID)
	if !exists {
		return
	}
	ns, ok := v.(*nodeState)
	if !ok {
		c.h.logger.Error("OnSnapshotCompleted: nodeState type assertion failed",
			"shard_id", shardID,
			"error", &TypeAssertionError{
				Context:      "OnSnapshotCompleted nodeState lookup",
				ExpectedType: "*nodeState",
				ActualValue:  v,
			})
		return
	}

	// Mirror the recovered snapshot membership into the host routing table.
	if err == nil && op == engine.SnapshotRecover {
		c.reconcileMembershipFromPeer(shardID, ns)
	}

	// Fire event listener callbacks — dispatched, never inline: see
	// dispatchListener.
	if el := c.h.cfg.EventListener; el != nil {
		replicaID := ns.replicaID
		c.dispatchListener("OnSnapshotCompleted", shardID, func() { fireSnapshotListeners(el, shardID, replicaID, index, op, err) })
	}
}

// OnSnapshotRequestCompleted resolves the snapshot request under requestKey with the index or translated error.
func (c *hostEngineCallback) OnSnapshotRequestCompleted(shardID, requestKey, index uint64, err error) {
	v, exists := c.h.nodes.Load(shardID)
	if !exists {
		return
	}
	ns, ok := v.(*nodeState)
	if !ok {
		c.h.logger.Error("OnSnapshotRequestCompleted: nodeState type assertion failed",
			"shard_id", shardID,
			"error", &TypeAssertionError{
				Context:      "OnSnapshotRequestCompleted nodeState lookup",
				ExpectedType: "*nodeState",
				ActualValue:  v,
			})
		return
	}
	ns.pendingSnapshotsMu.Lock()
	rs, present := ns.pendingSnapshots[requestKey]
	if present {
		delete(ns.pendingSnapshots, requestKey)
	}
	ns.pendingSnapshotsMu.Unlock()
	if !present {
		return
	}
	if err != nil {
		rs.complete(RequestResult{Err: translateSnapshotError(err)})
	} else {
		rs.complete(RequestResult{Value: index})
	}
	rs.releaseRef()
	c.h.completePendingOp()
}

// fireSnapshotListeners delivers a successful snapshot completion to the EventListener on the dispatcher.
func fireSnapshotListeners(el *config.EventListener, shardID, replicaID, index uint64, op engine.SnapshotOperation, err error) {
	if err != nil {
		return
	}
	switch op {
	case engine.SnapshotSave:
		if index == 0 {
			return
		}
		if el.OnSnapshotCreated != nil {
			el.OnSnapshotCreated(config.SnapshotInfo{
				ShardID:   shardID,
				ReplicaID: replicaID,
				Index:     index,
			})
		}
		if el.OnSnapshotCompacted != nil {
			el.OnSnapshotCompacted(config.SnapshotInfo{
				ShardID:   shardID,
				ReplicaID: replicaID,
				Index:     index,
			})
		}
	case engine.SnapshotRecover:
		if el.OnSnapshotRecovered != nil {
			el.OnSnapshotRecovered(config.SnapshotInfo{
				ShardID:   shardID,
				ReplicaID: replicaID,
			})
		}
	}
}

// reconcileMembershipFromPeer syncs nodeState.nodes and the registry with the peer's committed membership.
func (c *hostEngineCallback) reconcileMembershipFromPeer(shardID uint64, ns *nodeState) {
	engNode := c.h.engine.GetNode(shardID)
	if engNode == nil || engNode.Peer() == nil {
		return
	}

	pm := engNode.Peer().GetMembership()

	// Build the full member set from all membership categories.
	newNodes := make(map[uint64]string,
		len(pm.Addresses)+len(pm.Observers)+len(pm.Witnesses))
	for k, v := range pm.Addresses {
		newNodes[k] = v
	}
	for k, v := range pm.Observers {
		newNodes[k] = v
	}
	for k, v := range pm.Witnesses {
		newNodes[k] = v
	}

	ns.nodesMu.Lock()
	// Remove nodes that are no longer in the membership.
	for replicaID := range ns.nodes {
		if _, exists := newNodes[replicaID]; !exists {
			delete(ns.nodes, replicaID)
			c.h.registry.Remove(shardID, replicaID)
		}
	}
	// Add or update nodes from the snapshot membership.
	for replicaID, addr := range newNodes {
		ns.nodes[replicaID] = addr
		c.h.publishCommittedAddress(shardID, replicaID, addr)
	}
	ns.nodesMu.Unlock()

	// Seed configChangeIndex from the snapshot's membership, whose config entries were compacted away.
	advanceConfigChangeIndex(ns, pm.ConfigChangeID)
}

// advanceConfigChangeIndex raises ns.configChangeIndex to idx, never lowering it.
func advanceConfigChangeIndex(ns *nodeState, idx uint64) {
	for {
		cur := ns.configChangeIndex.Load()
		if idx <= cur {
			return
		}
		if ns.configChangeIndex.CompareAndSwap(cur, idx) {
			return
		}
	}
}

// OnCampaignLaunched is called by the step worker when the raft state
// machine starts an election. This is a cold path.
func (c *hostEngineCallback) OnCampaignLaunched(shardID, replicaID, term uint64) {
	if el := c.h.cfg.EventListener; el != nil && el.OnCampaignLaunched != nil {
		el.OnCampaignLaunched(config.EventShardInfo{
			ShardID:   shardID,
			ReplicaID: replicaID,
			Term:      term,
		})
	}
	c.h.logger.Info("campaign launched",
		"shard", shardID,
		"replica", replicaID,
		"term", term,
	)
}

// OnSnapshotRejected reports a refused, abandoned or failed snapshot to the listener through the dispatcher; err is the cause.
func (c *hostEngineCallback) OnSnapshotRejected(shardID, replicaID uint64, err error) {
	if el := c.h.cfg.EventListener; el != nil && el.OnSnapshotRejected != nil {
		info := config.SnapshotInfo{ShardID: shardID, ReplicaID: replicaID, Err: err}
		c.dispatchListener("OnSnapshotRejected", shardID, func() { el.OnSnapshotRejected(info) })
	}
	c.h.logger.Info("snapshot rejected",
		"shard", shardID,
		"replica", replicaID,
		"error", err,
	)
}

// OnCommitted notifies pending proposals once their entries are WAL-durable, before apply.
func (c *hostEngineCallback) OnCommitted(shardID uint64, entries []proto.Entry) {
	v, exists := c.h.nodes.Load(shardID)
	if !exists {
		return
	}
	ns, ok := v.(*nodeState)
	if !ok {
		c.h.logger.Error("OnCommitted: nodeState type assertion failed",
			"shard_id", shardID,
			"error", &TypeAssertionError{
				Context:      "OnCommitted nodeState lookup",
				ExpectedType: "*nodeState",
				ActualValue:  v,
			})
		return
	}
	for _, entry := range entries {
		if entry.Key == 0 {
			continue
		}
		v, ok := ns.pendingProposals.Load(entry.Key)
		if ok {
			pp, ppOK := v.(*pendingProposal)
			if ppOK && !pp.owns(entry) {
				// An older entry under this key is not this proposal's commit; see pendingProposal.floor.
				continue
			}
			if !ppOK {
				c.h.logger.Error("OnCommitted: pendingProposal type assertion failed",
					"shard_id", shardID,
					"entry_key", entry.Key,
					"error", &TypeAssertionError{
						Context:      "OnCommitted pendingProposal lookup",
						ExpectedType: "*pendingProposal",
						ActualValue:  v,
					})
				continue
			}
			// Leave the entry for OnApplied, and notify under a held reference so a concurrent claim cannot recycle it.
			if !pp.rs.tryAcquireRef() {
				continue
			}
			if v2, still := ns.pendingProposals.Load(entry.Key); !still || v2 != v {
				pp.rs.releaseRef()
				continue
			}
			pp.rs.commitNotify(RequestResult{Value: 0})
			pp.rs.releaseRef()
		}
	}
}

// OnProposalFailed fails proposals that reached raft but failed to persist, commit or apply, reporting them maybe-applied.
func (c *hostEngineCallback) OnProposalFailed(shardID uint64, entries []proto.Entry, err error) {
	c.failProposals(shardID, entries, err, true)
}

// OnProposalRejected completes pending proposals raft refused before
// appending them (no known leader, a transfer in progress, a config change
// already pending). None of them can ever commit, so the refusal is definite.
func (c *hostEngineCallback) OnProposalRejected(shardID uint64, entries []proto.Entry, err error) {
	c.failProposals(shardID, entries, err, false)
}

// failProposals completes the pending proposals for entries with the translated err.
func (c *hostEngineCallback) failProposals(shardID uint64, entries []proto.Entry, err error, handedOff bool) {
	v, exists := c.h.nodes.Load(shardID)
	if !exists {
		return
	}
	ns, ok := v.(*nodeState)
	if !ok {
		c.h.logger.Error("OnProposalFailed: nodeState type assertion failed",
			"shard_id", shardID,
			"error", &TypeAssertionError{
				Context:      "OnProposalFailed nodeState lookup",
				ExpectedType: "*nodeState",
				ActualValue:  v,
			})
		return
	}

	// Convert internal raft errors to public API errors.
	apiErr := c.h.translateProposalError(ns, err)

	for _, entry := range entries {
		v, ok := ns.pendingProposals.LoadAndDelete(entry.Key)
		if !ok {
			continue
		}
		pp, ppOK := v.(*pendingProposal)
		if !ppOK {
			c.h.logger.Error("OnProposalFailed: pendingProposal type assertion failed",
				"shard_id", shardID,
				"entry_key", entry.Key,
				"error", &TypeAssertionError{
					Context:      "OnProposalFailed pendingProposal lookup",
					ExpectedType: "*pendingProposal",
					ActualValue:  v,
				})
			continue
		}
		c.h.failClaimedProposal(pp, apiErr, handedOff)
	}
}

// proposalOutcomeUnknown wraps a post-handoff failure in a MaybeAppliedError once; it runs no user code.
func proposalOutcomeUnknown(err error) error {
	// A type assertion, not errors.As, so no user Unwrap or As code runs here.
	if _, ok := err.(*MaybeAppliedError); ok {
		return err
	}
	return &MaybeAppliedError{Err: err}
}

// failClaimedProposal completes the claimed pp with err, classified maybe-applied when handedOff.
func (h *Host) failClaimedProposal(pp *pendingProposal, err error, handedOff bool) {
	if handedOff {
		err = proposalOutcomeUnknown(err)
	}
	pp.failed()
	pp.rs.complete(RequestResult{Err: err})
	pp.rs.releaseRef()
	h.completePendingOp()
}

// OnReadIndexFailed completes the pending reads, batch and alias keys, whose ReadIndex submission failed.
func (c *hostEngineCallback) OnReadIndexFailed(shardID uint64, keys []uint64, err error) {
	v, exists := c.h.nodes.Load(shardID)
	if !exists {
		return
	}
	ns, ok := v.(*nodeState)
	if !ok {
		c.h.logger.Error("OnReadIndexFailed: nodeState type assertion failed",
			"shard_id", shardID,
			"error", &TypeAssertionError{
				Context:      "OnReadIndexFailed nodeState lookup",
				ExpectedType: "*nodeState",
				ActualValue:  v,
			})
		return
	}

	// Convert internal raft errors to public API errors.
	apiErr := c.h.translateProposalError(ns, err)

	ns.pendingReadsMu.Lock()
	// fail completes key's read with apiErr and returns its batch aliases,
	// read before the release recycles the RequestState.
	fail := func(key uint64) []uint64 {
		pr, ok := ns.pendingReads[key]
		if !ok {
			return nil
		}
		aliases := pr.aliases
		pr.rs.complete(RequestResult{Err: apiErr})
		delete(ns.pendingReads, key)
		pr.rs.releaseRef()
		c.h.completePendingOp()
		return aliases
	}
	for _, key := range keys {
		// A batch key's aliases fail with it.
		for _, aliasKey := range fail(key) {
			fail(aliasKey)
		}
	}
	ns.pendingReadsMu.Unlock()
}

// OnShardFailed fails every pending proposal (maybe-applied) and read (bare ErrShardFailed) once the shard's failure latch trips.
func (c *hostEngineCallback) OnShardFailed(shardID uint64, err error) {
	v, exists := c.h.nodes.Load(shardID)
	if !exists {
		return
	}
	ns, ok := v.(*nodeState)
	if !ok {
		c.h.logger.Error("OnShardFailed: nodeState type assertion failed",
			"shard_id", shardID,
			"error", &TypeAssertionError{
				Context:      "OnShardFailed nodeState lookup",
				ExpectedType: "*nodeState",
				ActualValue:  v,
			})
		return
	}

	// Pending proposals may already be committed, so they fail maybe-applied; reads get the bare sentinel.
	c.h.failPendingClientRequests(ns, ErrShardFailed)

	// Wake leader-executed requests so they return ErrShardFailed.
	ns.signalLeaderChange()

	// Log the halt once, after every pending request has failed.
	replicaID, logger := ns.replicaID, c.h.logger
	supervise.LogContained(func() {
		logger.Error("shard halted: failure latch tripped, refusing reads and proposals until the shard is reloaded",
			"shard_id", shardID,
			"replica_id", replicaID,
			"error", err,
		)
	})
}

// translateProposalError maps internal raft errors to public API errors.
func (h *Host) translateProposalError(ns *nodeState, err error) error {
	var nle *raft.NotLeaderError
	if errors.As(err, &nle) {
		return h.buildNotLeaderError(ns)
	}
	if errors.Is(err, raft.ErrReadIndexOverloaded) {
		return ErrSystemBusy
	}
	if errors.Is(err, raft.ErrReadIndexNotReady) {
		return ErrReadIndexNotReady
	}
	if errors.Is(err, raft.ErrDecompressionBomb) {
		// An entry whose snappy header exceeds MaxEntrySize maps to the public ErrDecompressionBomb.
		return ErrDecompressionBomb
	}
	// Shard-failure errors are already public and pass through untranslated.
	if errors.Is(err, engine.ErrStopped) {
		// A proposal batch the step worker drained but could not hand to
		// raft because its node stopped first: definite, and the engine's
		// sentinel is internal, so it is mapped to the public one.
		return ErrStopped
	}
	if errors.Is(err, raft.ErrPendingConfigChange) {
		// A second in-flight config change (thesis §4.1) maps to a public, retryable error.
		return ErrRejected
	}
	var stale *raft.StaleConfigChangeError
	if errors.As(err, &stale) {
		// A bound add whose ConfigChangeID is stale: re-read the membership and retry.
		var shardID uint64
		if ns != nil {
			shardID = ns.shardID
		}
		return &StaleConfigChangeError{ShardID: shardID, Expected: stale.Expected, Got: stale.Got}
	}
	if errors.Is(err, raft.ErrBoundConfigChangeNotLeader) {
		// A bound add this replica no longer leads for: re-read the membership and retry on the leader.
		var shardID uint64
		if ns != nil {
			shardID = ns.shardID
		}
		return fmt.Errorf("%w: shard %d: bound config change reached a replica that is no longer the leader",
			ErrStaleConfigChange, shardID)
	}
	return err
}

// translateSnapshotSubmitError maps the snapshot pool's refusal to accept a request to a public error.
func translateSnapshotSubmitError(err error) error {
	var poolBusy *engine.SnapshotPoolBusyError
	if errors.As(err, &poolBusy) {
		return ErrBusy
	}
	return ErrSnapshotInProgress
}

// translateSnapshotError maps a snapshot worker's error to a public error identity.
func translateSnapshotError(err error) error {
	if err == nil {
		return nil
	}
	if errors.Is(err, engine.ErrStopped) {
		return ErrStopped
	}
	if errors.Is(err, engine.ErrShardFailed) {
		return ErrShardFailed
	}
	if errors.Is(err, engine.ErrSnapshotCorrupt) {
		return ErrSnapshotCorrupt
	}
	if errors.Is(err, engine.ErrSnapshotSizeMismatch) {
		return ErrSnapshotSizeMismatch
	}
	if errors.Is(err, engine.ErrSnapshotMembershipAhead) {
		// A transient frontier mismatch maps to ErrAborted, which is temporary.
		return ErrAborted
	}
	var tooLarge *engine.SnapshotTooLargeError
	if errors.As(err, &tooLarge) {
		return &SnapshotTooLargeError{ShardID: tooLarge.ShardID, MaxSize: tooLarge.Limit}
	}
	var snapErr *engine.SnapshotError
	if errors.As(err, &snapErr) {
		return &SnapshotFailedError{
			ShardID:   snapErr.ShardID,
			ReplicaID: snapErr.ReplicaID,
			Recovery:  !snapErr.Save,
			Err:       snapErr.Err,
		}
	}
	return err
}

// Compile-time assertion that hostEngineCallback implements Callback.
var _ engine.Callback = (*hostEngineCallback)(nil)

// Host is the main entry point for the QuicRaft multi-group Raft library.
// One Host per process. It manages shard lifecycle, proposal routing,
// read operations, session management, and cluster membership.
type Host struct {
	cfg config.HostConfig
	// clock measures the leaderless windows (nodeState.leaderlessSince); nil
	// is the system clock. Tests swap it to step the wall clock.
	clock         leaderlessClock
	logger        *slog.Logger
	logdb         logdb.LogDB
	barrier       Barrier
	shardBarriers func(shardID uint64) Barrier // per-shard at-rest barrier resolver; nil when WithShardBarriers unused
	// plaintextHostRoute is set when WithoutEncryption declared the host-wide
	// route plaintext, so a shard shardBarriers resolves to nil takes that
	// route rather than failing with ErrShardBarrierUnresolved.
	plaintextHostRoute bool
	// shardBarriersGiven records every barrier shardBarriers has returned, so
	// Close seals them. Nil when WithShardBarriers is unused.
	shardBarriersGiven *givenBarriers
	// peerAdmitMu serializes peer admission with ReloadMTLS; admittingPeers counts admissions not yet published.
	peerAdmitMu    sync.Mutex
	admittingPeers map[string]int
	// barrierFallbackWarned records the shards already warned about falling
	// back from an unresolved per-shard barrier to the host-wide one, so the
	// warning fires once per shard instead of once per snapshot operation.
	barrierFallbackWarned sync.Map // map[uint64]struct{}
	metrics               MetricsCollector
	engine                *engine.Engine
	registry              *registry.Registry
	sender                engine.MessageSender
	transport             *transport.QUICTransport // nil when transport is disabled
	// dirLock is the exclusive lock on NodeHostDir (hostLockFileName),
	// held from NewHost until Close.
	dirLock     *fsutil.DirLock
	nodes       sync.Map     // map[uint64]*nodeState — lock-free reads for StaleRead/QueryLocalNode
	activeNodes atomic.Int64 // tracks the number of loaded shards (replaces len(h.nodes))
	closed      atomic.Bool  // shutdown admitted: set at the start of Close
	// closeDone is set once Close's shutdown work completes.
	closeDone atomic.Bool
	// barriersSealed is set once every barrier the host was given is sealed.
	barriersSealed atomic.Bool
	draining       atomic.Bool
	diskFull       *atomic.Bool // set by disk full callback in buildWALOptions
	// diskGate is the proposal-side disk monitor (newProposalDiskGate); nil
	// without a MaxWALDiskSize or with a caller-supplied LogDB.
	diskGate *waldb.DiskMonitor
	id       string
	stopOnce sync.Once
	// closeErr is the first Close's outcome, returned by every Close.
	closeErr    error
	pendingOps  atomic.Int64  // tracks outstanding pending operations for drain
	keyRotStopC chan struct{} // closed to stop the key rotation goroutine
	keyRotDone  chan struct{} // closed when the key rotation goroutine exits
	sweepStopC  chan struct{} // closed to stop the pending proposal sweeper goroutine
	sweepDone   chan struct{} // closed when the sweeper goroutine exits
	drainC      chan struct{} // closed when pendingOps reaches 0 during drain
	drainCMu    sync.Mutex    // protects drainC creation
	// admitMu orders admissions against Drain so each is counted or refused.
	admitMu sync.RWMutex
	// drainOnce runs one Drain attempt and joins every other caller to its outcome.
	drainOnce sync.Once
	drainErr  error
	// fsys routes snapshot persistence I/O. nil means the real filesystem;
	// only tests set it (withSnapshotFS), so the injection never reaches the
	// public API.
	fsys snapshotFS

	// wire is this host's wire window.
	wire transport.WireWindow
	// wireSwitchWG counts the retryWireSwitch goroutines; Close joins them
	// once the engine, the only thing that starts one, has stopped.
	wireSwitchWG sync.WaitGroup
	// wireSwitchFault is hostOptions.wireSwitchFault.
	wireSwitchFault func() error

	// joiningShards holds join=true shards still waiting for their bootstrap snapshot.
	joiningShards sync.Map

	// anchors holds each loaded shard's operator anchor: replica ID to node identity from Config.MemberIdentities.
	anchors sync.Map

	// importLocks serializes ImportSnapshot per shard; a record exists only while held or awaited.
	importLocks shardLocks

	// startingShards holds shards inside StartShard, so a concurrent start reports ErrShardNotReady.
	startingShards sync.Map

	// leaderExec holds the leader-executed request handlers, the requests
	// this host has in flight to a remote leader, and the bound on the
	// requests it is executing for remote callers. See ExecuteOnLeader.
	leaderExec *leaderExecRouter
}

// acquireImportLock takes the shard's import lock, waiting until it is free or ctx is done; the returned function releases it.
func (h *Host) acquireImportLock(ctx context.Context, shardID uint64) (func(), error) {
	return h.importLocks.acquire(ctx, shardID)
}

// tryAcquireImportLock takes the shard's import lock if free; the release is nil when it was not taken.
func (h *Host) tryAcquireImportLock(shardID uint64) (func(), bool) {
	return h.importLocks.tryAcquire(shardID)
}

// lockStoppedShardForImport takes the shard's import lock and re-checks the shard is still stopped.
func (h *Host) lockStoppedShardForImport(ctx context.Context, shardID, replicaID uint64) (func(), error) {
	release, err := h.acquireImportLock(ctx, shardID)
	if err != nil {
		return nil, err
	}
	lockedReplicaID, err := h.importPreflight(ctx, shardID)
	if err != nil {
		release()
		return nil, err
	}
	// Refuse if the shard was re-bootstrapped under another replica since the preflight.
	if lockedReplicaID != replicaID {
		release()
		return nil, ErrInvalidOperation
	}
	return release, nil
}

// markShardJoining records that a shard is in its join bootstrap window.
func (h *Host) markShardJoining(shardID uint64) {
	h.joiningShards.Store(shardID, struct{}{})
}

// clearShardJoining ends a shard's join bootstrap window. It is idempotent.
func (h *Host) clearShardJoining(shardID uint64) {
	h.joiningShards.Delete(shardID)
}

// checkRecoveredIdentities checks a restart against the recovered committed identity bindings.
func (h *Host) checkRecoveredIdentities(cfg config.Config, m proto.Membership) error {
	refuse := func(id uint64, reason string) error {
		return &InvalidMemberError{ShardID: cfg.ShardID, ReplicaID: id, Reason: reason}
	}
	if committed := m.Identities[cfg.ReplicaID]; committed != "" && committed != h.NodeIdentity() {
		return refuse(cfg.ReplicaID, fmt.Sprintf("the committed identity for this replica is %s but "+
			"this host presents %s; every peer would refuse it — restore the keypair, or remove "+
			"and re-add the replica", committed, h.NodeIdentity()))
	}
	holder := make(map[string]uint64, len(m.Identities))
	for id, identity := range m.Identities {
		holder[identity] = id
	}
	removed := make(map[string]uint64, len(m.RemovedIdentities))
	for id, identity := range m.RemovedIdentities {
		removed[identity] = id
	}
	for _, id := range slices.Sorted(maps.Keys(cfg.MemberIdentities)) {
		identity := cfg.MemberIdentities[id]
		if committed := m.Identities[id]; committed != "" && committed != identity {
			return refuse(id, fmt.Sprintf("Config.MemberIdentities names %s for a replica whose "+
				"committed identity is %s", identity, committed))
		}
		if other, ok := holder[identity]; ok && other != id {
			return refuse(id, fmt.Sprintf("Config.MemberIdentities binds the committed identity "+
				"of replica %d to replica %d; one keypair cannot be two members", other, id))
		}
		if r, ok := removed[identity]; ok {
			logger := h.logger
			supervise.LogContained(func() {
				logger.Warn("quicraft: Config.MemberIdentities names an identity this shard has removed; "+
					"the entry is inert (the removal deny-list is consulted first) and can be dropped",
					"shard", cfg.ShardID,
					"replica", id,
					"removed_replica", r,
				)
			})
		}
	}
	return nil
}

// anchorsRemoteReplica reports whether cfg.MemberIdentities names a replica
// other than cfg.ReplicaID — one a first-start joiner could take its
// bootstrap from. Its own entry vouches for no sender.
func anchorsRemoteReplica(cfg config.Config) bool {
	return namesRemoteReplica(cfg.MemberIdentities, cfg.ReplicaID)
}

// namesRemoteReplica reports whether a replica-keyed map names a replica
// other than self.
func namesRemoteReplica(byReplica map[uint64]string, self uint64) bool {
	for id := range byReplica {
		if id != self {
			return true
		}
	}
	return false
}

// isShardJoining reports whether a shard is still in its join bootstrap
// window: started with join=true and not yet given a snapshot.
func (h *Host) isShardJoining(shardID uint64) bool {
	_, ok := h.joiningShards.Load(shardID)
	return ok
}

// validateMemberIdentities checks the form of Config.MemberIdentities on a host that requires identities.
func (h *Host) validateMemberIdentities(cfg config.Config) error {
	for replicaID, identity := range cfg.MemberIdentities {
		if replicaID == 0 {
			// Replica 0 is reserved and never named.
			return &InvalidMemberError{
				ShardID: cfg.ShardID,
				Reason:  "Config.MemberIdentities names replica ID 0, which is reserved",
			}
		}
		if identity == "" {
			return &InvalidMemberError{
				ShardID:   cfg.ShardID,
				ReplicaID: replicaID,
				Reason:    "Config.MemberIdentities names the replica with an empty node identity",
			}
		}
	}
	// This host's own entry must be its identity.
	if self, ok := cfg.MemberIdentities[cfg.ReplicaID]; ok {
		if own := h.NodeIdentity(); own != "" && self != own {
			return &InvalidMemberError{
				ShardID:   cfg.ShardID,
				ReplicaID: cfg.ReplicaID,
				Reason: "Config.MemberIdentities names this replica with an identity that " +
					"is not this host's (Host.NodeIdentity); peers would refuse every " +
					"message it sends",
			}
		}
	}
	// One identity may not be bound to two replicas.
	if a, b, dup := duplicateIdentity(cfg.MemberIdentities); dup {
		return &InvalidMemberError{
			ShardID:   cfg.ShardID,
			ReplicaID: b,
			Reason: fmt.Sprintf("node identity is already held by replica %d; one keypair "+
				"cannot be two members of a shard", a),
		}
	}
	return nil
}

// requireFoundersIdentified requires an identity for every founder at bootstrap.
func requireFoundersIdentified(cfg config.Config, members map[uint64]string) error {
	for _, replicaID := range slices.Sorted(maps.Keys(members)) {
		if cfg.MemberIdentities[replicaID] == "" {
			return &InvalidMemberError{
				ShardID:   cfg.ShardID,
				ReplicaID: replicaID,
				Address:   members[replicaID],
				Reason: "no node identity in Config.MemberIdentities; a member is admitted " +
					"with its identity or not at all on a host with a transport",
			}
		}
	}
	return nil
}

// duplicateIdentity reports two replica IDs (lower first) that identities
// binds to one identity, and whether there are any. Deterministic — the
// lowest-numbered pair — so a refusal reads the same on every run.
func duplicateIdentity(identities map[uint64]string) (uint64, uint64, bool) {
	ids := make([]uint64, 0, len(identities))
	for id, identity := range identities {
		if identity != "" {
			ids = append(ids, id)
		}
	}
	slices.Sort(ids)
	seen := make(map[string]uint64, len(ids))
	for _, id := range ids {
		if first, dup := seen[identities[id]]; dup {
			return first, id, true
		}
		seen[identities[id]] = id
	}
	return 0, 0, false
}

// anchorIdentity returns the identity the operator anchor binds replicaID to, and whether it names it.
func (h *Host) anchorIdentity(shardID, replicaID uint64) (string, bool) {
	v, ok := h.anchors.Load(shardID)
	if !ok {
		return "", false
	}
	anchor, _ := v.(map[uint64]string)
	identity, ok := anchor[replicaID]
	if !ok || identity == "" {
		return "", false
	}
	return identity, true
}

// setShardAnchor records the operator anchor for a shard being started; see
// anchorIdentity. An empty map records nothing, so a shard without one (a
// host without a transport) answers unknown for every replica.
func (h *Host) setShardAnchor(shardID uint64, identities map[uint64]string) {
	if anchor := copyIdentityMap(identities); anchor != nil {
		h.anchors.Store(shardID, anchor)
	}
}

// identitiesOf restricts identities to the replicas in members.
func identitiesOf(members, identities map[uint64]string) map[uint64]string {
	var out map[uint64]string
	for id := range members {
		if identity := identities[id]; identity != "" {
			if out == nil {
				out = make(map[uint64]string, len(members))
			}
			out[id] = identity
		}
	}
	return out
}

// IsShardJoining reports whether shardID was started with join=true and has not yet received a snapshot.
func (h *Host) IsShardJoining(shardID uint64) bool {
	return h.isShardJoining(shardID)
}

// NewHost validates cfg, loads or generates the host ID and starts every subsystem.
//
//nolint:gocyclo // subsystem initialization requires many sequential validation and setup steps
func NewHost(cfg config.HostConfig, opts ...HostOption) (*Host, error) {
	options := &hostOptions{}
	for _, opt := range opts {
		opt(options)
	}

	// At-rest encryption must be stated, and is checked before anything touches the disk.
	encrypted := options.barrier != nil || options.shardBarriers != nil
	switch {
	case options.withoutEncryption && options.barrier != nil:
		return nil, ErrAtRestEncryptionConflict
	case !options.withoutEncryption && !encrypted:
		return nil, ErrAtRestEncryptionUnspecified
	}
	plaintextHostRoute := options.withoutEncryption && options.shardBarriers != nil

	// Record every barrier the per-shard resolver hands out, so Close can seal
	// them. The wrapped resolver replaces the caller's before any consumer is
	// wired, so every path below resolves through it.
	var shardBarriersGiven *givenBarriers
	if options.shardBarriers != nil {
		shardBarriersGiven = &givenBarriers{}
		options.shardBarriers = shardBarriersGiven.recording(options.shardBarriers)
	}

	// Propagate transport disabled state to the config before validation
	// so that MTLSConfig is only required when transport is enabled.
	if options.disableTransport {
		cfg.TransportConfig.TransportDisabled = true
	}
	// Honour TransportDisabled when set directly, since validation then skips the mTLS check.
	transportDisabled := options.disableTransport || cfg.TransportConfig.TransportDisabled

	cfg.SetDefaults()
	if err := cfg.Validate(); err != nil {
		return nil, err
	}

	// Use the WithLogger logger, else one built from LogConfig; never the global slog default.
	var logger *slog.Logger
	if options.logger != nil {
		logger = options.logger
	} else {
		logger = config.NewLogger(cfg.LogConfig)
	}

	// Create the host directories and fsync the chain so they survive a crash.
	if err := fsutil.MkdirAllSynced(cfg.NodeHostDir, 0o750); err != nil {
		return nil, &HostInitError{Field: "NodeHostDir", Err: err}
	}
	if err := fsutil.MkdirAllSynced(cfg.WALDir, 0o750); err != nil {
		return nil, &HostInitError{Field: "WALDir", Err: err}
	}

	// One live host and one deployment per NodeHostDir, settled before the log opens.
	dirLock, err := fsutil.LockDir(cfg.NodeHostDir, hostLockFileName)
	if err != nil {
		return nil, &HostInitError{Field: "NodeHostDir", Err: err}
	}
	// Every failure from here releases what this call opened, so a refused
	// start never leaves its directories held.
	var createdWAL *waldb.DB
	started := false
	defer func() {
		if started {
			return
		}
		if createdWAL != nil {
			if closeErr := createdWAL.Close(); closeErr != nil {
				logger.Debug("closing the WAL after a failed host start failed", "error", closeErr)
			}
		}
		if closeErr := dirLock.Close(); closeErr != nil {
			logger.Debug("releasing NodeHostDir after a failed host start failed", "error", closeErr)
		}
	}()
	if err := checkHostIdentity(logger, cfg.NodeHostDir, hostIdentity{
		deploymentID: cfg.DeploymentID,
		raftAddress:  cfg.RaftAddress,
	}); err != nil {
		return nil, err
	}

	// Shared disk full flag set by the WAL's disk monitor callback and
	// read by Host.HealthCheck(). Allocated before the WAL is opened so
	// the callback closure captures the same atomic as the Host struct.
	var diskFullFlag atomic.Bool
	// diskGate refuses proposals below the WAL's own full threshold; built
	// with the default WAL only (newProposalDiskGate).
	var diskGate *waldb.DiskMonitor

	// Per-shard barriers without a host-wide barrier require a LogDB that is encrypted at rest.
	if options.shardBarriers != nil && options.barrier == nil {
		if err := requireEncryptedLogDB(options.logdb, nil, plaintextHostRoute, ErrShardBarriersPlaintextWAL); err != nil {
			return nil, err
		}
	}
	// WithBarrier with a caller-supplied LogDB requires that LogDB to encrypt under the same barrier.
	if options.barrier != nil && options.logdb != nil {
		if err := requireEncryptedLogDB(options.logdb, options.barrier, false, ErrBarrierPlaintextLogDB); err != nil {
			return nil, err
		}
	}

	// Create the default WAL-based LogDB when no LogDB was provided.
	// This ensures all production hosts persist raft state to disk.
	if options.logdb == nil {
		// The disk callbacks are built before the WAL they monitor is open,
		// so they reach it through walDB, published once Open returns.
		var walDB atomic.Pointer[waldb.DB]
		walShards := func() []uint64 {
			if db := walDB.Load(); db != nil {
				return walShardIDs(db)
			}
			return nil
		}
		gateMon, walMon := hostDiskMonitorOptions(cfg, &diskFullFlag, walShards)
		walOpts := walDBOptions(cfg, walMon)
		diskGate = newProposalDiskGate(cfg, gateMon)
		// Wire barrier encryption into the WAL when a barrier is configured.
		if options.barrier != nil {
			walOpts = append(walOpts, waldb.WithBarrier(options.barrier))
			// Per-shard barriers with the default WAL encrypt the log under the host-wide key only.
			if options.shardBarriers != nil {
				logger.Warn("per-shard barriers do not cover the default WAL; " +
					"log entries are encrypted under the host-wide barrier, not " +
					"per-shard keys. Supply WithLogDB with a multiplexing LogDB " +
					"for per-shard WAL encryption.")
			}
		}
		// Wire LogDB metrics when a MetricsCollector is provided.
		if options.metrics != nil {
			walOpts = append(walOpts, waldb.WithMetrics(options.metrics))
		}
		db, err := waldb.Open(cfg.WALDir, walOpts...)
		if err != nil {
			return nil, &HostInitError{Field: "logdb", Err: err}
		}
		walDB.Store(db)
		createdWAL = db
		options.logdb = db
	}

	// Generate or load host ID.
	hostID, err := loadOrGenerateHostID(logger, cfg.NodeHostDir, cfg.HostID)
	if err != nil {
		return nil, err
	}

	// Create the registry for address resolution.
	reg := registry.NewRegistry()

	// Determine the message sender. When transport is disabled (unit
	// testing), use a no-op sender that silently drops messages. When
	// enabled, the QUIC transport itself satisfies MessageSender.
	var sender engine.MessageSender
	var tr *transport.QUICTransport
	var msgHandler *hostMessageHandler

	wire := options.wire
	if wire == (transport.WireWindow{}) {
		wire = transport.DefaultWireWindow()
	}

	if !transportDisabled {
		trCfg := buildTransportConfig(cfg)
		trCfg.Wire = wire
		// Wire transport metrics when a MetricsCollector is provided. The
		// transport's internal TransportMetrics interface matches the
		// public MetricsCollector's TransportMetrics methods.
		if options.metrics != nil {
			trCfg.Metrics = options.metrics
		}

		// The handler's engine and host are wired below; the transport starts last.
		msgHandler = &hostMessageHandler{}
		// Outbound traffic is held to the identity membership binds to the
		// replica it is for, as inbound senders are (see replicaIdentity).
		trCfg.ReplicaIdentity = msgHandler.replicaIdentity

		var trErr error
		tr, trErr = transport.NewQUICTransport(trCfg, msgHandler, reg)
		if trErr != nil {
			return nil, &HostInitError{Field: "transport", Err: trErr}
		}

		// transportSender streams snapshots, decrypting through the per-shard or host-wide barrier.
		sender = &transportSender{
			transport:     tr,
			barrier:       options.barrier,
			shardBarriers: options.shardBarriers,
			logger:        logger,
			fsys:          options.fsys,
		}
	} else {
		sender = &noopSender{}
	}

	// The engine takes the collector's RaftMetrics methods.
	var raftMetrics engine.RaftMetrics
	if options.metrics != nil {
		raftMetrics = options.metrics
	}
	eng := engine.NewEngine(cfg, options.logdb, sender, options.barrier, nil, raftMetrics)

	// A sealed-barrier write refusal holds the shard until unseal instead of failing it.
	eng.SetSealHold(engine.SealHold{
		IsSealed:  isSealedLogWrite,
		UnsealedC: logWriteUnsealedC(options.barrier, options.shardBarriers),
	})

	// Route snapshot pool encryption through per-shard barriers when configured.
	if options.shardBarriers != nil {
		shardFn := options.shardBarriers
		eng.SetShardBarriers(func(shardID uint64) engine.SnapshotBarrier {
			b := shardFn(shardID)
			if b == nil {
				return nil
			}
			return b
		})
		if plaintextHostRoute {
			eng.SetPlaintextHostRoute()
		}
	}

	// Wire the engine into the message handler so inbound messages
	// from the transport are delivered to the correct shard nodes.
	if msgHandler != nil {
		msgHandler.engine = eng
	}

	// Wire the engine into the transportSender so snapshot status
	// feedback can be delivered to the raft layer after transfers.
	if ts, ok := sender.(*transportSender); ok {
		ts.engine = eng
	}

	// The barrier tracks in-flight snapshot epochs so a purge cannot remove their DEKs.
	if tr != nil {
		// Outbound traffic is stamped with each shard's committed wire
		// version; a shard not loaded here sends the previous version.
		tr.SetWireVersionLookup(func(shardID uint64) uint64 {
			if n := eng.GetNode(shardID); n != nil {
				return n.Peer().WireVersion()
			}
			return 0
		})
		// Receiver-side snapshot rejections reach OnSnapshotRejected through the
		// engine's listener dispatcher, as the snapshot pool's do, so the event
		// has one delivery class whatever produced it.
		tr.SetListenerDispatcher(eng.DispatchListener)
		if options.shardBarriers != nil {
			shardFn := options.shardBarriers
			tr.SetShardEpochTrackers(func(shardID uint64) transport.EpochTracker {
				b := shardFn(shardID)
				if b == nil {
					if options.barrier != nil {
						return options.barrier
					}
					return nil
				}
				return b
			})
		} else if options.barrier != nil {
			tr.SetEpochTracker(options.barrier)
		}
	}

	h := &Host{
		cfg:                cfg,
		logger:             logger,
		logdb:              options.logdb,
		barrier:            options.barrier,
		shardBarriers:      options.shardBarriers,
		plaintextHostRoute: plaintextHostRoute,
		metrics:            options.metrics,
		shardBarriersGiven: shardBarriersGiven,
		engine:             eng,
		registry:           reg,
		sender:             sender,
		transport:          tr,
		diskFull:           &diskFullFlag,
		diskGate:           diskGate,
		id:                 hostID,
		keyRotStopC:        make(chan struct{}),
		keyRotDone:         make(chan struct{}),
		sweepStopC:         make(chan struct{}),
		sweepDone:          make(chan struct{}),
		fsys:               options.fsys,
		wire:               wire,
		wireSwitchFault:    options.wireSwitchFault,
		leaderExec:         newLeaderExecRouter(),
		dirLock:            dirLock,
	}

	// Wire the host into the message handler so HandleSnapshot can
	// access host configuration (snapshot dirs, logdb) for received
	// snapshot processing.
	if msgHandler != nil {
		msgHandler.host = h
	}

	// Wire the engine callback so applied entries and leader changes
	// flow back to the Host for proposal completion.
	cb := &hostEngineCallback{h: h}
	eng.SetCallback(cb)

	// Start the engine worker pipeline.
	eng.Start()

	// Start the transport only after all wiring, so received messages see a complete handler.
	if tr != nil {
		if startErr := tr.Start(); startErr != nil {
			eng.Stop()
			if stopErr := tr.Stop(); stopErr != nil {
				logger.Debug("transport stop failed after start error", "error", stopErr)
			}
			return nil, &HostInitError{Field: "transport_start", Err: startErr}
		}
		// A RaftAddress with port 0 asked for a kernel-assigned port: the
		// host's own identity is the address the transport now advertises.
		// No caller can see h yet, and the receive path never reads it.
		cfg.RaftAddress = tr.AdvertisedAddr()
		h.cfg.RaftAddress = cfg.RaftAddress
	}

	// Run key rotation whenever a barrier is configured: the AEAD invocation-budget check runs even with no interval.
	if options.barrier != nil {
		// Supervised: a runtime.Goexit in the barrier's own code ends the
		// goroutine, which the loop's panic recovery cannot see, and it is
		// restarted on a fresh one. keyRotDone closes only on the final exit.
		interval := cfg.KeyRotationInterval
		closeDone := sync.OnceFunc(func() { close(h.keyRotDone) })
		supervise.Supervise(nil, h.keyRotStopC,
			func() {
				h.keyRotationLoop(interval)
				closeDone()
			},
			closeDone)
	} else {
		close(h.keyRotDone)
	}

	// Start the sweeper that expires lost pending proposals, reads and snapshots.
	go h.pendingSweepLoop()

	transportMode := "disabled"
	if tr != nil {
		transportMode = "quic"
	}

	h.logger.Info("host created",
		"id", hostID,
		"raft_address", cfg.RaftAddress,
		"listen_address", cfg.ListenAddress,
		"transport", transportMode,
	)

	started = true
	return h, nil
}

// copyIdentityMap returns a deep copy of src without empty values, or nil when empty; the caller's map must not enter replicated state.
func copyIdentityMap(src map[uint64]string) map[uint64]string {
	if len(src) == 0 {
		return nil
	}
	dst := make(map[uint64]string, len(src))
	for id, identity := range src {
		if identity == "" {
			continue
		}
		dst[id] = identity
	}
	if len(dst) == 0 {
		return nil
	}
	return dst
}

// admitMemberAddresses rejects hosts this node's own certificate also covers and records the rest as being admitted until release runs.
func (h *Host) admitMemberAddresses(members map[uint64]string) (release func(), err error) {
	if h.transport == nil || len(members) == 0 {
		return func() {}, nil
	}
	peers := make([]string, 0, len(members))
	for _, addr := range members {
		if addr != "" && addr != h.cfg.RaftAddress {
			peers = append(peers, addr)
		}
	}
	if len(peers) == 0 {
		return func() {}, nil
	}

	h.peerAdmitMu.Lock()
	defer h.peerAdmitMu.Unlock()
	if err := h.transport.VerifyPeerAddressExclusivity(h.cfg.RaftAddress, peers); err != nil {
		return nil, err
	}
	if h.admittingPeers == nil {
		h.admittingPeers = make(map[string]int)
	}
	for _, addr := range peers {
		h.admittingPeers[addr]++
	}
	var once sync.Once
	return func() {
		once.Do(func() {
			h.peerAdmitMu.Lock()
			defer h.peerAdmitMu.Unlock()
			for _, addr := range peers {
				if h.admittingPeers[addr]--; h.admittingPeers[addr] <= 0 {
					delete(h.admittingPeers, addr)
				}
			}
		})
	}, nil
}

// publishCommittedAddress registers a committed member address under peerAdmitMu, so ReloadMTLS checks it consistently.
func (h *Host) publishCommittedAddress(shardID, replicaID uint64, addr string) {
	h.peerAdmitMu.Lock()
	defer h.peerAdmitMu.Unlock()
	if err := h.peerAddressCoveredLocked(addr); err != nil {
		h.logger.Error("quicraft: a committed member address is covered by this host's certificate; "+
			"registering it because a committed entry is never refused, and the next certificate reload must not cover it",
			"shard", shardID,
			"replica", replicaID,
			"address", addr,
			"error", err,
		)
	}
	h.registry.Register(shardID, replicaID, addr)
}

// publishLearnedAddress runs publish for an address learned from an inbound message, under peerAdmitMu, refusing it when the certificate covers addr.
func (h *Host) publishLearnedAddress(shardID, replicaID uint64, addr string, publish func()) {
	h.peerAdmitMu.Lock()
	defer h.peerAdmitMu.Unlock()
	if err := h.peerAddressCoveredLocked(addr); err != nil {
		h.logger.Warn("quicraft: not learning a source address this host's certificate covers",
			"shard", shardID,
			"replica", replicaID,
			"address", addr,
			"error", err,
		)
		return
	}
	publish()
}

// peerAddressCoveredLocked returns the exclusivity error when the installed
// certificate covers addr, a peer's address. Callers hold peerAdmitMu.
func (h *Host) peerAddressCoveredLocked(addr string) error {
	if h.transport == nil || addr == "" || addr == h.cfg.RaftAddress {
		return nil
	}
	return h.transport.VerifyPeerAddressExclusivity(h.cfg.RaftAddress, []string{addr})
}

// StartShard starts a Raft shard: members maps replicaID to address, join joins an existing shard, create builds the state machine; witnesses and observers start with join=true.
//
//nolint:gocyclo // shard startup has many validation and configuration branches
func (h *Host) StartShard(members map[uint64]string, join bool, create sm.CreateFunc, cfg config.Config) error {
	if h.closed.Load() {
		return ErrClosed
	}
	// Reject new shards once the host is draining: a graceful shutdown is
	// in progress, so the SM Open() work would be wasted and the lifecycle
	// events misleading. Draining is irreversible (see Drain).
	if h.draining.Load() {
		return ErrClosed
	}

	if cfg.ShardID == 0 {
		return ErrInvalidConfig
	}
	if cfg.ReplicaID == 0 {
		return ErrInvalidConfig
	}

	cfg.SetDefaults()
	if err := cfg.Validate(); err != nil {
		return err
	}
	if err := cfg.ValidateElectionTimeout(h.cfg.RTTMillisecond); err != nil {
		return err
	}

	if create == nil {
		return ErrInvalidConfig
	}

	// A host with a transport admits a member only with its identity; see config.Config.RequireMemberIdentities.
	cfg.RequireMemberIdentities = h.transport != nil

	// Validate the initial members map when not joining an existing
	// cluster. A non-joining shard requires at least one member, and
	// the local replica must be included.
	if !join {
		if len(members) == 0 {
			return &EmptyMembersError{ShardID: cfg.ShardID}
		}
		if _, selfPresent := members[cfg.ReplicaID]; !selfPresent {
			return &SelfNotInMembersError{
				ShardID:   cfg.ShardID,
				ReplicaID: cfg.ReplicaID,
			}
		}
	}
	// Validate each member entry; two replicas at one address are refused.
	if len(members) > proto.MaxMembershipEntries {
		return &InvalidMemberError{
			ShardID: cfg.ShardID,
			Reason: fmt.Sprintf("members map has %d entries; the membership format "+
				"accepts at most %d", len(members), proto.MaxMembershipEntries),
		}
	}
	// Bootstrap is the only admission path that enforces the voting-set cap.
	if len(members) > raft.MaxVotingMembers {
		return &InvalidMemberError{
			ShardID: cfg.ShardID,
			Reason: fmt.Sprintf("members map has %d voting members; a shard accepts "+
				"at most %d (the ReadIndex acknowledgement bitfield is a uint64)",
				len(members), raft.MaxVotingMembers),
		}
	}
	seenAddresses := make(map[string]uint64, len(members))
	for replicaID, addr := range members {
		if replicaID == 0 {
			return &InvalidMemberError{
				ShardID:   cfg.ShardID,
				ReplicaID: replicaID,
				Address:   addr,
				Reason:    "replicaID must be non-zero",
			}
		}
		if addr == "" {
			return &InvalidMemberError{
				ShardID:   cfg.ShardID,
				ReplicaID: replicaID,
				Address:   addr,
				Reason:    "address must be non-empty",
			}
		}
		if len(addr) > proto.MaxAddressLength {
			return &InvalidMemberError{
				ShardID:   cfg.ShardID,
				ReplicaID: replicaID,
				Address:   addr,
				Reason: fmt.Sprintf("address is %d bytes; the membership format "+
					"accepts at most %d", len(addr), proto.MaxAddressLength),
			}
		}
		canonical := netaddr.Canonical(addr)
		if other, dup := seenAddresses[canonical]; dup {
			return &InvalidMemberError{
				ShardID:   cfg.ShardID,
				ReplicaID: replicaID,
				Address:   addr,
				Reason: fmt.Sprintf("address is already assigned to replica %d; "+
					"one address cannot identify two members of a shard", other),
			}
		}
		seenAddresses[canonical] = replicaID
	}

	// Bound cfg.MemberIdentities like addresses: they are committed in the same index-1 entry.
	if len(cfg.MemberIdentities) > proto.MaxMembershipEntries {
		return &InvalidMemberError{
			ShardID: cfg.ShardID,
			Reason: fmt.Sprintf("MemberIdentities has %d entries; the membership format "+
				"accepts at most %d", len(cfg.MemberIdentities), proto.MaxMembershipEntries),
		}
	}
	for replicaID, identity := range cfg.MemberIdentities {
		if len(identity) > proto.MaxAddressLength {
			return &InvalidMemberError{
				ShardID:   cfg.ShardID,
				ReplicaID: replicaID,
				Reason: fmt.Sprintf("node identity is %d bytes; the membership format "+
					"accepts at most %d", len(identity), proto.MaxAddressLength),
			}
		}
	}

	// Require per-node certificate SANs before the shard exists; transport authorization depends on them.
	releaseMembers, err := h.admitMemberAddresses(members)
	if err != nil {
		return err
	}
	defer releaseMembers()

	// With per-node certificates, every member must be admitted with the identity its certificate proves.
	if cfg.RequireMemberIdentities {
		if err := h.validateMemberIdentities(cfg); err != nil {
			return err
		}
	}

	// Resolve the shard's at-rest barrier before anything is written for it.
	shardBarrier := h.barrier
	if h.shardBarriers != nil {
		var barrierErr error
		shardBarrier, barrierErr = h.barrierForShard(cfg.ShardID)
		if barrierErr != nil {
			return barrierErr
		}
		if shardBarrier != nil && shardBarrier.IsSealed() {
			h.logger.Error("refusing to start shard: its at-rest barrier is sealed",
				"shard_id", cfg.ShardID)
			return ErrBarrierSealed
		}
		if err := h.verifyShardLogDBBarrier(cfg.ShardID, shardBarrier); err != nil {
			return err
		}
	}

	// Early duplicate check. The authoritative check uses LoadOrStore
	// below, but this catches the common case without doing expensive
	// peer/SM initialization.
	if _, loaded := h.nodes.Load(cfg.ShardID); loaded {
		// A start that has published its node can still fail and withdraw
		// it (a failed bootstrap save rolls it back), so while that start
		// holds its claim the shard is starting, not running.
		if _, starting := h.startingShards.Load(cfg.ShardID); starting {
			return ErrShardNotReady
		}
		return ErrShardAlreadyExists
	}

	// Hold the shard's import lock for the whole start, so no import commits beneath it.
	if _, starting := h.startingShards.LoadOrStore(cfg.ShardID, struct{}{}); starting {
		return ErrShardNotReady
	}
	defer h.startingShards.Delete(cfg.ShardID)
	releaseImportLock, importIdle := h.tryAcquireImportLock(cfg.ShardID)
	if !importIdle {
		return ErrImportInProgress
	}
	defer releaseImportLock()

	// A replica ID retired by RemoveData never runs again (ReplicaRetiredError); checked under the import lock.
	if err := h.refuseRetiredReplica(cfg.ShardID, cfg.ReplicaID); err != nil {
		return err
	}

	// Copy the members map to avoid aliasing.
	nodesCopy := make(map[uint64]string, len(members))
	for k, v := range members {
		nodesCopy[k] = v
	}

	// Create a LogReader backed by the host's LogDB (or a nil-LogDB
	// in-memory reader for testing). The LogReader provides the raft
	// layer with access to persisted log entries and state.
	lr := logdb.NewLogReader(cfg.ShardID, cfg.ReplicaID, h.logdb)

	// Recover LogReader state from LogDB on restart. On first boot the
	// LogDB will return ErrNoSnapshot / ErrNoState / empty entry range
	// and we fall through to the bootstrap membership below.
	hasPersistedState, err := h.recoverLogReader(lr, cfg.ShardID, cfg.ReplicaID)
	if err != nil {
		return &HostInitError{Field: "log_recovery", Err: err}
	}

	// Seed the LogReader's membership with the bootstrap members, unless recovery already restored it.
	recoveredState, recovered := lr.NodeState()
	nothingRecovered := recovered.ConfigChangeID == 0 &&
		len(recovered.Addresses) == 0 &&
		len(recovered.Observers) == 0 &&
		len(recovered.Witnesses) == 0

	// With committed entries but no snapshot membership, reconstruct membership from the committed log.
	if hasPersistedState && nothingRecovered {
		reconstructed, ok, rErr := h.reconstructShardMembership(lr, recoveredState.Commit)
		if rErr != nil {
			return &HostInitError{Field: "membership_reconstruct", Err: rErr}
		}
		if ok {
			lr.SetMembership(reconstructed)
			nothingRecovered = false
		}
	}

	// When bootstrapping, refuse a witness or observer seeded through the members map, which is the voter set.
	firstStart := !hasPersistedState
	if firstStart && (cfg.IsWitness || cfg.IsObserver) {
		if _, listed := members[cfg.ReplicaID]; listed {
			role := "observer"
			if cfg.IsWitness {
				role = "witness"
			}
			return &NonVotingRoleInMembersError{
				ShardID:   cfg.ShardID,
				ReplicaID: cfg.ReplicaID,
				Role:      role,
			}
		}
	}

	// A joiner on its first start needs an anchor naming the replicas it may take its bootstrap snapshot from.
	if cfg.RequireMemberIdentities && join && nothingRecovered &&
		!namesRemoteReplica(nodesCopy, cfg.ReplicaID) && !anchorsRemoteReplica(cfg) {
		return &InvalidMemberError{
			ShardID:   cfg.ShardID,
			ReplicaID: cfg.ReplicaID,
			Reason: "a joining replica must name at least one OTHER replica to accept its " +
				"bootstrap snapshot from (Config.MemberIdentities is the anchor)",
		}
	}

	if nothingRecovered && (!join || len(nodesCopy) > 0) {
		// Every founder in the seeding members map needs its identity.
		if cfg.RequireMemberIdentities {
			if err := requireFoundersIdentified(cfg, nodesCopy); err != nil {
				return err
			}
		}
		// Seed the replicated witness policy from config; afterwards it changes only through UpdateWitnessPolicy.
		initialMembership := proto.Membership{
			Addresses:                  nodesCopy,
			Identities:                 identitiesOf(nodesCopy, cfg.MemberIdentities),
			MaxWitnesses:               uint64(max(0, cfg.Witness.MaxWitnesses)),
			RequireFullReplicaMajority: cfg.Witness.GetRequireFullReplicaMajority(),
		}
		initialMembership.SetSessionPolicy(configSessionPolicy(cfg))
		// In-memory seed so the peer has membership available immediately at
		// creation — before the first apply cycle reconstructs it from the log —
		// for the initial election's quorum calculation.
		lr.SetMembership(initialMembership)

		// A founding member commits the initial configuration as EntryInitialMembership at index 1.
		if firstStart && !join {
			entry, err := raft.BuildInitialMembershipEntry(initialMembership)
			if err != nil {
				return &HostInitError{Field: "bootstrap_membership", Err: err}
			}
			if err := lr.PersistBootstrap([]proto.Entry{entry},
				proto.State{Term: entry.Term, Commit: entry.Index}); err != nil {
				return &HostInitError{Field: "bootstrap_persist", Err: err}
			}
		}
	}

	// Route by the effective membership, which after a restart is the recovered one.
	effectiveNodes := nodesCopy
	if !nothingRecovered {
		_, m := lr.NodeState()
		effectiveNodes = make(map[uint64]string,
			len(m.Addresses)+len(m.Observers)+len(m.Witnesses))
		for _, group := range []map[uint64]string{m.Addresses, m.Observers, m.Witnesses} {
			for id, addr := range group {
				effectiveNodes[id] = addr
			}
		}
		if cfg.RequireMemberIdentities {
			if err := h.checkRecoveredIdentities(cfg, m); err != nil {
				return err
			}
		}
	}

	// Create the Raft peer. NewPeer validates the config and
	// initializes the raft state machine as a follower.
	peer, err := raft.NewPeer(cfg, lr, h.engine.GlobalRateLimiter())
	if err != nil {
		return &HostInitError{Field: "peer", Err: err}
	}
	h.logRecoveryClamp(cfg.ShardID, cfg.ReplicaID, peer.RecoveryClamp())
	// The diagnostics the step worker reports about the recovered
	// configuration tail go to the sink the operator configured, not the
	// process slog default.
	peer.SetLogger(h.logger)
	// A recovered membership already switched past this build's window is
	// a shard this build cannot rejoin; refuse it before it runs.
	if err := peer.SetWireWindow(h.wire.Proto); err != nil {
		return &HostInitError{Field: "wire_version", Err: err}
	}
	// The read lease is measured in real time, so the peer must know how
	// long the ticks that its election timeout is counted in really are.
	peer.SetTickInterval(h.engine.TickInterval())
	// Mark a joining node so per-shard message auth waits for the bootstrap snapshot.
	if join {
		peer.MarkJoining()
	}

	// Create the managed state machine; its snapshot tree roots at Config.DataDir when set, else under NodeHostDir.
	snapshotDir := shardSnapshotBaseDir(h.cfg.NodeHostDir, cfg.DataDir, cfg.ShardID)

	// Reclaim plaintext decrypt scratch files orphaned by a crash before anything touches the snapshot tree.
	engine.SweepStaleDecryptTmpTree(snapshotDir, cfg.ShardID, cfg.ReplicaID)

	// Create the replica's snapshot directory here only; scratch writers never create it.
	if err := h.snapshotFsys().MkdirAll(engine.ReplicaSnapshotDir(snapshotDir, cfg.ShardID, cfg.ReplicaID), 0o750); err != nil {
		return &HostInitError{Field: "snapshot_dir", Err: err}
	}

	// Reclaim a plaintext import staging directory orphaned by a crash.
	engine.SweepStaleImportStageDirs(snapshotDir, cfg.ShardID, cfg.ReplicaID)

	// Reclaim a receive staging directory orphaned by a crash.
	engine.SweepStaleRecvStageDirs(snapshotDir, cfg.ShardID, cfg.ReplicaID)
	// A local save stages the same way, under its own prefix, and the same
	// argument holds: no node, no save that could still publish.
	engine.SweepStaleSaveStageDirs(snapshotDir, cfg.ShardID, cfg.ReplicaID)

	// The state machine reads the replicated session policy, falling back to local config until one is known.
	stateMachine, err := rsm.NewStateMachine(
		cfg.ShardID, cfg.ReplicaID, create, snapshotDir,
		configSessionPolicy(cfg), peer.SessionPolicy,
		cfg.ZeroCopyEntryCmd,
		rsm.WithLogger(h.logger),
	)
	if err != nil {
		// Map the rsm-internal error to the public sentinel so callers
		// can check errors.Is(err, ErrInvalidSMType) without knowing
		// about the internal rsm package.
		if errors.Is(err, rsm.ErrInvalidSMType) {
			return ErrInvalidSMType
		}
		// A restarting replica already knows the shard's committed session
		// policy here, so a Config that disagrees with it is refused before
		// the shard runs. The error names the setting and both values.
		if errors.Is(err, rsm.ErrSessionPolicyMismatch) {
			return err
		}
		return &HostInitError{Field: "state_machine", Err: err}
	}

	// Create the openStopper channel that can cancel a long-running
	// DiskStateMachine.Open() during Host.Close() or StopShard().
	openStopper := make(chan struct{})

	// Open the state machine. For on-disk SMs, this initializes persistent
	// storage and returns the last applied index for crash recovery.
	// For in-memory SMs, this is a no-op (returns 0).
	lastApplied, err := stateMachine.Open(openStopper)
	if err != nil {
		return &HostInitError{Field: "state_machine_open", Err: err}
	}

	// Close the opened state machine on every failure path until the node is published.
	smPublished := false
	defer func() {
		if !smPublished {
			if cerr := stateMachine.Close(); cerr != nil {
				h.logger.Warn("state machine close failed during StartShard rollback",
					"shard_id", cfg.ShardID, "error", cerr)
			}
		}
	}()

	// recoveredIndex and recoveredTerm are the highest index durably reflected in the state machine, and its term; 0 on a fresh start.
	var recoveredIndex uint64
	var recoveredTerm uint64

	// Recover state machine state on restart: from an on-disk SM's Open, or from the persisted snapshot.
	onDiskRecovered := stateMachine.IsOnDisk() && lastApplied > 0
	if onDiskRecovered && h.logdb != nil {
		ss, ssErr := h.logdb.GetSnapshot(cfg.ShardID, cfg.ReplicaID)
		if ssErr == nil && ss.Index > lastApplied && ss.Filepath != "" {
			onDiskRecovered = false
		}
	}
	if onDiskRecovered {
		// An on-disk SM's Open gives the index only; resolve its term from the log or the snapshot record, never 0.
		recoveredIndex = lastApplied
	} else if h.logdb != nil {
		ss, ssErr := h.logdb.GetSnapshot(cfg.ShardID, cfg.ReplicaID)
		if ssErr == nil && ss.Index > 0 && ss.Filepath != "" {
			dataPath := filepath.Join(ss.Filepath, "snapshot.dat")

			// Verify the artifact against its snapshot.meta before the state machine sees it.
			if _, statErr := h.snapshotFsys().Stat(dataPath); os.IsNotExist(statErr) {
				return &HostInitError{Field: "snapshot_open", Err: statErr}
			}
			if err := engine.VerifySnapshotArtifact(ss.Filepath, dataPath); err != nil {
				return &HostInitError{Field: "snapshot_integrity", Err: err}
			}

			// Also check the FileSize recorded in LogDB, so a truncated snapshot.dat fails.
			if err := engine.VerifySnapshotFileSize(dataPath, ss.FileSize); err != nil {
				return &HostInitError{Field: "snapshot_integrity", Err: err}
			}

			// Decrypt an encrypted snapshot.dat to an anonymous scratch file before recovery.
			var f snapshotFile
			if ss.Epoch > 0 {
				barrier, barrierErr := h.barrierForShard(cfg.ShardID)
				if barrierErr != nil {
					return &HostInitError{Field: "snapshot_decrypt", Err: barrierErr}
				}
				if barrier == nil || barrier.IsSealed() {
					return &HostInitError{
						Field: "snapshot_decrypt",
						Err:   ErrSnapshotEncryptedBarrierUnavailable,
					}
				}
				scratch, decErr := decryptSnapshotToAnonymousScratch(
					h.snapshotFsys(), engine.ReplicaSnapshotDir(snapshotDir, cfg.ShardID, cfg.ReplicaID),
					dataPath, cfg.ShardID, cfg.ReplicaID, ss.Index, ss.Epoch, barrier,
				)
				if decErr != nil {
					return &HostInitError{Field: "snapshot_decrypt", Err: decErr}
				}
				f = scratch
			} else {
				plain, openErr := h.snapshotFsys().Open(dataPath)
				if openErr != nil {
					// A recorded snapshot whose data file is missing or unreadable fails the start.
					return &HostInitError{Field: "snapshot_open", Err: openErr}
				}
				f = plain
			}
			// Read the artifact through the reader chain its snapshot record names.
			recoverReader, readerErr := engine.DecompressingSnapshotReader(f, ss.Compression)
			if readerErr != nil {
				if closeErr := f.Close(); closeErr != nil {
					h.logger.Debug("snapshot file close failed after reader build error",
						"shard_id", cfg.ShardID,
						"replica_id", cfg.ReplicaID,
						"error", closeErr)
				}
				return &HostInitError{Field: "snapshot_compression", Err: readerErr}
			}
			stopper := make(chan struct{})
			recoverErr := stateMachine.RecoverFromSnapshot(
				context.Background(), recoverReader, stopper,
			)
			if closeErr := f.Close(); closeErr != nil {
				return &HostInitError{Field: "snapshot_close", Err: closeErr}
			}
			if recoverErr != nil {
				return &HostInitError{Field: "snapshot_recovery", Err: recoverErr}
			}
			// Seed from the snapshot's raft log index, not its OnDiskIndex.
			recoveredIndex = ss.Index
			recoveredTerm = ss.Term
		}
	}

	// For an on-disk SM with durable state, set raft's processed marker to lastApplied.
	if recoveredIndex > 0 {
		// Adopt the value raft applied after clamping to the recovered commit index.
		var setErr error
		recoveredIndex, setErr = peer.SetProcessedIndex(recoveredIndex)
		h.logAppliedClamp(cfg.ShardID, cfg.ReplicaID, peer.AppliedClamp())
		h.logUnfoldedConfig(cfg.ShardID, cfg.ReplicaID, peer.UnfoldedConfig())
		if setErr != nil {
			return &HostInitError{Field: "config_replay", Err: setErr}
		}

		// Resolve the term of the clamped index; a 0 term would mark every candidate up to date.
		if onDiskRecovered && recoveredIndex > 0 {
			term, termErr := h.resolveRecoveredTerm(lr, cfg.ShardID, cfg.ReplicaID, recoveredIndex)
			if termErr != nil {
				return &HostInitError{Field: "recovered_term", Err: termErr}
			}
			recoveredTerm = term
		}
	}

	// ns.nodes is the mutable routing table guarded by nodesMu, a copy of effectiveNodes.
	routingNodes := make(map[uint64]string, len(effectiveNodes))
	for id, addr := range effectiveNodes {
		routingNodes[id] = addr
	}
	ns := &nodeState{
		shardID:          cfg.ShardID,
		replicaID:        cfg.ReplicaID,
		cfg:              cfg,
		createFn:         create,
		smType:           stateMachine.SMType().TypeCode(),
		barrier:          shardBarrier,
		nodes:            routingNodes,
		requestPool:      NewRequestStatePool(),
		rsm:              stateMachine,
		openStopper:      openStopper,
		pendingReads:     make(map[uint64]*pendingRead),
		pendingSnapshots: make(map[uint64]*RequestState),
	}
	ns.health.Store(uint32(ShardHealthy))
	// A starting shard knows no leader yet: its election window opens now.
	ns.leaderlessSince.Store(h.leaderlessNow())

	// Seed lastApplied to the recovered index so reads are served on restart.
	if recoveredIndex > 0 {
		ns.lastApplied.Store(recoveredIndex)
	}

	// Seed configChangeIndex from the recovered membership.
	if pm := peer.GetMembership(); pm.ConfigChangeID > 0 {
		advanceConfigChangeIndex(ns, pm.ConfigChangeID)
	}

	// Atomically store the nodeState, detecting concurrent StartShard
	// calls for the same shardID.
	if _, loaded := h.nodes.LoadOrStore(cfg.ShardID, ns); loaded {
		return ErrShardAlreadyExists
	}
	h.activeNodes.Add(1)
	// Record the join window and operator anchor only once the shard is published.
	if join {
		h.markShardJoining(cfg.ShardID)
	}
	h.setShardAnchor(cfg.ShardID, cfg.MemberIdentities)

	// Save bootstrap info if we have a LogDB and this is not a join.
	if h.logdb != nil && !join {
		bootstrap := logdb.Bootstrap{
			Addresses: nodesCopy,
			Join:      join,
			Type:      ns.smType,
			// Record the shard's on-disk root for operations on a stopped shard.
			DataDir: cfg.DataDir,
		}
		if err := h.logdb.SaveBootstrap(cfg.ShardID, cfg.ReplicaID, bootstrap); err != nil {
			// Mark the nodeState stopped before removing it from h.nodes.
			ns.stopped.Store(true)
			h.nodes.Delete(cfg.ShardID)
			h.clearShardJoining(cfg.ShardID)
			h.anchors.Delete(cfg.ShardID)
			h.activeNodes.Add(-1)
			return ErrLogDBWrite
		}
	}

	// Create the engine Node and load it into the engine pipeline.
	engNode := engine.NewNode(
		peer,
		stateMachine, // rsm.StateMachine implements engine.Applier
		lr,
		cfg,
		stateMachine, // rsm.StateMachine implements engine.Snapshotter
		snapshotDir,
		h.cfg.EventListener,
	)
	engNode.SetNotifyCommit(h.cfg.NotifyCommit)
	// Seed the engine node's applied and snapshot markers to the recovered index.
	engNode.SeedLastApplied(recoveredIndex, recoveredTerm)
	// Seed the state machine's own applied marker to the same index.
	stateMachine.SetLastApplied(recoveredIndex)
	h.engine.LoadNode(engNode)
	// The node is now live in the engine; StopShard/Close own its rsm.Close from
	// here, so the StartShard rollback close must not also run.
	smPublished = true

	// If Close began during the start, tear the shard down rather than leave it in a stopped engine.
	if h.closed.Load() || h.draining.Load() || ns.stopped.Load() {
		if ns.stopped.CompareAndSwap(false, true) {
			// ErrClosed is the caller's answer here — the shard never became
			// usable — so the state machine's Close error is logged rather
			// than returned in its place.
			if cerr := h.teardownShard(ns, cfg.ShardID, ErrClosed); cerr != nil {
				h.logger.Warn("state machine close failed while aborting shard start",
					"shard_id", cfg.ShardID, "error", cerr)
			}
		} else {
			h.engine.UnloadNode(cfg.ShardID)
		}
		h.logger.Warn("shard start aborted: host is closing or the shard was stopped",
			"shard_id", cfg.ShardID, "replica_id", cfg.ReplicaID)
		return ErrClosed
	}

	// Pre-bind metrics for this shard to avoid expensive label lookups
	// in hot paths. This must be called before any metrics are recorded.
	if h.metrics != nil {
		h.metrics.RegisterShard(cfg.ShardID)
	}

	// Register all members in the registry for address resolution.
	for replicaID, addr := range effectiveNodes {
		h.registry.Register(cfg.ShardID, replicaID, addr)
	}

	// Notify the engine that this shard has work so the Raft peer
	// can begin election or heartbeat processing.
	h.engine.NotifyWork(cfg.ShardID)

	// Fire OnNodeReady event listener.
	if el := h.cfg.EventListener; el != nil && el.OnNodeReady != nil {
		el.OnNodeReady(config.EventShardInfo{
			ShardID:   cfg.ShardID,
			ReplicaID: cfg.ReplicaID,
		})
	}

	h.logger.Info("shard started",
		"shard_id", cfg.ShardID,
		"replica_id", cfg.ReplicaID,
		"sm_type", proto.StateMachineTypeName(ns.smType),
		"members", len(members),
		"join", join,
	)

	return nil
}

// logRecoveryClamp logs a recovered commit index raft clamped down, through the host's contained logger.
func (h *Host) logRecoveryClamp(shardID, replicaID uint64, c raft.RecoveryClamp) {
	if !c.Applied() {
		return
	}
	logger := h.logger
	if c.EmptyLog() {
		supervise.LogContained(func() {
			logger.Error("recovered commit index over an EMPTY log: entry records were lost while the state record survived; clamping to 0 and re-learning the log from the leader",
				"shard", shardID,
				"replica", replicaID,
				"recovered_commit", c.RecoveredCommit,
				"recovered_term", c.RecoveredTerm,
				"snapshot_index", c.SnapshotIndex)
		})
		return
	}
	supervise.LogContained(func() {
		logger.Warn("recovered commit index exceeds last log entry, clamping",
			"shard", shardID,
			"replica", replicaID,
			"recovered_commit", c.RecoveredCommit,
			"last_index", c.LastIndex,
			"snapshot_index", c.SnapshotIndex)
	})
}

// logAppliedClamp logs a state machine applied index raft clamped to the recovered commit index.
func (h *Host) logAppliedClamp(shardID, replicaID uint64, c raft.AppliedClamp) {
	if !c.Applied() {
		return
	}
	logger := h.logger
	supervise.LogContained(func() {
		logger.Warn("state machine applied index exceeds recovered commit index, clamping",
			"shard", shardID,
			"replica", replicaID,
			"sm_applied", c.SMApplied,
			"committed", c.Committed)
	})
}

// logUnfoldedConfig logs committed config changes below the applied index that SetProcessedIndex could not fold.
func (h *Host) logUnfoldedConfig(shardID, replicaID uint64, c raft.UnfoldedConfig) {
	if !c.Recorded() {
		return
	}
	logger := h.logger
	supervise.LogContained(func() {
		logger.Error("committed config changes below the state machine's applied index "+
			"were not folded: this replica will not campaign until they are",
			"shard", shardID,
			"replica", replicaID,
			"index", c.Index,
			"folded_through", c.FoldedThrough,
			"stale_through", c.StaleThrough,
			"error", c.Err)
	})
}

// StopShard stops all replicas of the given shard on this host.
func (h *Host) StopShard(shardID uint64) error {
	if h.closed.Load() {
		return ErrClosed
	}

	v, exists := h.nodes.Load(shardID)
	if !exists {
		return ErrShardNotFound
	}
	ns, ok := v.(*nodeState)
	if !ok {
		return &TypeAssertionError{
			Context:      "StopShard nodeState lookup",
			ExpectedType: "*nodeState",
			ActualValue:  v,
		}
	}

	// Only the caller that flips stopped performs the teardown.
	if !ns.stopped.CompareAndSwap(false, true) {
		return ErrShardNotFound // another StopShard/Close already owns teardown
	}
	// Wake ExecuteOnLeader callers waiting on this shard's leadership view;
	// they find it stopped and return ErrStopped.
	ns.signalLeaderChange()
	// The state machine's Close error is returned: it is the shard's last durability signal.
	closeErr := h.teardownShard(ns, shardID, ErrAborted)

	// Fire OnNodeUnloaded event listener.
	if el := h.cfg.EventListener; el != nil && el.OnNodeUnloaded != nil {
		el.OnNodeUnloaded(config.EventShardInfo{
			ShardID:   shardID,
			ReplicaID: ns.replicaID,
		})
	}

	h.logger.Info("shard stopped", "shard_id", shardID)
	return closeErr
}

// teardownShard releases every resource of a loaded shard; the caller must have won ns.stopped's CompareAndSwap.
func (h *Host) teardownShard(ns *nodeState, shardID uint64, pendingErr error) error {
	ns.health.Store(uint32(ShardUnloaded))

	// Signal cancellation to any in-progress DiskStateMachine.Open()
	// or other operations blocking on the open stopper channel.
	ns.openStopOnce.Do(func() { close(ns.openStopper) })

	// Unload the engine node first, while the nodeState is still findable by in-flight OnApplied.
	h.engine.UnloadNode(shardID)

	// Fail all remaining pending proposals, reads, and snapshots. Any
	// items completed by OnApplied during the drain are already removed
	// from these maps, so only truly orphaned items receive the error.
	h.failPendingRequests(ns, pendingErr)

	// Clean up the registry and join window before removing the nodeState.
	ns.nodesMu.RLock()
	for replicaID := range ns.nodes {
		h.registry.Remove(shardID, replicaID)
	}
	ns.nodesMu.RUnlock()
	// The join bootstrap window belongs to the loaded shard; a stopped shard
	// must not leave a relaxed snapshot authorization behind.
	h.clearShardJoining(shardID)
	h.anchors.Delete(shardID)

	// Remove nodeState last, after the engine is fully unloaded, all pending
	// items have been resolved, and the registry/join state is cleaned up. No
	// more callbacks will reference this shardID.
	h.nodes.Delete(shardID)
	h.activeNodes.Add(-1)

	// Close the state machine to release its resources, such as file locks.
	var smCloseErr error
	if ns.rsm != nil {
		if err := ns.rsm.Close(); err != nil {
			smCloseErr = err
			h.logger.Warn("state machine close failed during shard teardown",
				"shard_id", shardID, "error", err)
		}
		// Leave ns.rsm set: concurrent readers may still read it and gate on ns.stopped.
	}

	// Unregister pre-bound metrics for this shard.
	if h.metrics != nil {
		h.metrics.UnregisterShard(shardID)
	}
	return smCloseErr
}

// SyncPropose proposes cmd through Raft consensus and blocks until the
// result is available or ctx is cancelled. Handles RequestState lifecycle
// internally.
func (h *Host) SyncPropose(ctx context.Context, shardID uint64, cmd []byte) (sm.Result, error) {
	rs, err := h.Propose(ctx, shardID, cmd)
	if err != nil {
		return sm.Result{}, err
	}
	defer rs.Release()

	// awaitResult selects on ctx.Done() directly instead of rs.Result(), so
	// no time.NewTimer is allocated per SyncPropose call.
	result, err := awaitResult(ctx, rs)
	if err != nil {
		return sm.Result{}, err
	}
	return sm.Result{Value: result.Value, Data: result.Data}, nil
}

// SyncRead performs a linearizable read: ReadIndex, wait for the local apply, then query.
func (h *Host) SyncRead(ctx context.Context, shardID uint64, query interface{}) (interface{}, error) {
	// Step 1: Get a confirmed read index via ReadIndex.
	rs, err := h.ReadIndex(ctx, shardID)
	if err != nil {
		return nil, err
	}
	defer rs.Release()

	// Step 2: Wait for the SM to catch up to the read index.
	// Try a non-blocking receive first to catch immediately available
	// results (common for single-node ReadIndex via ReadOnly bypass).
	resultC := rs.ResultC()
	select {
	case result := <-resultC:
		if result.Err != nil {
			return nil, result.Err
		}
		goto lookup
	default:
	}
	// Result not immediately available. Block on the channel.
	select {
	case result := <-resultC:
		if result.Err != nil {
			return nil, result.Err
		}
	case <-ctx.Done():
		if ctx.Err() == context.DeadlineExceeded {
			return nil, ErrTimeout
		}
		return nil, ErrCanceled
	}
lookup:

	// Re-check context after the goto label. The non-blocking receive
	// path above jumps here without passing through the ctx.Done()
	// select case, so the context may have been canceled in between.
	if ctxErr := ctx.Err(); ctxErr != nil {
		return nil, mapContextError(ctxErr)
	}

	// Step 3: Execute the query on the caught-up state machine.
	ns, err := h.getNode(shardID)
	if err != nil {
		return nil, err
	}
	// Bail if the shard is stopping/stopped: its state machine may be closing
	// (Host.Close sets stopped before closing the SM). This avoids issuing a
	// Lookup against a state machine that is being torn down.
	if ns.stopped.Load() {
		return nil, ErrClosed
	}
	if ns.rsm == nil {
		return nil, ErrShardNotReady
	}
	return ns.rsm.Lookup(ctx, query)
}

// SyncReadBuf is SyncRead for []byte queries returning a pooled buffer; call release when done and do not use result after.
func (h *Host) SyncReadBuf(ctx context.Context, shardID uint64, key []byte) (result []byte, release func(), err error) {
	rs, readErr := h.ReadIndex(ctx, shardID)
	if readErr != nil {
		return nil, nopHostRelease, readErr
	}
	defer rs.Release()

	resultC := rs.ResultC()
	select {
	case r := <-resultC:
		if r.Err != nil {
			return nil, nopHostRelease, r.Err
		}
		goto lookup
	default:
	}
	select {
	case r := <-resultC:
		if r.Err != nil {
			return nil, nopHostRelease, r.Err
		}
	case <-ctx.Done():
		if ctx.Err() == context.DeadlineExceeded {
			return nil, nopHostRelease, ErrTimeout
		}
		return nil, nopHostRelease, ErrCanceled
	}
lookup:
	if ctxErr := ctx.Err(); ctxErr != nil {
		return nil, nopHostRelease, mapContextError(ctxErr)
	}
	ns, nsErr := h.getNode(shardID)
	if nsErr != nil {
		return nil, nopHostRelease, nsErr
	}
	// Shard is stopping/stopped: its state machine may be closing.
	if ns.stopped.Load() {
		return nil, nopHostRelease, ErrClosed
	}
	if ns.rsm == nil {
		return nil, nopHostRelease, ErrShardNotReady
	}
	buf, rel, supported, lookupErr := ns.rsm.NALookupBuf(key)
	if lookupErr != nil {
		return nil, nopHostRelease, lookupErr
	}
	if supported {
		return buf, rel, nil
	}
	// Fall back to the generic Lookup path (result is []byte or interface{}).
	v, lookupErr2 := ns.rsm.Lookup(ctx, key)
	if lookupErr2 != nil {
		return nil, nopHostRelease, lookupErr2
	}
	if b, ok := v.([]byte); ok {
		return b, nopHostRelease, nil
	}
	// SM returned a non-[]byte result. The caller should use SyncRead instead.
	return nil, nopHostRelease, ErrInvalidOperation
}

// StaleReadBuf is StaleRead for []byte queries returning a pooled buffer; call release when done and do not use result after.
func (h *Host) StaleReadBuf(ctx context.Context, shardID uint64, key []byte) (result []byte, release func(), err error) {
	// Same already-cancelled guard as the other read paths. NALookupBuf takes
	// no context, so this is the only place the cancellation can be honoured.
	if err := ctx.Err(); err != nil {
		return nil, nopHostRelease, mapContextError(err)
	}
	if h.closed.Load() {
		return nil, nopHostRelease, ErrClosed
	}
	ns, nsErr := h.getNode(shardID)
	if nsErr != nil {
		return nil, nopHostRelease, nsErr
	}
	// Shard is stopping/stopped: its state machine may be closing.
	if ns.stopped.Load() {
		return nil, nopHostRelease, ErrClosed
	}
	if ns.rsm == nil {
		return nil, nopHostRelease, ErrShardNotReady
	}
	// Refused while paused (see Host.PauseShard).
	if ns.paused.Load() {
		return nil, nopHostRelease, ErrShardPaused
	}
	// Refused while sealed, as StaleRead is.
	if ns.barrierSealed() {
		return nil, nopHostRelease, ErrBarrierSealed
	}
	buf, rel, supported, lookupErr := ns.rsm.NALookupBuf(key)
	if lookupErr != nil {
		return nil, nopHostRelease, lookupErr
	}
	if supported {
		return buf, rel, nil
	}
	v, lookupErr2 := ns.rsm.Lookup(ctx, key)
	if lookupErr2 != nil {
		return nil, nopHostRelease, lookupErr2
	}
	if b, ok := v.([]byte); ok {
		return b, nopHostRelease, nil
	}
	// SM returned a non-[]byte result. The caller should use StaleRead instead.
	return nil, nopHostRelease, ErrInvalidOperation
}

// nopHostRelease is a no-op release function returned when no pool buffer was
// used on the SyncReadBuf / StaleReadBuf path.
func nopHostRelease() {}

// Propose submits an asynchronous proposal through Raft. Returns a
// RequestState that the caller must poll via Result and release via Release.
func (h *Host) Propose(ctx context.Context, shardID uint64, cmd []byte) (*RequestState, error) {
	// A context already done submits nothing.
	if err := ctx.Err(); err != nil {
		return nil, mapContextError(err)
	}
	if h.closed.Load() {
		return nil, ErrClosed
	}
	if h.draining.Load() {
		return nil, ErrDraining
	}

	ns, err := h.getNode(shardID)
	if err != nil {
		return nil, err
	}

	// An empty command is proposed normally and never reaches the state machine.

	// Witnesses vote but don't store log data; they cannot propose.
	if ns.cfg.IsWitness {
		return nil, ErrInvalidOperation
	}

	// Reject proposals when the WAL disk is full to prevent unbounded
	// log growth and potential data loss.
	if h.proposalDiskFull() {
		return nil, ErrDiskFull
	}

	// Refused while paused (see Host.PauseShard).
	if ns.paused.Load() {
		return nil, ErrShardPaused
	}
	// Refuse while the shard's barrier is sealed: the entry could not be
	// encrypted into the log (see nodeState.barrierSealed).
	if ns.barrierSealed() {
		return nil, ErrBarrierSealed
	}

	// A proposal must fit both MaxProposalPayloadSize and MaxEntrySize.
	effectiveMax := effectiveMaxPayload(ns.cfg.MaxProposalPayloadSize, ns.cfg.MaxEntrySize)
	if uint64(len(cmd)) > effectiveMax {
		return nil, &EntryTooLargeError{
			Size:    uint64(len(cmd)),
			MaxSize: effectiveMax,
		}
	}

	// If there is no known leader, fail fast. When a leader is known,
	// allow the Raft engine to forward the proposal automatically.
	if !ns.isLeader.Load() && ns.leaderID.Load() == 0 {
		return nil, h.buildNotLeaderError(ns)
	}

	entry := proto.Entry{Cmd: cmd}
	return h.proposeEntry(ctx, ns, shardID, entry, nil)
}

// ProposeWithSession submits an at-most-once proposal on a registered session; call session.ProposalCompleted after a success.
func (h *Host) ProposeWithSession(ctx context.Context, session *Session, cmd []byte) (*RequestState, error) {
	// See Propose: an already-done context must not submit work, or the
	// outcome is a coin flip between the result and the cancellation.
	if err := ctx.Err(); err != nil {
		return nil, mapContextError(err)
	}
	if h.closed.Load() {
		return nil, ErrClosed
	}
	if h.draining.Load() {
		return nil, ErrDraining
	}
	if session == nil {
		return nil, ErrInvalidSession
	}

	shardID := session.ShardID()

	ns, err := h.getNode(shardID)
	if err != nil {
		return nil, err
	}

	// Witnesses vote but don't store log data; they cannot propose.
	if ns.cfg.IsWitness {
		return nil, ErrInvalidOperation
	}

	// Refuse a session proposal on an on-disk state machine before the entry exists.
	if !session.IsNoOPSession() {
		if err := rejectSessionsOnDisk(ns); err != nil {
			return nil, err
		}
	}

	if !session.ValidForProposal(shardID) {
		return nil, ErrInvalidSession
	}

	// Reject proposals when the WAL disk is full to prevent unbounded
	// log growth and potential data loss.
	if h.proposalDiskFull() {
		return nil, ErrDiskFull
	}

	// Refused while paused (see Host.PauseShard).
	if ns.paused.Load() {
		return nil, ErrShardPaused
	}
	// Refuse while the shard's barrier is sealed: the entry could not be
	// encrypted into the log (see nodeState.barrierSealed).
	if ns.barrierSealed() {
		return nil, ErrBarrierSealed
	}

	// A proposal must fit both MaxProposalPayloadSize and MaxEntrySize.
	effectiveMax := effectiveMaxPayload(ns.cfg.MaxProposalPayloadSize, ns.cfg.MaxEntrySize)
	if uint64(len(cmd)) > effectiveMax {
		return nil, &EntryTooLargeError{
			Size:    uint64(len(cmd)),
			MaxSize: effectiveMax,
		}
	}

	// If there is no known leader, fail fast. When a leader is known,
	// allow the Raft engine to forward the proposal automatically.
	if !ns.isLeader.Load() && ns.leaderID.Load() == 0 {
		return nil, h.buildNotLeaderError(ns)
	}

	// Reserve the series before building the entry; proposeEntry owns releasing it on failure.
	series, ok := session.beginCurrentProposal()
	if !ok {
		return nil, ErrSessionBusy
	}
	entry := proto.Entry{
		Cmd:         cmd,
		ClientID:    session.ClientID(),
		SeriesID:    series,
		RespondedTo: session.RespondedTo(),
	}
	return h.proposeEntry(ctx, ns, shardID, entry, session)
}

// SyncProposeWithSession blocks until the session proposal applies, then calls session.ProposalCompleted; on error the session is unchanged.
func (h *Host) SyncProposeWithSession(ctx context.Context, session *Session, cmd []byte) (sm.Result, error) {
	rs, err := h.ProposeWithSession(ctx, session, cmd)
	if err != nil {
		return sm.Result{}, err
	}
	defer rs.Release()

	result, err := awaitResult(ctx, rs)
	if err != nil {
		return sm.Result{}, err
	}
	session.ProposalCompleted()
	return sm.Result{Value: result.Value, Data: result.Data}, nil
}

// pendingDeadline returns ctx's deadline, or defaultPendingRequestTimeout from now when it has none.
func pendingDeadline(ctx context.Context) time.Time {
	return pendingDeadlineWithin(ctx, defaultPendingRequestTimeout)
}

// pendingDeadlineWithin is pendingDeadline with an explicit backstop for a context without a deadline.
func pendingDeadlineWithin(ctx context.Context, backstop time.Duration) time.Time {
	if deadline, ok := ctx.Deadline(); ok {
		return deadline
	}
	return time.Now().Add(backstop)
}

// nextRequestKey returns a random non-zero 64-bit key for a pending request.
func nextRequestKey() uint64 {
	for {
		if key := mathrand.Uint64(); key != 0 {
			return key
		}
	}
}

// proposeEntry registers entry as a pending proposal on shardID and delivers it to the step worker.
func (h *Host) proposeEntry(ctx context.Context, ns *nodeState, shardID uint64, entry proto.Entry, session *Session) (rs *RequestState, err error) {
	// The caller reserved entry.SeriesID; release it here on failure until the request is bound.
	bound := false
	defer func() {
		if err != nil && session != nil && !bound {
			session.endProposal(entry.SeriesID)
		}
	}()

	// Check global rate limiter.
	if globalRL := h.engine.GlobalRateLimiter(); globalRL != nil && globalRL.RateLimited() {
		return nil, ErrSystemBusy
	}

	// Verify the engine node exists before creating state.
	engNode := h.engine.GetNode(shardID)
	if engNode == nil || engNode.Peer() == nil {
		return nil, ErrShardNotReady
	}

	// Refuse once the shard's failure latch has tripped.
	if engNode.IsFailed() {
		return nil, ErrShardFailed
	}

	// Check per-shard rate limiter.
	if peer := engNode.Peer(); peer != nil {
		if shardRL := peer.RateLimiter(); shardRL != nil && shardRL.IsLimited() {
			return nil, &BusyError{
				ShardID:     shardID,
				CurrentSize: shardRL.Get(),
				MaxSize:     shardRL.MaxSize(),
			}
		}
	}

	// The proposal key travels with the entry so OnApplied can match it.
	proposalKey := nextRequestKey()
	entry.Key = proposalKey

	// Create request state with the deadline from the context, clamped to
	// defaultPendingRequestTimeout when the caller supplied none so the sweeper
	// can always reclaim this entry.
	deadline := pendingDeadline(ctx)
	rs = newRequestState(ns.requestPool, proposalKey, deadline, h.metrics != nil)
	// Bind to the session before registering, so an error completion can release the series.
	if session != nil {
		rs.bindSession(session, entry.SeriesID)
		bound = true
	}

	// Register before delivery so a fast OnApplied finds it.
	pp := &pendingProposal{
		rs:       rs,
		deadline: deadline,
		floor:    engNode.CommittedIndex(),
	}
	if err := h.registerPendingProposal(ns, engNode, proposalKey, pp); err != nil {
		return nil, err
	}

	// Deliver through the MPSC proposal queue; only the step worker touches the peer.
	if !engNode.DeliverTrackedProposal([]proto.Entry{entry}, &pp.enqueuedAt) {
		// Queue full: claim the entry with LoadAndDelete and let the caller retry.
		if _, removed := ns.pendingProposals.LoadAndDelete(proposalKey); removed {
			rs.releaseRef()
			h.completePendingOp()
		}
		rs.Release()
		// The queue refuses for good once the node failed or stopped (it is
		// closed); that is not an overflow, and says so.
		if refusal := closedQueueRefusal(engNode); refusal != nil {
			return nil, refusal
		}
		if h.metrics != nil {
			h.metrics.IncProposalDropped(shardID)
		}
		if el := h.cfg.EventListener; el != nil && el.OnProposalDropped != nil {
			el.OnProposalDropped(config.EventShardInfo{
				ShardID:   shardID,
				ReplicaID: ns.replicaID,
			})
		}
		// A full per-shard queue is ErrDropped, not the global ErrSystemBusy.
		return nil, ErrDropped
	}

	// Wake the step worker so it processes the proposal queue.
	h.engine.NotifyWork(shardID)

	return rs, nil
}

// registerPendingProposal registers pp under key and re-checks the terminal states, rolling back and returning the refusal on failure.
func (h *Host) registerPendingProposal(ns *nodeState, engNode *engine.Node, key uint64, pp *pendingProposal) error {
	// Count before registering, so a finished Drain refuses with nothing to undo.
	if err := h.admitPendingOp(); err != nil {
		pp.rs.Release()
		return err
	}
	pp.node = engNode
	pp.rs.acquireRef()
	// From here a failure is classified by the entry's hand-off marker; the rollback's refusal stays definite.
	pp.rs.proposal.Store(pp)
	ns.pendingProposals.Store(key, pp)
	refusal := ErrClosed
	if !h.closed.Load() && !ns.stopped.Load() {
		if !engNode.IsFailed() {
			return nil
		}
		refusal = ErrShardFailed
	}
	if _, removed := ns.pendingProposals.LoadAndDelete(key); removed {
		pp.rs.releaseRef()
		h.completePendingOp()
	}
	// The caller's own reference is dropped whether or not the claim won: a
	// sweep that beat us to the entry drops only ITS reference, so leaving
	// Release() inside the claim would strand the object outside the pool.
	pp.rs.Release()
	return refusal
}

// ReadIndex confirms linearizability; the returned RequestState resolves once the local SM has applied the confirmed index, carried in Value.
func (h *Host) ReadIndex(ctx context.Context, shardID uint64) (*RequestState, error) {
	// See Propose: an already-done context must not submit work, or the
	// outcome is a coin flip between the result and the cancellation.
	if err := ctx.Err(); err != nil {
		return nil, mapContextError(err)
	}
	if h.closed.Load() {
		return nil, ErrClosed
	}
	if h.draining.Load() {
		return nil, ErrDraining
	}

	ns, err := h.getNode(shardID)
	if err != nil {
		return nil, err
	}

	// Witnesses vote but don't store log data; they cannot serve reads.
	if ns.cfg.IsWitness {
		return nil, ErrInvalidOperation
	}

	// Refused while paused (see Host.PauseShard).
	if ns.paused.Load() {
		return nil, ErrShardPaused
	}
	// A sealed replica serves no linearizable read.
	if ns.barrierSealed() {
		return nil, ErrBarrierSealed
	}

	// Verify the engine node and Peer exist.
	engNode := h.engine.GetNode(shardID)
	if engNode == nil || engNode.Peer() == nil {
		return nil, ErrShardNotReady
	}

	// Refuse a read against a halted shard, which never applies again.
	if engNode.IsFailed() {
		return nil, ErrShardFailed
	}

	// The read key travels in the ReadIndex Hint and returns in ReadyToRead.Key.
	readKey := nextRequestKey()

	deadline := pendingDeadline(ctx)
	rs := newRequestState(ns.requestPool, readKey, deadline, h.metrics != nil)

	// Admission counts the operation for Drain, or refuses it once Drain has started.
	if err := h.admitPendingOp(); err != nil {
		rs.Release()
		return nil, err
	}

	// Register before delivery so OnReadyToRead finds the entry; acquireRef pins the RS.
	rs.acquireRef()
	ns.pendingReadsMu.Lock()
	if ns.pendingReads == nil {
		ns.pendingReads = make(map[uint64]*pendingRead)
	}
	ns.pendingReads[readKey] = &pendingRead{rs: rs}
	ns.pendingReadsMu.Unlock()

	// Re-check the terminal states after registering and roll back, so the RS is never orphaned.
	if h.closed.Load() || ns.stopped.Load() || engNode.IsFailed() {
		ns.pendingReadsMu.Lock()
		_, present := ns.pendingReads[readKey]
		if present {
			delete(ns.pendingReads, readKey)
		}
		ns.pendingReadsMu.Unlock()
		if present {
			rs.releaseRef()
			h.completePendingOp()
		}
		// The caller's own reference is dropped whether or not the claim
		// won; a drain that beat us drops only its own.
		rs.Release()
		if engNode.IsFailed() {
			return nil, ErrShardFailed
		}
		return nil, ErrClosed
	}

	// Deliver to the ReadIndex queue; the step worker batches concurrent reads into one heartbeat round.
	if !engNode.DeliverReadIndex(readKey) {
		// Claim the entry before rolling back, so a concurrent completion is not released twice.
		ns.pendingReadsMu.Lock()
		_, claimed := ns.pendingReads[readKey]
		if claimed {
			delete(ns.pendingReads, readKey)
		}
		ns.pendingReadsMu.Unlock()
		if claimed {
			rs.releaseRef()
			h.completePendingOp()
		}
		rs.Release()
		if el := h.cfg.EventListener; el != nil && el.OnReadIndexDropped != nil {
			el.OnReadIndexDropped(config.EventShardInfo{
				ShardID:   shardID,
				ReplicaID: ns.replicaID,
			})
		}
		return nil, ErrSystemBusy
	}

	// Wake the step worker so it processes the ReadIndex queue.
	h.engine.NotifyWork(shardID)

	return rs, nil
}

// QueryLocalNode queries this replica's state machine with no consensus check; the result is not linearizable.
func (h *Host) QueryLocalNode(ctx context.Context, shardID uint64, query interface{}) (interface{}, error) {
	// An already-cancelled context never reaches the state machine.
	if err := ctx.Err(); err != nil {
		return nil, mapContextError(err)
	}
	if h.closed.Load() {
		return nil, ErrClosed
	}

	ns, err := h.getNode(shardID)
	if err != nil {
		return nil, err
	}

	// Shard is stopping/stopped: its state machine may be closing.
	if ns.stopped.Load() {
		return nil, ErrClosed
	}
	if ns.rsm == nil {
		return nil, ErrShardNotReady
	}
	// Refused while paused (see Host.PauseShard).
	if ns.paused.Load() {
		return nil, ErrShardPaused
	}
	// A sealed barrier refuses local reads too: sealing withdraws access to
	// the shard's data, not only the ability to write it.
	if ns.barrierSealed() {
		return nil, ErrBarrierSealed
	}

	return ns.rsm.Lookup(ctx, query)
}

// StaleRead queries this replica's state machine with no consensus check; the result may be arbitrarily stale.
func (h *Host) StaleRead(ctx context.Context, shardID uint64, query interface{}) (interface{}, error) {
	return h.QueryLocalNode(ctx, shardID, query)
}

// FollowerRead queries the local state machine once it has applied everything committed when the read began.
func (h *Host) FollowerRead(ctx context.Context, shardID uint64, query interface{}) (interface{}, error) {
	// An already-cancelled context never reaches the state machine.
	if err := ctx.Err(); err != nil {
		return nil, mapContextError(err)
	}
	if h.closed.Load() {
		return nil, ErrClosed
	}

	ns, err := h.getNode(shardID)
	if err != nil {
		return nil, err
	}

	// Witnesses vote but don't store log data; they cannot serve reads.
	if ns.cfg.IsWitness {
		return nil, ErrInvalidOperation
	}

	// Shard is stopping/stopped: its state machine may be closing.
	if ns.stopped.Load() {
		return nil, ErrClosed
	}
	if ns.rsm == nil {
		return nil, ErrShardNotReady
	}
	// Refused while paused (see Host.PauseShard).
	if ns.paused.Load() {
		return nil, ErrShardPaused
	}
	// A sealed barrier refuses local reads too: sealing withdraws access to
	// the shard's data, not only the ability to write it.
	if ns.barrierSealed() {
		return nil, ErrBarrierSealed
	}

	engNode := h.engine.GetNode(shardID)
	if engNode == nil {
		return nil, ErrShardNotReady
	}

	// A halted shard never catches up, so refuse before waiting.
	if engNode.IsFailed() {
		return nil, ErrShardFailed
	}

	committed := engNode.CommittedIndex()

	// Fast path: SM has already applied up to the committed index.
	if ns.lastApplied.Load() >= committed {
		return ns.rsm.Lookup(ctx, query)
	}

	// Poll with backoff until lastApplied catches up, the shard or host stops, or ctx ends.
	const (
		followerReadPollInterval = time.Millisecond
		followerReadFastPolls    = 50
		followerReadSlowInterval = 20 * time.Millisecond
	)
	ticker := time.NewTicker(followerReadPollInterval)
	defer ticker.Stop()
	polls := 0
	for {
		select {
		case <-ticker.C:
			if ns.lastApplied.Load() >= committed {
				return ns.rsm.Lookup(ctx, query)
			}
			if ns.stopped.Load() || h.closed.Load() {
				return nil, ErrClosed
			}
			// The latch can trip while this read is already waiting; the
			// entry it is waiting on is the one that failed.
			if engNode.IsFailed() {
				return nil, ErrShardFailed
			}
			polls++
			if polls == followerReadFastPolls {
				ticker.Reset(followerReadSlowInterval)
			}
		case <-ctx.Done():
			return nil, mapContextError(ctx.Err())
		}
	}
}

// ErrSessionsUnsupportedOnDisk is returned by session entry points on an sm.DiskStateMachine shard; propose without a session and make operations idempotent.
var ErrSessionsUnsupportedOnDisk = errors.New(
	"quicraft: client sessions are not supported on on-disk state machines")

// rejectSessionsOnDisk returns ErrSessionsUnsupportedOnDisk when the shard is
// backed by an on-disk state machine, which cannot carry a session table
// across a restart. See ErrSessionsUnsupportedOnDisk.
func rejectSessionsOnDisk(ns *nodeState) error {
	if ns.smType != rsm.SMTypeOnDisk.TypeCode() {
		return nil
	}
	return ErrSessionsUnsupportedOnDisk
}

// NewSession registers a new at-most-once client session through Raft and blocks until it applies or ctx ends.
func (h *Host) NewSession(ctx context.Context, shardID uint64) (*Session, error) {
	return h.NewSessionWithOwner(ctx, shardID, 0)
}

// NewSessionWithOwner is NewSession counted against ownerID's replicated session cap (Config.MaxSessionsPerOwner).
func (h *Host) NewSessionWithOwner(
	ctx context.Context, shardID, ownerID uint64,
) (*Session, error) {
	if h.closed.Load() {
		return nil, ErrClosed
	}

	ns, err := h.getNode(shardID)
	if err != nil {
		return nil, err
	}
	if err := rejectSessionsOnDisk(ns); err != nil {
		return nil, err
	}

	clientID, err := newClientID(rand.Reader)
	if err != nil {
		return nil, err
	}

	session := newSession(shardID, clientID, ownerID)
	session.PrepareForRegister()

	// Propose session registration through Raft.
	rs, err := h.proposeSession(ctx, ns, session)
	if err != nil {
		return nil, err
	}
	defer rs.Release()

	// Wait for the registration to be committed and applied, respecting
	// ctx cancellation/deadline so a cancelable-but-deadline-less context
	// does not block forever on quorum loss.
	result, err := awaitResult(ctx, rs)
	if err != nil {
		return nil, err
	}
	if result.Value != clientID {
		return nil, &SessionError{
			ClientID: clientID,
			Op:       "register",
			Err:      ErrInvalidOperation,
		}
	}

	// Registration succeeded — prepare session for normal proposals.
	session.PrepareForPropose()
	return session, nil
}

// clientIDRerollLimit bounds the zero-rejection re-roll; exhausting it means the entropy source is broken.
const clientIDRerollLimit = 8

// newClientID draws a non-zero session client ID from the system CSPRNG; zero marks an entry without a session.
func newClientID(entropy io.Reader) (uint64, error) {
	for i := 0; i < clientIDRerollLimit; i++ {
		var buf [8]byte
		if _, err := io.ReadFull(entropy, buf[:]); err != nil {
			return 0, &HostInitError{Field: "session_rand", Err: err}
		}
		clientID := binary.LittleEndian.Uint64(buf[:])
		if clientID != proto.NotSessionManagedClientID {
			return clientID, nil
		}
	}
	return 0, &HostInitError{Field: "session_rand", Err: ErrEntropyDegenerate}
}

// ErrEntropyDegenerate is returned when the system CSPRNG returned the rejected value on every attempt.
var ErrEntropyDegenerate = errors.New(
	"quicraft: the system random source returned a degenerate value repeatedly")

// RegisterSession registers session through Raft, keeping its existing client ID.
func (h *Host) RegisterSession(ctx context.Context, session *Session) (*Session, error) {
	if h.closed.Load() {
		return nil, ErrClosed
	}

	if session == nil {
		return nil, ErrInvalidSession
	}

	clientID := session.ClientID()
	if clientID == 0 {
		return nil, &SessionError{
			ClientID: clientID,
			Op:       "register",
			Err:      ErrInvalidSession,
		}
	}

	shardID := session.ShardID()
	ns, err := h.getNode(shardID)
	if err != nil {
		return nil, err
	}
	if err := rejectSessionsOnDisk(ns); err != nil {
		return nil, err
	}

	session.PrepareForRegister()

	// Propose session registration through Raft.
	rs, err := h.proposeSession(ctx, ns, session)
	if err != nil {
		return nil, err
	}
	defer rs.Release()

	// Wait for the registration to be committed and applied, respecting
	// ctx cancellation/deadline (see awaitResult).
	result, err := awaitResult(ctx, rs)
	if err != nil {
		return nil, err
	}
	if result.Value != clientID {
		return nil, &SessionError{
			ClientID: clientID,
			Op:       "register",
			Err:      ErrInvalidOperation,
		}
	}

	// Registration succeeded — prepare session for normal proposals.
	session.PrepareForPropose()
	return session, nil
}

// CloseSession proposes the session's unregistration; ErrSessionsUnsupportedOnDisk on an on-disk shard.
func (h *Host) CloseSession(ctx context.Context, shardID uint64, session *Session) (*RequestState, error) {
	if h.closed.Load() {
		return nil, ErrClosed
	}

	ns, err := h.getNode(shardID)
	if err != nil {
		return nil, err
	}
	if err := rejectSessionsOnDisk(ns); err != nil {
		return nil, err
	}

	if session == nil {
		return nil, ErrInvalidOperation
	}

	if !session.ValidForSessionOp(shardID) {
		return nil, ErrInvalidSession
	}

	session.PrepareForUnregister()

	return h.proposeSession(ctx, ns, session)
}

// proposeSession proposes a session register or unregister prepared with PrepareForRegister or PrepareForUnregister.
func (h *Host) proposeSession(ctx context.Context, ns *nodeState, session *Session) (*RequestState, error) {
	// An already-cancelled context appends no session entry.
	if err := ctx.Err(); err != nil {
		return nil, mapContextError(err)
	}
	if h.closed.Load() {
		return nil, ErrClosed
	}
	if h.draining.Load() {
		return nil, ErrDraining
	}
	// Refused while paused (see Host.PauseShard).
	if ns.paused.Load() {
		return nil, ErrShardPaused
	}
	// A register or unregister entry is written to the log like any other
	// proposal, so a sealed barrier refuses it the same way.
	if ns.barrierSealed() {
		return nil, ErrBarrierSealed
	}

	// If there is no known leader, fail fast. When a leader is known,
	// allow the Raft engine to forward the proposal automatically.
	if !ns.isLeader.Load() && ns.leaderID.Load() == 0 {
		return nil, h.buildNotLeaderError(ns)
	}

	// Build the session entry; OwnerID is stamped here, the only way it enters replicated state.
	entry := proto.Entry{
		ClientID:    session.ClientID(),
		SeriesID:    session.SeriesID(),
		RespondedTo: session.RespondedTo(),
		OwnerID:     session.OwnerID(),
	}
	return h.proposeEntry(ctx, ns, session.ShardID(), entry, nil)
}

// NoOPSession returns a session with client ID 0 and no state, for idempotent proposals; ProposeWithSession refuses it.
func (h *Host) NoOPSession(shardID uint64) *Session {
	return NewNoOPSession(shardID)
}

// SyncGetShardMembership returns the shard's committed membership after confirming leadership through ReadIndex.
func (h *Host) SyncGetShardMembership(ctx context.Context, shardID uint64) (*Membership, error) {
	// Step 1: Linearizable fence via ReadIndex.
	rs, err := h.ReadIndex(ctx, shardID)
	if err != nil {
		return nil, err
	}
	defer rs.Release()

	// Step 2: Wait for the SM to catch up to the confirmed read index,
	// respecting context cancellation and deadlines.
	select {
	case result := <-rs.ResultC():
		if result.Err != nil {
			return nil, result.Err
		}
	case <-ctx.Done():
		if ctx.Err() == context.DeadlineExceeded {
			return nil, ErrTimeout
		}
		return nil, ErrCanceled
	}

	// Step 3: Read membership from the Raft peer. GetMembership returns a
	// deep copy (all maps are copied), so this is safe outside the step
	// worker goroutine.
	return h.getMembershipFromPeer(shardID)
}

// ShardMembership returns the shard's membership from local state, which may be stale.
func (h *Host) ShardMembership(shardID uint64) (*Membership, error) {
	if h.closed.Load() {
		return nil, ErrClosed
	}
	return h.getMembershipFromPeer(shardID)
}

// getMembershipFromPeer returns a copy of the peer's committed membership; safe from any goroutine.
func (h *Host) getMembershipFromPeer(shardID uint64) (*Membership, error) {
	engNode := h.engine.GetNode(shardID)
	if engNode == nil || engNode.Peer() == nil {
		return nil, ErrShardNotReady
	}

	pm := engNode.Peer().GetMembership()
	return protoMembershipToPublic(&pm), nil
}

// configSessionPolicy reads the local session limits from cfg, used only to seed the replicated policy and as its fallback.
func configSessionPolicy(cfg config.Config) proto.SessionPolicy {
	return proto.SessionPolicy{
		ExpiryEntries:          cfg.SessionExpiryEntries,
		MaxTotalSessions:       cfg.MaxTotalSessions,
		MaxResponsesPerSession: cfg.MaxResponsesPerSession,
		MaxSessionsPerOwner:    cfg.MaxSessionsPerOwner,
	}
}

// protoMembershipToPublic converts a proto.Membership to a copied public Membership.
func protoMembershipToPublic(pm *proto.Membership) *Membership {
	m := &Membership{
		ConfigChangeID:             pm.ConfigChangeID,
		MaxWitnesses:               pm.MaxWitnesses,
		RequireFullReplicaMajority: pm.RequireFullReplicaMajority,
		SessionExpiryEntries:       pm.SessionExpiryEntries,
		MaxTotalSessions:           pm.MaxTotalSessions,
		MaxResponsesPerSession:     pm.MaxResponsesPerSession,
		MaxSessionsPerOwner:        pm.MaxSessionsPerOwner,
		WireVersion:                pm.WireVersion,
		Nodes:                      make(map[uint64]string, len(pm.Addresses)),
		Observers:                  make(map[uint64]string, len(pm.Observers)),
		Witnesses:                  make(map[uint64]string, len(pm.Witnesses)),
		Removed:                    make(map[uint64]struct{}, len(pm.Removed)),
		Identities:                 make(map[uint64]string, len(pm.Identities)),
		RemovedIdentities:          make(map[uint64]string, len(pm.RemovedIdentities)),
	}
	for k, v := range pm.Identities {
		m.Identities[k] = v
	}
	for k, v := range pm.RemovedIdentities {
		m.RemovedIdentities[k] = v
	}
	for k, v := range pm.Addresses {
		m.Nodes[k] = v
	}
	for k, v := range pm.Observers {
		m.Observers[k] = v
	}
	for k, v := range pm.Witnesses {
		m.Witnesses[k] = v
	}
	for k := range pm.Removed {
		m.Removed[k] = struct{}{}
	}
	return m
}

// ShardInfo returns a point-in-time snapshot of the shard's status.
func (h *Host) ShardInfo(shardID uint64) (*ShardInfo, error) {
	if h.closed.Load() {
		return nil, ErrClosed
	}

	ns, err := h.getNode(shardID)
	if err != nil {
		return nil, err
	}

	return h.buildShardInfo(ns), nil
}

// LeaderID returns the current leader's replicaID for the given shard
// and whether a leader is known. The result is from local in-memory state
// and may be stale.
func (h *Host) LeaderID(shardID uint64) (uint64, bool, error) {
	if h.closed.Load() {
		return 0, false, ErrClosed
	}

	ns, err := h.getNode(shardID)
	if err != nil {
		return 0, false, err
	}

	leaderID := ns.leaderID.Load()
	return leaderID, leaderID != 0, nil
}

// LeaderIDAndTerm returns the shard's leader, term and whether a leader is known, read consistently through the seqlock.
func (h *Host) LeaderIDAndTerm(shardID uint64) (uint64, uint64, bool, error) {
	if h.closed.Load() {
		return 0, 0, false, ErrClosed
	}

	ns, err := h.getNode(shardID)
	if err != nil {
		return 0, 0, false, err
	}

	leaderID, term, _ := ns.leaderAndTerm()
	return leaderID, term, leaderID != 0, nil
}

// ShardReadReady reports, without blocking, whether a ReadIndex here would pass Raft's read gate, with the leader and term it is about.
func (h *Host) ShardReadReady(shardID uint64) (leaderID, term uint64, ready bool, err error) {
	if h.closed.Load() {
		return 0, 0, false, ErrClosed
	}
	if h.draining.Load() {
		return 0, 0, false, ErrDraining
	}
	ns, err := h.getNode(shardID)
	if err != nil {
		return 0, 0, false, err
	}
	if ns.cfg.IsWitness {
		return 0, 0, false, ErrInvalidOperation
	}
	engNode := h.engine.GetNode(shardID)
	if engNode == nil || engNode.Peer() == nil {
		return 0, 0, false, ErrShardNotReady
	}
	if engNode.IsFailed() {
		return 0, 0, false, ErrShardFailed
	}
	leaderID, term, _, ready = ns.leaderTermReady()
	return leaderID, term, ready, nil
}

// RequestLeaderTransfer asks the leader to transfer to targetReplicaID, which must be a current voter; anything else is *InvalidMemberError.
func (h *Host) RequestLeaderTransfer(ctx context.Context, shardID, targetReplicaID uint64) error {
	if err := ctx.Err(); err != nil {
		return mapContextError(err)
	}
	if h.closed.Load() {
		return ErrClosed
	}

	ns, err := h.getNode(shardID)
	if err != nil {
		return err
	}

	// Refused while paused (see Host.PauseShard).
	if ns.paused.Load() {
		return ErrShardPaused
	}

	if !ns.isLeader.Load() {
		return h.buildNotLeaderError(ns)
	}

	engNode := h.engine.GetNode(shardID)
	if engNode == nil || engNode.Peer() == nil {
		return ErrShardNotReady
	}

	// Validate the target here: the raft layer's error is only logged.
	if targetReplicaID == 0 {
		return &InvalidMemberError{
			ShardID:   shardID,
			ReplicaID: targetReplicaID,
			Reason:    "leader transfer target must be non-zero",
		}
	}
	if targetReplicaID == ns.replicaID {
		// Transfer to self: the target already holds leadership, so the
		// requested end state is already true. Idempotent no-op success.
		return nil
	}
	membership := engNode.Peer().GetMembership()
	if _, isVoter := membership.Addresses[targetReplicaID]; !isVoter {
		if _, removed := membership.Removed[targetReplicaID]; removed {
			return &InvalidMemberError{
				ShardID:   shardID,
				ReplicaID: targetReplicaID,
				Reason:    "leader transfer target was removed from the shard",
			}
		}
		if _, isObserver := membership.Observers[targetReplicaID]; isObserver {
			return &InvalidMemberError{
				ShardID:   shardID,
				ReplicaID: targetReplicaID,
				Reason:    "leader transfer target is a non-voting observer",
			}
		}
		if _, isWitness := membership.Witnesses[targetReplicaID]; isWitness {
			return &InvalidMemberError{
				ShardID:   shardID,
				ReplicaID: targetReplicaID,
				Reason:    "leader transfer target is a witness",
			}
		}
		return &ReplicaNotFoundError{ShardID: shardID, ReplicaID: targetReplicaID}
	}

	// Check context again before delivery.
	if err := ctx.Err(); err != nil {
		return mapContextError(err)
	}

	// Deliver the leader transfer via the engine inbox to respect the
	// single-owner invariant on the Peer.
	msg := proto.Message{
		Type: proto.LeaderTransfer,
		From: ns.replicaID,
		Hint: targetReplicaID,
	}
	if !engNode.DeliverLocal(msg) {
		return ErrSystemBusy
	}
	h.engine.NotifyWork(shardID)

	h.logger.Info("leader transfer requested",
		"shard_id", shardID,
		"target_replica_id", targetReplicaID,
	)
	return nil
}

// PauseShard stops the local replica without unloading it, so a seal can withdraw its key with no work in flight; ResumeShard restarts it.
func (h *Host) PauseShard(ctx context.Context, shardID uint64) error {
	// No early ctx check: a ctx error always means the shard is paused, even
	// for a ctx already done on entry.
	if h.closed.Load() {
		return ErrClosed
	}
	ns, err := h.getNode(shardID)
	if err != nil {
		return err
	}
	engNode := h.engine.GetNode(shardID)
	if engNode == nil {
		return ErrShardNotFound
	}

	// Refuse new work first; the gate is taken without ctx.
	ns.pauseGate.lock(nil)
	if !ns.paused.Swap(true) && ns.isLeader.Load() {
		h.transferBeforePause(ctx, ns, engNode)
	}
	h.engine.BeginPause(engNode)
	ns.pauseGate.unlock()
	if err := h.engine.AwaitPause(ctx, engNode); err != nil {
		// The shard stopped while the pause waited.
		if errors.Is(err, engine.ErrStopped) {
			return ErrClosed
		}
		return err
	}
	return nil
}

// transferBeforePause hands leadership to the most up-to-date reachable voter and waits up to one election timeout for this replica to step down.
func (h *Host) transferBeforePause(ctx context.Context, ns *nodeState, engNode *engine.Node) {
	timeout := time.Duration(ns.cfg.ElectionRTT*h.cfg.RTTMillisecond) * time.Millisecond
	deadline := time.NewTimer(timeout)
	defer deadline.Stop()
	tick := time.NewTicker(time.Duration(h.cfg.RTTMillisecond) * time.Millisecond)
	defer tick.Stop()

	requested := false
	for ns.isLeader.Load() {
		if !requested {
			// The progress view is published by the step worker once it is
			// asked for, so the first read may still be empty.
			if target := pauseTransferTarget(engNode.ReplicaProgress(), ns.replicaID); target != 0 {
				msg := proto.Message{Type: proto.LeaderTransfer, From: ns.replicaID, Hint: target}
				if engNode.DeliverLocal(msg) {
					h.engine.NotifyWork(ns.shardID)
					requested = true
					h.logger.Info("leader transfer before pause",
						"shard_id", ns.shardID,
						"target_replica_id", target,
					)
				}
			}
		}
		select {
		case <-ctx.Done():
			return
		case <-deadline.C:
			if requested {
				h.logger.Warn("leader transfer before pause did not finish; pausing",
					"shard_id", ns.shardID,
				)
			}
			return
		case <-tick.C:
		}
	}
}

// pauseGate serializes one shard's pause and resume transitions; the zero value is ready.
type pauseGate struct {
	once sync.Once
	ch   chan struct{}
}

// lock takes the gate, or reports false if done closes first. A nil done
// waits for as long as the holder: a pause holds the gate for at most its
// leadership transfer, one election timeout.
func (g *pauseGate) lock(done <-chan struct{}) bool {
	g.once.Do(func() { g.ch = make(chan struct{}, 1) })
	select {
	case g.ch <- struct{}{}:
		return true
	case <-done:
		return false
	}
}

func (g *pauseGate) unlock() { <-g.ch }

// pauseTransferTarget picks the leadership-transfer target for a pausing
// leader: the active voter with the highest match index, 0 if none.
func pauseTransferTarget(progress []raft.RemoteProgress, self uint64) uint64 {
	var target, match uint64
	for _, p := range progress {
		if p.ReplicaID == self || p.Role != raft.RoleVoter || !p.Active {
			continue
		}
		if target == 0 || p.Match > match {
			target, match = p.ReplicaID, p.Match
		}
	}
	return target
}

// ResumeShard restarts a paused replica after its barrier is unsealed; it rejoins as a follower and applies what the pause held.
func (h *Host) ResumeShard(ctx context.Context, shardID uint64) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if h.closed.Load() {
		return ErrClosed
	}
	ns, err := h.getNode(shardID)
	if err != nil {
		return err
	}
	engNode := h.engine.GetNode(shardID)
	if engNode == nil {
		return ErrShardNotFound
	}
	// A pause in its leadership transfer holds the gate; ctx bounds the wait.
	if !ns.pauseGate.lock(ctx.Done()) {
		return ctx.Err()
	}
	wasPaused := ns.paused.Load()
	h.engine.ResumeNode(engNode)
	// Restart the leaderless window before publishing the shard as unpaused.
	if wasPaused {
		ns.restartLeaderlessWindow(h.leaderlessNow())
	}
	ns.paused.Store(false)
	ns.pauseGate.unlock()
	return nil
}

// RequestSnapshot requests a snapshot of the shard; the returned RequestState carries the snapshot index in Value.
func (h *Host) RequestSnapshot(ctx context.Context, shardID uint64, opt SnapshotOption) (*RequestState, error) {
	// An already-cancelled context claims no snapshot slot.
	if err := ctx.Err(); err != nil {
		return nil, mapContextError(err)
	}
	if h.closed.Load() {
		return nil, ErrClosed
	}

	// Validate per-request options before acquiring any resources.
	if err := opt.Validate(); err != nil {
		return nil, err
	}

	ns, err := h.getNode(shardID)
	if err != nil {
		return nil, err
	}

	// Refused while paused (see Host.PauseShard).
	if ns.paused.Load() {
		return nil, ErrShardPaused
	}
	// A snapshot is written under the shard's barrier; refuse up front
	// rather than claim the slot for a save that cannot encrypt.
	if ns.barrierSealed() {
		return nil, ErrBarrierSealed
	}

	engNode := h.engine.GetNode(shardID)
	if engNode == nil {
		return nil, ErrShardNotReady
	}

	// Atomically claim the snapshotting slot for this shard.
	if !engNode.TryStartSnapshot() {
		return nil, ErrSnapshotInProgress
	}

	// Generate a unique key for tracking this snapshot request.
	// Uses a dedicated counter separate from proposals and reads.
	snapshotKey := nextRequestKey()

	// A snapshot save is local work sized by the state machine, not by the
	// commit path, so it gets the snapshot backstop rather than the shorter
	// commit-path one (see defaultPendingSnapshotTimeout).
	deadline := pendingDeadlineWithin(ctx, defaultPendingSnapshotTimeout)
	rs := newRequestState(ns.requestPool, snapshotKey, deadline, h.metrics != nil)

	// Admission counts the operation for Drain, or refuses it and clears the snapshotting flag.
	if err := h.admitPendingOp(); err != nil {
		rs.Release()
		engNode.ClearSnapshotting()
		return nil, err
	}

	// Register the pending snapshot before submitting to avoid a race
	// where OnSnapshotRequestCompleted fires before the map entry exists.
	// acquireRef pins the RS until this map entry is removed.
	rs.acquireRef()
	ns.pendingSnapshotsMu.Lock()
	ns.pendingSnapshots[snapshotKey] = rs
	ns.pendingSnapshotsMu.Unlock()

	// Re-check closed after registering and roll back, so the RS is never orphaned.
	if h.closed.Load() || ns.stopped.Load() {
		ns.pendingSnapshotsMu.Lock()
		_, present := ns.pendingSnapshots[snapshotKey]
		if present {
			delete(ns.pendingSnapshots, snapshotKey)
		}
		ns.pendingSnapshotsMu.Unlock()
		if present {
			rs.releaseRef()
			h.completePendingOp()
		}
		// The caller's own reference is dropped whether or not the claim
		// won; a sweep that beat us drops only its own.
		rs.Release()
		engNode.ClearSnapshotting()
		return nil, ErrClosed
	}

	// Build engine-level option from the public SnapshotOption.
	engineOpt := engine.SnapshotRequestOption{
		ExportPath:         opt.ExportPath,
		CompactionOverhead: opt.CompactionOverhead,
		OverrideCompaction: opt.OverrideCompactionOverhead,
		Exported:           opt.IsExport(),
		RequestKey:         snapshotKey,
	}

	// Submit the snapshot request to the engine's snapshot pool.
	if submitErr := h.engine.RequestSnapshot(shardID, ns.replicaID, engNode, engineOpt); submitErr != nil {
		// The pool refused: claim the entry before cleaning up, so nothing is released twice.
		engNode.ClearSnapshotting()
		ns.pendingSnapshotsMu.Lock()
		_, claimed := ns.pendingSnapshots[snapshotKey]
		if claimed {
			delete(ns.pendingSnapshots, snapshotKey)
		}
		ns.pendingSnapshotsMu.Unlock()
		if claimed {
			rs.releaseRef()
			h.completePendingOp()
		}
		rs.Release()
		return nil, translateSnapshotSubmitError(submitErr)
	}

	return rs, nil
}

// ErrSnapshotEncryptedBarrierUnavailable means an encrypted snapshot cannot be read because its barrier is absent or sealed.
var ErrSnapshotEncryptedBarrierUnavailable = engine.ErrSnapshotEncryptedBarrierUnavailable

// ErrSnapshotBarrierSealed means a snapshot could not be written because the shard's barrier is sealed.
var ErrSnapshotBarrierSealed = engine.ErrSnapshotBarrierSealed

// snapshotArtifactMagic identifies a QuicRaft exported-snapshot artifact.
// It is written as the first 8 bytes of every export so that ImportSnapshot
// can detect a well-formed artifact (and reject foreign/legacy data).
var snapshotArtifactMagic = [8]byte{'Q', 'R', 'S', 'N', 'A', 'P', '0', '1'}

// snapshotArtifactVersion is the artifact header version; version 2 added the Compression field.
const snapshotArtifactVersion uint32 = 2

// previousSnapshotArtifactVersion is the header version the previous minor release wrote (docs/compatibility).
const previousSnapshotArtifactVersion = snapshotArtifactVersion

// acceptsSnapshotArtifactVersion reports whether v is in the closed read set.
// It is a set, not a range, so it never widens when a constant moves.
func acceptsSnapshotArtifactVersion(v uint32) bool {
	return v == snapshotArtifactVersion || v == previousSnapshotArtifactVersion
}

// snapshotArtifactFixedHeaderSize is the size of the fixed portion of the
// artifact header that precedes the variable-length membership block:
//
//	magic[8] | version:u32 | index:u64 | term:u64 | onDiskIndex:u64 |
//	compression:u16 | membLen:u32
const snapshotArtifactFixedHeaderSize = 8 + 4 + 8 + 8 + 8 + 2 + 4

// maxSnapshotArtifactMembLen bounds an imported header's membership block before allocation.
const maxSnapshotArtifactMembLen = 16 * 1024 * 1024

// snapshotArtifactHeader precedes an exported artifact's data with its Index, Term, OnDiskIndex, membership and compression.
type snapshotArtifactHeader struct {
	Index       uint64
	Term        uint64
	OnDiskIndex uint64
	Compression config.CompressionType
	Membership  proto.Membership
}

// marshalSnapshotArtifactHeader encodes the header into a freshly allocated
// byte slice using the fixed little-endian layout. The membership is encoded
// via proto.Membership.MarshalTo and length-prefixed.
func marshalSnapshotArtifactHeader(h snapshotArtifactHeader) ([]byte, error) {
	membSize := h.Membership.Size()
	buf := make([]byte, snapshotArtifactFixedHeaderSize+membSize)
	copy(buf[0:8], snapshotArtifactMagic[:])
	binary.LittleEndian.PutUint32(buf[8:12], snapshotArtifactVersion)
	binary.LittleEndian.PutUint64(buf[12:20], h.Index)
	binary.LittleEndian.PutUint64(buf[20:28], h.Term)
	binary.LittleEndian.PutUint64(buf[28:36], h.OnDiskIndex)
	binary.LittleEndian.PutUint16(buf[36:38], uint16(h.Compression))
	binary.LittleEndian.PutUint32(buf[38:42], uint32(membSize))
	if membSize > 0 {
		if _, err := h.Membership.MarshalTo(buf[snapshotArtifactFixedHeaderSize:]); err != nil {
			return nil, err
		}
	}
	return buf, nil
}

// readSnapshotArtifactHeader reads and validates the artifact header from r, returning it and its raw bytes.
func readSnapshotArtifactHeader(r io.Reader) (hdr snapshotArtifactHeader, raw []byte, err error) {
	var fixed [snapshotArtifactFixedHeaderSize]byte
	if _, err := io.ReadFull(r, fixed[:]); err != nil {
		return snapshotArtifactHeader{}, nil, &SnapshotArtifactError{Reason: "read header", Err: err}
	}
	if !bytesEqual(fixed[0:8], snapshotArtifactMagic[:]) {
		return snapshotArtifactHeader{}, nil, &SnapshotArtifactError{Reason: "bad magic"}
	}
	version := binary.LittleEndian.Uint32(fixed[8:12])
	if !acceptsSnapshotArtifactVersion(version) {
		return snapshotArtifactHeader{}, nil, &SnapshotArtifactError{
			Reason: fmt.Sprintf("unsupported version %d (want %d)", version, snapshotArtifactVersion),
		}
	}
	hdr = snapshotArtifactHeader{
		Index:       binary.LittleEndian.Uint64(fixed[12:20]),
		Term:        binary.LittleEndian.Uint64(fixed[20:28]),
		OnDiskIndex: binary.LittleEndian.Uint64(fixed[28:36]),
		Compression: config.CompressionType(binary.LittleEndian.Uint16(fixed[36:38])),
	}
	// Index 0 means no snapshot and is refused.
	if hdr.Index == 0 {
		return snapshotArtifactHeader{}, nil, &SnapshotArtifactError{
			Reason: "snapshot index is zero",
		}
	}
	// An algorithm this binary cannot decompress makes the payload
	// unreadable; reject the artifact rather than import bytes that will be
	// handed to a state machine in an unknown encoding.
	if !hdr.Compression.IsValid() {
		return snapshotArtifactHeader{}, nil, &SnapshotArtifactError{
			Reason: fmt.Sprintf("unsupported compression %d", uint16(hdr.Compression)),
		}
	}
	membLen := binary.LittleEndian.Uint32(fixed[38:42])
	if membLen > maxSnapshotArtifactMembLen {
		return snapshotArtifactHeader{}, nil, &SnapshotArtifactError{
			Reason: fmt.Sprintf("membership length %d exceeds maximum %d", membLen, maxSnapshotArtifactMembLen),
		}
	}
	raw = make([]byte, snapshotArtifactFixedHeaderSize+int(membLen))
	copy(raw, fixed[:])
	if membLen > 0 {
		membBuf := raw[snapshotArtifactFixedHeaderSize:]
		if _, err := io.ReadFull(r, membBuf); err != nil {
			return snapshotArtifactHeader{}, nil, &SnapshotArtifactError{Reason: "read membership", Err: err}
		}
		if _, err := hdr.Membership.UnmarshalFrom(membBuf); err != nil {
			return snapshotArtifactHeader{}, nil, &SnapshotArtifactError{Reason: "decode membership", Err: err}
		}
	}
	return hdr, raw, nil
}

// bytesEqual reports whether two byte slices are equal. Used for the
// fixed-length magic comparison without pulling in bytes.Equal at the
// single call site.
func bytesEqual(a, b []byte) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

// SnapshotArtifactError indicates that an exported snapshot artifact could
// not be parsed during ImportSnapshot, for example because the magic or
// version did not match or the embedded membership was malformed.
type SnapshotArtifactError struct {
	Reason string
	Err    error
}

// Error returns a human-readable description of the artifact parse failure.
func (e *SnapshotArtifactError) Error() string {
	if e.Err != nil {
		return fmt.Sprintf("quicraft: invalid snapshot artifact: %s: %v", e.Reason, e.Err)
	}
	return fmt.Sprintf("quicraft: invalid snapshot artifact: %s", e.Reason)
}

// Unwrap returns the underlying error, if any.
func (e *SnapshotArtifactError) Unwrap() error { return e.Err }

// ExportSnapshot writes the shard's latest snapshot to w as a self-describing artifact: header, plaintext data and an integrity trailer.
func (h *Host) ExportSnapshot(ctx context.Context, shardID uint64, w io.Writer) error {
	if h.closed.Load() {
		return ErrClosed
	}

	if h.logdb == nil {
		return ErrSnapshotNotFound
	}

	ns, err := h.getNode(shardID)
	if err != nil {
		return err
	}

	logdbSnap, err := h.logdb.GetSnapshot(shardID, ns.replicaID)
	if err != nil {
		return err
	}
	if logdbSnap.Index == 0 {
		return ErrSnapshotNotFound
	}

	snapDir := logdbSnap.Filepath
	if snapDir == "" {
		return ErrSnapshotNotFound
	}

	dataPath := filepath.Join(snapDir, "snapshot.dat")
	f, err := openVerifiedSnapshotFile(h.snapshotFsys(), dataPath)
	if err != nil {
		return ErrSnapshotNotFound
	}
	defer func() {
		if closeErr := f.Close(); closeErr != nil {
			slog.Debug("snapshot export file close failed",
				"shard", shardID,
				"path", dataPath,
				"error", closeErr)
		}
	}()

	// Resolve this shard's own barrier, as the receive and recover paths do.
	barrier, barrierErr := h.barrierForShard(shardID)
	if barrierErr != nil {
		return barrierErr
	}

	// Refuse early if the snapshot is encrypted but cannot be decrypted, so
	// we never emit a (partial) artifact header for an unrecoverable export.
	if logdbSnap.Epoch > 0 && (barrier == nil || barrier.IsSealed()) {
		return ErrSnapshotEncryptedBarrierUnavailable
	}
	if err := ctx.Err(); err != nil {
		return mapContextError(err)
	}

	// The header carries the original snapshot identity for ImportSnapshot.
	headerBytes, err := marshalSnapshotArtifactHeader(snapshotArtifactHeader{
		Index:       logdbSnap.Index,
		Term:        logdbSnap.Term,
		OnDiskIndex: logdbSnap.OnDiskIndex,
		Compression: logdbSnap.Compression,
		Membership:  logdbMembershipToProto(logdbSnap.Membership),
	})
	if err != nil {
		return err
	}

	// Decrypt an at-rest encrypted snapshot before writing it.
	if logdbSnap.Epoch > 0 {
		// Pin the snapshot epoch for the read so a purge cannot remove its DEK.
		barrier.RegisterInFlightEpoch(logdbSnap.Epoch)
		defer barrier.DeregisterInFlightEpoch(logdbSnap.Epoch)

		// ctxWriter stops the decrypt promptly when ctx is cancelled.
		hw := &deferredHeaderWriter{w: &ctxWriter{ctx: ctx, w: w}, header: headerBytes}
		pw := newArtifactPayloadWriter(hw, headerBytes)
		if _, err = decryptSnapshotFrames(barrier, shardID, ns.replicaID, logdbSnap.Index, f, pw); err != nil {
			return err
		}
		// Check the recorded integrity; the trailer is withheld on failure.
		if err = f.Verify(); err != nil {
			return err
		}
		// The trailer goes through hw, so a frameless (empty-data) stream
		// that never triggered a write still emits the header first: header
		// and trailer alone are a complete, importable artifact.
		_, err = hw.Write(pw.trailer())
		return err
	}

	// A plaintext snapshot is copied raw, withholding its final bytes until the record check passes.
	hw := &deferredHeaderWriter{w: w, header: headerBytes}
	pw := newArtifactPayloadWriter(hw, headerBytes)
	if err = copyVerifiedWithContext(ctx, pw, f, f.Verify); err != nil {
		return err
	}
	// The trailer is written last and only here, once the payload has passed
	// its check. An empty data file wrote no payload; the header is flushed
	// ahead of the trailer and the two are the artifact.
	_, err = hw.Write(pw.trailer())
	return err
}

// copyVerifiedWithContext copies src to dst, withholding the final bytes until verify accepts everything read.
func copyVerifiedWithContext(ctx context.Context, dst io.Writer, src io.Reader, verify func() error) error {
	bufs := [2][]byte{make([]byte, copyWithContextChunkSize), make([]byte, copyWithContextChunkSize)}
	var held []byte
	for i := 0; ; i ^= 1 {
		if err := ctx.Err(); err != nil {
			return mapContextError(err)
		}
		n, readErr := io.ReadFull(src, bufs[i])
		if errors.Is(readErr, io.EOF) || errors.Is(readErr, io.ErrUnexpectedEOF) {
			if err := verify(); err != nil {
				return err
			}
			if len(held) > 0 {
				if _, err := dst.Write(held); err != nil {
					return err
				}
			}
			if n > 0 {
				if _, err := dst.Write(bufs[i][:n]); err != nil {
					return err
				}
			}
			return nil
		}
		if readErr != nil {
			return readErr
		}
		if len(held) > 0 {
			if _, err := dst.Write(held); err != nil {
				return err
			}
		}
		held = bufs[i][:n]
	}
}

// ctxWriter fails a Write, before writing, once its context is done.
type ctxWriter struct {
	ctx context.Context
	w   io.Writer
}

// Write emits p unless the context is already done.
func (c *ctxWriter) Write(p []byte) (int, error) {
	if err := c.ctx.Err(); err != nil {
		return 0, mapContextError(err)
	}
	return c.w.Write(p)
}

// deferredHeaderWriter withholds a header until the first payload write, so an early failure writes nothing.
type deferredHeaderWriter struct {
	w      io.Writer
	header []byte // pending header; nil once written
}

// Write emits the pending header (once) before the first payload bytes.
func (d *deferredHeaderWriter) Write(p []byte) (int, error) {
	if err := d.flushHeader(); err != nil {
		return 0, err
	}
	return d.w.Write(p)
}

// flushHeader writes the header if it has not been written yet. Idempotent.
func (d *deferredHeaderWriter) flushHeader() error {
	if d.header == nil {
		return nil
	}
	header := d.header
	d.header = nil
	_, err := d.w.Write(header)
	return err
}

// protoMembershipToLogDB converts a proto.Membership to a deep-copied logdb.Membership, Identities included.
func protoMembershipToLogDB(m proto.Membership) logdb.Membership {
	result := logdb.Membership{
		ConfigChangeID:             m.ConfigChangeID,
		MaxWitnesses:               m.MaxWitnesses,
		RequireFullReplicaMajority: m.RequireFullReplicaMajority,
		SessionExpiryEntries:       m.SessionExpiryEntries,
		MaxTotalSessions:           m.MaxTotalSessions,
		MaxResponsesPerSession:     m.MaxResponsesPerSession,
		MaxSessionsPerOwner:        m.MaxSessionsPerOwner,
		WireVersion:                m.WireVersion,
	}
	if len(m.Addresses) > 0 {
		result.Addresses = make(map[uint64]string, len(m.Addresses))
		for k, v := range m.Addresses {
			result.Addresses[k] = v
		}
	}
	if len(m.Observers) > 0 {
		result.Observers = make(map[uint64]string, len(m.Observers))
		for k, v := range m.Observers {
			result.Observers[k] = v
		}
	}
	if len(m.Witnesses) > 0 {
		result.Witnesses = make(map[uint64]string, len(m.Witnesses))
		for k, v := range m.Witnesses {
			result.Witnesses[k] = v
		}
	}
	if len(m.Removed) > 0 {
		result.Removed = make(map[uint64]bool, len(m.Removed))
		for k, v := range m.Removed {
			result.Removed[k] = v
		}
	}
	if len(m.Identities) > 0 {
		result.Identities = make(map[uint64]string, len(m.Identities))
		for k, v := range m.Identities {
			result.Identities[k] = v
		}
	}
	if len(m.RemovedIdentities) > 0 {
		result.RemovedIdentities = make(map[uint64]string, len(m.RemovedIdentities))
		for k, v := range m.RemovedIdentities {
			result.RemovedIdentities[k] = v
		}
	}
	return result
}

// ImportOption configures ImportSnapshot.
type ImportOption func(*importOptions)

// importOptions carries operator assertions ImportSnapshot cross-checks against the shard's bootstrap record.
type importOptions struct {
	dataDir       string
	dataDirSet    bool
	membership    map[uint64]string
	membershipSet bool
	identities    map[uint64]string
}

// WithImportMembership replaces the artifact's membership, to reconstitute a shard whose quorum is permanently lost from its survivors.
func WithImportMembership(members map[uint64]string) ImportOption {
	return func(o *importOptions) {
		// Copy: the caller's map must not be able to change under the import,
		// and this value ends up in a persisted snapshot record.
		cp := make(map[uint64]string, len(members))
		for id, addr := range members {
			cp[id] = addr
		}
		o.membership = cp
		o.membershipSet = true
	}
}

// WithImportMemberIdentities supplies node identities for the imported membership, overriding the artifact's per replica.
func WithImportMemberIdentities(identities map[uint64]string) ImportOption {
	return func(o *importOptions) {
		o.identities = copyIdentityMap(identities)
	}
}

// importMembershipFor resolves the imported membership and requires an identity for every active member on a host with a transport.
func importMembershipFor(
	shardID, replicaID uint64,
	artifact proto.Membership,
	opts []ImportOption,
	requireIdentities bool,
) (proto.Membership, error) {
	var iopts importOptions
	for _, opt := range opts {
		opt(&iopts)
	}
	ms, err := resolveImportMembership(shardID, replicaID, artifact, iopts)
	if err != nil {
		return proto.Membership{}, err
	}
	if requireIdentities {
		if id, missing := firstMemberWithoutIdentity(ms); missing {
			return proto.Membership{}, &ImportMembershipInvalidError{
				ShardID: shardID, ReplicaID: replicaID,
				Reason: fmt.Sprintf("replica %d has no node identity; a member is authorized "+
					"by its identity and by nothing else, so it could never be heard from "+
					"(WithImportMemberIdentities)", id),
			}
		}
	}
	// One identity may not be bound to two members.
	if a, b, dup := duplicateIdentity(ms.Identities); dup {
		return proto.Membership{}, &ImportMembershipInvalidError{
			ShardID: shardID, ReplicaID: replicaID,
			Reason: fmt.Sprintf("replicas %d and %d hold the same node identity; one keypair "+
				"cannot be two members of a shard", a, b),
		}
	}
	// An identity the shard removed stays removed.
	if id, removed := identityRemovedByArtifact(ms, artifact.RemovedIdentities); id != 0 {
		return proto.Membership{}, &ImportMembershipInvalidError{
			ShardID: shardID, ReplicaID: replicaID,
			Reason: fmt.Sprintf("replica %d holds a node identity this shard removed (replica %d's); "+
				"a removed keypair is not re-admitted by import", id, removed),
		}
	}
	return ms, nil
}

// identityRemovedByArtifact reports the lowest-numbered active member of ms
// whose identity is one the artifact's tombstones hold, and the removed
// replica it belonged to.
func identityRemovedByArtifact(ms proto.Membership, removedIdentities map[uint64]string) (member uint64, removed uint64) {
	if len(removedIdentities) == 0 {
		return 0, 0
	}
	holder := make(map[string]uint64, len(removedIdentities))
	for id, identity := range removedIdentities {
		holder[identity] = id
	}
	var hits []uint64
	for _, roles := range []map[uint64]string{ms.Addresses, ms.Observers, ms.Witnesses} {
		for id := range roles {
			if _, ok := holder[ms.Identities[id]]; ok {
				hits = append(hits, id)
			}
		}
	}
	if len(hits) == 0 {
		return 0, 0
	}
	member = slices.Min(hits)
	return member, holder[ms.Identities[member]]
}

// refuseStrayIdentities refuses, lowest first, a supplied identity whose replica the membership does not name.
func refuseStrayIdentities(shardID, replicaID uint64, identities map[uint64]string, named func(uint64) bool, which string) error {
	var stray []uint64
	for id, identity := range identities {
		if identity != "" && !named(id) {
			stray = append(stray, id)
		}
	}
	if len(stray) == 0 {
		return nil
	}
	return &ImportMembershipInvalidError{
		ShardID: shardID, ReplicaID: replicaID,
		Reason: fmt.Sprintf("an identity is supplied for replica %d, which %s", slices.Min(stray), which),
	}
}

// holdsRole reports whether the membership names replicaID in an active role
// — voter, observer or witness.
func holdsRole(ms proto.Membership, replicaID uint64) bool {
	for _, roles := range []map[uint64]string{ms.Addresses, ms.Observers, ms.Witnesses} {
		if _, ok := roles[replicaID]; ok {
			return true
		}
	}
	return false
}

// firstMemberWithoutIdentity reports the lowest-numbered active member — voter,
// observer or witness — the membership holds no identity for, and whether there
// is one. Lowest-numbered so the refusal is deterministic across runs.
func firstMemberWithoutIdentity(ms proto.Membership) (uint64, bool) {
	var missing []uint64
	for _, roles := range []map[uint64]string{ms.Addresses, ms.Observers, ms.Witnesses} {
		for id := range roles {
			if ms.Identities[id] == "" {
				missing = append(missing, id)
			}
		}
	}
	if len(missing) == 0 {
		return 0, false
	}
	return slices.Min(missing), true
}

// resolveImportMembership returns the artifact's membership or the caller's validated override.
func resolveImportMembership(
	shardID, replicaID uint64,
	artifact proto.Membership,
	iopts importOptions,
) (proto.Membership, error) {
	if !iopts.membershipSet {
		if len(iopts.identities) == 0 {
			return artifact, nil
		}
		// Supplied identities without a rewrite complete the artifact's membership; the supplied value wins.
		if err := refuseStrayIdentities(shardID, replicaID, iopts.identities,
			func(id uint64) bool { return holdsRole(artifact, id) },
			"the artifact's membership holds no role for; supply WithImportMembership to change the membership"); err != nil {
			return proto.Membership{}, err
		}
		out := artifact
		out.Identities = copyIdentityMap(artifact.Identities)
		for id, identity := range iopts.identities {
			if identity == "" {
				continue
			}
			if out.Identities == nil {
				out.Identities = make(map[uint64]string, len(iopts.identities))
			}
			out.Identities[id] = identity
		}
		return out, nil
	}
	if len(iopts.membership) == 0 {
		return proto.Membership{}, &ImportMembershipInvalidError{
			ShardID: shardID, ReplicaID: replicaID,
			Reason: "the membership is empty; a shard must have at least one voting member",
		}
	}
	if len(iopts.membership) > raft.MaxVotingMembers {
		return proto.Membership{}, &ImportMembershipInvalidError{
			ShardID: shardID, ReplicaID: replicaID,
			Reason: fmt.Sprintf("%d voting members exceeds the %d-member limit",
				len(iopts.membership), raft.MaxVotingMembers),
		}
	}
	if _, ok := iopts.membership[replicaID]; !ok {
		return proto.Membership{}, &ImportMembershipInvalidError{
			ShardID: shardID, ReplicaID: replicaID,
			Reason: "the importing replica is not in the membership; it would " +
				"restore the data and then be unable to participate in the shard",
		}
	}
	for id, addr := range iopts.membership {
		if id == 0 {
			return proto.Membership{}, &ImportMembershipInvalidError{
				ShardID: shardID, ReplicaID: replicaID,
				Reason: "replica ID 0 is not a valid member",
			}
		}
		if addr == "" {
			return proto.Membership{}, &ImportMembershipInvalidError{
				ShardID: shardID, ReplicaID: replicaID,
				Reason: fmt.Sprintf("replica %d has an empty address; the "+
					"survivors cannot reach each other", id),
			}
		}
	}

	// Refuse an identity keyed by a replica the new membership does not name.
	if err := refuseStrayIdentities(shardID, replicaID, iopts.identities,
		func(id uint64) bool { _, ok := iopts.membership[id]; return ok },
		"the rewritten membership does not name"); err != nil {
		return proto.Membership{}, err
	}

	// Policy and ConfigChangeID carry over to the rewritten membership.
	out := proto.Membership{
		ConfigChangeID:             artifact.ConfigChangeID,
		MaxWitnesses:               artifact.MaxWitnesses,
		RequireFullReplicaMajority: artifact.RequireFullReplicaMajority,
		WireVersion:                artifact.WireVersion,
		Addresses:                  make(map[uint64]string, len(iopts.membership)),
		Identities:                 make(map[uint64]string, len(iopts.membership)),
	}
	for id, addr := range iopts.membership {
		out.Addresses[id] = addr
		// The supplied identity wins; a survivor keeps the artifact's.
		if identity := iopts.identities[id]; identity != "" {
			out.Identities[id] = identity
		} else if identity := artifact.Identities[id]; identity != "" {
			out.Identities[id] = identity
		}
	}
	out.SetSessionPolicy(artifact.SessionPolicy())

	slog.Warn("quicraft: importing a snapshot with REWRITTEN membership; "+
		"committed entries held only by the excluded replicas are discarded",
		"shard", shardID,
		"replica", replicaID,
		"artifact_members", len(artifact.Addresses),
		"new_members", len(out.Addresses),
	)
	return out, nil
}

// WithImportDataDir asserts the shard's Config.DataDir ("" for none), checked against the recorded value.
func WithImportDataDir(dir string) ImportOption {
	return func(o *importOptions) {
		o.dataDir = dir
		o.dataDirSet = true
	}
}

// resolveImportDataDir returns the shard's snapshot-tree root, preferring the bootstrap record and never guessing.
func (h *Host) resolveImportDataDir(shardID, replicaID uint64, iopts importOptions) (string, error) {
	bs, ok, err := h.logdb.GetBootstrap(shardID, replicaID)
	if err != nil {
		return "", err
	}
	// A bootstrap record's DataDir is authoritative; without a record the layout is unknown.
	if !ok {
		// No recorded layout. Only the caller can supply it, and a wrong
		// guess costs the artifact's DEK, so refuse rather than assume.
		if !iopts.dataDirSet {
			return "", &ImportLayoutUnknownError{ShardID: shardID, ReplicaID: replicaID}
		}
		return iopts.dataDir, nil
	}
	if iopts.dataDirSet && iopts.dataDir != bs.DataDir {
		return "", &ImportDataDirMismatchError{
			ShardID:   shardID,
			ReplicaID: replicaID,
			Declared:  iopts.dataDir,
			Recorded:  bs.DataDir,
		}
	}
	return bs.DataDir, nil
}

// ImportSnapshot imports an ExportSnapshot artifact from r for a stopped shard, recording the original snapshot identity.
func (h *Host) ImportSnapshot(ctx context.Context, shardID uint64, r io.Reader, opts ...ImportOption) error {
	replicaID, err := h.importPreflight(ctx, shardID)
	if err != nil {
		return err
	}

	// Parse the header before touching disk.
	header, rawHeader, err := readSnapshotArtifactHeader(r)
	if err != nil {
		return err
	}

	// Resolve the restart membership before anything is staged.
	recoveredMembership, err := importMembershipFor(shardID, replicaID, header.Membership, opts,
		h.transport != nil)
	if err != nil {
		return err
	}
	// The importing replica's own committed identity must be this host's, for
	// the same reason StartShard checks its own entry: a wrong value here is
	// persisted and every peer refuses this replica afterwards.
	if own := h.NodeIdentity(); own != "" {
		if self := recoveredMembership.Identities[replicaID]; self != "" && self != own {
			return &ImportMembershipInvalidError{
				ShardID: shardID, ReplicaID: replicaID,
				Reason: "the importing replica's identity in the membership is not this " +
					"host's (Host.NodeIdentity); peers would refuse every message it sends",
			}
		}
	}

	// Serialize imports for this shard.
	release, err := h.lockStoppedShardForImport(ctx, shardID, replicaID)
	if err != nil {
		return err
	}
	defer release()

	// Refuse an artifact that does not advance the recorded snapshot, before the tree is touched.
	if err := h.checkImportAdvancesSnapshot(shardID, replicaID, header.Index); err != nil {
		return err
	}

	// Check context before performing I/O.
	if err := ctx.Err(); err != nil {
		return mapContextError(err)
	}

	snapBaseDir, snapDir, err := h.importSnapshotDir(shardID, replicaID, header.Index, opts)
	if err != nil {
		return err
	}

	fsys := h.snapshotFsys()
	// A destination that already exists is a directory this import did not
	// create. It may be a retained snapshot the shard still recovers from, so
	// neither writing into it nor removing it is permissible.
	if _, statErr := fsys.Stat(snapDir); statErr == nil {
		return &ImportSnapshotDirExistsError{
			ShardID:   shardID,
			ReplicaID: replicaID,
			Index:     header.Index,
			Dir:       snapDir,
		}
	} else if !os.IsNotExist(statErr) {
		// The destination's existence is indeterminate; fail closed rather
		// than proceed towards a rename that might clobber it.
		return statErr
	}

	// Record the deepest existing ancestor so the fsync covers every directory created.
	syncRoot := fsutil.DeepestExistingAncestorWith(fsys.Stat, snapDir)
	stageParent := filepath.Dir(snapDir)
	if mkdirErr := fsys.MkdirAll(stageParent, 0o750); mkdirErr != nil {
		return mkdirErr
	}

	// Stage under a name snapshot listing never parses.
	stageDir, tempErr := fsys.MkdirTemp(stageParent, importStageDirPattern)
	if tempErr != nil {
		return tempErr
	}

	// Term is floored at 1 and shared by snapshot.meta and the LogDB record.
	term := header.Term
	if term == 0 {
		term = 1
	}

	fileSize, epoch, unpin, stageErr := h.stageImportedArtifact(
		ctx, fsys, shardID, replicaID, header, rawHeader, term, stageDir, r)
	// Keep the epoch pin until SaveSnapshot records it.
	defer unpin()
	if stageErr != nil {
		return stageErr
	}

	// Make the staged directory's entries durable before publishing it.
	if syncErr := syncDir(fsys, stageDir); syncErr != nil {
		discardImportDir(fsys, shardID, stageDir)
		return syncErr
	}

	// Publish with a rename that adds a name, never replaces one.
	if renameErr := fsys.Rename(stageDir, snapDir); renameErr != nil {
		discardImportDir(fsys, shardID, stageDir)
		return renameErr
	}

	// This import created the canonical directory; published records it for the cleanup.
	published := true
	defer func() {
		if published {
			discardImportDir(fsys, shardID, snapDir)
		}
	}()

	// Fsync the directory chain before recording the snapshot in LogDB.
	if syncErr := syncDirChain(fsys, snapDir, syncRoot); syncErr != nil {
		return syncErr
	}

	// Check context before committing to LogDB.
	if err := ctx.Err(); err != nil {
		return mapContextError(err)
	}

	// Persist the artifact's original snapshot metadata so the import is the same logical snapshot.
	logdbSnap := logdb.Snapshot{
		Index:       header.Index,
		Term:        term,
		Membership:  protoMembershipToLogDB(recoveredMembership),
		Filepath:    snapDir,
		FileSize:    fileSize,
		OnDiskIndex: header.OnDiskIndex,
		Epoch:       epoch,
		// The artifact's own compression, NOT this host's config: the bytes
		// were written by the exporting host and are stored verbatim.
		Compression: header.Compression,
	}
	if saveErr := h.logdb.SaveSnapshot(shardID, replicaID, logdbSnap); saveErr != nil {
		return saveErr
	}
	// Recorded: set before compaction so a compaction error cannot remove the recorded directory.
	published = false

	h.gcShardSnapshots(snapBaseDir, shardID, replicaID)

	// Advance the log compaction floor to the imported index, removing every entry at or below it.
	if rmErr := h.logdb.RemoveEntriesTo(shardID, replicaID, header.Index); rmErr != nil {
		return rmErr
	}
	// The restore is complete here; a Compact failure only leaves space unreclaimed.
	if compactErr := h.logdb.Compact(shardID, replicaID); compactErr != nil {
		h.logger.Warn("imported snapshot recorded and log floor advanced, but WAL "+
			"segment garbage collection failed; the restore is complete and the "+
			"obsolete segments will be reclaimed by a later compaction",
			"shard_id", shardID, "replica_id", replicaID, "error", compactErr)
	}
	return nil
}

// importPreflight refuses an import before anything touches disk and resolves the shard's replicaID from the bootstrap records.
func (h *Host) importPreflight(ctx context.Context, shardID uint64) (uint64, error) {
	if err := ctx.Err(); err != nil {
		return 0, mapContextError(err)
	}
	if h.closed.Load() {
		return 0, ErrClosed
	}

	// The import writes a full snapshot artifact to disk; starting one while
	// the WAL disk monitor reports the volume full would only deepen the
	// exhaustion and leave a truncated artifact behind.
	if h.proposalDiskFull() {
		return 0, ErrDiskFull
	}

	// Verify shard is NOT running. A paused shard is still loaded; it says
	// so, since the pause and not the import is what the caller must end.
	if v, running := h.nodes.Load(shardID); running {
		if ns, ok := v.(*nodeState); ok && ns.paused.Load() {
			return 0, ErrShardPaused
		}
		return 0, ErrInvalidOperation
	}

	if h.logdb == nil {
		return 0, ErrInvalidOperation
	}

	// Determine the replicaID from LogDB bootstrap info.
	nodes, err := h.logdb.ListNodeInfo()
	if err != nil {
		return 0, err
	}
	// The shard must have exactly one replica on this host.
	var replicaID uint64
	for _, ni := range nodes {
		if ni.ShardID != shardID {
			continue
		}
		if replicaID != 0 && ni.ReplicaID != replicaID {
			return 0, fmt.Errorf("%w: shard %d has data for more than one replica on this host (%d and %d); "+
				"remove the stale replica's data before importing", ErrInvalidOperation, shardID, replicaID, ni.ReplicaID)
		}
		replicaID = ni.ReplicaID
	}
	if replicaID == 0 {
		return 0, ErrShardNotFound
	}
	if err := h.refuseRetiredReplica(shardID, replicaID); err != nil {
		return 0, err
	}

	// A configured but sealed barrier refuses the import before anything is written.
	if _, barrierErr := h.snapshotBarrierForShard(shardID); barrierErr != nil {
		return 0, barrierErr
	}
	return replicaID, nil
}

// gcShardSnapshots removes superseded snapshot directories; call it only after the new snapshot is durably recorded.
func (h *Host) gcShardSnapshots(baseDir string, shardID, replicaID uint64, spare ...string) {
	recorded, recErr := h.recordedSnapshotPath(shardID, replicaID)
	if recErr != nil {
		// Without the record there is no telling which directory GC must
		// spare, so it reclaims nothing this time.
		h.logger.Warn("snapshot GC skipped: the snapshot record cannot be read",
			"shard_id", shardID, "replica_id", replicaID, "error", recErr)
		return
	}
	if err := engine.GCSnapshotDirs(baseDir, shardID, replicaID, recorded, spare...); err != nil {
		h.logger.Warn("snapshot recorded, but reclaiming the superseded snapshot "+
			"directories failed; they will be reclaimed by a later snapshot",
			"shard_id", shardID, "replica_id", replicaID, "error", err)
	}
}

// importStageDirPattern names the staging directory, which snapshot listing never parses and StartShard reclaims.
const importStageDirPattern = engine.ImportStageDirPattern

// checkImportAdvancesSnapshot refuses an artifact whose index does not advance the recorded snapshot.
func (h *Host) checkImportAdvancesSnapshot(shardID, replicaID, index uint64) error {
	existing, err := h.logdb.GetSnapshot(shardID, replicaID)
	if err != nil {
		if errors.Is(err, logdb.ErrNoSnapshot) {
			return nil
		}
		return err
	}
	if index > existing.Index {
		return nil
	}
	return &ImportSnapshotOutOfDateError{
		ShardID:   shardID,
		ReplicaID: replicaID,
		Index:     index,
		Recorded:  existing.Index,
	}
}

// importSnapshotDir resolves the canonical directory for the imported artifact, built as every snapshot writer builds it.
func (h *Host) importSnapshotDir(shardID, replicaID, index uint64, opts []ImportOption) (baseDir, snapDir string, err error) {
	var iopts importOptions
	for _, opt := range opts {
		opt(&iopts)
	}
	// The layout comes from what StartShard recorded; resolveImportDataDir fails closed.
	importDataDir, err := h.resolveImportDataDir(shardID, replicaID, iopts)
	if err != nil {
		return "", "", err
	}
	base := shardSnapshotBaseDir(h.cfg.NodeHostDir, importDataDir, shardID)
	return base, engine.SnapshotSaveDir(base, shardID, replicaID, index), nil
}

// writeImportedSnapshotData writes the payload in its at-rest form, checking it against the trailer, and makes it durable.
func (h *Host) writeImportedSnapshotData(ctx context.Context, fsys snapshotFS, shardID, replicaID, index uint64,
	dataPath string, header []byte, r io.Reader, barrier Barrier) (epoch uint64, pinned []uint64, err error) {
	f, createErr := fsys.OpenFile(dataPath, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0o600)
	if createErr != nil {
		return 0, nil, createErr
	}
	// closeFile logs and returns the Close error; the success path must check it.
	closeFile := func(when string) error {
		closeErr := f.Close()
		if closeErr != nil {
			slog.Debug("snapshot import file close failed",
				"shard", shardID,
				"path", dataPath,
				"when", when,
				"error", closeErr)
		}
		return closeErr
	}
	// Every failure after the seal must release the pins it took, since the
	// caller never receives the set on those paths.
	unpinAll := func() {
		for _, e := range pinned {
			barrier.DeregisterInFlightEpoch(e)
		}
	}

	// Bound the plaintext copy by the maximum snapshot size.
	src := &importPayloadReader{
		ctx:     ctx,
		r:       newArtifactPayloadReader(header, io.LimitReader(r, int64(config.DefaultMaxSnapshotSize)+1+snapshotArtifactTrailerSize)),
		maxSize: config.DefaultMaxSnapshotSize,
		shardID: shardID,
	}
	var copyErr error
	if barrier == nil {
		_, copyErr = io.Copy(f, src)
	} else {
		epoch, pinned, copyErr = encryptSnapshotFrames(barrier, shardID, replicaID, index, src, f)
	}
	if copyErr != nil {
		_ = closeFile("copy_error")
		return 0, nil, copyErr
	}
	if syncErr := f.Sync(); syncErr != nil {
		_ = closeFile("sync_error")
		unpinAll()
		return 0, nil, syncErr
	}
	// Close is part of the durability chain: it can report deferred write failures.
	if closeErr := closeFile("write_complete"); closeErr != nil {
		unpinAll()
		return 0, nil, closeErr
	}
	return epoch, pinned, nil
}

// importPayloadReader reads the payload, honouring ctx and refusing more than maxSize bytes; r must be limited to maxSize+1.
type importPayloadReader struct {
	ctx     context.Context
	r       io.Reader
	maxSize uint64
	shardID uint64
	read    uint64
}

// Read reads from the payload, failing once ctx is done or the payload has
// exceeded its maximum size.
func (p *importPayloadReader) Read(b []byte) (int, error) {
	if err := p.ctx.Err(); err != nil {
		return 0, mapContextError(err)
	}
	n, err := p.r.Read(b)
	p.read += uint64(n)
	if p.read > p.maxSize {
		return n, &SnapshotTooLargeError{ShardID: p.shardID, MaxSize: p.maxSize}
	}
	return n, err
}

// stageImportedArtifact writes the payload, encrypted under this host's barrier, and its snapshot.meta into the staging directory.
func (h *Host) stageImportedArtifact(
	ctx context.Context,
	fsys snapshotFS,
	shardID, replicaID uint64,
	header snapshotArtifactHeader,
	rawHeader []byte,
	term uint64,
	stageDir string,
	r io.Reader,
) (fileSize, epoch uint64, unpin func(), err error) {
	unpin = func() {}

	// Encrypt the plaintext import under this host's own barrier.
	importBarrier, barrierErr := h.barrierForShard(shardID)
	if barrierErr != nil {
		discardImportDir(fsys, shardID, stageDir)
		return 0, 0, unpin, barrierErr
	}
	if importBarrier != nil && importBarrier.IsSealed() {
		discardImportDir(fsys, shardID, stageDir)
		return 0, 0, unpin, ErrSnapshotBarrierSealed
	}
	dataPath := filepath.Join(stageDir, "snapshot.dat")
	epoch, pinned, writeErr := h.writeImportedSnapshotData(ctx, fsys, shardID, replicaID, header.Index,
		dataPath, rawHeader, r, importBarrier)
	if writeErr != nil {
		// Do not leave a partial (possibly multi-gigabyte) artifact behind
		// for an import that will never be recorded in LogDB.
		discardImportDir(fsys, shardID, stageDir)
		return 0, 0, unpin, writeErr
	}
	// The pins must outlive this call: they are the imported snapshot's only
	// protection from a DEK purge until the caller's SaveSnapshot records the
	// epoch.
	unpin = func() {
		for _, e := range pinned {
			importBarrier.DeregisterInFlightEpoch(e)
		}
	}

	// Write snapshot.meta recording the epoch used and hashing the data file as stored.
	if metaErr := writeReceivedSnapshotMetadata(fsys, stageDir, header.Index, term,
		shardID, replicaID, epoch, header.Compression); metaErr != nil {
		discardImportDir(fsys, shardID, stageDir)
		return 0, 0, unpin, metaErr
	}

	// FileSize is the data file's size as written.
	fi, statErr := fsys.Stat(dataPath)
	if statErr != nil {
		discardImportDir(fsys, shardID, stageDir)
		return 0, 0, unpin, statErr
	}
	return uint64(fi.Size()), epoch, unpin, nil
}

// discardImportDir removes an import's staging directory that will never be recorded, logging a failure.
func discardImportDir(fsys snapshotFS, shardID uint64, dir string) {
	if rmErr := fsys.RemoveAll(dir); rmErr != nil {
		slog.Debug("snapshot import directory cleanup failed",
			"shard", shardID,
			"dir", dir,
			"error", rmErr)
	}
}

// clampNonNegative converts an activeNodes count to a slice capacity, floored at 0.
func clampNonNegative(n int64) int {
	if n < 0 {
		return 0
	}
	return int(n)
}

// NodeHostInfo returns a snapshot of the host's state for monitoring.
func (h *Host) NodeHostInfo(opt NodeHostInfoOption) *NodeHostInfo {
	info := &NodeHostInfo{
		RaftAddress: h.cfg.RaftAddress,
	}

	info.ShardInfoList = make([]ShardInfo, 0, clampNonNegative(h.activeNodes.Load()))
	h.nodes.Range(func(_, value any) bool {
		ns, ok := value.(*nodeState)
		if !ok {
			h.logger.Error("NodeHostInfo: nodes map value type assertion failed",
				"error", &TypeAssertionError{
					Context:      "NodeHostInfo nodes range",
					ExpectedType: "*nodeState",
					ActualValue:  value,
				})
			return true // skip malformed entry
		}
		info.ShardInfoList = append(info.ShardInfoList, *h.buildShardInfo(ns))
		return true
	})

	if !opt.SkipLogInfo && h.logdb != nil {
		logInfo, err := h.logdb.ListNodeInfo()
		if err == nil {
			info.LogInfo = logInfo
		}
	}

	return info
}

// DiagPipelineState returns diagnostic pipeline state for the given shard.
// Used for debugging pipeline stalls. The snapshot gates that can also stall
// a shard are reported by DiagShardGates.
func (h *Host) DiagPipelineState(shardID uint64) (PipelineDiag, error) {
	if h.closed.Load() {
		return PipelineDiag{}, ErrClosed
	}
	engNode := h.engine.GetNode(shardID)
	if engNode == nil {
		return PipelineDiag{}, ErrShardNotReady
	}
	d := engNode.DiagState() // readIndexQLen not exposed in DiagPipelineState
	v, exists := h.nodes.Load(shardID)
	pp := 0
	if exists {
		ns, ok := v.(*nodeState)
		if !ok {
			h.logger.Error("DiagPipelineState: nodeState type assertion failed",
				"shard_id", shardID,
				"error", &TypeAssertionError{
					Context:      "DiagPipelineState nodeState lookup",
					ExpectedType: "*nodeState",
					ActualValue:  v,
				})
		} else {
			ns.pendingProposals.Range(func(_, _ any) bool {
				pp++
				return true
			})
		}
	}
	return PipelineDiag{
		CommitPending:    d.CommitPending,
		ProposalQueueLen: d.ProposalQLen,
		InboxLen:         d.InboxLen,
		PendingProposals: pp,
	}, nil
}

// DiagShardGates reports the snapshot gates suspending a shard's state machine work and the apply closures in flight; see ShardGatesDiag.
func (h *Host) DiagShardGates(shardID uint64) (ShardGatesDiag, error) {
	if h.closed.Load() {
		return ShardGatesDiag{}, ErrClosed
	}
	engNode := h.engine.GetNode(shardID)
	if engNode == nil {
		return ShardGatesDiag{}, ErrShardNotReady
	}
	d := engNode.DiagState()
	return ShardGatesDiag{
		Recovering:    d.Recovering,
		Saving:        d.Saving,
		ApplyInFlight: d.ApplyInFlight,
	}, nil
}

// DiagRaftState returns diagnostic raft state for the given shard.
// Used for debugging pipeline stalls.
func (h *Host) DiagRaftState(shardID uint64) (RaftStateDiag, error) {
	if h.closed.Load() {
		return RaftStateDiag{}, ErrClosed
	}
	engNode := h.engine.GetNode(shardID)
	if engNode == nil {
		return RaftStateDiag{}, ErrShardNotReady
	}
	t, c, p, li, m, s := engNode.DiagRaftState()
	return RaftStateDiag{
		Term:            t,
		Committed:       c,
		Processed:       p,
		LastIndex:       li,
		PendingMessages: m,
		State:           s,
	}, nil
}

// DiagRemotes returns diagnostic remote state for debugging commit stalls.
func (h *Host) DiagRemotes(shardID uint64) (RemotesDiag, error) {
	if h.closed.Load() {
		return RemotesDiag{}, ErrClosed
	}
	engNode := h.engine.GetNode(shardID)
	if engNode == nil {
		return RemotesDiag{}, ErrShardNotReady
	}
	return RemotesDiag{Text: engNode.DiagRemotes()}, nil
}

// Close drains, stops every shard and releases resources; it is idempotent and every call returns the first outcome.
func (h *Host) Close() error {
	// Only the first caller fires the event; the CAS is what elects it. The
	// closed flag also fails new work fast, so it is set before any of the
	// (potentially slow) shutdown work below begins.
	if h.closed.CompareAndSwap(false, true) {
		if el := h.cfg.EventListener; el != nil && el.OnNodeHostShuttingDown != nil {
			el.OnNodeHostShuttingDown()
		}
	}

	// sync.Once.Do returns only after f completes, which publishes h.closeErr to every caller.
	var closeErr error
	h.stopOnce.Do(func() {
		// Drain first; a drain timeout is reported in Close's result.
		if drainErr := h.Drain(); drainErr != nil {
			h.logger.Warn("drain incomplete", "error", drainErr)
			closeErr = errors.Join(closeErr, drainErr)
		}

		// Stop the key rotation goroutine and wait for it to exit
		// before stopping the engine to prevent rotation during shutdown.
		close(h.keyRotStopC)
		<-h.keyRotDone

		// Stop the pending proposal sweeper and wait for it to exit.
		close(h.sweepStopC)
		<-h.sweepDone

		// End every leader-executed request: callers return ErrStopped and
		// the handlers running for remote callers see their context end.
		// Waits for those handlers, so none outlives the shard state it reads.
		h.leaderExec.close()

		// Stop the engine first: its step workers are the only snapshot-send producers.
		if h.engine != nil {
			h.engine.Stop()
		}
		// The engine's callbacks start the wire-switch retries, so none
		// starts after this; each has seen h.closed and is returning.
		h.wireSwitchWG.Wait()

		// Wait for the snapshot sends launched before the engine stopped.
		if ts, ok := h.sender.(*transportSender); ok {
			ts.Wait()
		}

		// Stop the transport last, after all snapshot sends have drained,
		// so no send goroutine touches a torn-down transport.
		if h.transport != nil {
			if err := h.transport.Stop(); err != nil {
				h.logger.Debug("transport stop failed during host close", "error", err)
			}
		}

		// Stop all nodes and fail their pending requests with ErrClosed.
		var stoppedNodes []*nodeState
		h.nodes.Range(func(key, value any) bool {
			shardID, ok := key.(uint64)
			if !ok {
				h.logger.Error("Close: nodes map key type assertion failed",
					"error", &TypeAssertionError{
						Context:      "Close nodes range key",
						ExpectedType: "uint64",
						ActualValue:  key,
					})
				return true // skip malformed entry
			}
			ns, ok := value.(*nodeState)
			if !ok {
				h.logger.Error("Close: nodes map value type assertion failed",
					"shard_id", shardID,
					"error", &TypeAssertionError{
						Context:      "Close nodes range value",
						ExpectedType: "*nodeState",
						ActualValue:  value,
					})
				return true // skip malformed entry
			}
			// Atomically claim teardown so a shard already being torn down by a
			// concurrent StopShard is not unloaded/decremented twice (see the
			// CompareAndSwap rationale in StopShard).
			if !ns.stopped.CompareAndSwap(false, true) {
				return true // claimed by a concurrent StopShard; it owns teardown
			}
			ns.health.Store(uint32(ShardUnloaded))
			// Signal cancellation to any in-progress DiskStateMachine.Open().
			ns.openStopOnce.Do(func() { close(ns.openStopper) })
			h.failPendingRequests(ns, ErrClosed)
			stoppedNodes = append(stoppedNodes, ns)
			h.nodes.Delete(shardID)
			h.anchors.Delete(shardID)
			h.activeNodes.Add(-1)
			return true
		})

		// A second sweep catches requests registered during the first.
		for _, ns := range stoppedNodes {
			h.failPendingRequests(ns, ErrClosed)
		}

		// Close each state machine to release resources such as file locks.
		for _, ns := range stoppedNodes {
			if ns.rsm != nil {
				if err := ns.rsm.Close(); err != nil {
					h.logger.Warn("state machine close failed during host close",
						"shard_id", ns.shardID, "error", err)
					closeErr = errors.Join(closeErr, err)
				}
				// Leave ns.rsm set: in-flight readers may still read it.
			}
		}

		// Close LogDB.
		if h.logdb != nil {
			if err := h.logdb.Close(); err != nil {
				closeErr = errors.Join(closeErr, err)
			}
		}

		// Seal every barrier the host was given, last.
		sealErr := h.sealGivenBarriers()
		closeErr = errors.Join(closeErr, sealErr)
		if sealErr == nil {
			h.barriersSealed.Store(true)
		}

		// Release NodeHostDir last, once nothing the host owns writes under it.
		closeErr = errors.Join(closeErr, h.dirLock.Close())

		h.logger.Info("host closed", "id", h.id)
		h.closeErr = closeErr
		h.closeDone.Store(true)
	})

	return h.closeErr
}

// sealGivenBarriers seals the host-wide and every per-shard barrier and joins their failures; an already-sealed barrier is not a failure.
func (h *Host) sealGivenBarriers() error {
	err := sealOnClose(h.barrier)
	h.shardBarriersGiven.each(func(b Barrier) {
		if !logdb.SameBarrier(b, h.barrier) {
			err = errors.Join(err, sealOnClose(b))
		}
	})
	return err
}

// sealOnClose seals b during Host.Close and returns the failure Close must
// report. A nil barrier has nothing to seal, and one already sealed is not an
// error during shutdown.
func sealOnClose(b Barrier) error {
	if b == nil {
		return nil
	}
	if err := b.Seal(); err != nil && !errors.Is(err, ErrBarrierSealed) {
		return err
	}
	return nil
}

// Drain permanently refuses new proposals and reads with ErrDraining and blocks until in-flight work completes or ShutdownTimeout passes.
func (h *Host) Drain() error {
	if h.closeDone.Load() {
		return ErrClosed
	}
	// Raise the flag under admitMu before the Once, so new work is refused immediately; it is never cleared.
	h.admitMu.Lock()
	h.draining.Store(true)
	h.admitMu.Unlock()

	// sync.Once.Do returns only after the elected attempt completes, publishing drainErr.
	h.drainOnce.Do(func() {
		h.drainErr = h.drainOnceLocked()
	})
	return h.drainErr
}

// drainOnceLocked is the single drain attempt Drain elects through drainOnce.
// It waits for pendingOps to reach zero or for ShutdownTimeout to expire.
func (h *Host) drainOnceLocked() error {
	// Fast path: no pending operations.
	if h.pendingOps.Load() <= 0 {
		return nil
	}

	// Create the drain notification channel. CompletePendingOp will
	// close it when pendingOps reaches 0.
	h.drainCMu.Lock()
	if h.drainC == nil {
		h.drainC = make(chan struct{})
	}
	drainCh := h.drainC
	h.drainCMu.Unlock()

	// Check again after channel creation to avoid a race.
	if h.pendingOps.Load() <= 0 {
		return nil
	}

	timer := time.NewTimer(h.cfg.ShutdownTimeout)
	defer timer.Stop()

	select {
	case <-drainCh:
		return nil
	case <-timer.C:
		return ErrShutdownTimeout
	}
}

// admitPendingOp counts an operation for Drain, or refuses it with ErrClosed or ErrDraining.
func (h *Host) admitPendingOp() error {
	h.admitMu.RLock()
	defer h.admitMu.RUnlock()
	if h.closed.Load() {
		return ErrClosed
	}
	if h.draining.Load() {
		return ErrDraining
	}
	h.pendingOps.Add(1)
	return nil
}

// addPendingOp increments the outstanding pending operations counter
// unconditionally. Admission paths use admitPendingOp, which refuses once the
// host is closed or draining; this is for tests that seed pendingOps.
func (h *Host) addPendingOp() {
	h.pendingOps.Add(1)
}

// completePendingOp decrements the pending count and unblocks Drain at zero.
func (h *Host) completePendingOp() {
	for {
		current := h.pendingOps.Load()
		if current <= 0 {
			// Already at zero or below; do not decrement further.
			return
		}
		if h.pendingOps.CompareAndSwap(current, current-1) {
			if current-1 <= 0 && h.draining.Load() {
				h.drainCMu.Lock()
				if h.drainC != nil {
					select {
					case <-h.drainC:
						// Already closed.
					default:
						close(h.drainC)
					}
				}
				h.drainCMu.Unlock()
			}
			return
		}
		// Yield the processor on CAS failure to prevent CPU spinning
		// under extreme contention from concurrent completePendingOp
		// callers.
		runtime.Gosched()
	}
}

// failPendingRequests fails every pending proposal, read and snapshot of ns with err, for teardown.
func (h *Host) failPendingRequests(ns *nodeState, err error) {
	h.failPendingClientRequests(ns, err)

	ns.pendingSnapshotsMu.Lock()
	for k, rs := range ns.pendingSnapshots {
		rs.complete(RequestResult{Err: err})
		delete(ns.pendingSnapshots, k)
		rs.releaseRef()
		h.completePendingOp()
	}
	ns.pendingSnapshotsMu.Unlock()
}

// failPendingClientRequests fails pending proposals and reads with err, leaving snapshots.
func (h *Host) failPendingClientRequests(ns *nodeState, err error) {
	h.failPendingProposals(ns, err, false)
	h.failPendingReads(ns, err)
}

// failLeaderChangeRequests fails every pending read and every proposal already handed to raft (maybe-applied).
func (h *Host) failLeaderChangeRequests(ns *nodeState, err error) {
	h.failPendingProposals(ns, err, true)
	h.failPendingReads(ns, err)
}

// failPendingProposals fails ns's pending proposals with err, maybe-applied when they may have reached raft.
func (h *Host) failPendingProposals(ns *nodeState, err error, onlyHandedOff bool) {
	ns.pendingProposals.Range(func(key, value any) bool {
		pp, ok := value.(*pendingProposal)
		if !ok {
			// Claim it so the malformed entry does not linger; nothing to
			// complete.
			if ns.pendingProposals.CompareAndDelete(key, value) {
				h.logger.Error("failPendingClientRequests: pendingProposals value type assertion failed",
					"error", &TypeAssertionError{
						Context:      "failPendingClientRequests pendingProposals range",
						ExpectedType: "*pendingProposal",
						ActualValue:  value,
					})
			}
			return true
		}
		handedOff := true
		if pp.node != nil {
			taken, final := pp.node.ProposalTaken(&pp.enqueuedAt)
			if onlyHandedOff && !taken {
				return true
			}
			handedOff = taken || !final
		}
		// Claim each entry atomically so a concurrent completion is not released twice.
		if !ns.pendingProposals.CompareAndDelete(key, value) {
			return true
		}
		h.failClaimedProposal(pp, err, handedOff)
		return true
	})
}

// failPendingReads completes every pending read of ns with err, as given: a
// read appends nothing that could commit, so it is never maybe-applied.
func (h *Host) failPendingReads(ns *nodeState, err error) {
	ns.pendingReadsMu.Lock()
	for k, pr := range ns.pendingReads {
		pr.rs.complete(RequestResult{Err: err})
		delete(ns.pendingReads, k)
		pr.rs.releaseRef()
		h.completePendingOp()
	}
	ns.pendingReadsMu.Unlock()
}

// IsDraining reports whether the host is in drain mode.
func (h *Host) IsDraining() bool {
	return h.draining.Load()
}

// keyRotationLoop rotates the barrier epoch on schedule and at the AEAD invocation threshold until keyRotStopC closes.
func (h *Host) keyRotationLoop(interval time.Duration) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() {
		select {
		case <-h.keyRotStopC:
			cancel()
		case <-ctx.Done():
		}
	}()
	for {
		panicked := h.keyRotationLoopOnce(ctx, interval)
		if !panicked {
			return
		}
		// Check for shutdown before restarting.
		select {
		case <-h.keyRotStopC:
			return
		default:
		}
		t := time.NewTimer(loopPanicRestartDelay)
		select {
		case <-h.keyRotStopC:
			t.Stop()
			return
		case <-t.C:
		}
	}
}

// keyRotationLoopOnce runs the key rotation loop body and recovers from
// panics. Returns true if the loop panicked, false if it returned
// normally (stop channel closed).
func (h *Host) keyRotationLoopOnce(ctx context.Context, interval time.Duration) (panicked bool) {
	normalReturn := false
	defer func() {
		if r, abnormal := supervise.ClassifyExit(recover(), normalReturn); abnormal {
			panicked = true
			stack := debug.Stack()
			panicErr := &LoopPanicError{
				LoopName: "key-rotation",
				Value:    r,
				Stack:    stack,
			}
			logger := h.logger
			supervise.LogContained(func() {
				logger.Error("key rotation loop panic recovered, restarting",
					"panic", fmt.Sprint(r),
					"stack", string(stack),
					"error", panicErr,
				)
			})
		}
	}()

	// Scheduled rotation is optional (interval == 0 disables it); the
	// invocation-threshold check below always runs.
	var scheduledC <-chan time.Time
	if interval > 0 {
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		scheduledC = ticker.C
	}

	// Threshold check runs at a faster cadence to detect invocation-based
	// rotation needs between scheduled rotations.
	thresholdCheck := time.NewTicker(30 * time.Second)
	defer thresholdCheck.Stop()

	for {
		select {
		case <-h.keyRotStopC:
			normalReturn = true
			return false
		case <-scheduledC:
			h.rotateBarrierKey(ctx)
		case <-thresholdCheck.C:
			if h.barrier != nil && !h.barrier.IsSealed() && h.barrier.ShouldRotateKey() {
				h.logger.Info("invocation threshold reached, triggering key rotation")
				h.rotateBarrierKey(ctx)
			}
		}
	}
}

// endedBy reports whether err was caused by ctx ending.
func endedBy(ctx context.Context, err error) bool {
	ended := ctx.Err()
	return ended != nil && errors.Is(err, ended)
}

// rotateBarrierKey rotates the barrier key and purges old DEKs when MaxRetainedEpochs is set.
func (h *Host) rotateBarrierKey(ctx context.Context) {
	if h.barrier == nil || h.barrier.IsSealed() {
		return
	}
	newEpoch, err := h.barrier.Rotate(ctx)
	if err != nil && endedBy(ctx, err) {
		// The host is closing and cancelled the rotation. Nothing was
		// persisted, the current epoch stays active, and the misconfiguration
		// report below would be untrue.
		h.logger.Debug("key rotation abandoned: host is closing", "error", err)
		return
	}
	if err != nil {
		// Rotate fails closed without a state persister; that is a permanent misconfiguration.
		h.logger.Error("at-rest key rotation is DISABLED: the barrier rejected the rotation; "+
			"configure a barrier state persister to enable it, otherwise the current epoch key "+
			"is never replaced and writes will eventually fail at the AEAD invocation limit",
			"error", err)
		return
	}
	h.logger.Info("key rotation completed", "epoch", newEpoch)

	if h.cfg.MaxRetainedEpochs > 0 && newEpoch > h.cfg.MaxRetainedEpochs {
		purgeThreshold := newEpoch - h.cfg.MaxRetainedEpochs

		// Clamp the purge threshold to the global safe-purge floor of retained snapshots and live WAL records.
		safeFloor, ok := h.engine.GlobalSafePurgeFloor()
		if !ok {
			h.logger.Debug("epoch purge after rotation skipped: global safe floor unknown",
				"purge_threshold", purgeThreshold,
			)
			return
		}
		purgeFloor := purgeThreshold
		if safeFloor < purgeFloor {
			purgeFloor = safeFloor
		}

		purged, purgeErr := h.barrier.PurgeEpochsBefore(ctx, purgeFloor)
		if purgeErr != nil && endedBy(ctx, purgeErr) {
			// The host is closing and cancelled the purge before anything was
			// persisted or dropped; the next rotation after a restart purges
			// again.
			h.logger.Debug("epoch purge after rotation abandoned: host is closing",
				"purge_floor", purgeFloor,
				"error", purgeErr,
			)
			return
		}
		if purgeErr != nil {
			h.logger.Error("epoch purge after rotation failed",
				"purge_floor", purgeFloor,
				"retention_threshold", purgeThreshold,
				"global_safe_floor", safeFloor,
				"error", purgeErr,
			)
		} else if purged > 0 {
			h.logger.Info("purged old encryption epochs",
				"purged", purged,
				"purge_floor", purgeFloor,
				"retention_threshold", purgeThreshold,
				"global_safe_floor", safeFloor,
			)
		}
	}
}

// pendingSweepLoop expires pending proposals, reads and snapshots past their deadlines, restarting after a panic.
func (h *Host) pendingSweepLoop() {
	defer close(h.sweepDone)
	for {
		panicked := h.pendingSweepLoopOnce(pendingSweepInterval)
		if !panicked {
			return
		}
		// Check for shutdown before restarting.
		select {
		case <-h.sweepStopC:
			return
		default:
		}
		t := time.NewTimer(loopPanicRestartDelay)
		select {
		case <-h.sweepStopC:
			t.Stop()
			return
		case <-t.C:
		}
	}
}

// pendingSweepLoopOnce runs the sweep loop at interval and reports whether it panicked.
func (h *Host) pendingSweepLoopOnce(interval time.Duration) (panicked bool) {
	defer func() {
		if r := recover(); r != nil {
			panicked = true
			stack := debug.Stack()
			panicErr := &LoopPanicError{
				LoopName: "pending-sweep",
				Value:    r,
				Stack:    stack,
			}
			logger := h.logger
			supervise.LogContained(func() {
				logger.Error("pending sweep loop panic recovered, restarting",
					"panic", fmt.Sprint(r),
					"stack", string(stack),
					"error", panicErr,
				)
			})
		}
	}()

	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-h.sweepStopC:
			return false
		case <-ticker.C:
			h.sweepExpiredPending()
		}
	}
}

// sweepExpiredPending fails expired pending proposals, reads and snapshots with ErrTimeout.
func (h *Host) sweepExpiredPending() {
	now := time.Now()

	h.nodes.Range(func(_, value any) bool {
		ns, ok := value.(*nodeState)
		if !ok {
			h.logger.Error("sweepExpiredPending: nodes map value type assertion failed",
				"error", &TypeAssertionError{
					Context:      "sweepExpiredPending nodes range",
					ExpectedType: "*nodeState",
					ActualValue:  value,
				})
			return true // skip malformed entry
		}
		if ns.stopped.Load() {
			return true
		}
		h.sweepPendingProposals(ns, now)
		h.sweepPendingReads(ns, now)
		h.sweepPendingSnapshots(ns, now)
		return true
	})
}

// sweepPendingProposals expires proposals whose deadline has passed.
func (h *Host) sweepPendingProposals(ns *nodeState, now time.Time) {
	ns.pendingProposals.Range(func(key, value any) bool {
		pp, ok := value.(*pendingProposal)
		if !ok {
			h.logger.Error("sweepPendingProposals: pendingProposals value type assertion failed",
				"error", &TypeAssertionError{
					Context:      "sweepPendingProposals pendingProposals range",
					ExpectedType: "*pendingProposal",
					ActualValue:  value,
				})
			return true // skip malformed entry
		}
		if pp.deadline.IsZero() || now.Before(pp.deadline) {
			return true
		}
		// Classify before the claim: the timeout is maybe-applied unless the node provably never handed it to raft.
		handedOff := proposalMayReachRaft(pp)
		// Claim atomically so a concurrent completion is not released twice.
		if _, removed := ns.pendingProposals.LoadAndDelete(key); !removed {
			return true
		}
		h.failClaimedProposal(pp, ErrTimeout, handedOff)
		return true
	})
}

// sweepPendingReads expires overdue reads, and a batch key's unconfirmed aliases with it.
func (h *Host) sweepPendingReads(ns *nodeState, now time.Time) {
	ns.pendingReadsMu.Lock()
	for key, pr := range ns.pendingReads {
		deadline := pr.rs.Deadline()
		if deadline.IsZero() || now.Before(deadline) {
			continue
		}
		pr.rs.complete(RequestResult{Err: ErrTimeout})
		delete(ns.pendingReads, key)
		pr.rs.releaseRef()
		h.completePendingOp()
		for _, aliasKey := range pr.aliases {
			aliasPR, ok := ns.pendingReads[aliasKey]
			if !ok || aliasPR.confirmed {
				continue
			}
			aliasPR.rs.complete(RequestResult{Err: ErrTimeout})
			delete(ns.pendingReads, aliasKey)
			aliasPR.rs.releaseRef()
			h.completePendingOp()
		}
	}
	ns.pendingReadsMu.Unlock()
}

// sweepPendingSnapshots expires snapshot requests whose deadline has passed.
func (h *Host) sweepPendingSnapshots(ns *nodeState, now time.Time) {
	ns.pendingSnapshotsMu.Lock()
	for key, rs := range ns.pendingSnapshots {
		deadline := rs.Deadline()
		if deadline.IsZero() || now.Before(deadline) {
			continue
		}
		rs.complete(RequestResult{Err: ErrTimeout})
		delete(ns.pendingSnapshots, key)
		rs.releaseRef()
		h.completePendingOp()
	}
	ns.pendingSnapshotsMu.Unlock()
}

// ID returns the unique host identifier.
func (h *Host) ID() string {
	return h.id
}

// RaftAddress returns this host's Raft network address.
func (h *Host) RaftAddress() string {
	return h.cfg.RaftAddress
}

// EmergencySeal seals the barriers immediately; in-flight operations fail with ErrBarrierSealed.
func (h *Host) EmergencySeal() error {
	if h.closeDone.Load() {
		if h.barriersSealed.Load() {
			return ErrClosed
		}
		if err := h.sealGivenBarriers(); err != nil {
			return err
		}
		h.barriersSealed.Store(true)
		return nil
	}
	if h.barrier == nil {
		return ErrBarrierNotInit
	}
	return h.barrier.Seal()
}

// IsSealed returns true if the barrier is sealed. Returns false if no
// barrier is configured.
func (h *Host) IsSealed() bool {
	if h.barrier == nil {
		return false
	}
	return h.barrier.IsSealed()
}

// ShardCount returns the number of shards loaded on this host.
func (h *Host) ShardCount() int {
	return int(h.activeNodes.Load())
}

// HealthCheck returns the host's barrier, disk, shard and dropped-log-record status.
func (h *Host) HealthCheck() *HealthStatus {
	sealed := h.barrier != nil && h.barrier.IsSealed()
	// The flag is the reconciled state of both monitors (diskFullState), so
	// DiskOK recovers on a newer healthy WAL sample even while the gate's
	// own latch waits for the next proposal to re-sample.
	diskOK := h.diskFull == nil || !h.diskFull.Load()

	shards := make([]ShardInfo, 0, clampNonNegative(h.activeNodes.Load()))
	// Shards leaderless for longer than their election timeout.
	var notReady []uint64
	now := h.leaderlessNow()
	h.nodes.Range(func(_, value any) bool {
		ns, ok := value.(*nodeState)
		if !ok {
			h.logger.Error("HealthStatus: nodes map value type assertion failed",
				"error", &TypeAssertionError{
					Context:      "HealthStatus nodes range",
					ExpectedType: "*nodeState",
					ActualValue:  value,
				})
			return true // skip malformed entry
		}
		info := h.buildShardInfo(ns)
		shards = append(shards, *info)
		if info.LeaderID == 0 && !info.Paused && h.leaderlessPastElectionTimeout(ns, now) {
			notReady = append(notReady, ns.shardID)
		}
		return true
	})
	slices.Sort(notReady)

	status := &HealthStatus{
		BarrierSealed:      sealed,
		DiskOK:             diskOK,
		ShardCount:         len(shards),
		Shards:             shards,
		NotReadyShards:     notReady,
		DiagnosticsDropped: supervise.DiagnosticsDropped(),
	}

	switch {
	case sealed:
		status.Status = "sealed"
	case len(shards) == 0:
		status.Status = "unhealthy"
	default:
		status.Status = "ok"
		for i := range shards {
			if shards[i].Health == ShardError {
				status.Status = "degraded"
				break
			}
		}
		// A shard with no leader past its election timeout commits no
		// write: the host is not ready. ShardError keeps precedence.
		if status.Status == "ok" && len(notReady) > 0 {
			status.Status = "not-ready"
		}
	}

	return status
}

// leaderlessPastElectionTimeout reports whether ns has had no leader for longer than leaderlessGrace as of now.
func (h *Host) leaderlessPastElectionTimeout(ns *nodeState, now int64) bool {
	since := ns.leaderlessSince.Load()
	if since == 0 {
		return false
	}
	return time.Duration(now-since) > h.leaderlessGrace(ns)
}

// leaderlessGrace is 3 * ElectionRTT * RTTMillisecond, the window a leaderless shard gets before HealthCheck reports not-ready.
func (h *Host) leaderlessGrace(ns *nodeState) time.Duration {
	nominal := time.Duration(ns.cfg.ElectionRTT*h.cfg.RTTMillisecond) * time.Millisecond
	if nominal > math.MaxInt64/3 {
		return math.MaxInt64
	}
	return 3 * nominal
}

// restartLeaderlessWindow opens a fresh election window for a leaderless shard, as ResumeShard does.
func (ns *nodeState) restartLeaderlessWindow(now int64) {
	if since := ns.leaderlessSince.Load(); since != 0 {
		ns.leaderlessSince.CompareAndSwap(since, now)
	}
}

// leaderlessClock is the clock the leaderless windows are measured on.
type leaderlessClock interface {
	// wallNanos reads the system wall clock (unix nanoseconds), which an
	// operator or NTP can step backward or forward.
	wallNanos() int64
	// monoNanos reads monotonic nanoseconds since a process-local origin,
	// which a wall-clock step never moves.
	monoNanos() int64
}

// systemClock is the production leaderlessClock.
type systemClock struct{}

// monoOrigin is the process-local origin of systemClock.monoNanos. It
// carries Go's monotonic reading, so time.Since(monoOrigin) is monotonic.
var monoOrigin = time.Now()

func (systemClock) wallNanos() int64 { return time.Now().UnixNano() }

// monoNanos is at least 1, so a reading never collides with the 0 that
// leaderlessSince stores while a leader is known.
func (systemClock) monoNanos() int64 { return int64(time.Since(monoOrigin)) + 1 }

// leaderlessNow is the reading leaderlessSince stores and
// leaderlessPastElectionTimeout compares.
func (h *Host) leaderlessNow() int64 {
	var c leaderlessClock = systemClock{}
	if h.clock != nil {
		c = h.clock
	}
	// Monotonic, so a wall-clock step does not change a window.
	return c.monoNanos()
}

// NodeIdentity returns this host's stable node identity, which peers must have committed for it.
func (h *Host) NodeIdentity() string {
	if h.transport == nil {
		return ""
	}
	return h.transport.LocalNodeIdentity()
}

// ReloadMTLS replaces the host's TLS certificate, key and CA pool without a restart (docs/quic-transport/ca-rollover.md).
func (h *Host) ReloadMTLS(mtls *config.MTLSConfig) error {
	if h.closed.Load() {
		return ErrClosed
	}
	if h.transport == nil {
		return &MTLSReloadError{Err: ErrNoLocalCertificate}
	}
	var tm *transport.MTLSConfig
	if mtls != nil {
		tm = &transport.MTLSConfig{
			CACert:      mtls.CACert,
			Cert:        mtls.Cert,
			Key:         mtls.Key,
			Certificate: mtls.Certificate,
		}
	}
	// The peer set is read and the certificate published under peerAdmitMu:
	// an admission in flight is in admittingPeers, one that has finished is
	// in the registry, and none can start until the new certificate is live.
	h.peerAdmitMu.Lock()
	defer h.peerAdmitMu.Unlock()
	var peers []string
	for _, addr := range h.registry.Addresses() {
		if addr != "" && addr != h.cfg.RaftAddress {
			peers = append(peers, addr)
		}
	}
	for addr := range h.admittingPeers {
		peers = append(peers, addr)
	}
	return h.transport.ReloadMTLS(tm, h.cfg.RaftAddress, peers)
}

// RequestAddNode proposes adding a replica; configChangeIndex, from SyncGetShardMembership, is checked when non-zero.
func (h *Host) RequestAddNode(ctx context.Context, shardID, replicaID uint64, addr string, configChangeIndex uint64, opts ...ConfigChangeOption) (*RequestState, error) {
	return h.RequestAddNodeWithIdentity(ctx, shardID, replicaID, addr, "", configChangeIndex, opts...)
}

// ConfigChangeOption adjusts a membership add request (RequestAddNode,
// RequestAddObserver, RequestAddWitness and their WithIdentity and Sync
// forms).
type ConfigChangeOption func(*configChangeOptions)

type configChangeOptions struct {
	bound bool
}

// WithBoundConfigChangeIndex checks configChangeIndex even when it is 0.
func WithBoundConfigChangeIndex() ConfigChangeOption {
	return func(o *configChangeOptions) { o.bound = true }
}

// testHookBeforeConfigChangeDeliver runs before a config change is delivered; nil in production.
var testHookBeforeConfigChangeDeliver func(cc proto.ConfigChange)

// boundConfigChange reports whether opts carry WithBoundConfigChangeIndex.
func boundConfigChange(opts []ConfigChangeOption) bool {
	var o configChangeOptions
	for _, opt := range opts {
		if opt != nil {
			opt(&o)
		}
	}
	return o.bound
}

// RequestAddNodeWithIdentity proposes adding a replica with the node identity it reports from Host.NodeIdentity.
func (h *Host) RequestAddNodeWithIdentity(ctx context.Context, shardID, replicaID uint64,
	addr, identity string, configChangeIndex uint64, opts ...ConfigChangeOption) (*RequestState, error) {

	return h.requestConfigChange(ctx, shardID, proto.ConfigChange{
		ConfigChangeID:          configChangeIndex,
		Type:                    proto.AddNode,
		ReplicaID:               replicaID,
		Address:                 addr,
		NodeIdentity:            identity,
		MustMatchConfigChangeID: boundConfigChange(opts),
	})
}

// SyncRequestAddNodeWithIdentity is RequestAddNodeWithIdentity, blocking until the change commits.
func (h *Host) SyncRequestAddNodeWithIdentity(ctx context.Context, shardID, replicaID uint64,
	addr, identity string, configChangeIndex uint64, opts ...ConfigChangeOption) error {

	rs, err := h.RequestAddNodeWithIdentity(ctx, shardID, replicaID, addr, identity, configChangeIndex, opts...)
	return awaitConfigChange(ctx, rs, err)
}

// awaitConfigChange waits for a submitted change to commit, mapping ctx expiry to ErrTimeout or ErrCanceled.
func awaitConfigChange(ctx context.Context, rs *RequestState, err error) error {
	if err != nil {
		return err
	}
	defer rs.Release()
	_, err = awaitResult(ctx, rs)
	return err
}

// RequestRemoveNode requests removing a replica from a shard via Raft
// config change. The configChangeIndex should be obtained from
// SyncGetShardMembership().ConfigChangeID.
func (h *Host) RequestRemoveNode(ctx context.Context, shardID, replicaID uint64, configChangeIndex uint64) (*RequestState, error) {
	return h.requestConfigChange(ctx, shardID, proto.ConfigChange{
		ConfigChangeID: configChangeIndex,
		Type:           proto.RemoveNode,
		ReplicaID:      replicaID,
	})
}

// SyncRequestAddNode is RequestAddNode, blocking until the change commits.
func (h *Host) SyncRequestAddNode(ctx context.Context, shardID, replicaID uint64, addr string, configChangeIndex uint64, opts ...ConfigChangeOption) error {
	rs, err := h.RequestAddNode(ctx, shardID, replicaID, addr, configChangeIndex, opts...)
	return awaitConfigChange(ctx, rs, err)
}

// SyncRequestRemoveNode is RequestRemoveNode, blocking until the change commits.
func (h *Host) SyncRequestRemoveNode(ctx context.Context, shardID, replicaID uint64, configChangeIndex uint64) error {
	rs, err := h.RequestRemoveNode(ctx, shardID, replicaID, configChangeIndex)
	return awaitConfigChange(ctx, rs, err)
}

// SyncRequestAddObserver adds an observer and blocks until the change commits; ctx must carry a deadline.
func (h *Host) SyncRequestAddObserver(ctx context.Context, shardID, replicaID uint64, addr string, configChangeIndex uint64, opts ...ConfigChangeOption) error {
	rs, err := h.RequestAddObserver(ctx, shardID, replicaID, addr, configChangeIndex, opts...)
	return awaitConfigChange(ctx, rs, err)
}

// SyncRequestAddObserverWithIdentity is SyncRequestAddObserver carrying the
// observer's node identity; see RequestAddNodeWithIdentity.
func (h *Host) SyncRequestAddObserverWithIdentity(ctx context.Context, shardID, replicaID uint64,
	addr, identity string, configChangeIndex uint64, opts ...ConfigChangeOption) error {

	rs, err := h.RequestAddObserverWithIdentity(ctx, shardID, replicaID, addr, identity, configChangeIndex, opts...)
	return awaitConfigChange(ctx, rs, err)
}

// SyncRequestAddWitness adds a witness and blocks until the change commits; ctx must carry a deadline.
func (h *Host) SyncRequestAddWitness(ctx context.Context, shardID, replicaID uint64, addr string, configChangeIndex uint64, opts ...ConfigChangeOption) error {
	rs, err := h.RequestAddWitness(ctx, shardID, replicaID, addr, configChangeIndex, opts...)
	return awaitConfigChange(ctx, rs, err)
}

// SyncRequestAddWitnessWithIdentity is SyncRequestAddWitness carrying the
// witness's node identity; see RequestAddNodeWithIdentity.
func (h *Host) SyncRequestAddWitnessWithIdentity(ctx context.Context, shardID, replicaID uint64,
	addr, identity string, configChangeIndex uint64, opts ...ConfigChangeOption) error {

	rs, err := h.RequestAddWitnessWithIdentity(ctx, shardID, replicaID, addr, identity, configChangeIndex, opts...)
	return awaitConfigChange(ctx, rs, err)
}

// RequestAddObserver proposes adding a non-voting observer; configChangeIndex is checked as for RequestAddNode.
func (h *Host) RequestAddObserver(ctx context.Context, shardID, replicaID uint64, addr string, configChangeIndex uint64, opts ...ConfigChangeOption) (*RequestState, error) {
	return h.RequestAddObserverWithIdentity(ctx, shardID, replicaID, addr, "", configChangeIndex, opts...)
}

// RequestAddObserverWithIdentity adds an observer with its node identity.
func (h *Host) RequestAddObserverWithIdentity(ctx context.Context, shardID, replicaID uint64,
	addr, identity string, configChangeIndex uint64, opts ...ConfigChangeOption) (*RequestState, error) {

	return h.requestConfigChange(ctx, shardID, proto.ConfigChange{
		ConfigChangeID:          configChangeIndex,
		Type:                    proto.AddNonVoting,
		ReplicaID:               replicaID,
		Address:                 addr,
		NodeIdentity:            identity,
		MustMatchConfigChangeID: boundConfigChange(opts),
	})
}

// RequestAddWitness proposes adding a voting witness, which stores no state machine; configChangeIndex is checked as for RequestAddNode.
func (h *Host) RequestAddWitness(ctx context.Context, shardID, replicaID uint64, addr string, configChangeIndex uint64, opts ...ConfigChangeOption) (*RequestState, error) {
	return h.RequestAddWitnessWithIdentity(ctx, shardID, replicaID, addr, "", configChangeIndex, opts...)
}

// RequestAddWitnessWithIdentity adds a witness with its node identity.
func (h *Host) RequestAddWitnessWithIdentity(ctx context.Context, shardID, replicaID uint64,
	addr, identity string, configChangeIndex uint64, opts ...ConfigChangeOption) (*RequestState, error) {

	return h.requestConfigChange(ctx, shardID, proto.ConfigChange{
		ConfigChangeID:          configChangeIndex,
		Type:                    proto.AddWitness,
		ReplicaID:               replicaID,
		Address:                 addr,
		NodeIdentity:            identity,
		MustMatchConfigChangeID: boundConfigChange(opts),
	})
}

// RequestUpdateWitnessPolicy proposes replacing the shard's replicated witness policy.
func (h *Host) RequestUpdateWitnessPolicy(ctx context.Context, shardID uint64, policy config.WitnessConfig, configChangeIndex uint64) (*RequestState, error) {
	if policy.MaxWitnesses < 0 {
		return nil, &HostInitError{
			Field: "witness_policy",
			Err: &config.ValidationError{
				Field:   "MaxWitnesses",
				Message: fmt.Sprintf("must be >= 0, got %d", policy.MaxWitnesses),
			},
		}
	}
	return h.requestConfigChange(ctx, shardID, proto.ConfigChange{
		ConfigChangeID:             configChangeIndex,
		Type:                       proto.UpdateWitnessPolicy,
		MaxWitnesses:               uint64(policy.MaxWitnesses),
		RequireFullReplicaMajority: policy.GetRequireFullReplicaMajority(),
	})
}

// SyncRequestUpdateWitnessPolicy proposes a witness-policy change and
// blocks until it commits or the context is canceled. See
// RequestUpdateWitnessPolicy.
func (h *Host) SyncRequestUpdateWitnessPolicy(ctx context.Context, shardID uint64, policy config.WitnessConfig, configChangeIndex uint64) error {
	rs, err := h.RequestUpdateWitnessPolicy(ctx, shardID, policy, configChangeIndex)
	return awaitConfigChange(ctx, rs, err)
}

// SyncRequestSnapshot requests a snapshot and blocks until it exists, returning its index.
func (h *Host) SyncRequestSnapshot(ctx context.Context, shardID uint64, opt SnapshotOption) (uint64, error) {
	rs, err := h.RequestSnapshot(ctx, shardID, opt)
	if err != nil {
		return 0, err
	}
	defer rs.Release()

	select {
	case result := <-rs.ResultC():
		if result.Err != nil {
			return 0, result.Err
		}
		return result.Value, nil
	case <-ctx.Done():
		if ctx.Err() == context.DeadlineExceeded {
			return 0, ErrTimeout
		}
		return 0, ErrCanceled
	}
}

// RemoveData removes the stopped shard/replica's LogDB data and retires the replica ID on this host.
func (h *Host) RemoveData(shardID, replicaID uint64) error {
	if h.closed.Load() {
		return ErrClosed
	}

	// Try the shard's import lock so the removal cannot interleave with a start.
	releaseImportLock, idle := h.tryAcquireImportLock(shardID)
	if !idle {
		if _, starting := h.startingShards.Load(shardID); starting {
			return ErrShardNotReady
		}
		return ErrImportInProgress
	}
	defer releaseImportLock()

	// Check if the shard is still loaded on this host.
	if v, loaded := h.nodes.Load(shardID); loaded {
		ns, ok := v.(*nodeState)
		if !ok {
			h.logger.Error("RemoveData: nodeState type assertion failed",
				"shard_id", shardID,
				"error", &TypeAssertionError{
					Context:      "RemoveData nodeState lookup",
					ExpectedType: "*nodeState",
					ActualValue:  v,
				})
		} else if !ns.stopped.Load() && ns.replicaID == replicaID {
			return &ShardNotStoppedError{ShardID: shardID, ReplicaID: replicaID}
		}
	}

	if err := h.logdb.RemoveNodeData(shardID, replicaID); err != nil {
		return err
	}

	// Fire OnNodeDeleted after successfully removing node data.
	if el := h.cfg.EventListener; el != nil && el.OnNodeDeleted != nil {
		el.OnNodeDeleted(config.EventShardInfo{
			ShardID:   shardID,
			ReplicaID: replicaID,
		})
	}

	return nil
}

// refuseRetiredReplica returns *ReplicaRetiredError when RemoveData retired
// the given replica on this host, and a LogDB error when that cannot be
// determined: a replica is started only when it is known not to be retired.
func (h *Host) refuseRetiredReplica(shardID, replicaID uint64) error {
	if h.logdb == nil {
		return nil
	}
	retired, err := h.logdb.NodeRemoved(shardID, replicaID)
	if err != nil {
		return err
	}
	if retired {
		return &ReplicaRetiredError{ShardID: shardID, ReplicaID: replicaID}
	}
	return nil
}

// SyncRemoveData is a synchronous alias for RemoveData. Since LogDB
// operations are already blocking, it behaves identically. Provided
// for API consistency with other Sync* methods.
func (h *Host) SyncRemoveData(ctx context.Context, shardID, replicaID uint64) error {
	select {
	case <-ctx.Done():
		if ctx.Err() == context.DeadlineExceeded {
			return ErrTimeout
		}
		return ErrCanceled
	default:
	}
	return h.RemoveData(shardID, replicaID)
}

// StopReplica stops a specific replica on a shard. Since each host runs
// at most one replica per shard, this validates the replicaID matches
// and delegates to the shard stop logic.
func (h *Host) StopReplica(shardID, replicaID uint64) error {
	if h.closed.Load() {
		return ErrClosed
	}

	v, exists := h.nodes.Load(shardID)
	if !exists {
		return ErrShardNotFound
	}
	ns, ok := v.(*nodeState)
	if !ok {
		return &TypeAssertionError{
			Context:      "StopReplica nodeState lookup",
			ExpectedType: "*nodeState",
			ActualValue:  v,
		}
	}
	if ns.replicaID != replicaID {
		return &ReplicaNotFoundError{ShardID: shardID, ReplicaID: replicaID}
	}

	return h.StopShard(shardID)
}

// RequestCompaction triggers explicit log compaction for the given
// shard/replica pair. This reclaims disk space by removing entries
// that have been superseded by snapshots.
func (h *Host) RequestCompaction(shardID, replicaID uint64) error {
	if h.closed.Load() {
		return ErrClosed
	}

	v, exists := h.nodes.Load(shardID)
	if !exists {
		// Not loaded: assume it was retired and reclaim what is left.
		return h.logdb.Compact(shardID, replicaID)
	}
	ns, ok := v.(*nodeState)
	if !ok {
		return &TypeAssertionError{
			Context:      "RequestCompaction nodeState lookup",
			ExpectedType: "*nodeState",
			ActualValue:  v,
		}
	}
	if ns.replicaID != replicaID {
		return &ReplicaNotFoundError{ShardID: shardID, ReplicaID: replicaID}
	}
	// A paused replica writes nothing to its log (see Host.PauseShard).
	if ns.paused.Load() {
		return ErrShardPaused
	}

	return h.logdb.Compact(shardID, replicaID)
}

// HasNodeInfo checks whether bootstrap information exists for the given
// shard/replica pair in the LogDB. Returns false if the host is closed
// or the node has never been bootstrapped on this host.
func (h *Host) HasNodeInfo(shardID, replicaID uint64) bool {
	if h.closed.Load() {
		return false
	}

	nodes, err := h.logdb.ListNodeInfo()
	if err != nil {
		return false
	}
	for _, ni := range nodes {
		if ni.ShardID == shardID && ni.ReplicaID == replicaID {
			return true
		}
	}
	return false
}

// LogReader returns a read-only view of the Raft log for the given
// shard. The returned ReadonlyLogReader provides access to log range
// and node state without permitting mutations.
func (h *Host) LogReader(shardID uint64) (ReadonlyLogReader, error) {
	if h.closed.Load() {
		return nil, ErrClosed
	}

	engNode := h.engine.GetNode(shardID)
	if engNode == nil {
		return nil, ErrShardNotFound
	}

	return &logReaderAdapter{
		reader: engNode.LogReader(),
		currentTerm: func() uint64 {
			if p := engNode.Peer(); p != nil {
				return p.Term()
			}
			return 0
		},
	}, nil
}

// LogRange represents the valid index range of a shard's Raft log.
// FirstIndex is the first available entry index (inclusive) and
// LastIndex is the last available entry index (inclusive).
type LogRange struct {
	FirstIndex uint64
	LastIndex  uint64
}

// QueryRaftLog returns the shard's local log entries in [firstIndex, lastIndex), up to maxSize bytes.
func (h *Host) QueryRaftLog(ctx context.Context, shardID uint64, firstIndex uint64, lastIndex uint64, maxSize uint64) ([]proto.Entry, error) {
	if err := ctx.Err(); err != nil {
		return nil, mapContextError(err)
	}
	if h.closed.Load() {
		return nil, ErrClosed
	}
	if firstIndex >= lastIndex {
		return nil, ErrInvalidRange
	}
	if maxSize == 0 {
		return nil, ErrInvalidRange
	}

	engNode := h.engine.GetNode(shardID)
	if engNode == nil {
		return nil, ErrShardNotFound
	}

	lr := engNode.LogReader()
	if lr == nil {
		return nil, ErrShardNotFound
	}

	// GetRangeCopy returns entries deep-copied under the reader lock, safe on a caller goroutine.
	result, err := lr.GetRangeCopy(firstIndex, lastIndex, maxSize)
	if err != nil {
		return nil, err
	}
	return result, nil
}

// LogRange returns the shard's first and last available log indexes, inclusive.
func (h *Host) LogRange(shardID uint64) (LogRange, error) {
	if h.closed.Load() {
		return LogRange{}, ErrClosed
	}

	engNode := h.engine.GetNode(shardID)
	if engNode == nil {
		return LogRange{}, ErrShardNotFound
	}

	lr := engNode.LogReader()
	if lr == nil {
		return LogRange{}, ErrShardNotFound
	}

	first, last := lr.IndexRange()
	return LogRange{
		FirstIndex: first,
		LastIndex:  last,
	}, nil
}

// requestConfigChange encodes cc as an EntryConfigChange and delivers it to the engine.
//
//nolint:gocyclo // one funnel for every membership change: role validation, witness-policy gating, cert exclusivity, claim/rollback and the pending-op bookkeeping; splitting it would move validation away from the rollback that pairs with it, which is exactly where this path has produced defects
func (h *Host) requestConfigChange(ctx context.Context, shardID uint64, cc proto.ConfigChange) (*RequestState, error) {
	// An already-cancelled context submits no membership change.
	if err := ctx.Err(); err != nil {
		return nil, mapContextError(err)
	}
	configChangeIndex := cc.ConfigChangeID
	if h.closed.Load() {
		return nil, ErrClosed
	}
	// Refuse config changes while draining, like every other new work.
	if h.draining.Load() {
		return nil, ErrDraining
	}

	ns, err := h.getNode(shardID)
	if err != nil {
		return nil, err
	}

	// Refused while paused (see Host.PauseShard).
	if ns.paused.Load() {
		return nil, ErrShardPaused
	}
	// A config change is a log entry like any proposal; a sealed barrier
	// refuses it up front (see nodeState.barrierSealed).
	if ns.barrierSealed() {
		return nil, ErrBarrierSealed
	}

	if !ns.isLeader.Load() {
		return nil, h.buildNotLeaderError(ns)
	}

	// Refuse a non-zero configChangeIndex that does not match the shard's current configuration.
	if configChangeIndex != 0 || cc.MustMatchConfigChangeID {
		current := ns.configChangeIndex.Load()
		if configChangeIndex != current {
			return nil, &StaleConfigChangeError{
				ShardID:  shardID,
				Expected: current,
				Got:      configChangeIndex,
			}
		}
	}

	// Check global rate limiter.
	if globalRL := h.engine.GlobalRateLimiter(); globalRL != nil && globalRL.RateLimited() {
		return nil, ErrSystemBusy
	}

	engNode := h.engine.GetNode(shardID)
	if engNode == nil || engNode.Peer() == nil {
		return nil, ErrShardNotReady
	}

	// Validate the change against the current membership before delivering it.
	if err := engNode.Peer().ValidateConfigChange(cc); err != nil {
		var stale *raft.StaleConfigChangeError
		if errors.As(err, &stale) {
			return nil, h.translateProposalError(ns, err)
		}
		return nil, err
	}

	// Re-check certificate exclusivity for the address this change introduces.
	if cc.Address != "" {
		releaseAddr, admitErr := h.admitMemberAddresses(map[uint64]string{
			cc.ReplicaID: cc.Address,
		})
		if admitErr != nil {
			return nil, admitErr
		}
		defer releaseAddr()
	}

	// Pre-register an added replica's address so the leader can reach it when the entry commits.
	preRegistered := cc.Address != "" && (cc.Type == proto.AddNode || cc.Type == proto.AddNonVoting || cc.Type == proto.AddWitness)
	var priorAddr string
	var hadPrior bool
	if preRegistered {
		if addr, resolveErr := h.registry.Resolve(shardID, cc.ReplicaID); resolveErr == nil && addr != "" {
			priorAddr, hadPrior = addr, true
		}
		h.registry.Register(shardID, cc.ReplicaID, cc.Address)
	}
	// Roll back the pre-registration if the proposal never commits.
	rollbackPreRegister := func() {
		if !preRegistered {
			return
		}
		if hadPrior {
			h.registry.CompareAndRegister(shardID, cc.ReplicaID, cc.Address, priorAddr)
			return
		}
		h.registry.CompareAndRemove(shardID, cc.ReplicaID, cc.Address)
	}

	buf := make([]byte, cc.Size())
	if _, marshalErr := cc.MarshalTo(buf); marshalErr != nil {
		rollbackPreRegister()
		return nil, &HostInitError{Field: "config_change_marshal", Err: marshalErr}
	}

	proposalKey := nextRequestKey()

	deadline := pendingDeadline(ctx)
	rs := newRequestState(ns.requestPool, proposalKey, deadline, h.metrics != nil)

	// Register pending before delivery to avoid a race where OnApplied
	// fires before the map entry exists.
	pp := &pendingProposal{
		rs:       rs,
		deadline: deadline,
		floor:    engNode.CommittedIndex(),
		// An appended-but-never-committed config change (leader deposed,
		// log truncated) must not leave a registry entry behind for a
		// replica that never became a member.
		onFailure: rollbackPreRegister,
	}
	// Bind the MustMatchConfigChangeID marker to the proposal key; a non-zero ID is in the entry.
	peer := engNode.Peer()
	bound := cc.MustMatchConfigChangeID && cc.ConfigChangeID == 0
	if bound {
		peer.BindConfigChange(proposalKey)
	}
	if err := h.registerPendingProposal(ns, engNode, proposalKey, pp); err != nil {
		if bound {
			peer.UnbindConfigChange(proposalKey)
		}
		pp.onFailure()
		return nil, err
	}

	entry := proto.Entry{
		Type: proto.EntryConfigChange,
		Key:  proposalKey,
		Cmd:  buf,
	}
	if hook := testHookBeforeConfigChangeDeliver; hook != nil {
		hook(cc)
	}
	if !engNode.DeliverTrackedProposal([]proto.Entry{entry}, &pp.enqueuedAt) {
		// Queue full: claim the entry before rolling back.
		if _, removed := ns.pendingProposals.LoadAndDelete(proposalKey); removed {
			rs.releaseRef()
			h.completePendingOp()
		}
		rs.Release()
		if bound {
			peer.UnbindConfigChange(proposalKey)
		}
		pp.onFailure()
		if refusal := closedQueueRefusal(engNode); refusal != nil {
			return nil, refusal
		}
		return nil, ErrSystemBusy
	}

	h.engine.NotifyWork(shardID)
	return rs, nil
}

// getNode retrieves the nodeState for a shard, returning ErrShardNotFound
// if the shard is not loaded.
func (h *Host) getNode(shardID uint64) (*nodeState, error) {
	v, ok := h.nodes.Load(shardID)
	if !ok {
		return nil, ErrShardNotFound
	}
	ns, nsOK := v.(*nodeState)
	if !nsOK {
		return nil, &TypeAssertionError{
			Context:      "getNode nodeState lookup",
			ExpectedType: "*nodeState",
			ActualValue:  v,
		}
	}
	if ns.stopped.Load() {
		return nil, ErrShardNotFound
	}
	return ns, nil
}

// buildShardInfo creates a ShardInfo snapshot from a nodeState.
func (h *Host) buildShardInfo(ns *nodeState) *ShardInfo {
	ns.nodesMu.RLock()
	nodesCopy := make(map[uint64]string, len(ns.nodes))
	for k, v := range ns.nodes {
		nodesCopy[k] = v
	}
	ns.nodesMu.RUnlock()

	// Count pending proposals for the Pending field.
	var pending uint64
	ns.pendingProposals.Range(func(_, _ any) bool {
		pending++
		return true
	})

	// One consistent triple: a ShardInfo reporting a (LeaderID, Term, IsLeader)
	// combination that never held would tell a caller this replica led in a
	// term it never led.
	leaderID, term, isLeader := ns.leaderAndTerm()
	// A paused replica reports no leader.
	paused := ns.paused.Load()
	if paused {
		leaderID, isLeader = 0, false
	}
	lastApplied := ns.lastApplied.Load()

	return &ShardInfo{
		ShardID:              ns.shardID,
		ReplicaID:            ns.replicaID,
		LeaderID:             leaderID,
		Term:                 term,
		IsLeader:             isLeader,
		IsObserver:           h.shardIsObserver(ns),
		IsWitness:            ns.cfg.IsWitness,
		StateMachineType:     ns.smType,
		StateMachineTypeName: proto.StateMachineTypeName(ns.smType),
		Pending:              pending,
		Nodes:                nodesCopy,
		Health:               h.shardHealth(ns, leaderID, lastApplied),
		LastApplied:          lastApplied,
		ConfigChangeIndex:    ns.configChangeIndex.Load(),
		Paused:               paused,
		Replicas:             h.replicaProgress(ns.shardID),
	}
}

// shardIsObserver reports whether this replica is a non-voting observer in the committed membership.
func (h *Host) shardIsObserver(ns *nodeState) bool {
	engNode := h.engine.GetNode(ns.shardID)
	if engNode == nil || engNode.Peer() == nil {
		return ns.cfg.IsObserver
	}
	return engNode.Peer().IsObserver()
}

// replicaProgress returns the leader's published follower progress, or nil on a follower, an unknown shard or before the first publication.
func (h *Host) replicaProgress(shardID uint64) []ReplicaProgress {
	engNode := h.engine.GetNode(shardID)
	if engNode == nil {
		return nil
	}
	progress := engNode.ReplicaProgress()
	if len(progress) == 0 {
		return nil
	}
	out := make([]ReplicaProgress, len(progress))
	for i := range progress {
		out[i] = ReplicaProgress{
			ReplicaID:  progress[i].ReplicaID,
			Role:       ReplicaRole(progress[i].Role),
			Match:      progress[i].Match,
			Next:       progress[i].Next,
			State:      progress[i].State,
			Paused:     progress[i].Paused,
			Active:     progress[i].Active,
			Inflights:  progress[i].Inflights,
			IsWitness:  progress[i].IsWitness,
			IsObserver: progress[i].IsObserver,
		}
	}
	return out
}

// shardLaggingThreshold is the committed-but-unapplied backlog at which a led shard reports ShardLagging.
const shardLaggingThreshold uint64 = 1000

// shardHealth derives a loaded shard's live health from the engine node.
func (h *Host) shardHealth(ns *nodeState, leaderID, lastApplied uint64) ShardHealth {
	stored := ShardHealth(ns.health.Load())
	if stored != ShardHealthy {
		return stored
	}

	engNode := h.engine.GetNode(ns.shardID)
	if engNode == nil {
		// No engine node to derive live state from; report the stored value.
		return stored
	}
	// The engine trips node.failed on a Peer.Commit error or an apply gap
	// (the shard failure latch). The shard is wedged at that point: every
	// proposal times out, so HealthCheck must report degraded rather than ok.
	if engNode.IsFailed() {
		return ShardError
	}
	// While a snapshot gate is up the replica applies nothing; report that state.
	if engNode.IsRecovering() {
		return ShardRecovering
	}
	if engNode.IsSaving() {
		return ShardSnapshotting
	}
	if leaderID == 0 {
		return ShardNoLeader
	}
	// Witnesses do not store log data and never apply, so a committed-vs-
	// applied comparison is meaningless for them.
	if !ns.cfg.IsWitness {
		if committed := engNode.CommittedIndex(); committed > lastApplied &&
			committed-lastApplied >= shardLaggingThreshold {
			return ShardLagging
		}
	}
	return ShardHealthy
}

// buildNotLeaderError creates a NotLeaderError with leader info from the
// node's cached state.
func (h *Host) buildNotLeaderError(ns *nodeState) error {
	leaderID := ns.leaderID.Load()
	var leaderAddr string
	if leaderID != 0 {
		ns.nodesMu.RLock()
		leaderAddr = ns.nodes[leaderID]
		ns.nodesMu.RUnlock()
	}
	return &NotLeaderError{
		ShardID:       ns.shardID,
		LeaderID:      leaderID,
		LeaderAddress: leaderAddr,
	}
}

// loadOrGenerateHostID loads an existing host ID from disk or generates a
// new one. If overrideID is non-empty, it is used as-is. The logger is
// used for warnings about insecure file permissions or invalid formats.
func loadOrGenerateHostID(logger *slog.Logger, dir, overrideID string) (string, error) {
	if overrideID != "" {
		return overrideID, nil
	}

	idPath := filepath.Join(dir, hostIDFileName)

	// Try to load existing ID.
	data, err := os.ReadFile(idPath)
	if err == nil {
		id := string(data)
		if len(id) == hostIDLength {
			// Validate hex encoding.
			if _, err := hex.DecodeString(id); err == nil {
				// Check file permissions.
				info, statErr := os.Stat(idPath)
				if statErr == nil && info.Mode().Perm() != hostIDFilePerms {
					logger.Warn("host ID file has insecure permissions",
						"current", fmt.Sprintf("%04o", info.Mode().Perm()),
						"expected", fmt.Sprintf("%04o", hostIDFilePerms),
					)
					if chmodErr := os.Chmod(idPath, hostIDFilePerms); chmodErr != nil {
						logger.Warn("failed to fix host ID file permissions",
							"path", idPath,
							"error", chmodErr,
						)
					}
				}
				return id, nil
			}
		}
		// Invalid format: regenerate.
		logger.Warn("host ID file has invalid format, regenerating", "path", idPath)
	}

	// Generate new ID.
	buf := make([]byte, hostIDLength/2)
	if _, err := rand.Read(buf); err != nil {
		return "", &HostInitError{Field: "host_id", Err: err}
	}
	id := hex.EncodeToString(buf)

	if err := os.WriteFile(idPath, []byte(id), hostIDFilePerms); err != nil {
		return "", &HostInitError{Field: "host_id", Err: err}
	}

	return id, nil
}

// buildTransportConfig maps a HostConfig to the internal transport.Config.
func buildTransportConfig(hc config.HostConfig) transport.Config {
	tc := hc.TransportConfig
	var mtls *transport.MTLSConfig
	if tc.MTLSConfig != nil {
		mtls = &transport.MTLSConfig{
			CACert:      tc.MTLSConfig.CACert,
			Cert:        tc.MTLSConfig.Cert,
			Key:         tc.MTLSConfig.Key,
			Certificate: tc.MTLSConfig.Certificate,
		}
	}
	// Map the public RevocationConfig to the internal type.
	var revCfg *revocation.Config
	if tc.RevocationConfig != nil {
		rc := tc.RevocationConfig
		revCfg = &revocation.Config{
			CRLPaths:           rc.CRLPaths,
			OCSPResponderURL:   rc.OCSPResponderURL,
			OCSPCacheSeconds:   rc.OCSPCacheSeconds,
			OCSPTimeoutSeconds: rc.OCSPTimeoutSeconds,
			CheckInterval:      rc.CheckInterval,
			Mode:               revocation.Mode(rc.Mode),
			// Revocation fails closed unless the operator opted into soft-fail.
			AllowSoftFail: rc.AllowSoftFail,
			OnCertRevoked: rc.OnCertRevoked,
		}
	}

	cfg := transport.Config{
		ListenAddress:             hc.ListenAddress,
		AdvertiseAddress:          hc.RaftAddress,
		DeploymentID:              hc.DeploymentID,
		Enable0RTT:                tc.Enable0RTT,
		StreamPoolSize:            tc.StreamPoolSize,
		MaxStreamPoolSize:         tc.MaxStreamPoolSize,
		MaxSnapshotReceiveRate:    tc.MaxSnapshotReceiveRate,
		MaxConcurrentSnapshotRecv: tc.MaxConcurrentSnapshotReceives,
		MaxSnapshotReceiveMemory:  tc.MaxSnapshotReceiveMemory,
		MaxMessageReceiveMemory:   tc.MaxMessageReceiveMemory,
		SendBatchMaxSize:          tc.SendBatchMaxSize,
		MaxDecompressedSize:       tc.MaxDecompressedSize,
		MaxIncomingConnections:    tc.MaxIncomingConnections,
		MaxConnectionsPerIP:       tc.MaxConnectionsPerIP,
		MaxStreamsPerConnection:   tc.MaxStreamsPerConnection,
		StreamReadIdleTimeout:     tc.StreamReadIdleTimeout,
		UDPRecvBufSize:            tc.UDPRecvBufSize,
		UDPSendBufSize:            tc.UDPSendBufSize,
		DisableCompression:        tc.DisableCompression,
		MTLSConfig:                mtls,
		RevocationConfig:          revCfg,
		EventListener:             hc.EventListener,
	}
	cfg.SetDefaults()
	return cfg
}

// maxMembershipReconstructBytes bounds each log read during membership reconstruction.
const maxMembershipReconstructBytes = 4 << 20 // 4 MiB

// reconstructShardMembership recovers a shard's initial membership from its committed log when no snapshot carries it.
func (h *Host) reconstructShardMembership(lr *logdb.LogReader, commit uint64) (proto.Membership, bool, error) {
	first, last := lr.IndexRange()
	if first == 0 || commit < first {
		return proto.Membership{}, false, nil
	}
	// If the log starts above the bootstrap index, a snapshot carries the membership.
	if first > raft.BootstrapMembershipIndex || last < raft.BootstrapMembershipIndex ||
		commit < raft.BootstrapMembershipIndex {
		return proto.Membership{}, false, nil
	}
	entries, err := lr.GetRange(raft.BootstrapMembershipIndex,
		raft.BootstrapMembershipIndex+1, maxMembershipReconstructBytes)
	if err != nil {
		// ErrCompacted and ErrUnavailable fall back to the seed; any other error fails the start.
		if errors.Is(err, logdb.ErrCompacted) || errors.Is(err, logdb.ErrUnavailable) {
			return proto.Membership{}, false, nil
		}
		return proto.Membership{}, false, err
	}
	if len(entries) == 0 || entries[0].Index != raft.BootstrapMembershipIndex {
		// The entry could not be read at all, which is the same
		// "not available" answer the ErrCompacted/ErrUnavailable branch above
		// gives: benign, fall back to the seed plus commit-time repair.
		return proto.Membership{}, false, nil
	}
	if entries[0].Type&proto.EntryTypeMask != proto.EntryInitialMembership {
		// Index 1 exists but is not the founding configuration: refuse with BootstrapEntryMissingError.
		return proto.Membership{}, false, &BootstrapEntryMissingError{
			Index:     entries[0].Index,
			EntryType: entries[0].Type & proto.EntryTypeMask,
		}
	}
	ms, err := raft.ReconstructMembership([]proto.Entry{entries[0]})
	if err != nil {
		logger, e := h.logger, entries[0]
		supervise.LogContained(func() {
			logger.Error("committed initial-membership entry does not decode during recovery",
				"entry_index", e.Index,
				"entry_term", e.Term,
				"entry_type", e.Type,
				"error", err)
		})
		return proto.Membership{}, false, err
	}
	return ms, true, nil
}

// resolveRecoveredTerm returns the Raft term of index, from the log first and the snapshot record second.
func (h *Host) resolveRecoveredTerm(lr *logdb.LogReader, shardID, replicaID, index uint64) (uint64, error) {
	if term, err := lr.Term(index); err == nil && term > 0 {
		return term, nil
	}
	if h.logdb != nil {
		// Only an exact match is usable: a snapshot at a different index says
		// nothing about the term at this one.
		if ss, err := h.logdb.GetSnapshot(shardID, replicaID); err == nil && ss.Index == index && ss.Term > 0 {
			return ss.Term, nil
		}
	}
	return 0, ErrRecoveredTermUnknown
}

// recoverLogReader restores a LogReader from LogDB on restart: snapshot, hard state and entry range.
func (h *Host) recoverLogReader(lr *logdb.LogReader, shardID, replicaID uint64) (bool, error) {
	if h.logdb == nil {
		return false, nil
	}

	// persisted reports whether LogDB holds any durable Raft state for this replica, distinguishing a first start from a restart.
	var persisted bool

	// Step 1: Restore snapshot metadata. This sets markerIndex,
	// markerTerm, and membership on the LogReader.
	ss, err := h.logdb.GetSnapshot(shardID, replicaID)
	if err != nil && err != logdb.ErrNoSnapshot {
		return false, err
	}
	if err == nil && ss.Index > 0 {
		persisted = true
		// The snapshot data file is snapshot.dat in the recorded directory.
		var snapFilePath string
		var snapFileSize uint64
		if ss.Filepath != "" {
			dataPath := filepath.Join(ss.Filepath, "snapshot.dat")
			// Fail the start when the artifact cannot be measured.
			fi, statErr := h.snapshotFsys().Stat(dataPath)
			if statErr != nil && !errors.Is(statErr, os.ErrNotExist) {
				return false, statErr
			}
			snapFilePath = dataPath
			if statErr == nil {
				snapFileSize = uint64(fi.Size())
			}
		}
		protoSS := proto.Snapshot{
			ShardID:    shardID,
			ReplicaID:  replicaID,
			Index:      ss.Index,
			Term:       ss.Term,
			Membership: logdbMembershipToProto(ss.Membership),
			Filepath:   snapFilePath,
			FileSize:   snapFileSize,
			Epoch:      ss.Epoch,
			// Carry the record's full description: the send path ships it to followers.
			OnDiskIndex: ss.OnDiskIndex,
			Compression: uint64(ss.Compression),
		}
		if applyErr := lr.ApplySnapshot(protoSS); applyErr != nil {
			return false, applyErr
		}
	}

	// Step 2: Restore hard state (term, vote, commit).
	state, membership, err := h.logdb.ReadState(shardID, replicaID)
	if err != nil && err != logdb.ErrNoState {
		return false, err
	}
	if err == nil {
		persisted = true
		lr.SetState(proto.State{
			Term:       state.Term,
			Vote:       state.Vote,
			Commit:     state.Commit,
			VoteWindow: state.VoteWindow,
		})
		// Apply the membership ReadState returned, if any.
		if membership.ConfigChangeID > 0 || len(membership.Addresses) > 0 {
			lr.SetMembership(logdbMembershipToProto(membership))
		}
	}

	// Step 3: Restore the tracked entry range so LogReader knows which
	// indices are available in LogDB. This is O(1) -- it does not load
	// entry payloads.
	firstIndex, entryCount, err := h.logdb.EntryRange(shardID, replicaID)
	if err != nil {
		// Repair a stale log prefix left below a recorded snapshot.
		if !errors.Is(err, logdb.ErrLogGapDetected) || ss.Index == 0 {
			return false, err
		}
		h.logger.Warn("log entry index holed below a recorded snapshot; removing the "+
			"subsumed prefix to repair recovery",
			"shard_id", shardID, "replica_id", replicaID, "snapshot_index", ss.Index)
		if rmErr := h.logdb.RemoveEntriesTo(shardID, replicaID, ss.Index); rmErr != nil {
			// Report why the repair failed, not the gap.
			return false, rmErr
		}
		firstIndex, entryCount, err = h.logdb.EntryRange(shardID, replicaID)
		if err != nil {
			return false, err
		}
		h.logger.Info("log entry index repaired",
			"shard_id", shardID, "replica_id", replicaID,
			"first_index", firstIndex, "entry_count", entryCount)
	}
	if entryCount > 0 {
		persisted = true
		if rangeErr := lr.SetRange(firstIndex, entryCount); rangeErr != nil {
			return false, rangeErr
		}
	}

	return persisted, nil
}

// logdbMembershipToProto converts a logdb.Membership to a deep-copied proto.Membership.
func logdbMembershipToProto(m logdb.Membership) proto.Membership {
	result := proto.Membership{
		ConfigChangeID:             m.ConfigChangeID,
		MaxWitnesses:               m.MaxWitnesses,
		RequireFullReplicaMajority: m.RequireFullReplicaMajority,
		SessionExpiryEntries:       m.SessionExpiryEntries,
		MaxTotalSessions:           m.MaxTotalSessions,
		MaxResponsesPerSession:     m.MaxResponsesPerSession,
		MaxSessionsPerOwner:        m.MaxSessionsPerOwner,
		WireVersion:                m.WireVersion,
	}
	if len(m.Addresses) > 0 {
		result.Addresses = make(map[uint64]string, len(m.Addresses))
		for k, v := range m.Addresses {
			result.Addresses[k] = v
		}
	}
	if len(m.Observers) > 0 {
		result.Observers = make(map[uint64]string, len(m.Observers))
		for k, v := range m.Observers {
			result.Observers[k] = v
		}
	}
	if len(m.Witnesses) > 0 {
		result.Witnesses = make(map[uint64]string, len(m.Witnesses))
		for k, v := range m.Witnesses {
			result.Witnesses[k] = v
		}
	}
	if len(m.Removed) > 0 {
		result.Removed = make(map[uint64]bool, len(m.Removed))
		for k, v := range m.Removed {
			result.Removed[k] = v
		}
	}
	if len(m.Identities) > 0 {
		result.Identities = make(map[uint64]string, len(m.Identities))
		for k, v := range m.Identities {
			result.Identities[k] = v
		}
	}
	if len(m.RemovedIdentities) > 0 {
		result.RemovedIdentities = make(map[uint64]string, len(m.RemovedIdentities))
		for k, v := range m.RemovedIdentities {
			result.RemovedIdentities[k] = v
		}
	}
	return result
}

// HostInitError is returned when Host initialization fails for a specific
// component or configuration field.
type HostInitError struct {
	Field string
	Err   error
}

// Error returns a human-readable description of the initialization failure.
func (e *HostInitError) Error() string {
	return fmt.Sprintf("quicraft: host init failed for %s: %v", e.Field, e.Err)
}

// Unwrap returns the underlying error.
func (e *HostInitError) Unwrap() error {
	return e.Err
}

// BootstrapEntryMissingError reports a durable log whose bootstrap index holds something other than the founding configuration.
type BootstrapEntryMissingError struct {
	// Index is the log index inspected (raft.BootstrapMembershipIndex).
	Index uint64
	// EntryType is the masked proto entry type actually found there.
	EntryType uint64
}

// Error returns a human-readable description of the damaged bootstrap entry.
func (e *BootstrapEntryMissingError) Error() string {
	return fmt.Sprintf("quicraft: log index %d holds entry type %d, not the "+
		"bootstrap membership entry; the shard's durable log is damaged",
		e.Index, e.EntryType)
}

// releaseRemovedMemberConn drops the connection to a replica a committed change removed, once no other shard needs that peer.
func (h *Host) releaseRemovedMemberConn(shardID, replicaID uint64, address string) {
	if h.transport == nil || address == "" {
		return
	}
	if h.registry.AddressInUse(address) {
		return
	}
	if h.transport.CloseTarget(address) {
		h.logger.Info("released connection to removed replica",
			"shard", shardID,
			"replica", replicaID,
			"address", address,
		)
	}
}

// leaderAndTerm returns the cached leader ID, term and leadership flag as a combination that co-existed.
func (ns *nodeState) leaderAndTerm() (leaderID, term uint64, isLeader bool) {
	leaderID, term, isLeader, _ = ns.leaderTermReady()
	return leaderID, term, isLeader
}

// leaderTermReady is leaderAndTerm plus the pair's read readiness; safe from any goroutine.
func (ns *nodeState) leaderTermReady() (leaderID, term uint64, isLeader, readReady bool) {
	for {
		before := ns.leaderSeq.Load()
		if before&1 != 0 {
			// A write is in flight; its window is four stores.
			continue
		}
		leaderID = ns.leaderID.Load()
		term = ns.term.Load()
		isLeader = ns.isLeader.Load()
		readReady = ns.readReady.Load()
		if ns.leaderSeq.Load() == before {
			return leaderID, term, isLeader, readReady
		}
	}
}

// updateNodeLeader updates the leader information for a node. This is
// called by the engine when a leader change is detected.
func (h *Host) updateNodeLeader(shardID, leaderID, term uint64) {
	v, exists := h.nodes.Load(shardID)
	if !exists {
		return
	}
	ns, ok := v.(*nodeState)
	if !ok {
		h.logger.Error("updateNodeLeader: nodeState type assertion failed",
			"shard_id", shardID,
			"error", &TypeAssertionError{
				Context:      "updateNodeLeader nodeState lookup",
				ExpectedType: "*nodeState",
				ActualValue:  v,
			})
		return
	}

	// Odd for the duration of the update, even once the triple is settled: see
	// nodeState.leaderSeq.
	ns.leaderSeq.Add(1)
	ns.leaderID.Store(leaderID)
	ns.term.Store(term)
	ns.isLeader.Store(leaderID == ns.replicaID)
	ns.readReady.Store(false)
	ns.leaderSeq.Add(1)
	// Outside the seqlock: no reader pairs it with the triple. A lost
	// leader opens the window once (CAS keeps the first loss time across
	// repeated leaderless updates); a known leader closes it.
	if leaderID == 0 {
		ns.leaderlessSince.CompareAndSwap(0, h.leaderlessNow())
	} else {
		ns.leaderlessSince.Store(0)
	}
	ns.signalLeaderChange()
}

// markLeaderReady marks the cached (leaderID, term) pair read-ready, refusing a pair that is no longer cached.
func (ns *nodeState) markLeaderReady(leaderID, term uint64) bool {
	if ns.leaderID.Load() != leaderID || ns.term.Load() != term {
		return false
	}
	ns.leaderSeq.Add(1)
	ns.readReady.Store(true)
	ns.leaderSeq.Add(1)
	ns.signalLeaderChange()
	return true
}

// leaderChanged returns the channel the next leadership-view change closes; take it before reading the view.
func (ns *nodeState) leaderChanged() <-chan struct{} {
	for {
		if c := ns.leaderWake.Load(); c != nil {
			return *c
		}
		c := make(chan struct{})
		if ns.leaderWake.CompareAndSwap(nil, &c) {
			return c
		}
	}
}

// signalLeaderChange wakes every leaderChanged waiter. Each channel is closed
// by the one call that swapped it out, so concurrent signals never close the
// same channel twice. It must follow the write it announces.
func (ns *nodeState) signalLeaderChange() {
	c := make(chan struct{})
	if old := ns.leaderWake.Swap(&c); old != nil {
		close(*old)
	}
}

// buildWALOptions builds the waldb options for cfg, with the WAL disk monitor publishing into diskFullFlag.
//
//nolint:unparam // shards kept: same inputs as hostDiskMonitorOptions; NewHost wires both monitors through it
func buildWALOptions(cfg config.HostConfig, diskFullFlag *atomic.Bool, shards func() []uint64) []waldb.Option {
	_, walMon := hostDiskMonitorOptions(cfg, diskFullFlag, shards)
	return walDBOptions(cfg, walMon)
}

// walDBOptions returns the WAL options, with a disk monitor only when MaxWALDiskSize is set.
func walDBOptions(cfg config.HostConfig, monOpts []waldb.DiskMonitorOption) []waldb.Option {
	var opts []waldb.Option

	// Disable fsync when NoSyncWAL is set (testing only — never production).
	if cfg.NoSyncWAL {
		opts = append(opts, waldb.WithNoSync(true))
	}
	if cfg.MaxWALDiskSize > 0 {
		opts = append(opts, waldb.WithDiskMonitorOpts(cfg.MaxWALDiskSize, monOpts...))
	}
	return opts
}

// hostDiskMonitorOptions returns the options of the proposal-gate and WAL disk monitors.
func hostDiskMonitorOptions(cfg config.HostConfig, diskFullFlag *atomic.Bool, shards func() []uint64) (gate, wal []waldb.DiskMonitorOption) {
	state := newDiskFullState(cfg, diskFullFlag, shards)
	clock := waldb.WithSampleClock(&state.clock)
	gate = []waldb.DiskMonitorOption{
		waldb.WithFullPercent(proposalDiskFullPercent),
		waldb.WithCheckEvery(proposalDiskCheckEvery),
		clock,
		waldb.WithOnSample(state.publisher(true)),
	}
	wal = []waldb.DiskMonitorOption{clock, waldb.WithOnSample(state.publisher(false))}
	if cfg.EventListener != nil && cfg.EventListener.OnLogDBDiskWarning != nil {
		walDir := cfg.WALDir
		warningCb := cfg.EventListener.OnLogDBDiskWarning
		wal = append(wal, waldb.WithOnWarning(func(used, maxBytes uint64) {
			warningCb(config.DiskInfo{
				WALDir:    walDir,
				UsedBytes: used,
				MaxBytes:  maxBytes,
				Shards:    diskShards(shards),
			})
		}))
	}
	return gate, wal
}

// proposalDiskFullPercent is the share of MaxWALDiskSize at which proposals are refused, below the WAL's own limit.
const proposalDiskFullPercent = 0.90

// proposalDiskCheckEvery is the proposals between proposal-gate samples, a tenth of the WAL's cadence.
const proposalDiskCheckEvery = waldb.DefaultCheckEvery / 10

// diskFullState derives the host's disk-full flag from the latest samples of both disk monitors.
type diskFullState struct {
	flag *atomic.Bool
	// clock is the sample clock both monitors share (waldb.WithSampleClock).
	clock atomic.Uint64

	mu   sync.Mutex
	gate waldb.DiskSample // latest proposal gate sample
	wal  waldb.DiskSample // latest WAL monitor sample

	fullCb func(config.DiskInfo)
	walDir string
	shards func() []uint64
}

func newDiskFullState(cfg config.HostConfig, diskFullFlag *atomic.Bool, shards func() []uint64) *diskFullState {
	s := &diskFullState{flag: diskFullFlag, walDir: cfg.WALDir, shards: shards}
	if cfg.EventListener != nil {
		s.fullCb = cfg.EventListener.OnLogDBDiskFull
	}
	return s
}

// publisher returns the waldb.WithOnSample hook of the gate (fromGate) or of
// the WAL monitor.
func (s *diskFullState) publisher(fromGate bool) func(waldb.DiskSample) func() {
	return func(sample waldb.DiskSample) func() {
		return s.publish(fromGate, sample)
	}
}

// full derives the flag from the newest sample; s.mu must be held.
func (s *diskFullState) full() bool {
	if s.wal.Seq <= s.gate.Seq {
		return s.gate.Full
	}
	if s.wal.Full {
		return true
	}
	gateThreshold := uint64(float64(s.wal.MaxBytes) * proposalDiskFullPercent)
	return s.gate.Full && s.wal.Used >= gateThreshold
}

// publish records sample and recomputes the flag under s.mu, returning the notification to run when it rises.
func (s *diskFullState) publish(fromGate bool, sample waldb.DiskSample) func() {
	s.mu.Lock()
	latest := &s.wal
	if fromGate {
		latest = &s.gate
	}
	if sample.Seq < latest.Seq {
		s.mu.Unlock()
		return nil
	}
	*latest = sample
	full := s.full()
	rose := !s.flag.Swap(full) && full
	s.mu.Unlock()
	if !rose || s.fullCb == nil {
		return nil
	}
	fullCb, walDir, shards := s.fullCb, s.walDir, s.shards
	return func() {
		fullCb(config.DiskInfo{
			WALDir:    walDir,
			UsedBytes: sample.Used,
			MaxBytes:  sample.MaxBytes,
			Shards:    diskShards(shards),
		})
	}
}

// newProposalDiskGate builds the proposal-path disk monitor, or nil when MaxWALDiskSize is zero.
func newProposalDiskGate(cfg config.HostConfig, gateOpts []waldb.DiskMonitorOption) *waldb.DiskMonitor {
	if cfg.MaxWALDiskSize == 0 {
		return nil
	}
	return waldb.NewDiskMonitor(cfg.WALDir, cfg.MaxWALDiskSize, gateOpts...)
}

// proposalDiskFull reports whether a proposal or import must be refused with ErrDiskFull.
func (h *Host) proposalDiskFull() bool {
	if h.diskGate != nil && h.diskGate.MaybeCheck() != nil {
		return true
	}
	return h.diskFull != nil && h.diskFull.Load()
}

// diskShards calls shards when it is set, for DiskInfo.Shards.
func diskShards(shards func() []uint64) []uint64 {
	if shards == nil {
		return nil
	}
	return shards()
}

// walShardIDs returns the sorted IDs of the shards db holds data for, or nil when db is nil or cannot list them.
func walShardIDs(db logdb.LogDB) []uint64 {
	if db == nil {
		return nil
	}
	nodes, err := db.ListNodeInfo()
	if err != nil || len(nodes) == 0 {
		return nil
	}
	ids := make([]uint64, 0, len(nodes))
	for _, ni := range nodes {
		ids = append(ids, ni.ShardID)
	}
	slices.Sort(ids)
	return slices.Compact(ids)
}

// mapContextError translates a context error into the corresponding
// quicraft sentinel error. This ensures callers always see ErrCanceled
// or ErrTimeout rather than raw context.Canceled / context.DeadlineExceeded.
func mapContextError(err error) error {
	if err == context.DeadlineExceeded {
		return ErrTimeout
	}
	if err == context.Canceled {
		return ErrCanceled
	}
	return err
}

// effectiveMaxPayload returns the smaller non-zero of the two payload limits.
func effectiveMaxPayload(maxProposal, maxEntry uint64) uint64 {
	switch {
	case maxProposal > 0 && maxEntry > 0:
		if maxProposal < maxEntry {
			return maxProposal
		}
		return maxEntry
	case maxProposal > 0:
		return maxProposal
	default:
		return maxEntry
	}
}

// copyWithContextChunkSize is the number of bytes copied between context
// cancellation checks in copyWithContext.
const copyWithContextChunkSize = 1 << 20 // 1 MB

// copyWithContext copies src to dst, checking ctx every 1 MB.
func copyWithContext(ctx context.Context, dst io.Writer, src io.Reader) error {
	_, err := copyWithContextN(ctx, dst, src)
	return err
}

// copyWithContextN is copyWithContext, also returning the bytes written.
func copyWithContextN(ctx context.Context, dst io.Writer, src io.Reader) (int64, error) {
	buf := make([]byte, copyWithContextChunkSize)
	var written int64
	for {
		if err := ctx.Err(); err != nil {
			return written, mapContextError(err)
		}
		n, readErr := src.Read(buf)
		if n > 0 {
			w, writeErr := dst.Write(buf[:n])
			written += int64(w)
			if writeErr != nil {
				return written, writeErr
			}
		}
		if readErr != nil {
			if readErr == io.EOF {
				return written, nil
			}
			return written, readErr
		}
	}
}

// recordedSnapshotPath returns the cleaned snapshot directory LogDB records for the node, or "" when there is none.
func (h *Host) recordedSnapshotPath(shardID, replicaID uint64) (string, error) {
	if h.logdb == nil {
		return "", nil
	}
	rec, err := h.logdb.GetSnapshot(shardID, replicaID)
	if errors.Is(err, logdb.ErrNoSnapshot) {
		return "", nil
	}
	if err != nil {
		return "", err
	}
	if rec.Filepath == "" {
		return "", nil
	}
	return filepath.Clean(rec.Filepath), nil
}

// snapshotRecordMayName reports whether the shard's LogDB snapshot record
// names dir — or might: a record that cannot be read is answered true, so a
// caller about to delete dir keeps it instead.
func (h *Host) snapshotRecordMayName(shardID, replicaID uint64, dir string) bool {
	recorded, err := h.recordedSnapshotPath(shardID, replicaID)
	return err != nil || (recorded != "" && recorded == filepath.Clean(dir))
}

// StateMachineHash returns a hash of this replica's applied state machine and the index it was taken at, to detect divergent replicas.
func (h *Host) StateMachineHash(shardID uint64) (hash uint64, appliedIndex uint64, ok bool, err error) {
	if h.closed.Load() {
		return 0, 0, false, ErrClosed
	}
	ns, nsErr := h.getNode(shardID)
	if nsErr != nil {
		return 0, 0, false, nsErr
	}
	if ns.rsm == nil {
		return 0, 0, false, ErrShardNotFound
	}
	// Read the index first so it is never newer than the hashed state.
	appliedIndex = ns.rsm.LastApplied()
	hash, ok, err = ns.rsm.GetHash()
	if err != nil {
		return 0, 0, false, err
	}
	return hash, appliedIndex, ok, nil
}
