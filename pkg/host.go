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

	// pendingSweepInterval is how often the background sweeper scans
	// all node states for orphaned pending proposals, reads, and
	// snapshots whose deadlines have expired. Expired entries are
	// completed with ErrTimeout to prevent callers from blocking
	// indefinitely when entries are lost (leader crash, partition).
	pendingSweepInterval = 10 * time.Second

	// loopPanicRestartDelay is the delay before restarting a panicked
	// background loop. This prevents tight panic-restart loops from
	// consuming CPU and flooding logs when a loop hits a persistent
	// panic trigger.
	loopPanicRestartDelay = 100 * time.Millisecond

	// defaultPendingRequestTimeout bounds a pending proposal, read, snapshot,
	// or config change whose caller supplied a context with NO deadline
	// (context.Background() is the common case for async callers).
	//
	// Every pending entry must carry a deadline the sweeper can act on. The
	// sweeper skips zero deadlines by design — a zero value there means "not
	// set yet", and expiring on it would complete every freshly registered
	// entry — so an entry stored with a zero deadline is never swept: its
	// RequestState never returns to the pool and h.pendingOps never drops back
	// to zero, which makes EVERY later Drain() burn the full ShutdownTimeout
	// and return ErrShutdownTimeout. Substituting this bound at REGISTRATION
	// keeps the sweeper's rule intact while guaranteeing the entry expires.
	//
	// It is deliberately far longer than any healthy commit path: it is a
	// backstop for entries that can never complete (leader crash, partition,
	// a lost apply), not a request SLA. Callers that need a tighter bound pass
	// their own context deadline.
	defaultPendingRequestTimeout = 5 * time.Minute

	// defaultPendingSnapshotTimeout is the same backstop for a pending SNAPSHOT
	// request, which is not a commit-path operation and must not inherit the
	// commit-path bound.
	//
	// A snapshot save is local work whose duration scales with the state machine,
	// not with consensus: a multi-gigabyte on-disk SM legitimately takes far
	// longer than any proposal. Clamping it to defaultPendingRequestTimeout makes
	// SyncRequestSnapshot return ErrTimeout while the save is still running
	// correctly, and the caller's retry then hits ErrSnapshotInProgress — the
	// save is fine, the answer is wrong. One hour matches the snapshot transfer
	// ceiling the receive path already enforces, so the two bounds agree.
	defaultPendingSnapshotTimeout = 1 * time.Hour
)

// Barrier is the interface for at-rest encryption barriers. This interface
// is defined in the quicraft package to break the import cycle between
// quicraft and quicraft/crypto. The crypto.Barrier type satisfies this
// interface.
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

	// DecryptWithContext decrypts ciphertext that was sealed with the same
	// caller context folded into the AAD. A different context — or none where
	// one was present — fails authentication, and there is deliberately no
	// weaker retry: the fallback is the vulnerability.
	DecryptWithContext(dst, ciphertext, contextAAD []byte) ([]byte, error)

	// EncryptWithEpoch encrypts plaintext and returns both the ciphertext
	// and the epoch used for encryption.
	EncryptWithEpoch(dst, plaintext []byte) ([]byte, uint64, error)

	// EncryptWithEpochContext is EncryptWithEpoch with a caller-supplied
	// context folded into the AAD alongside the epoch. An empty context is
	// byte-identical to EncryptWithEpoch.
	EncryptWithEpochContext(dst, plaintext, contextAAD []byte) ([]byte, uint64, error)

	// EncryptWithEpochPinnedContext is EncryptWithEpochContext that also
	// registers the epoch it sealed under as in-flight, in ONE atomic step
	// under the barrier's own lock; the caller must DeregisterInFlightEpoch it.
	//
	// It is required, not optional. EncryptWithEpoch releases the lock before
	// returning, so a Rotate followed by PurgeEpochsBefore could otherwise
	// complete in the gap before the caller pinned the returned epoch —
	// purging the DEK of ciphertext about to be persisted, which would then be
	// permanently undecryptable.
	EncryptWithEpochPinnedContext(dst, plaintext, contextAAD []byte) ([]byte, uint64, error)

	// DecryptForEpoch decrypts ciphertext using a specific epoch's DEK.
	DecryptForEpoch(dst, ciphertext []byte, epoch uint64) ([]byte, error)

	// IsHardwareMode reports whether the barrier delegates sealing to an
	// external encryptor. It says nothing about whether that encryptor can
	// authenticate caller context — use SupportsContextAAD for that.
	IsHardwareMode() bool

	// SupportsContextAAD reports whether the barrier can ACTUALLY authenticate
	// the caller context snapshot frames are bound to: true for a software
	// barrier and for a delegated encryptor that carries AAD end to end, false
	// for a delegated encryptor that cannot and in no-encryption mode. When it
	// is false, frames carry only the barrier's own binding — a capability
	// limitation of the deployment's barrier, never a per-artifact choice.
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

// WithShardBarriers enables PER-SHARD at-rest encryption: each Raft shard can
// be sealed under its OWN barrier (per-tenant WAL/snapshot keys) instead of a
// single host-wide barrier. The resolver fn maps a shardID to the barrier that
// must encrypt that shard's snapshots and WAL records; returning nil for a
// shard falls back to the host-wide route: the barrier configured via
// WithBarrier, or plaintext when the host declared WithoutEncryption.
//
// Per-shard barriers are independent of the host-wide posture. They combine
// with WithBarrier (including a barrier still in plaintext mode before
// EnableEncryption) and with WithoutEncryption alike, and a shard with a
// barrier of its own gets its snapshots encrypted under it in every posture.
// With neither host-wide option, a nil result has no route to fall back to and
// the shard fails closed with ErrShardBarrierUnresolved. A shard whose barrier
// is sealed does not start (ErrBarrierSealed); nothing stands in for it.
//
// This option is purely ADDITIVE and backward compatible. When it is not used,
// the host behaves exactly as before: WithBarrier installs a single barrier
// that encrypts every shard, and the snapshot pool's DEK-purge floor is global.
// When WithShardBarriers IS used, snapshot encrypt/decrypt, the snapshot
// transport (decrypt-for-send and in-flight epoch protection), and the
// snapshot-pool DEK purge all dispatch per shard, so purging one tenant's
// barrier can never remove epochs still live under another tenant's barrier.
//
// The caller that owns the per-shard barriers is responsible for their key
// rotation; the host-wide key-rotation loop only rotates the WithBarrier
// barrier. For per-shard barriers to also carry their own WAL records, the
// caller supplies a multiplexing LogDB via WithLogDB that implements
// logdb.PerBarrierEpochAwareLogDB (one dedicated single-barrier WAL per
// tenant) and reports logdb.AtRestHostRouted when its shared WAL is the
// host-wide route. StartShard then requires each barriered shard's records to
// be stored under that shard's own barrier (ErrShardBarrierLogDBMismatch).
// Without WithBarrier, a LogDB that would hold a barriered shard's log in
// plaintext is refused (ErrShardBarriersPlaintextWAL), WithoutEncryption or
// not. All barriers must be initialized and unsealed.
//
// A barrier's dynamic value need not be comparable; the host never compares
// two barriers with == or uses one as a map key. Two shards share a barrier
// when the resolver returns the same pointer for both. A barrier that is not a
// pointer is distinct for every shard that resolves to it: its shards are not
// grouped, the snapshot pool declines per-barrier DEK purges it cannot prove
// safe, and a per-barrier LogDB answers that it does not own it.
//
// Host.Close seals every barrier the resolver returned during the host's
// lifetime, after the shards and the LogDB using them have stopped. A pointer
// barrier shared by several shards, or the WithBarrier barrier itself, is
// sealed once; a non-pointer barrier is sealed per shard, and an
// ErrBarrierSealed from a barrier already sealed is not a Close error. Closing
// the SealingStrategy behind each barrier remains the caller's job.
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
		// Normalize a typed-nil collector — e.g. a caller "disabling" metrics
		// with WithMetrics((*PrometheusMetricsCollector)(nil)) — to a real nil.
		// Otherwise the interface is non-nil (it wraps a nil pointer), every
		// `metrics != nil` guard in the host passes, and the first hot-path
		// metric call dereferences the nil receiver and panics.
		if isNilMetricsCollector(c) {
			o.metrics = nil
			return
		}
		// Wrap so a panic in the caller's collector cannot escape into engine
		// code. The engine calls it from inside critical sections in its step
		// and commit workers; an escaping panic would unwind the worker and
		// cost a restart that drops the in-flight update. See
		// safeMetricsCollector.
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

// WithoutEncryption declares that this host's HOST-WIDE route stores the Raft
// log and the snapshots UNENCRYPTED at rest.
//
// NewHost refuses a host that neither configures at-rest encryption
// (WithBarrier or WithShardBarriers) nor makes this declaration: plaintext at
// rest is a deployment decision, and a host that reached it by omitting an
// option would give no sign that it had. Anyone with read access to the data
// directories can then read every proposed command and every snapshot.
//
// Use it for tests, benchmarks and deployments whose storage is protected by
// other means. It cannot be combined with WithBarrier
// (ErrAtRestEncryptionConflict). It can be combined with WithShardBarriers: a
// shard with a barrier of its own is still encrypted under it, and a shard the
// resolver returns nil for takes this plaintext route.
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

// noopSender is a no-op MessageSender used when no transport is
// configured. Messages are silently dropped. This is safe because
// in a single-node cluster no remote delivery is needed, and the
// transport will be wired in a later phase.
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

// maxSnapshotFrameLen bounds a single encrypted snapshot frame read during
// send/export decryption. Frames are written by the engine at a 64 KiB
// plaintext chunk size plus AEAD framing overhead; 64 KiB + 256 is a safe
// ceiling that rejects a corrupt length prefix before allocation.
const maxSnapshotFrameLen = 64*1024 + 256

// snapshotEncryptChunkSize is the plaintext chunk size for receive-side snapshot
// re-encryption. It matches engine.snapshotEncryptChunkSize so frames stay
// within maxSnapshotFrameLen (chunk + AEAD overhead) and the on-disk frame
// format is identical to save-time encryption (decryptable by the recover path).
const snapshotEncryptChunkSize = 64 * 1024

// transportSender wraps a QUICTransport to implement the full
// engine.MessageSender interface, including the snapshot streaming
// path. It bridges the gap between the engine's message-level API
// and the transport's chunk-level snapshot API.
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

// barrierFor returns the barrier that encrypts the given shard's snapshots.
// With per-shard barriers it routes by shardID, falling back to the host-wide
// barrier when the resolver returns nil. Without per-shard barriers it always
// returns the host-wide barrier.
func (s *transportSender) barrierFor(shardID uint64) Barrier {
	if s.shardBarriers != nil {
		if b := s.shardBarriers(shardID); b != nil {
			return b
		}
	}
	return s.barrier
}

// givenBarriers records every barrier a WithShardBarriers resolver has handed
// the host, so Host.Close can seal each one. The resolver is a function rather
// than a set, so the host only learns which barriers it was given by watching
// what the resolver returns.
//
// The Barrier interface does not require a comparable dynamic value, so the
// record never keys on or compares a Barrier: either panics for a value that
// holds a slice or map. A barrier with pointer identity (logdb.SameBarrier) is
// recorded once however many shards resolve to it; one without is recorded
// per shard, as the latest value the resolver returned for that shard. Close
// treats an already-sealed result as benign, so copies of one value-type
// barrier recorded under several shards are sealed once in effect. The zero
// value is ready to use.
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

// recording wraps resolve so that every non-nil barrier it returns is
// recorded. Every consumer of the resolver (snapshot pool, transport epoch
// tracking, snapshot sends and the host's own barrierForShard) is handed the
// wrapped function, so no path can use a barrier Close would not seal.
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

// logDBAtRestProtection reports a LogDB's declared at-rest guarantee. A LogDB
// that does not implement logdb.AtRestProtectedLogDB declares nothing, and a
// nil LogDB is not a store at all; both read as UNPROTECTED so every caller
// fails closed on silence.
func logDBAtRestProtection(db logdb.LogDB) logdb.AtRestProtection {
	p, ok := db.(logdb.AtRestProtectedLogDB)
	if !ok {
		return logdb.AtRestUnprotected
	}
	return p.AtRestProtection()
}

// requireEncryptedLogDB refuses a LogDB whose Raft log would be readable off
// the machine's storage in a way the host never declared. It backs
// ErrShardBarriersPlaintextWAL and ErrBarrierPlaintextLogDB.
//
// AtRestNonDurable is accepted for the reason it is a distinct state: an
// in-memory LogDB writes no file, so there is no at-rest artifact to protect
// and no crash-surviving copy of a user command. That is a property of the
// STORE, not a property of the caller having passed WithLogDB.
//
// AtRestHostRouted is accepted only against the host-wide route the HOST
// declared, because its one possibly-plaintext route is that route:
//
//   - plaintextRoute (WithoutEncryption): a plaintext host-wide route is what
//     the host declared.
//   - hostBarrier (WithBarrier): the route must write through that very
//     barrier, by identity, which judges the LogDB exactly as the default WAL
//     opened under the same barrier is judged, plaintext mode included.
//
// With neither there is no declared route to accept it against. Each per-shard
// route is checked when its shard starts (verifyShardLogDBBarrier).
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

// logDBWritesUnder reports whether db stores records under barrier, answered by
// barrier identity through logdb.PerBarrierEpochAwareLogDB. A nil barrier, one
// without pointer identity, a LogDB that cannot answer, and an error all read
// as no.
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

// barrierForShard resolves the at-rest barrier for a shard, mirroring
// transportSender.barrierFor on the send path: the per-shard barrier when
// WithShardBarriers is configured and returns non-nil, otherwise the host-wide
// barrier. This is what lets a follower re-encrypt a received snapshot under
// the SAME per-shard barrier it will later read it through, so per-node/
// per-shard keys stay consistent across the receive→recover→read cycle.
//
// It fails CLOSED with ErrShardBarrierUnresolved when WithShardBarriers is
// configured, the resolver returns nil, and there is no host-wide barrier to
// fall back on. That combination has no encrypting outcome: the caller asked
// for per-shard encryption and this shard resolved to nothing, so writing it in
// plaintext would downgrade exactly one tenant invisibly. An ABSENT barrier
// therefore fails closed exactly as a SEALED one does everywhere else.
//
// The exception is a host that DECLARED its host-wide route plaintext with
// WithoutEncryption: the nil is then the documented fallback to that route,
// exactly as it is a fallback to the WithBarrier barrier when one is set, and
// the shard is written in plaintext because the host said so.
//
// A nil return with a nil error is otherwise the legitimate unencrypted
// deployment: WithShardBarriers was never configured and no host-wide barrier
// exists.
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

// isSealedLogWrite reports that a LogDB SaveState failure is exactly the
// barrier's sealed refusal, which the engine holds the shard on until the
// unseal instead of failing it (#296; engine.SealHold). The WAL refuses a
// sealed write before any byte of it is durable and rolls the batch back to
// its checkpoint, so the write is retried unchanged. An error that also
// carries a disk fault is never a hold: a poisoned log (failed fsync), a
// rollback that did not complete (BatchWriteError) and a failed segment
// recovery (WALWriteError) are permanent and still fail the shard.
//
// It classifies by error shape alone, so it accepts a single-update refusal
// exactly as it accepts the batch refusal of the same updates — the contract
// engine.SealHold.IsSealed states. A caller-supplied LogDB meets it by
// surfacing its stores' errors on the errors.Is/As chain, wrapped or joined.
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

// verifyShardLogDBBarrier refuses to start a shard whose Raft log the supplied
// LogDB does not store under the shard's OWN barrier.
//
// It closes the second half of the per-shard axis. requireEncryptedLogDB proves
// the log is encrypted; it does not prove it is encrypted under the key this
// shard resolved to. A waldb opened with tenant A's barrier and a resolver that
// hands shard 7 tenant B's barrier is encrypted at rest AND stores tenant B's
// commands under tenant A's key -- exactly the cross-tenant key mixing
// WithShardBarriers exists to prevent.
//
// Which LogDBs are checked follows from what each declares:
//
//   - AtRestHostRouted: always, whatever the host-wide posture. The LogDB
//     claims each barriered shard has a dedicated route under its own barrier,
//     and this is where that claim is held to the shard. A shard with no
//     barrier of its own (barrier == nil, reachable only on the plaintext route
//     WithoutEncryption declared) is on the host-wide route NewHost judged.
//   - AtRestBarrierEncrypted: only when there is no host-wide barrier. With
//     WithBarrier such a WAL is encrypted host-wide by design (documented, and
//     warned about at construction) and the per-shard barriers cover snapshots
//     only, so demanding per-shard WAL ownership would refuse a supported
//     configuration.
//   - AtRestNonDurable has no at-rest artifact to attribute to a barrier, and
//     AtRestUnprotected was already refused by NewHost.
//
// logdb.PerBarrierEpochAwareLogDB answers by barrier IDENTITY, which is pointer
// identity: known == true means "my records for this barrier are these", which
// for waldb.DB is decided by the barrier it was opened with. A LogDB that
// cannot answer cannot prove isolation, so it is refused rather than trusted;
// a barrier that is not a pointer has no identity to answer for, so its shard
// is refused too.
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

// snapshotBarrierForShard resolves the shard's at-rest barrier for a path that
// is about to WRITE a snapshot artifact, collapsing the two ways encryption can
// be unavailable into one error:
//
//   - ErrShardBarrierUnresolved  — per-shard encryption is configured and this
//     shard resolved to nothing (see barrierForShard).
//   - ErrSnapshotBarrierSealed   — the shard HAS a barrier and it is sealed, so
//     encryption is temporarily unavailable.
//
// A nil barrier with a nil error is the configured plaintext deployment.
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

// SendSnapshot initiates an asynchronous snapshot file transfer. It
// splits the InstallSnapshot message into chunks by reading the
// snapshot data file from disk, sends them via the transport's chunked
// streaming path, and delivers a SnapshotStatus message back to the
// raft layer on completion.
//
// The transfer runs in a background goroutine so the step worker is
// not blocked. On success, a non-rejected SnapshotStatus is injected;
// on failure, a rejected SnapshotStatus tells the leader to transition
// the remote out of snapshot state so replication can be retried.
func (s *transportSender) SendSnapshot(msg proto.Message) {
	// Open the artifact NOW, before the asynchronous hand-off, and read only
	// through the handle from here on. Opening inside the goroutine left a
	// window in which a newer snapshot's GC removed the directory raft had
	// just chosen; an open file stays readable whatever happens to its name.
	// The snapshot.meta record the send verifies the handle's bytes against is
	// read here too, for the same reason. Both are a handful of syscalls on a
	// path taken once per lagging follower, cheap enough for the step worker.
	//
	// The send is registered with the shard's pause first: a paused leader
	// sends no snapshot, and a PauseShard that lands mid-send waits for it.
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
				// Notify the raft layer that the snapshot was rejected so
				// the leader transitions the remote out of snapshot state.
				// Without this, the follower is permanently stuck. It comes
				// before the diagnostic, which is contained: the logger is
				// user code and must not be able to skip the notification.
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

// safeNotifySnapshotRejected attempts to deliver a rejected SnapshotStatus
// notification, recovering from any secondary panic (e.g., if the engine
// is already torn down). This prevents a double-panic in the deferred
// recovery handler of SendSnapshot from crashing the process.
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

// doSendSnapshot performs the synchronous snapshot transfer work.
// It reads the file, splits it into chunks, and sends them via
// the transport. On completion, it injects a SnapshotStatus message
// into the sender's raft inbox.
//
// When the snapshot was encrypted at rest (Epoch > 0) and a barrier
// is available, the file is decrypted to a temporary file before
// sending. The receiver gets plaintext data and stores it with
// Epoch=0, so its own barrier (which has a different key) does not
// need to decrypt it. Each node's barrier encrypts independently
// for at-rest protection; the QUIC transport provides in-transit
// encryption via mTLS.
//
// Every byte read from src is checked against the length and hash recorded
// in the artifact's snapshot.meta, the check recovery makes, without a second
// read of the file: a plaintext artifact is verified before its LAST chunk is
// emitted, an encrypted one as soon as the decrypt has consumed it and before
// any chunk is. The receiver discards a transfer that ends short of its
// declared chunk count, so a mismatch fails the send without the follower
// adopting the damaged bytes as the snapshot at this index.
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

	// src was opened by SendSnapshot before this goroutine was handed the
	// transfer, and every read below goes through it or through the
	// anonymous decrypt scratch file — never through a path. A snapshot
	// directory the GC removes meanwhile (a newer receive or save, the next
	// incarnation, an offline import) takes only its names with it.
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

	// A snapshot with NO data file — a witness's — is metadata only: there is
	// nothing to decrypt and nothing to open, whatever epoch its record
	// carries. It is shipped as its single empty chunk at epoch 0. Letting it
	// reach the decrypt below read from a nil source, and a witness in an
	// encrypted cluster then panicked on every send the leader retried.
	if msg.Snapshot.Filepath == "" {
		msg.Snapshot.Epoch = 0
	}

	// If the snapshot is encrypted, decrypt it so the receiver (which has a
	// different barrier key) can read it. The barrier is routed by shardID so
	// per-tenant snapshots decrypt under the correct barrier; without
	// per-shard barriers this is the host-wide barrier.
	sendBarrier := s.barrierFor(msg.ShardID)
	if msg.Snapshot.Epoch > 0 {
		// The snapshot is encrypted at rest and MUST be decrypted to plaintext
		// before it goes on the wire: the receiver's barrier holds a different
		// key and can never decrypt our ciphertext. If the barrier that sealed
		// it is unavailable (sealed, or absent), refuse the send and notify the
		// leader rejected. Shipping ciphertext the peer cannot decrypt — while
		// pinning a meaningless at-rest epoch on the receiver — would leave the
		// follower permanently unable to apply the snapshot and stuck in
		// snapshot state. A rejection lets the leader retry once the barrier is
		// unsealed.
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
		// The decrypt read the whole artifact through src, so its recorded
		// integrity is checked now, before a single chunk is emitted. The
		// per-frame AEAD has already authenticated every byte under this node's
		// key; the check holds an encrypted artifact to the same record a
		// recovery of it would be held to, at the cost of one hash over bytes
		// the decrypt read anyway.
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

	// Stream snapshot chunks directly to the transport. The producer reads
	// one chunk at a time from the open file, and SendSnapshotStreaming
	// writes it to the wire immediately, so at most one chunk
	// (snapshotChunkSize = 2MB) is held in memory at any point rather than
	// the entire snapshot file.
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

// decryptSnapshotForSend decrypts an encrypted snapshot, read from the
// already-open src, into an ANONYMOUS scratch file for network transfer, and
// returns that file open and rewound, with the plaintext size. The encrypted
// format is a sequence of frames: [4-byte LE length][encrypted chunk]...
// The artifact was sealed by THIS node's snapshot pool under
// (msg.ShardID, msg.ReplicaID, msg.Snapshot.Index) — msg.ReplicaID is the
// sending replica, stamped by raft.send — so those identify the frames'
// positional context.
//
// The scratch file is unlinked the moment it is created, so the plaintext
// never has a name: a crash cannot orphan it, the snapshot GC cannot delete
// it from under the stream, and the caller's Close is the whole cleanup. It
// is created in the loaded replica's own snapshot directory — never beside
// the artifact, and never in a shared temporary directory. A scratch whose
// name cannot be unlinked fails the send before anything is decrypted into it
// (engine.ErrScratchNotUnlinked); the leader retries the transfer later. The
// caller owns both files.
func (s *transportSender) decryptSnapshotForSend(src snapshotFile, msg proto.Message, barrier Barrier) (snapshotFile, uint64, error) {
	if src == nil {
		return nil, 0, &SnapshotSendError{ShardID: msg.ShardID, ReplicaID: msg.To, Op: "read", Err: os.ErrNotExist}
	}
	barrier.RegisterInFlightEpoch(msg.Snapshot.Epoch)
	defer barrier.DeregisterInFlightEpoch(msg.Snapshot.Epoch)

	// In this replica's own snapshot directory, never beside the artifact:
	// the GC may already have removed the artifact's directory, and a record
	// may name one outside the replica's tree, in a directory quicraft does
	// not own and does not sweep. The replica directory is never removed and
	// is swept at every start and recovery whatever the record names. A
	// shard no longer loaded — or loaded again as a different replica, while a
	// send of the old one's artifact was still running — has no replica to
	// send for. The file is unlinked before a byte is written, so even a crash
	// between the create and the unlink leaves nothing but an empty name.
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

// sendDecryptTempPattern names the scratch file the send path decrypts into,
// and recoverDecryptTempPattern the one the restart-recovery path uses. Both
// are taken from the engine's shared constants, which is what guarantees
// engine.SweepStaleDecryptTmp reclaims either file when a crash orphans it: a
// send-path scratch file the sweeper did not match was a complete plaintext copy
// of the state machine left beside the ciphertext for the life of the snapshot
// directory.
//
// recvEncryptTempPattern and reencryptTempPattern name the ENCRYPT-side
// scratch files (the in-place re-encryption a received or imported artifact
// once went through, whose orphans the sweep still reclaims, and a
// barrier-to-barrier migration). They come from the same constants for the
// same structural reason. Their orphans hold CIPHERTEXT, so the exposure is a
// disk leak rather than a confidentiality one — but the invariant the sweeper
// rests on is that a name which can be CREATED is a name that can be RECLAIMED,
// and a locally-spelled pattern is exactly how a name escapes it.
const (
	sendDecryptTempPattern    = engine.SendDecryptTempPattern
	recoverDecryptTempPattern = engine.RecoverDecryptTempPattern
	recvEncryptTempPattern    = engine.RecvEncryptTempPattern
	reencryptTempPattern      = engine.ReencryptTempPattern
)

// shardSnapshotBaseDir returns the BASE snapshot directory for a shard — the
// directory engine.Node.SnapshotDir() reports and that every snapshot path
// (save, receive, import, garbage collection, and the DEK purge-floor scan)
// resolves against via snapshotSaveDir/listSnapshotDirs.
//
// It is the single definition of that layout. Any path that builds it by hand
// risks writing snapshot artifacts into a tree the GC and the purge-floor scan
// never enumerate; because the purge floor is computed by SCANNING these
// directories, an artifact outside the tree contributes no floor and its DEK is
// legitimately destroyed by the next PurgeEpochsBefore, leaving the snapshot
// permanently undecryptable.
//
// dataDir is the shard's optional Config.DataDir: when set, the shard's
// snapshot tree roots on the tenant's own volume instead of under NodeHostDir.
func shardSnapshotBaseDir(nodeHostDir, dataDir string, shardID uint64) string {
	if dataDir != "" {
		return filepath.Join(dataDir, fmt.Sprintf("shard-%d", shardID))
	}
	return filepath.Join(nodeHostDir, "snapshots", fmt.Sprintf("shard-%d", shardID))
}

// decryptSnapshotToAnonymousScratch decrypts the framed snapshot ciphertext at
// srcPath into an ANONYMOUS scratch file in scratchDir and returns that file
// open and rewound, for the caller to read the plaintext back through. The
// scratch is created and unlinked by engine.CreateAnonymousScratch — the one
// implementation the send and the engine's recovery use too — before a byte
// of plaintext is written into it, so a crash or power loss at any point
// leaves no readable plaintext on disk and the caller's Close is the whole
// cleanup.
//
// scratchDir must be the replica's own snapshot directory
// (engine.ReplicaSnapshotDir), which the stale-scratch sweep covers: a scratch
// whose unlink failed fails the recovery before anything is decrypted into it,
// and an empty name that survives the removal that follows is left to the
// sweep. Never the directory of a path a record names, which may lie outside
// the replica's tree. srcPath is never modified: the
// canonical encrypted snapshot.dat stays the only durable artifact, which is
// what keeps a failed decrypt retryable. Decrypting in place would leave
// plaintext on disk while LogDB still records Epoch>0, so every later read
// would re-attempt decryption on plaintext and fail permanently.
//
// On any error the scratch file is closed before returning.
//
// Each frame is decrypted with the epoch embedded in its own ciphertext header;
// epoch is only pinned live for the duration of the read so a concurrent purge
// cannot remove the oldest DEK a frame may reference mid-decrypt. This mirrors
// the belt-and-suspenders RegisterInFlightEpoch saveSnapshot takes around its
// own encrypt: the recorded epoch is the minimum across the frames.
//
// StartShard's local-snapshot recovery uses it.
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

// decryptSnapshotFrames reads the encrypted snapshot frame format from src,
// decrypts each frame, and writes the plaintext to dst. The on-disk encrypted
// format is a sequence of frames: [4-byte LE length][encrypted chunk]...
// terminated by EOF. Returns the total number of plaintext bytes written.
//
// Each frame is decrypted with the epoch EMBEDDED in its own ciphertext header
// (barrier.Decrypt), not a single snapshot-wide epoch: a snapshot saved across
// a key rotation contains frames sealed at different epochs, and using one
// epoch for all of them would fail AEAD authentication on the mismatched
// frames. Shared by the transport send path (decryptSnapshotForSend) and the
// ExportSnapshot API so both emit identical plaintext.
// encryptSnapshotFrames seals the plaintext snapshot data src yields into dst
// through engine.SealSnapshotFrames, the one implementation of the frame
// format the local save path writes: [4-byte LE length][AEAD ciphertext] per
// chunk, each frame bound to the artifact identity (shardID, replicaID,
// index), its ordinal, and whether it is the terminal frame. The output is
// what decryptSnapshotFrames and the recover path read, so a snapshot
// RE-ENCRYPTED on receipt under the RECEIVER's own (per-node) barrier recovers
// correctly with that same barrier — the basis for per-node at-rest encryption
// of received snapshots without any shared cross-node key.
//
// It returns the MINIMUM epoch across the frames (a concurrent rotation can
// advance the epoch mid-stream), which the caller records as the snapshot's
// at-rest epoch so the DEK-purge floor protects every frame, and every epoch a
// frame was sealed under, PINNED as in-flight on the barrier. The caller MUST
// DeregisterInFlightEpoch each one once the snapshot's durable record exists;
// pinning every frame's epoch rather than the minimum is what stops a
// concurrent rotate + PurgeEpochsBefore from dropping an intermediate frame's
// DEK. On error, every pin this call took is released before it returns.
func encryptSnapshotFrames(barrier Barrier, shardID, replicaID, index uint64,
	src io.Reader, dst io.Writer) (uint64, []uint64, error) {
	return engine.SealSnapshotFrames(barrier, shardID, replicaID, index, src, dst)
}

// decryptSnapshotFrames authenticates and decrypts a sealed snapshot artifact
// through engine.OpenSnapshotFrames, the one reader of the format: every frame
// is checked against its ordinal, the artifact identity (shardID, replicaID,
// index) it was sealed under, and whether it is the terminal frame, so a
// spliced, reordered or tail-truncated artifact fails. A mismatch is never
// retried with a weaker (epoch-only) AAD. Damage to the artifact is reported
// as ErrSnapshotCorrupt.
func decryptSnapshotFrames(barrier Barrier, shardID, replicaID, index uint64,
	src io.Reader, dst io.Writer) (uint64, error) {
	n, err := engine.OpenSnapshotFrames(barrier, shardID, replicaID, index, src, dst)
	if errors.Is(err, engine.ErrSnapshotCorrupt) {
		return 0, ErrSnapshotCorrupt
	}
	return n, err
}

// notifySnapshotStatus injects a SnapshotStatus message into the
// sender's raft inbox to inform the leader of the transfer outcome.
// The From field is set to the target replica (the follower that
// received or rejected the snapshot) so the leader's raft layer
// can look up the correct remote entry. Hint echoes the transfer
// sequence the leader stamped on the InstallSnapshot (raft
// snapshotSeq), so handleSnapshotStatus can discard a stale status
// from a superseded transfer attempt — even one re-sending the same
// snapshot index — instead of arming the ack countdown (or forcing a
// backoff) for a newer in-flight one. Delivery is retried briefly:
// this is a one-shot message with no protocol-level retransmission,
// and losing it leaves the remote paused with no countdown armed.
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

// forEachSnapshotChunk reads the snapshot data file referenced by the
// InstallSnapshot message and streams transport-level chunks to the
// provided emit callback one at a time. Only a single chunk's worth of
// data (snapshotChunkSize = 2MB) is held in memory at any point,
// preventing OOM on large snapshots (up to DefaultMaxSnapshotSize = 4GB).
//
// The emit callback is invoked synchronously for each chunk. If emit
// returns an error, iteration stops and the error is returned to the
// caller. This streaming design allows the transport layer to send
// each chunk over the wire as it is produced, avoiding the O(fileSize)
// memory overhead of materializing all chunks in a slice.
// emptySnapshotChunk builds the single metadata-only chunk that represents a
// snapshot with no data: a witness snapshot (no Filepath) or a data file that
// is genuinely zero bytes.
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
		BinVer:         proto.WireVersion,
		Epoch:          snap.Epoch,
	}
}

func forEachSnapshotChunk(fsys snapshotFS, msg proto.Message, emit func(proto.SnapshotChunk) error) error {
	snap := &msg.Snapshot
	filePath := snap.Filepath

	// A snapshot with NO data file — a witness, or a state machine that saved
	// nothing — is the only legitimate metadata-only transfer. A DECLARED size
	// of zero is not: the file exists, and treating a metadata/artifact
	// disagreement as "empty" ships a snapshot with no data, which the
	// receiving follower recovers from and then marks every index at or below
	// the snapshot index applied. That is silent, permanent state loss on the
	// follower, so a non-empty Filepath is always opened and verified below.
	if filePath == "" {
		return emit(emptySnapshotChunk(msg, snap))
	}

	// The defer for cleanup is registered immediately after a successful
	// open to ensure the file handle is always closed, even on panic.
	// The fileClosed flag allows explicit early close while preventing
	// double-close errors in the deferred cleanup.
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

// streamSnapshotChunks streams an ALREADY-OPEN snapshot data file as chunks,
// after verifying that its size is the one msg declares. Only one chunk's
// worth of data is held in memory at a time, avoiding OOM on large snapshots
// (up to DefaultMaxSnapshotSize = 4GB).
//
// Reading from a handle rather than a path is what lets the send survive the
// snapshot GC: a directory removed while the send runs takes only its names
// with it, and an open file stays readable. The caller owns f and closes it.
//
// verify is called once every byte has been read and BEFORE the last chunk is
// emitted; an error from it fails the stream with that chunk withheld. The
// receiver commits a transfer only when every declared chunk arrived, so a
// send that fails here delivers nothing the follower can install — which is
// what lets the integrity check ride along with the one read the stream makes
// instead of costing a second pass over the file.
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
			BinVer:         proto.WireVersion,
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

// verifiedSnapshotFile is an open snapshot data file whose every Read is also
// fed to a verifier for the integrity fields recorded in the snapshot.meta
// beside it, so an egress path checks the bytes it sends in the same pass that
// reads them. Seek is not meant to be used on it: a repositioned read would
// hash bytes out of order.
type verifiedSnapshotFile struct {
	snapshotFile
	verifier *engine.SnapshotDataVerifier
}

// openVerifiedSnapshotFile opens the snapshot data file at dataPath and reads
// the snapshot.meta record in its directory. The record is read at open, not
// at the end of the stream, so a directory the snapshot GC removes while the
// handle is being read cannot change which record the bytes are held to.
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

// pendingRead tracks a single ReadIndex request through the pipeline.
// The readIndex field is set once the leader confirms via heartbeat quorum.
//
// When ReadIndex batch coalescing is active, the first key in a batch is
// the "batch key" and its pendingRead carries an aliases slice of the
// other keys in the batch. When the batch key's ReadIndex is confirmed,
// all alias keys are resolved with the same readIndex value.
type pendingRead struct {
	rs        *RequestState
	readIndex uint64   // set when OnReadyToRead confirms
	confirmed bool     // true once readIndex is known
	aliases   []uint64 // other readKeys that share this ReadIndex result
}

// nodeState holds per-shard state managed by the Host. This is the
// Host's view of a running shard, independent of the engine.Node
// type which manages the Raft protocol. The Host maintains this state
// to serve local reads and routing without engine round-trips.
type nodeState struct {
	shardID   uint64
	replicaID uint64
	cfg       config.Config
	createFn  sm.CreateFunc
	smType    uint64

	// barrier is the at-rest barrier this shard's log and snapshots are
	// written under, resolved once at StartShard: the shard's own barrier
	// when a resolver supplies one, otherwise the host-wide barrier, nil when
	// the shard is not encrypted. The client entry points read it through
	// barrierSealed to refuse new work while it is sealed.
	barrier Barrier

	// paused is set by PauseShard and cleared by ResumeShard. The client
	// entry points load it once and refuse with ErrShardPaused; the engine
	// node carries its own flag, which gates the replica itself.
	paused atomic.Bool
	// pauseGate serializes PauseShard and ResumeShard on this shard.
	pauseGate pauseGate

	leaderID          atomic.Uint64
	term              atomic.Uint64
	lastApplied       atomic.Uint64
	configChangeIndex atomic.Uint64
	isLeader          atomic.Bool

	// readReady mirrors the last OnLeaderReady: the (leaderID, term) pair
	// above is read-ready. Every updateNodeLeader clears it — each names a new
	// pair — and markLeaderReady sets it, both under leaderSeq.
	readReady atomic.Bool

	// leaderSeq makes leaderID, term and isLeader readable as a CONSISTENT
	// TRIPLE via leaderAndTerm. They are three independent words, so a caller
	// that loads them separately can observe a combination that never existed —
	// updateNodeLeader may land between two of the loads — and report that a
	// replica led in a term it never led. GetLeaderIDAndTerm and GetShardInfo
	// return exactly that pair/triple to callers who route writes by it.
	//
	// This is a seqlock, the same construction Peer uses one level down for the
	// same reason (see raft.Peer.leaderSeq/LeaderAndTerm): the single writer —
	// updateNodeLeader, driven by the shard's step worker — makes the counter
	// ODD for the duration of the update and EVEN once the triple is settled,
	// so a reader that sees the same even value before and after its loads knows
	// no write interleaved. Writers never block; readers retry.
	leaderSeq atomic.Uint64

	// leaderWake is the channel the next change to the leadership view
	// closes: every updateNodeLeader and markLeaderReady, once its write has
	// settled, and the stop of the shard. ExecuteOnLeader waits on it for a
	// leader to appear, for the term to move past one it has spent, and for
	// the view to abandon the leader an attempt is in flight to. See
	// leaderChanged.
	leaderWake atomic.Pointer[chan struct{}]

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

	// pendingProposals maps proposal key to the *RequestState waiting for
	// that entry to be applied. Entries are added by Propose and
	// removed by OnApplied or OnLeaderUpdated (on leadership loss).
	// Uses sync.Map for lock-free reads on the hot OnApplied path
	// and sharded writes under concurrent propose load.
	pendingProposals sync.Map

	// pendingReadsMu protects pendingReads only.
	pendingReadsMu sync.Mutex

	// pendingReads maps read request key to the pendingRead tracking
	// the ReadIndex confirmation and SM catch-up. Entries are added by
	// ReadIndex and removed when lastApplied >= readIndex or on
	// leadership loss / shard stop.
	pendingReads map[uint64]*pendingRead

	// pendingSnapshotsMu protects pendingSnapshots only.
	pendingSnapshotsMu sync.Mutex

	// pendingSnapshots maps snapshot key to the RequestState waiting for
	// the snapshot to complete. Entries are added by RequestSnapshot and
	// removed by OnSnapshotCompleted.
	pendingSnapshots map[uint64]*RequestState
}

// barrierSealed reports whether the shard's at-rest barrier is sealed. It is
// the admit check of every client entry point that would produce a write
// (or a read whose answer depends on writes) on this replica: while the
// barrier is sealed, nothing for the shard can be encrypted, so the request
// is refused up front with ErrBarrierSealed instead of being handed to raft.
// One atomic load on the barrier's own state; an unencrypted shard is never
// sealed. It refuses client work only: raft's own persistence, messages and
// elections are unaffected by it.
func (ns *nodeState) barrierSealed() bool {
	return ns.barrier != nil && ns.barrier.IsSealed()
}

// pendingProposal bundles a RequestState with its deadline, captured
// at Store time. The sweep reads only pp.deadline (immutable after Store),
// never touching the RS fields that may be recycled by sync.Pool.
type pendingProposal struct {
	rs       *RequestState
	deadline time.Time

	// floor is the shard's committed index as last published to the host
	// when this proposal was registered (Node.CommittedIndex, an atomic the
	// step worker stores once per cycle). The entry this proposal became was
	// appended AFTER that, on the leader, at an index above everything
	// committed anywhere at the time, so an applied or committed entry at or
	// below floor cannot be this proposal no matter what its Key says. The
	// published marker can lag the peer's own commit by the cycle in
	// progress, so this is a bound, not the instant's value: it makes the
	// replay case certain and leaves the rest to the random draw.
	//
	// This is the second line of defence behind nextRequestKey's random draw,
	// and the exact one: a key repeats across incarnations only by a 2^-64
	// accident, but an entry at or below the floor is provably not this
	// proposal's, whatever its key. A deterministic key scheme such as
	// (replicaID, per-Host sequence) would let a restarted replica mint its
	// previous incarnation's keys while replaying that incarnation's log and
	// acknowledge new proposals with an old entry's result; the floor rules
	// that out regardless of how keys are drawn. It costs one atomic load
	// and turns a probabilistic guarantee into a certain one for every entry
	// a replay can deliver -- the marker
	// is seeded from the recovered log when the engine Node is built
	// (engine.NewNode), before StartShard returns, so no proposal can
	// register against a floor the replay has not already cleared.
	floor uint64

	// onFailure runs exactly once, at the completion site that claims this
	// entry with an error (OnProposalFailed, failPendingRequests, the
	// deadline sweep). It is NOT run on the applied path: an applied entry
	// committed, so any speculative side effect it owns is now legitimate.
	// Used by requestConfigChange to undo its speculative address
	// pre-registration for an entry that was appended but never committed
	// (leader deposed, log truncated). Nil for ordinary proposals.
	onFailure func()

	// node is the engine node the proposal was registered on, and enqueuedAt
	// where it entered that node's proposal queue (engine.Node.
	// DeliverTrackedProposal; zero until it is delivered). Together they tell
	// a drain whether the proposal has been handed to raft — the step worker
	// took it from the queue — and so whether a failure is maybe-applied or
	// definite (proposalMayReachRaft). enqueuedAt is written under the queue's
	// lock and read only through node.ProposalTaken.
	node       *engine.Node
	enqueuedAt uint64
}

// proposalMayReachRaft reports whether pp has been handed to raft, or may
// still be: the step worker has taken it from its node's proposal queue, or
// the queue is still open and may yet hand it off. Only a proposal its node
// can no longer hand off — never taken, and the queue closed by a failure or
// a stop — is provably outside raft. A proposal without a node is treated as
// handed off, the safe direction.
func proposalMayReachRaft(pp *pendingProposal) bool {
	if pp.node == nil {
		return true
	}
	taken, final := pp.node.ProposalTaken(&pp.enqueuedAt)
	return taken || !final
}

// closedQueueRefusal returns the definite refusal for a proposal whose
// delivery was refused because engNode's proposal queue is closed — the node
// failed (ErrShardFailed) or stopped (ErrClosed) — or nil when the node is
// running and the refusal was an overflow.
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

// completeProposal delivers an applied entry's result to the caller waiting
// on pp.rs, translating the reserved ResultSessionExpired value into an
// error so clients can distinguish expired sessions from legitimate
// zero-value results (PhD Figure 6.1, step 3). Shared by both branches of
// OnApplied's proposal completion so they cannot drift apart.
func completeProposal(h *Host, shardID uint64, entry proto.Entry, pp *pendingProposal, result sm.Result) {
	createdAt := deliverProposal(h, shardID, entry, pp, result)
	observeProposalLatency(h, shardID, createdAt)
}

// deliverProposal is completeProposal without the latency observation: it
// completes and releases pp's caller, runs no user code, and returns the
// request's creation time for observeProposalLatency. A caller that tracks
// which proposals it has completed records the completion between the two,
// so a MetricsCollector that ends the goroutine cannot have a completed
// proposal completed — and released — a second time by its recovery.
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

// observeProposalLatency records the end-to-end proposal latency (Propose ->
// OnApplied) for a request created at createdAt. It runs only after the
// caller has been completed: the MetricsCollector is user code, and nothing
// it does may stand between an applied entry and the caller waiting on it.
func observeProposalLatency(h *Host, shardID uint64, createdAt time.Time) {
	if h.metrics != nil && !createdAt.IsZero() {
		h.metrics.ObserveProposalLatency(shardID, time.Since(createdAt))
	}
}

// OnApplied completes pending proposals whose entries have been
// applied to the state machine. entries and results are parallel
// slices. For each entry with a registered RequestState, the result
// is delivered to the caller's channel.
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

	// results is engine-supplied and must be index-parallel with entries; the
	// apply worker slices it to len(entries) before calling. A SHORT slice is
	// therefore an engine invariant violation, but indexing past it below
	// would panic inside the apply worker's callback and, since that panic is
	// recovered as a worker panic, cost the whole worker partition — every
	// shard it serves — rather than the one shard with the bad batch.
	//
	// Clamp instead, and say so loudly. Proposals beyond the clamp are left in
	// pendingProposals for the sweeper to fail on their deadline, which is the
	// same outcome they would have had if the batch had never been offered;
	// lastApplied still advances from entries below, because the entries WERE
	// applied whatever the results slice says.
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

	// Claim every pending proposal in this batch — LoadAndDelete it out of
	// ns.pendingProposals — before anything else in this function, including
	// the pending-reads scan below and (for a membership-changing batch)
	// applyMembershipRouting. pendingProposals is a lock-free sync.Map
	// shared with the deadline sweep (sweepPendingProposals) and
	// OnProposalFailed; whichever of them calls LoadAndDelete on a key first
	// owns that entry. Claiming AFTER applyMembershipRouting would leave a
	// window in which an entry whose deadline expired while routing was
	// still running could be claimed by the sweeper instead, which treats
	// it as a timeout and, via pendingProposal.onFailure, could act on an
	// already-committed change. Claiming after the pending-reads scan
	// (rather than before it) would leave the same window one level up:
	// the reads scan can itself take a while (arbitrarily many confirmed
	// reads, a metrics call per one), and until claiming happens the
	// config-change proposal is just as exposed to the sweeper. Claiming
	// first, before any other work at all, closes both. Only the actual
	// completion (rs.complete, inside completeProposal) is deferred until
	// after routing, so ns.configChangeIndex reflects this batch before a
	// woken caller can round-trip its ConfigChangeID (see below) — and only for a
	// membership-changing batch, which is the only case that has routing to
	// defer it past.
	type claimedProposal struct {
		entry  proto.Entry
		result sm.Result
		pp     *pendingProposal
	}
	var claimed []claimedProposal
	if hasMembershipChange {
		claimed = make([]claimedProposal, 0, len(matched))
	}
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
		if hasMembershipChange {
			// Staged for completion after routing below — see the
			// claiming comment above for why. Confined to this branch so
			// the ordinary path (the ELSE below) never pays for the
			// staging allocation.
			claimed = append(claimed, claimedProposal{entry: entry, result: results[i], pp: pp})
		} else {
			// Ordinary path (no config change in this batch, the
			// overwhelming majority of apply traffic — regular client
			// proposals and follower replication): there is no routing
			// work to order around, so complete immediately.
			completeProposal(c.h, shardID, entry, pp, results[i])
		}
	}

	// Everything below — the pending-reads scan, applyMembershipRouting, and
	// completing the claimed proposals — runs inside ONE recover, installed
	// before any of it, rather than one scoped only around
	// applyMembershipRouting. Every piece
	// in here calls at least one user-supplied callback that can panic:
	// c.h.metrics.ObserveReadIndexLatency in the reads scan, c.h.logger in
	// applyMembershipRouting's per-entry log call, c.h.metrics.
	// ObserveProposalLatency in completeProposal. A narrower recover placed
	// around just one of them protects claimed only from THAT one — the
	// reads scan's own metrics call is exactly as capable of leaving
	// claimed's proposals unclaimed-from-the-map-but-never-completed as
	// applyMembershipRouting's logger call is, and a recover that only
	// wraps the latter does nothing for the former. One recover around the
	// whole region means the next user-callback panic this function grows
	// is covered without a bespoke guard of its own.
	//
	// This is harmless for a batch with no membership change: claimed is
	// nil there (proposals were already claimed AND completed together, in
	// the loop above, before this closure even starts), so the recover
	// handler's range over claimed is a no-op and a panic in the reads
	// scan reaches the apply worker's own recover and fails the shard.
	func() {
		// completedClaims counts how many of claimed[0:] the normal
		// completeProposal loop below has already finished — see the
		// recover for why a panic handler needs this rather than ranging
		// over all of claimed unconditionally.
		completedClaims := 0
		// normalReturn distinguishes a runtime.Goexit in user code (a
		// MetricsCollector, the logger) from a normal return: both leave
		// recover() nil, and the tail must be completed for either exit.
		normalReturn := false
		defer func() {
			if r, abnormal := supervise.ClassifyExit(recover(), normalReturn); abnormal {
				// Capturing the stack and constructing the typed error are
				// both safe: debug.Stack() is pure runtime introspection,
				// and storing r in a struct field does not call any method
				// on it. Nothing here is user code yet.
				stack := debug.Stack()
				panicErr := &LoopPanicError{
					LoopName: "OnApplied(membership)",
					Value:    r,
					Stack:    stack,
				}

				// Complete/release the NOT-yet-completed tail,
				// claimed[completedClaims:], BEFORE any logging below.
				// A panic value's concrete type is caller-controlled and
				// may implement String/Error/Format with a bug of its own,
				// and c.h.logger is the embedder's (WithLogger), whose
				// Handler can also panic. Logging
				// first would let a panic in fmt.Sprint(r) or the handler
				// itself skip this whole deferred function, recreating
				// exactly the stranding this recovery exists to prevent.
				//
				// Only the NOT-yet-completed tail: a panic reaching here
				// after the completeProposal loop below has already
				// finished some of claimed would otherwise complete/
				// release those a second time. RequestState.complete is
				// idempotent (CAS-guarded) but releaseRef is not — it
				// unconditionally decrements a reference count and
				// recycles the object at zero, so a second call for an
				// already-released RequestState underflows that count or
				// touches an object already back in the pool and
				// potentially reused by an unrelated request.
				//
				// This does NOT call pp.failed() (rollbackPreRegister) the
				// way the deadline sweep does, and must not:
				// rollbackPreRegister exists for a proposal that was
				// appended but never committed at all (leader deposed, log
				// truncated) — exactly sweepPendingProposals's case, where
				// the entry never reaches OnApplied. Every entry in THIS
				// batch, by contrast, is one OnApplied was invoked with,
				// which only happens after raft's own authoritative fold
				// (Peer.Commit -> processConfigChanges, earlier in the same
				// pipeline) has already committed it — durably, regardless
				// of whether this host-level mirror (registry, ns.nodes,
				// whatever panicked) finishes reflecting it. Rolling back
				// the speculative pre-registration for an already-committed
				// change would revert a registry entry applyMembershipRouting
				// had already correctly made, or a still-correct prior
				// mapping for an entry whose turn in the loop simply had not
				// come yet — the change is committed either way, and the
				// shard failing after this recovers will re-derive ns.nodes
				// and the registry from the persisted log on reload
				// regardless.
				// The entries were committed and applied, so the panic is
				// reported as maybe-applied (proposalOutcomeUnknown).
				for _, cp := range claimed[completedClaims:] {
					cp.pp.rs.complete(RequestResult{Err: proposalOutcomeUnknown(panicErr)})
					cp.pp.rs.releaseRef()
					c.h.completePendingOp()
				}

				// Logging last, and contained off this goroutine:
				// everything that must happen for correctness already has,
				// and neither fmt.Sprint(r) nor a slog handler that panics
				// or calls runtime.Goexit can replace the panic this
				// function exists to report, or skip the re-panic below
				// that fails the shard.
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

		// Advance lastApplied and complete confirmed pending reads whose
		// readIndex has been reached. This runs after proposal claiming
		// above (see that comment for why claiming must go first) but
		// before routing below — including before the membership branch's
		// applyMembershipRouting. A read confirmed by this exact batch has
		// nothing to do with routing or ns.configChangeIndex — it is keyed
		// off entries[].Index alone — so there is no correctness reason to
		// run it after routing, and every reason not to: running it later
		// would let a routing panic strand any read this batch had just
		// confirmed until its own deadline, and hold Drain's pendingOps
		// count with it.
		//
		// The lastApplied store MUST happen under pendingReadsMu, before
		// the read scan, and OnReadyToRead must read lastApplied under the
		// same lock. Otherwise a read confirmed by OnReadyToRead in the
		// window between this scan and a post-unlock store would be
		// completed by neither path (OnReadyToRead sees a stale
		// lastApplied; this scan ran before the read was confirmed) and
		// would hang until its deadline.
		//
		// The unlock is deferred, not explicit-at-the-end, and no user code
		// runs under the lock: ObserveReadIndexLatency, user-supplied code
		// that can panic or call runtime.Goexit, runs after it is released.
		// An explicit Unlock() after the loop is skipped entirely if the
		// loop exits abnormally, leaving pendingReadsMu locked forever — every
		// future ReadIndex registration and every future sweepPendingReads
		// call (which sweeps every shard's nodeState from one loop) would
		// then hang acquiring it, well beyond just this shard. Scoped to
		// its own closure so the lock is not held across routing below,
		// which does not need it.
		if len(entries) > 0 {
			c.completeAppliedReads(ns, shardID, entries[len(entries)-1].Index)
		}

		if hasMembershipChange {
			// Update the Host routing table (ns.nodes + registry, and —
			// critically — ns.configChangeIndex) BEFORE completing any
			// claimed proposal in this batch, not after. rs.complete
			// (inside completeProposal) is a cross-goroutine handoff: it
			// can wake a caller blocked on RequestState.ResultC() while
			// this function is still executing on the apply worker's
			// goroutine. A caller unblocked by a config-change proposal's
			// completion is documented (see SyncGetShardMembership) to be able to
			// immediately call SyncGetShardMembership and round-trip its
			// ConfigChangeID into the next config-change call; that
			// round-trip is only race-free if ns.configChangeIndex already
			// reflects this batch by the time completion is visible to the
			// caller. Running applyMembershipRouting first — same
			// goroutine, program order — makes that true; running it after
			// would leave a window where the caller's next call races
			// advanceConfigChangeIndex and loses, producing a
			// false-positive StaleConfigChangeError.
			//
			// This does NOT move the user-supplied OnMembershipChanged
			// listener earlier too — see fireMembershipChanged below. Only
			// the routing/index bookkeeping needs to happen before
			// completion; the listener is arbitrary user code, and
			// OnApplied runs on the apply worker's goroutine with no
			// dispatch boundary of its own (engine.go's UnloadNode doc:
			// synchronous engine-to-Host callbacks that fan out to user
			// code must be dispatched by the Host, or the hazard returns
			// through them). A panicking listener is caught by the apply
			// worker's recover, which calls OnProposalFailed for this
			// whole batch — but by the time the
			// listener runs (after completion, below), every proposal in
			// this batch has already been claimed, so that recovery path
			// finds nothing left in pendingProposals to fail.
			c.applyMembershipRouting(ns, shardID, entries)

			for i, cp := range claimed {
				createdAt := deliverProposal(c.h, shardID, cp.entry, cp.pp, cp.result)
				completedClaims = i + 1
				observeProposalLatency(c.h, shardID, createdAt)
			}
		}
		normalReturn = true
	}()

	// Fire OnMembershipChanged last, after every pending proposal and read in
	// this batch has been completed (this is why applyMembershipRouting
	// above does not fire it itself). See the
	// comment on the early applyMembershipRouting call for why: the
	// callback is arbitrary user code running synchronously on the apply
	// worker's goroutine, so a panic here is caught by that goroutine's
	// recover and turned into OnProposalFailed for this batch. Firing it
	// only after pendingProposals has been drained means that recovery path
	// has nothing left to fail — a panicking listener can no longer cause an
	// already-applied, already-durable change to be reported to its caller
	// as an error.
	if hasMembershipChange {
		c.fireMembershipChanged(ns, shardID)
	}
}

// completeAppliedReads stores newLastApplied as ns's applied index and
// completes every confirmed pending read it satisfies, both under
// pendingReadsMu (see OnApplied for why the store and the scan share the
// lock). The completed reads' latencies are observed after the lock is
// released, each after its read was completed: the MetricsCollector is user
// code and must neither run under pendingReadsMu nor stand between a
// confirmed read and its caller.
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

// fireMembershipChanged invokes the user-supplied OnMembershipChanged
// listener, if configured, for a shard whose committed batch included a
// config change or the bootstrap membership entry. Split out of
// applyMembershipRouting so OnApplied can run the routing/configChangeIndex
// update (applyMembershipRouting) before completing pending proposals, while
// keeping this listener invocation after — see the comments at both call
// sites in OnApplied.
func (c *hostEngineCallback) fireMembershipChanged(ns *nodeState, shardID uint64) {
	if el := c.h.cfg.EventListener; el != nil && el.OnMembershipChanged != nil {
		el.OnMembershipChanged(config.EventShardInfo{
			ShardID:   shardID,
			ReplicaID: ns.replicaID,
		})
	}
}

// applyMembershipRouting updates the host routing table (ns.nodes + registry)
// and ns.configChangeIndex from the committed membership entries in a batch,
// so the transport can resolve new/removed replica addresses. It handles both
// the bootstrap EntryInitialMembership and incremental config changes. It
// does NOT fire OnMembershipChanged — see fireMembershipChanged and the two
// call sites in OnApplied for why that is split out and called later.
//
// A DECODE FAILURE FAULTS THE SHARD. This is the host-side MIRROR of the
// authoritative fold in raft (Peer.Commit -> processConfigChanges), which runs
// on the same committed entries earlier in the same pipeline and already treats
// an undecodable payload as terminal — so by construction nothing undecodable
// reaches here at all. That is exactly why continuing on one is wrong: it means
// the invariant the two paths share has been broken, and the consequence is a
// routing table that silently disagrees with the membership raft is computing
// quorum over, on a replica that keeps acking the leader. Recovery has always
// treated these bytes as fatal (ReconstructMembership -> StartShard aborts);
// this closes the last place the runtime failed open over the same input.
func (c *hostEngineCallback) applyMembershipRouting(ns *nodeState, shardID uint64, entries []proto.Entry) {
	for _, entry := range entries {
		// The bootstrap membership entry carries the FULL initial configuration.
		// Register every member's address so a replica that learned its
		// membership from the LOG (a joiner given an empty members map) can route
		// to the founding members — not only one that received the map at
		// StartShard. Registration is idempotent, so a founding member that
		// already registered via effectiveNodes is unaffected.
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
			c.h.registry.Register(shardID, cc.ReplicaID, cc.Address)
			// The entry that binds THIS replica's identity is learned here
			// when it was committed after the snapshot a restart recovered
			// from — the one window checkRecoveredIdentities cannot see. A
			// binding this host does not present means every peer refuses
			// its traffic from now on; that cannot be refused at this point,
			// so it is said as loudly as a log can.
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
			// Capture the address BEFORE the registry entry is dropped: a
			// RemoveNode config change carries no Address (RequestRemoveNode
			// sets only ReplicaID), and membership tombstones are replica-ID
			// keyed and address-free, so this is the last point at which the
			// removed member's address is knowable.
			removedAddr, addrErr := c.h.registry.Resolve(shardID, cc.ReplicaID)
			ns.nodesMu.Lock()
			delete(ns.nodes, cc.ReplicaID)
			ns.nodesMu.Unlock()
			c.h.registry.Remove(shardID, cc.ReplicaID)
			if addrErr == nil && removedAddr != "" {
				c.h.releaseRemovedMemberConn(shardID, cc.ReplicaID, removedAddr)
			}
		}
		// Monotonic: a snapshot install may already have reconciled this shard
		// to a HIGHER config-change index, and an apply landing afterwards must
		// not walk it back. See advanceConfigChangeIndex.
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

// failShardOnCorruptConfig trips the shard's failure latch for a committed
// config-carrying entry whose payload does not decode, and reports it through
// the engine's terminal-error path so an operator sees it as a shard failure
// rather than one ERROR line in a log.
//
// It is the host-side counterpart of raft.ErrCorruptConfigEntry, and it exists
// so the two mirrors of the membership fold agree on failing CLOSED. A nil node
// (the shard was unloaded between the apply and this call) is a no-op: there is
// nothing left to fault, and the entry will be re-offered to a fresh fold if the
// shard is loaded again.
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

// registerBootstrapAddresses registers every member address carried by a
// committed EntryInitialMembership into the host's routing table (ns.nodes) and
// the transport registry, so a replica that learned its configuration from the
// log can resolve the founding members. Registration is idempotent.
//
// It reports whether the entry decoded. A false return has already faulted the
// shard: see applyMembershipRouting for why an undecodable committed membership
// is terminal here and not a skip.
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
			c.h.registry.Register(shardID, id, addr)
		}
	}
	return true
}

// OnReadyToRead processes confirmed read index requests from the step
// worker. For each ReadyToRead, it records the confirmed readIndex on
// the matching pendingRead and completes the request if lastApplied
// has already caught up. When the pendingRead has aliases (from batch
// coalescing), all alias keys are resolved with the same readIndex.
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
	// Reads are completed under pendingReadsMu and their latencies observed
	// only after it is released: the MetricsCollector is user code, and a
	// collector that panicked or called runtime.Goexit while the lock was
	// held would leave it held for the recovery's OnReadIndexFailed. Each
	// completion comes before its observation, so nothing the collector
	// does can strand a caller.
	var latencyBuf [8]time.Duration
	latencies := c.confirmReadyReads(ns, readyToRead, latencyBuf[:0])
	if c.h.metrics == nil {
		return
	}
	for _, d := range latencies {
		c.h.metrics.ObserveReadIndexLatency(shardID, d)
	}
}

// confirmReadyReads applies the ReadIndex confirmations in readyToRead to
// ns's pending reads under pendingReadsMu, completing each read whose index
// the state machine has already applied, and returns the completed reads'
// latencies appended to latencies. It runs no user code, and the lock is
// released by defer.
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
	// Load lastApplied under pendingReadsMu so it is consistent with
	// OnApplied's store (which also happens under this lock). Loading it
	// before the lock would risk seeing a value older than an OnApplied that
	// already scanned (and skipped) this not-yet-confirmed read, leaving the
	// read completed by neither path. See OnApplied.
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

// OnReadIndexBatched registers alias keys that should complete when the
// batchKey's ReadIndex is confirmed. This is called by the step worker
// BEFORE submitting the ReadIndex to raft, ensuring aliases are in place
// before the raft layer can produce a readState. The batchKey is the
// first key in the batch (used as the raft ReadIndex hint), and
// aliasKeys are the remaining keys that piggyback on the same heartbeat
// quorum round.
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
		// The batch key was already removed (expired by the sweep or failed
		// by a close/leadership sweep) before the aliases could attach.
		// Alias reads are reachable ONLY through the batch entry — the raft
		// confirmation arrives keyed by batchKey — so without it they could
		// never complete. Fail them now instead of leaving deadline-less
		// callers blocked forever.
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
	// Append alias keys. Pre-allocate if this is the first batch for
	// this key, otherwise append to existing aliases (multiple
	// processReady cycles could theoretically add aliases, though in
	// practice a ReadIndex resolves within one cycle).
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

	// Fail pending proposals only if we actually lost leadership
	// (were the leader before and no longer are).
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
		// failLeaderChangeRequests fails only the proposals already handed
		// to raft, which can still commit under the new leader (Leader
		// Completeness guarantees it if it reached a quorum), so it reports
		// them maybe-applied (proposalOutcomeUnknown); proposals the step
		// worker has not taken yet are left to it. The bare *NotLeaderError is
		// also produced as a pre-flight refusal where nothing was submitted;
		// the wrapper is the one bit that tells the two apart, while
		// errors.Is / Temporary / NotLeader behave identically. A pending read
		// gets the bare error: it appended nothing that could commit.
		c.h.failLeaderChangeRequests(ns, &NotLeaderError{ShardID: shardID, LeaderID: leaderID})
	}

	// Fail forwarded proposals on followers when the leader changes.
	// When a follower has proposals forwarded to leader L1, and the
	// leader changes to L2, those forwarded proposals will never be
	// completed by L1. Detect this by comparing the previous leader ID
	// with the new one. Only fail when:
	// 1. We were NOT the leader (follower path; leader loss is handled above)
	// 2. The leader actually changed (not the same leader re-elected)
	// 3. The new leader is not US — proposals we are about to commit
	//    ourselves must not be failed out from under the caller.
	//
	// prevLeaderID == 0 is INCLUDED deliberately. A dying leader takes the
	// shard through leaderID 0 first, so by the time the new leader is
	// observed prevLeaderID is already 0. Requiring prevLeaderID != 0 here
	// meant every proposal submitted during the leaderless window stayed
	// pending after a leader existed again, and the caller blocked until its
	// OWN deadline — measured as a ~5s tail on an election that completed in
	// ~530ms. Those proposals were never forwarded anywhere and cannot
	// complete, so the new leader's arrival is exactly when to fail them and
	// let the caller retry against it.
	if !wasLeader && prevLeaderID != leaderID && leaderID != ns.replicaID {
		c.h.logger.Debug("follower detected leader change, failing forwarded proposals",
			"shard", shardID,
			"replica", ns.replicaID,
			"prev_leader", prevLeaderID,
			"new_leader", leaderID,
			"term", term,
		)
		// failLeaderChangeRequests fails only the proposals already handed
		// to raft, which can still commit under the new leader (Leader
		// Completeness guarantees it if it reached a quorum), so it reports
		// them maybe-applied (proposalOutcomeUnknown); proposals the step
		// worker has not taken yet are left to it. The bare *NotLeaderError is
		// also produced as a pre-flight refusal where nothing was submitted;
		// the wrapper is the one bit that tells the two apart, while
		// errors.Is / Temporary / NotLeader behave identically. A pending read
		// gets the bare error: it appended nothing that could commit.
		c.h.failLeaderChangeRequests(ns, &NotLeaderError{ShardID: shardID, LeaderID: leaderID})
	}
}

// OnLeaderReady records that the shard's (leaderID, term) pair is read-ready
// for ShardReadReady and fires the OnLeaderReady listener. It runs on the
// listener dispatcher behind the OnLeaderUpdated that announced the same pair,
// so the mirror is set only after that update stored the pair. A witness
// cannot serve reads, so it is neither marked nor announced.
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
	if el := c.h.cfg.EventListener; el != nil && el.OnLeaderReady != nil {
		el.OnLeaderReady(config.LeaderInfo{
			ShardID:   shardID,
			ReplicaID: replicaID,
			Term:      term,
			LeaderID:  leaderID,
		})
	}
}

// OnLogCompacted fires the log compaction events for a snapshot save that
// actually compacted, each carrying the index compaction ran through. It is
// delivered after OnSnapshotCompleted, so a listener sees OnSnapshotCreated
// before the compaction it enabled. OnLogDBCompacted fires only when the
// LogDB also removed the entries: a LogDB step that failed was logged and is
// retried by the next snapshot, and reporting it would claim storage was
// reclaimed when it was not.
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

// dispatchListener runs a fan-out to user EventListener code on the engine's
// listener dispatcher. The snapshot callbacks are invoked INLINE on a snapshot
// pool worker, and the engine.Callback contract forbids fanning them out to
// user code without dispatching: a listener is free to call StopShard, and an
// unload can wait for that very worker — a recovery reports while holding the
// node's write lease, which UnloadNode waits out — so an inline listener
// calling StopShard would wait on itself forever. Without an engine (unit
// fixtures) there is no worker to deadlock with, and fn runs inline.
func (c *hostEngineCallback) dispatchListener(event string, shardID uint64, fn func()) {
	if c.h.engine == nil {
		fn()
		return
	}
	if !c.h.engine.DispatchListener(fn) {
		// Dropped, as the engine drops its own notifications on a full queue
		// (counted in ListenerNotificationsDropped); named here so an operator
		// can tell WHICH event a shard never delivered.
		c.h.logger.Error("listener notification dropped: the EventListener is not consuming",
			"event", event,
			"shard_id", shardID)
	}
}

// OnSnapshotCompleted processes snapshot completion callbacks from the
// engine snapshot pool. It reconciles membership after a recovery and fires
// the snapshot event listeners for op if configured. The requester, if any,
// is resolved separately by OnSnapshotRequestCompleted.
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

	// Reconcile nodeState.nodes from the snapshot membership on
	// successful snapshot recovery. The Raft Peer already applied
	// the snapshot membership to its internal state; we mirror it
	// here so the Host routing table (ns.nodes + registry) stays
	// in sync. This handles the case where config changes were
	// compacted into the snapshot and never individually observed
	// by OnApplied.
	//
	// A save needs none of this: every entry at or below its index was
	// applied first, and OnApplied updated the routing table from those
	// entries in hand, so compacting them afterwards loses nothing.
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

// OnSnapshotRequestCompleted resolves the snapshot RequestState registered
// under requestKey with the snapshot's index or translated error. The pool
// delivers it after releasing the shard's snapshot slots, so the caller can
// issue its next request the moment it is resolved. A key no longer present
// was already claimed by the deadline sweep or by teardown.
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

// fireSnapshotListeners delivers a snapshot completion to the user's
// EventListener. It runs on the listener dispatcher (see dispatchListener).
//
// A failed completion fires nothing here. OnShardFailed reports a shard whose
// failure latch has tripped, exactly once, and the only snapshot outcome that
// trips it — a failed recovery — is reported by the latch itself
// (the engine's latch-and-report path, called from the snapshot pool). Reporting it again here
// counted one failure twice. A failed save does not latch: the replica keeps
// applying and serving and the next save is attempted as usual, so it is
// reported where it happened — the pool logs it, and the caller of
// RequestSnapshot receives it as a *SnapshotFailedError. A recovery the engine
// DECLINED as stale is neither a failure nor a recovery and fires nothing.
//
// A save that wrote no snapshot — nothing applied yet, or no progress since
// the last one — completes without error at index 0 and fires nothing either:
// no snapshot was created, and it was never a recovery.
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

// reconcileMembershipFromPeer reads the committed membership from the
// Raft Peer and synchronizes the Host's nodeState.nodes map and registry
// with it. This is called after a snapshot is successfully applied to
// ensure the Host routing table reflects any membership changes that
// were compacted into the snapshot.
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
		c.h.registry.Register(shardID, replicaID, addr)
	}
	ns.nodesMu.Unlock()

	// Seed the config-change guard from the reconciled membership. A snapshot
	// compacts the config-change entries that produced this membership out of
	// the log, so the incremental OnApplied path never re-fires for them and
	// ns.configChangeIndex would otherwise stay behind (0 on a fresh
	// nodeState). pm.ConfigChangeID is the log index of the last applied config
	// change — the exact value SyncGetShardMembership reports — so storing it
	// keeps RequestAddNode/RemoveNode's staleness guard round-tripping after a
	// snapshot install. Never regress it: a concurrent higher incremental apply
	// must win.
	advanceConfigChangeIndex(ns, pm.ConfigChangeID)
}

// advanceConfigChangeIndex raises ns.configChangeIndex to idx, never lowering
// it.
//
// configChangeIndex is the log index of the last applied config change, and it
// is the value RequestAddNode/RequestRemoveNode check a caller's Expected
// against. Every writer must therefore be monotonic, because a value that goes
// BACKWARDS rejects the very number SyncGetShardMembership just reported —
// wedging every membership change on that shard until something else happens to
// advance it.
//
// Every writer goes through here so that all of them are monotonic: an
// unconditional store on the incremental OnApplied path would let an apply
// landing after a snapshot install undo it. There is no second copy of the
// loop to drift out of step with the first.
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

// OnCampaignSkipped is DEAD CODE at runtime: the engine never invokes it,
// so neither this method nor the EventListener.OnCampaignSkipped callback it
// forwards to is ever reached in production. The plumbing is kept complete
// and correct so that a future raft-to-engine notification path has a
// working destination — see engine.Callback.OnCampaignSkipped for why no
// such path exists today. Its unit tests exercise it by calling it directly.
func (c *hostEngineCallback) OnCampaignSkipped(shardID, replicaID, term uint64) {
	if el := c.h.cfg.EventListener; el != nil && el.OnCampaignSkipped != nil {
		el.OnCampaignSkipped(config.EventShardInfo{
			ShardID:   shardID,
			ReplicaID: replicaID,
			Term:      term,
		})
	}
	c.h.logger.Info("campaign skipped",
		"shard", shardID,
		"replica", replicaID,
		"term", term,
	)
}

// OnSnapshotRejected is called when the snapshot pool refuses a request
// (duplicate in-flight or pool full), abandons an accepted one (the pool
// stopping, or a save that panicked), or rejects a recovery that failed before
// the state machine restore began; err is the cause, delivered as
// SnapshotInfo.Err. The pool reports it inline, on its worker or on the
// requesting goroutine, so the user listener is delivered by the listener
// dispatcher (see dispatchListener) rather than run here.
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

// OnCommitted delivers early WAL-durable notifications to pending
// proposals. Called by the commit worker after entries are persisted
// to LogDB but before state machine apply. Only entries with a
// registered RequestState (Key > 0) are notified.
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
				// An entry that predates this proposal under its key; see
				// pendingProposal.floor. Not this proposal's commit, and a
				// caller waiting on CommittedC for a WAL-durable
				// acknowledgement must not be told otherwise.
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
			// Unlike the completion paths, OnCommitted must leave the map
			// entry in place for the later OnApplied, so it cannot claim
			// via LoadAndDelete. A bare commitNotify after Load would race
			// the claiming paths (OnApplied/OnProposalFailed/
			// failPendingRequests/sweep): they can remove, complete, and
			// releaseRef between the Load and the notify, recycle the
			// RequestState into a brand-new request, and the stale notify
			// would then win the fresh request's committedDone CAS and
			// push a bogus WAL-durable notification into it. Instead, pin
			// the RS with tryAcquireRef (safe against recycling — it never
			// increments from zero), then re-verify the map still holds
			// the SAME pendingProposal. The comparison is on POINTER
			// identity (v2 != v below), not on the key: keys are random
			// 64-bit draws, so a repeat is possible in principle,
			// but a re-registration under a repeated key allocates a new
			// pendingProposal and fails the identity test. A matching
			// re-Load therefore proves the entry — and the map's own
			// reference — was held continuously across the acquire, i.e.
			// the pinned RS is still this request's. If the verify fails
			// the request was claimed (or the count belonged to a recycled
			// successor); drop the balanced reference and skip.
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

// OnProposalFailed completes pending proposals that failed after they were
// handed to raft: entries the leader could not persist (which it may already
// have replicated), committed entries whose commit or apply failed, or a
// committed batch discarded unapplied. Every one of them may still commit and
// apply, so the caller is told the outcome is unknown: the error reports
// MaybeApplied and still matches its own type and sentinel.
func (c *hostEngineCallback) OnProposalFailed(shardID uint64, entries []proto.Entry, err error) {
	c.failProposals(shardID, entries, err, true)
}

// OnProposalRejected completes pending proposals raft refused before
// appending them (no known leader, a transfer in progress, a config change
// already pending). None of them can ever commit, so the refusal is definite.
func (c *hostEngineCallback) OnProposalRejected(shardID uint64, entries []proto.Entry, err error) {
	c.failProposals(shardID, entries, err, false)
}

// failProposals completes the pending proposals for entries with err,
// translated to the public API, and classified by whether they had been
// handed to raft (see failClaimedProposal). Callers of Host.Propose and
// SyncPropose get a prompt error instead of waiting for their deadline.
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

// proposalOutcomeUnknown is the one classification point for a failure
// delivered to a proposal, config change or session operation AFTER it was
// handed to raft: the entry may still commit and apply, so the failure is
// wrapped in a MaybeAppliedError — unless it already is one, which is never
// wrapped twice. errors.Is and errors.As still reach the failure's own type
// and sentinel through the wrapper. It runs no user code and cannot panic.
func proposalOutcomeUnknown(err error) error {
	// A type assertion, not MaybeApplied: errors.As would walk the error's
	// chain, calling user code — a state machine error's own As or Unwrap —
	// that can panic, and this runs after the proposal has been claimed, where
	// a panic strands it. Only the host's own wrapper is recognized, at the
	// top: a user error that wraps one deeper is wrapped again, which costs a
	// redundant layer and nothing else.
	if _, ok := err.(*MaybeAppliedError); ok {
		return err
	}
	return &MaybeAppliedError{Err: err}
}

// failClaimedProposal completes pp, already claimed out of pendingProposals by
// the caller, with err. handedOff reports whether the proposal may have
// reached raft, which the caller decides before the claim: false when raft
// refused it before appending (OnProposalRejected) or when its node provably
// never handed it off (proposalMayReachRaft). A handed-off failure is
// classified by proposalOutcomeUnknown. Nothing here runs user code or can
// panic, so a claimed proposal is always completed and released.
func (h *Host) failClaimedProposal(pp *pendingProposal, err error, handedOff bool) {
	if handedOff {
		err = proposalOutcomeUnknown(err)
	}
	pp.failed()
	pp.rs.complete(RequestResult{Err: err})
	pp.rs.releaseRef()
	h.completePendingOp()
}

// OnReadIndexFailed completes pending read requests whose ReadIndex
// submission to the Raft peer failed. The keys slice contains all keys
// in the batch (batch key + alias keys). This prevents orphaned pending
// reads that would otherwise block until context deadline timeout.
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
		// A batch key's aliases fail with it. A submit failure lists them
		// in keys already (they are gone by the time the loop reaches
		// them), but a leader's reject of a forwarded read, like a dropped
		// read state, names only the batch key, and its aliases are
		// reachable through nothing else.
		for _, aliasKey := range fail(key) {
			fail(aliasKey)
		}
	}
	ns.pendingReadsMu.Unlock()
}

// OnShardFailed fails every proposal and read still pending on a shard whose
// failure latch has just tripped with ErrShardFailed — a proposal's wrapped in
// a MaybeAppliedError, a read's bare.
//
// The engine fails the batch that caused the halt, with its own error, before
// it reports the halt. Everything else registered before the halt is stranded
// without this drain: a halted shard never steps, commits or applies again, so
// a proposal still queued behind the failed batch, or a read waiting for the
// OnApplied scan to reach its read index, would sit until the pending sweep
// expired it with ErrTimeout — pointing at consensus or the network, not at the
// failure. Snapshot requests are not drained: the snapshot pool answers each
// one it admitted whether or not the shard has failed.
//
// The admission gates handle requests that arrive AFTER the latch, and
// re-check it after registering (registerPendingProposal, ReadIndex) so that a
// registration racing this drain rolls itself back; this is the other half,
// for the requests already inside. The error is the sentinel rather than the
// halt's own error for the same reason the gates return it: a caller can match
// ErrShardFailed and know the shard is offline until reloaded, where the raw
// *ApplyError would need string-matching to classify.
//
// A proposal drained here was handed to raft, and its entry may already be
// committed: a recovery, for one, keeps the raft protocol running while it
// withholds applies, so entries commit behind it, and a reload replays and
// applies them. Its outcome is unknown, so the error reports MaybeApplied true
// — an unsessioned caller must not retry it blindly — while still matching
// ErrShardFailed. A read appends nothing, and a request refused at admission
// never reached raft; both get the bare ErrShardFailed.
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

	// Every proposal still pending had been handed to raft and may already be
	// committed, which a reload replays and applies, so
	// failPendingClientRequests reports it maybe-applied; reads get the bare
	// sentinel.
	c.h.failPendingClientRequests(ns, ErrShardFailed)

	// The latch leaves the cached leadership view as it was, so wake the
	// leader-executed requests waiting on it: a caller here returns
	// ErrShardFailed, and an execution waiting for this replica to be ready
	// refuses instead of waiting for a term that will never be.
	ns.signalLeaderChange()

	// One log line at the moment of the halt, naming shard and replica and
	// carrying the error that tripped the latch, so the read failures that
	// follow have something to correlate against. The halt's cause reaches
	// the operator here; the requests carry only the sentinel. It comes
	// after every pending request has been failed, and contained: the slog
	// handler is user code and must not be able to strand them.
	replicaID, logger := ns.replicaID, c.h.logger
	supervise.LogContained(func() {
		logger.Error("shard halted: failure latch tripped, refusing reads and proposals until the shard is reloaded",
			"shard_id", shardID,
			"replica_id", replicaID,
			"error", err,
		)
	})
}

// translateProposalError converts internal Raft errors to public API errors.
// The internal raft.NotLeaderError is translated to the public
// quicraft.NotLeaderError with cached leader info from the nodeState.
// Internal raft.ErrReadIndexNotReady is mapped to the public
// ErrReadIndexNotReady so callers can match with errors.Is.
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
		// A committed entry whose snappy header declares an expansion beyond
		// MaxEntrySize. The apply worker fails the whole batch with it, so it
		// reaches the caller of every proposal in that batch. Without the
		// mapping the public ErrDecompressionBomb had no producer at all and
		// the raft-internal one is unmatchable from outside the module.
		return ErrDecompressionBomb
	}
	// A shard-failure error is NOT translated. The public ErrShardFailed IS
	// engine.ErrShardFailed, and the errors the apply path produces for a
	// halted shard are public too: an *ApplyError (a state machine that
	// returned an error, or a decompression failure) wraps the state machine's
	// own error, and an *SMPanicError (a state machine that panicked) matches
	// ErrShardFailed through its own Is method. Passed through as themselves,
	// both reach the caller with errors.Is(err, ErrShardFailed) where it
	// applies and errors.As to the typed error and its detail — the panic
	// value and stack — which a translation to the bare sentinel discarded.
	if errors.Is(err, engine.ErrStopped) {
		// A proposal batch the step worker drained but could not hand to
		// raft because its node stopped first: definite, and the engine's
		// sentinel is internal, so it is mapped to the public one.
		return ErrStopped
	}
	if errors.Is(err, raft.ErrPendingConfigChange) {
		// A config change proposed while another is still uncommitted, or
		// two batched into one Propose (PhD 4.1 permits only one in flight).
		// Without this the internal error escapes verbatim, and since it
		// lives in an internal package no caller can match it with
		// errors.Is — every consumer falls through to its own catch-all and
		// reports a retryable rejection as a fatal error. It IS retryable:
		// nothing entered the log, so re-proposing once the pending change
		// commits succeeds.
		return ErrRejected
	}
	return err
}

// translateSnapshotSubmitError converts the snapshot pool's REFUSAL to accept a
// request into a public error.
//
// The pool refuses for two structurally different reasons and the caller's
// correct response differs, so they map to different errors rather than both
// to ErrSnapshotInProgress. A *SnapshotPoolBusyError means every snapshot worker
// in the process is occupied — nothing is snapshotting THIS shard, so the
// request simply found no worker and an operator told "already in progress"
// goes looking for a concurrent snapshot that does not exist. A
// *SnapshotDuplicateError is this shard's single-flight slot genuinely being
// held, which is the only case ErrSnapshotInProgress describes.
//
// Both are Temporary, so a retry loop is unaffected by the distinction; an
// operator can still tell the two conditions apart.
func translateSnapshotSubmitError(err error) error {
	var poolBusy *engine.SnapshotPoolBusyError
	if errors.As(err, &poolBusy) {
		return ErrBusy
	}
	return ErrSnapshotInProgress
}

// translateSnapshotError converts the error a snapshot pool worker reports
// into an identity the public API defines.
//
// The pool lives in an internal package, so everything it produces —
// engine.ErrStopped, *engine.SnapshotError, *engine.SnapshotTooLargeError —
// is unmatchable by any errors.Is a caller outside this module can write.
// Handed to the snapshot RequestState verbatim (which is what happened before
// this function existed) they left SyncRequestSnapshot callers with string
// comparison as their only option, and left the public sentinels for these
// very conditions — ErrStopped, ErrSnapshotRecoveryFailed — with no producer
// anywhere in the library.
//
// Anything unrecognised passes through unchanged. That is deliberate: the
// barrier failures (ErrShardBarrierUnresolved, ErrSnapshotBarrierSealed,
// ErrSnapshotEncryptedBarrierUnavailable) are declared in the engine precisely
// so both layers fail with ONE identity, and re-mapping them would break the
// property they exist for. Wrapping an error this function does not recognise
// would likewise claim knowledge the host does not have.
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
		// The save was refused because the commit frontier had folded a config
		// change the apply frontier had not reached — an internal state
		// transition that clears on its own, which is exactly what ErrAborted
		// means and why it classifies as Temporary. A retry takes the snapshot.
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
	cfg           config.HostConfig
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
	// barrierFallbackWarned records the shards already warned about falling
	// back from an unresolved per-shard barrier to the host-wide one, so the
	// warning fires once per shard instead of once per snapshot operation.
	barrierFallbackWarned sync.Map // map[uint64]struct{}
	metrics               MetricsCollector
	engine                *engine.Engine
	registry              *registry.Registry
	sender                engine.MessageSender
	transport             *transport.QUICTransport // nil when transport is disabled
	nodes                 sync.Map                 // map[uint64]*nodeState — lock-free reads for StaleRead/QueryLocalNode
	activeNodes           atomic.Int64             // tracks the number of loaded shards (replaces len(h.nodes))
	closed                atomic.Bool
	draining              atomic.Bool
	diskFull              *atomic.Bool // set by disk full callback in buildWALOptions
	id                    string
	stopOnce              sync.Once
	// closeErr is the shutdown outcome the FIRST Close computed. Every other
	// Close returns it too, which is only safe because stopOnce.Do publishes
	// the write to every caller that blocked on it.
	closeErr    error
	pendingOps  atomic.Int64  // tracks outstanding pending operations for drain
	keyRotStopC chan struct{} // closed to stop the key rotation goroutine
	keyRotDone  chan struct{} // closed when the key rotation goroutine exits
	sweepStopC  chan struct{} // closed to stop the pending proposal sweeper goroutine
	sweepDone   chan struct{} // closed when the sweeper goroutine exits
	drainC      chan struct{} // closed when pendingOps reaches 0 during drain
	drainCMu    sync.Mutex    // protects drainC creation
	// drainOnce elects the one Drain attempt and JOINS every other caller on
	// it, exactly as stopOnce does for Close. drainErr is written inside the
	// Once and published by it, so every caller reports the same outcome.
	drainOnce sync.Once
	drainErr  error
	// fsys routes snapshot persistence I/O. nil means the real filesystem;
	// only tests set it (withSnapshotFS), so the injection never reaches the
	// public API.
	fsys snapshotFS

	// joiningShards holds the shards started with join=true whose bootstrap
	// snapshot has not arrived yet (map[uint64]struct{}). Such a shard has no
	// membership; the operator anchor (anchorIdentity) is what authorizes the
	// leader's first snapshot and the messages before it. The entry is removed
	// as soon as a snapshot is accepted for the shard, closing the window.
	joiningShards sync.Map

	// anchors holds each loaded shard's OPERATOR ANCHOR
	// (map[uint64]map[uint64]string: replica ID -> node identity), a copy of
	// the Config.MemberIdentities the shard was started with. It is the third
	// source the receive gate's sender binding consults; see anchorIdentity.
	// Set at StartShard and removed with the shard.
	anchors sync.Map

	// importLocks serialises ImportSnapshot per shard. A shard's record
	// exists only while its lock is held or waited on, so the set does not
	// grow with every shard ID the host has seen.
	//
	// Nothing else serialises imports, and two concurrent imports of the same
	// shard race over the SAME on-disk directory: the artifact index names it,
	// so two calls carrying the same index resolve to one path, and both the
	// staleness check and the destination-exists check below would otherwise be
	// evaluated against a tree the other call is midway through changing. The
	// lock makes the check-then-write sequence atomic per shard.
	importLocks shardLocks

	// startingShards holds the shard IDs a StartShard is between its early
	// duplicate check and its return (map[uint64]struct{}). StartShard holds
	// the shard's import lock for that span too, so without this a second,
	// concurrent start of the same shard found the lock taken and was told an
	// import was in progress; claiming the start first lets it report the
	// transient ErrShardNotReady, and leaves ErrImportInProgress to imports.
	startingShards sync.Map

	// leaderExec holds the leader-executed request handlers, the requests
	// this host has in flight to a remote leader, and the bound on the
	// requests it is executing for remote callers. See ExecuteOnLeader.
	leaderExec *leaderExecRouter
}

// acquireImportLock takes the shard's exclusive import lock, waiting until it
// is free or ctx is done. The returned function releases it.
//
// The wait is cancellable because an import can run for as long as it takes
// to write a multi-gigabyte artifact, so blocking uninterruptibly behind one
// is not an acceptable answer to a cancelled request.
func (h *Host) acquireImportLock(ctx context.Context, shardID uint64) (func(), error) {
	return h.importLocks.acquire(ctx, shardID)
}

// tryAcquireImportLock takes the shard's exclusive import lock if it is free,
// reporting whether it did. The returned function releases it and is nil when
// the lock was not taken.
//
// It exists for callers that must never WAIT for an import — StartShard sweeps
// orphaned import staging directories and an import can run for as long as it
// takes to write a multi-gigabyte artifact — and for which "an import holds the
// lock" is itself the answer: whatever the sweep would have found belongs to
// that import.
func (h *Host) tryAcquireImportLock(shardID uint64) (func(), bool) {
	return h.importLocks.tryAcquire(shardID)
}

// lockStoppedShardForImport takes the shard's import lock and then re-checks,
// under it, that the shard is still stopped, returning the release.
//
// ImportSnapshot's preflight runs before the lock, and StartShard holds the
// lock for its whole start: a shard started between the two is running by the
// time the lock is free, and importing underneath it would compact the log a
// live node reads. While the lock is held, no start can begin.
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
	// The import resolved its membership and paths from the replica the
	// first, unlocked preflight found. A start and stop in between can have
	// re-bootstrapped the shard under another replica; importing under the
	// stale one would write a record and tree no incarnation reads.
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

// checkRecoveredIdentities holds a restart to the configuration it recovered.
// validateMemberIdentities runs before recovery and sees only the caller's
// map; this runs after it and sees the committed bindings, which are what
// every peer authorizes against.
//
// Two things are refused. This host must present the identity the shard
// committed for its replica: a changed keypair would start cleanly and then
// have every message refused by every peer, indefinitely, with nothing local
// saying why — the remedy is to restore the keypair, or remove and re-add the
// replica. And the operator anchor must not contradict a committed binding:
// an anchor entry for a committed member with a different identity is a typo
// the committed binding would silently win over, and an anchor entry binding
// a committed member's identity to ANOTHER replica ID would let that member's
// keypair send as the second ID — the anchor admits senders the committed
// configuration does not know, so nothing else would catch it.
//
// What it sees is the configuration recovery handed StartShard — the
// snapshot's, or the founding entry's when there is no snapshot yet — so a
// binding committed AFTER that point is outside its view. That window has
// its own backstops at the moment the binding is learned: the receive gate
// asks the committed configuration, current at every message, before the
// pending configuration or the anchor may admit a sender
// (Peer.IdentityHolder), and applyMembershipRouting reports a committed
// binding for this replica that the host does not present. Folding the
// committed tail here instead would read the whole log on every restart of a
// shard that never snapshots (the StartShard latency cost
// reconstructShardMembership exists to avoid).
//
// An anchor entry whose identity the shard has since REMOVED is tolerated,
// at WARN: the founding map is documented as identical across founders and
// supplied on every start, so after any RemoveNode every remaining founder's
// map names the removed replica, and refusing it would turn a routine
// removal into a start failure on the next rolling restart. The entry is
// inert — the removed-identity deny-list runs before the anchor is consulted
// (Peer.IsRemovedIdentity in authorizeInbound) — so nothing is admitted by it.
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

// validateMemberIdentities checks the FORM of Config.MemberIdentities on a
// host that requires identities, on every start: this host's own entry must
// be its identity, no identity may be bound to two replicas, no entry may be
// empty, and replica 0 is never named. Whether the map must COVER the members
// map is a bootstrap question (requireFoundersIdentified), and whether a
// joiner needs an anchor is judged once recovery has said whether this is a
// first start (anchorsRemoteReplica); a restart is instead held to what it
// recovered (checkRecoveredIdentities). Every refusal is a typed
// InvalidMemberError so the caller learns which replica.
func (h *Host) validateMemberIdentities(cfg config.Config) error {
	for replicaID, identity := range cfg.MemberIdentities {
		if replicaID == 0 {
			// Zero is the reserved no-replica ID, refused by every bootstrap
			// map and config change; an anchor entry for it would vouch for
			// a sender claiming From == 0, which raft does not uniformly
			// refuse.
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
	// This host's OWN entry must be this host's identity. A typo or a stale
	// value there is committed into the bootstrap entry and every peer then
	// refuses this replica's traffic as identity_mismatch for the life of the
	// shard — silently, if the others still form a quorum. It is the one
	// identity the host can check itself, so it does.
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
	// One identity is one keypair; two replicas holding it would be one host
	// with two votes, which is the forgery identities exist to defeat and the
	// shape a later add refuses (ErrDuplicateMemberIdentity). The bootstrap
	// entry does not run that validator, so it is asked here.
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

// requireFoundersIdentified enforces the founders' coverage at BOOTSTRAP: the
// members map seeds the shard's first configuration, and on a host with a
// transport every founder is admitted with its identity or not at all. It is
// asked only where that seeding happens — on a restart the members map is
// ignored in favour of the recovered configuration, so a founder that dropped
// a since-removed replica from its map must not be refused for it.
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

// anchorIdentity returns the node identity the OPERATOR ANCHOR — the
// config.Config.MemberIdentities the shard was started with — binds replicaID
// to, and whether it names that replica at all.
//
// The anchor is the third and last thing that can vouch for a sender the
// committed and pending configurations do not name (see
// hostMessageHandler.senderBinding), and it stands for the life of the shard,
// not only for the join window it was first introduced for. A joiner needs it
// because it holds no configuration at all. An ESTABLISHED replica needs it
// for the one case the pending configuration cannot cover: it was down while
// a member was added and elected, so its own log never received the AddNode
// that would admit the new leader, and nothing it holds names the replica now
// leading its shard. Restarting it with a Config.MemberIdentities that names
// that leader is the remedy, and it is one an operator's provisioning system
// has at hand — the current membership's identities — without wiping the
// replica. The anchor vouches for identities the operator asserted, exactly as
// the founding map does; a removed identity is refused by the tombstone check
// before the anchor is consulted.
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

// identitiesOf restricts identities to the replicas members names — what a
// founder's bootstrap entry commits. An anchor entry for a replica outside
// the members map is operator trust for the receive gate, not membership
// state, and must not be written into index 1 for a replica no role holds.
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

// IsShardJoining reports whether shardID is still in its join bootstrap
// window: started locally with join=true and not yet given its first
// snapshot. Once a snapshot is accepted the shard leaves this window
// permanently for the life of the process. Callers (e.g. qrdb's placement
// reconciler) use this to know when a dynamically added replica has caught
// up enough to be treated as an established member on future restarts.
func (h *Host) IsShardJoining(shardID uint64) bool {
	return h.isShardJoining(shardID)
}

// NewHost creates a new Host with the given configuration and options.
// It validates the configuration, generates or loads a host ID, and
// initializes all subsystems.
//
//nolint:gocyclo // subsystem initialization requires many sequential validation and setup steps
func NewHost(cfg config.HostConfig, opts ...HostOption) (*Host, error) {
	options := &hostOptions{}
	for _, opt := range opts {
		opt(options)
	}

	// At-rest encryption is a decision the caller states, never one it
	// reaches by omission. Checked before anything touches the disk, so a
	// refused host leaves no directories behind.
	//
	// The host-wide posture (WithBarrier or WithoutEncryption) and per-shard
	// barriers are independent axes: a shard keeps its own barrier whichever
	// host-wide route the others take. Only the two host-wide statements
	// contradict each other.
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
	// TransportDisabled is an exported field, so a caller can set it directly
	// without going through WithoutTransport. Config validation then skips
	// the MTLSConfig requirement, so transport creation must honour the same
	// flag: otherwise the field would silently do nothing but disable the
	// mTLS check, which is exactly backwards.
	transportDisabled := options.disableTransport || cfg.TransportConfig.TransportDisabled

	cfg.SetDefaults()
	if err := cfg.Validate(); err != nil {
		return nil, err
	}

	// Build the Host logger. When the caller provides a logger via
	// WithLogger, use it directly. Otherwise, create one from the
	// HostConfig's LogConfig. The logger is stored on the Host struct
	// and used for all Host-level logging instead of mutating the
	// global slog default.
	var logger *slog.Logger
	if options.logger != nil {
		logger = options.logger
	} else {
		logger = config.NewLogger(cfg.LogConfig)
	}

	// Ensure the host directories exist AND are durable. A directory entry
	// created by MkdirAll survives a crash only once every directory in the
	// created chain is fsynced; without this a crash can lose the directory a
	// durably-written WAL lives in, which is the identical defect the WAL
	// layer closes one level down for its own subdirectories.
	if err := fsutil.MkdirAllSynced(cfg.NodeHostDir, 0o750); err != nil {
		return nil, &HostInitError{Field: "NodeHostDir", Err: err}
	}
	if err := fsutil.MkdirAllSynced(cfg.WALDir, 0o750); err != nil {
		return nil, &HostInitError{Field: "WALDir", Err: err}
	}

	// Shared disk full flag set by the WAL's disk monitor callback and
	// read by Host.HealthCheck(). Allocated before the WAL is opened so
	// the callback closure captures the same atomic as the Host struct.
	var diskFullFlag atomic.Bool

	// Per-shard barriers with no host-wide barrier require a Raft log that is
	// already encrypted at rest. Without one, configuring per-shard encryption
	// yields encrypted snapshots over a PLAINTEXT log -- every user command
	// readable on disk, while the operator's spot-check of the snapshot
	// directory says encryption is on. Refuse rather than silently downgrade.
	//
	// The predicate is what the LogDB IS, not whether one was supplied. The
	// default WAL takes ONE barrier and none was configured, so it would be
	// plaintext; a supplied LogDB must declare its at-rest guarantee. Deciding
	// on "did you call WithLogDB" instead accepted an unencrypted
	// waldb.Open(walDir) -- the exact state this refusal exists to prevent,
	// reached by the one-line change an integrator makes to relocate the WAL.
	//
	// WithoutEncryption does not lift this: it declares the HOST-WIDE route
	// plaintext, not the barriered shards' logs. It does let a LogDB that
	// reports AtRestHostRouted through, since that LogDB's only plaintext route
	// is the one declared; the default WAL and a plain waldb are still refused,
	// because every barriered shard's log would sit in them in plaintext.
	if options.shardBarriers != nil && options.barrier == nil {
		if err := requireEncryptedLogDB(options.logdb, nil, plaintextHostRoute, ErrShardBarriersPlaintextWAL); err != nil {
			return nil, err
		}
	}
	// The same holds for the host-wide barrier, which is wired ONLY into the
	// default WAL built below. A caller-supplied LogDB never receives it, so
	// WithBarrier plus waldb.Open(dir) — the one-line change that relocates the
	// WAL — encrypted the snapshots and wrote every proposed command to disk in
	// plaintext, with nothing to say so. A stated encryption must cover the log.
	//
	// A supplied LogDB whose host-wide route writes through this barrier itself
	// is judged as the default WAL is, so a barrier still in plaintext mode
	// before EnableEncryption is accepted for it as it is for the default WAL.
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
		walOpts := buildWALOptions(cfg, &diskFullFlag, func() []uint64 {
			if db := walDB.Load(); db != nil {
				return walShardIDs(db)
			}
			return nil
		})
		// Wire barrier encryption into the WAL when a barrier is configured.
		if options.barrier != nil {
			walOpts = append(walOpts, waldb.WithBarrier(options.barrier))
			// Per-shard barriers plus the default WAL is legal but weaker
			// than it looks: the log is encrypted under the HOST-WIDE key,
			// so the per-tenant key isolation WithShardBarriers exists to
			// provide covers snapshots only.
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

	if !transportDisabled {
		trCfg := buildTransportConfig(cfg)
		// Wire transport metrics when a MetricsCollector is provided. The
		// transport's internal TransportMetrics interface matches the
		// public MetricsCollector's TransportMetrics methods.
		if options.metrics != nil {
			trCfg.Metrics = options.metrics
		}

		// Create the message handler with nil engine and host references.
		// Both are wired after creation below. The transport is started
		// last (after all wiring) so no messages arrive before the
		// handler's engine and host fields are fully initialized.
		msgHandler = &hostMessageHandler{}
		// Outbound traffic is held to the identity membership binds to the
		// replica it is for, as inbound senders are (see replicaIdentity).
		trCfg.ReplicaIdentity = msgHandler.replicaIdentity

		var trErr error
		tr, trErr = transport.NewQUICTransport(trCfg, msgHandler, reg)
		if trErr != nil {
			return nil, &HostInitError{Field: "transport", Err: trErr}
		}

		// Wrap the transport in a transportSender that handles both
		// regular messages and snapshot streaming. The engine reference
		// is set after engine creation below. shardBarriers, when set, routes
		// snapshot decrypt-for-send to the per-shard barrier; otherwise the
		// single host-wide barrier is used (unchanged behavior).
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

	// Create the engine with the resolved sender.
	// Pass the RaftMetrics sub-interface to the engine. The engine's
	// internal RaftMetrics interface matches the public MetricsCollector's
	// RaftMetrics methods, so the concrete implementation satisfies both.
	var raftMetrics engine.RaftMetrics
	if options.metrics != nil {
		raftMetrics = options.metrics
	}
	eng := engine.NewEngine(cfg, options.logdb, sender, options.barrier, nil, raftMetrics)

	// A log write the sealed barrier refuses holds its shard until the unseal
	// rather than failing it (#296): sealing is an operator state, not a disk
	// fault, and the WAL rolled the refused batch back.
	eng.SetSealHold(engine.SealHold{
		IsSealed:  isSealedLogWrite,
		UnsealedC: logWriteUnsealedC(options.barrier, options.shardBarriers),
	})

	// Wire per-shard barriers into the snapshot pool when configured. This is
	// additive: when WithShardBarriers is unset, the snapshot pool keeps using
	// the single host-wide barrier passed above. The adapter bridges the
	// host-level Barrier (a superset interface) to the engine's SnapshotBarrier
	// and preserves a true nil for shards that fall back to the host-wide
	// barrier.
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

	// Wire the barrier as the epoch tracker for in-flight snapshot
	// protection. This prevents PurgeEpochsBefore from removing DEKs
	// while snapshot transfers are in progress. With per-shard barriers the
	// tracker is routed by shardID so each received snapshot pins the epoch on
	// the correct tenant barrier; otherwise the single host-wide barrier is
	// used (unchanged behavior).
	if tr != nil {
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
		id:                 hostID,
		keyRotStopC:        make(chan struct{}),
		keyRotDone:         make(chan struct{}),
		sweepStopC:         make(chan struct{}),
		sweepDone:          make(chan struct{}),
		fsys:               options.fsys,
		leaderExec:         newLeaderExecRouter(),
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

	// Start the QUIC transport listener. This MUST happen after ALL
	// wiring is complete: msgHandler.host, msgHandler.engine, the engine
	// callback, and eng.Start(). Once the transport starts, its recv
	// goroutines can immediately call HandleMessage/HandleSnapshot which
	// read msgHandler.host (registry, cfg, logdb) and msgHandler.engine.
	// Starting the transport before these fields are set causes a data
	// race between the recv goroutine reading them and NewHost writing them.
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

	// Start the key rotation loop whenever a barrier is configured. With
	// KeyRotationInterval == 0 scheduled rotation is disabled, but the
	// invocation-threshold check must still run: it is what rotates the
	// epoch when the AEAD invocation budget nears its NIST limit
	// (including the pessimistic post-restart seeding done by Unseal).
	// The goroutine exits when keyRotStopC is closed.
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

	// Start the pending proposal sweeper goroutine. It periodically
	// scans all node states and completes expired pending proposals,
	// reads, and snapshots with ErrTimeout. This prevents orphaned
	// entries from accumulating when entries are lost due to leader
	// crashes or network partitions.
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

	return h, nil
}

// copyIdentityMap returns a deep copy of a replicaID -> node identity map,
// dropping empty values so a map with a blank entry is indistinguishable from
// one that never named that replica. Returns nil for an empty input, which is
// what a shard with no committed identities holds.
//
// The copy matters: the caller's map is the operator's, and it goes into
// replicated state that outlives the call.
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

// verifyMemberCertificateExclusivity rejects a configuration whose hosts this
// node's own leaf certificate also covers. See
// transport.QUICTransport.VerifyPeerAddressExclusivity for why SAN PRESENCE is
// only an authorization when SANs are EXCLUSIVE.
//
// Peers that share this node's machine are not flagged: a certificate is
// matched on host alone, so they are indistinguishable by construction and no
// certificate policy can separate them (transport.isCoLocatedCertHost defines
// the two forms — the same host under any spelling, and any loopback address).
// With no transport there is no in-band authorization to collapse, so the check
// does not apply.
//
// It is called from StartShard with the initial members and from
// requestConfigChange with the single address a membership add introduces; the
// latter is what covers the grow-by-join flow, whose initial members map is
// empty on every node.
func (h *Host) verifyMemberCertificateExclusivity(members map[uint64]string) error {
	if h.transport == nil || len(members) == 0 {
		return nil
	}
	peers := make([]string, 0, len(members))
	for _, addr := range members {
		if addr != "" && addr != h.cfg.RaftAddress {
			peers = append(peers, addr)
		}
	}
	if len(peers) == 0 {
		return nil
	}
	return h.transport.VerifyPeerAddressExclusivity(h.cfg.RaftAddress, peers)
}

// StartShard starts a Raft shard on this host. members maps replicaID to
// address for initial cluster members. join indicates whether this node
// is joining an existing cluster. create is the state machine factory.
// cfg provides per-shard Raft configuration.
//
// ShardID and ReplicaID are taken from cfg. Both must be greater than zero.
//
// A replica configured as a witness (cfg.IsWitness) or an observer
// (cfg.IsObserver) must NOT appear in members: that map seeds the full voting
// membership, so it is rejected with *NonVotingRoleInMembersError. Bootstrap
// the voters first, then start such a replica with join=true and add it with
// RequestAddWitness / RequestAddObserver.
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

	// The identity requirement is a property of the wire boundary: the
	// receive-side gate authorizes a member by its committed identity and by
	// nothing else, so a host with a transport must not admit a member it
	// could never hear from. The host decides, not the caller — see
	// config.Config.RequireMemberIdentities — and the same flag reaches the
	// leader's propose-time validator through the peer's config, so a later
	// AddNode, forwarded or local, is held to the same rule.
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
	// Validate each member entry regardless of join mode.
	//
	// Two replicas at ONE address are refused for the same reason a config
	// change adding a duplicate is (raft.membership.addressHolder): inbound
	// authorization is replica ID -> committed address -> certificate, so the
	// single host behind a shared address authenticates as both replicas and
	// holds two votes and two match indices in a quorum that believes they are
	// independent. Two replicas on one MACHINE at different ports remain
	// supported — the comparison is on the whole address, not the host.
	//
	// Addresses are compared in their netaddr.Canonical form, matching
	// raft.membership.addressHolder, so a members map that spells one endpoint
	// two ways ("NODE-B:5001" and "node-b:5001") is refused here rather than
	// bootstrapping the shard into a configuration the receive path then has to
	// fail closed on.
	//
	// The FORMAT limits are checked here too, and they are not defensive
	// pedantry: members is copied verbatim into proto.Membership.Addresses,
	// raft.BuildInitialMembershipEntry marshals it, and the result is COMMITTED
	// as the EntryInitialMembership at log index 1. An over-length address or an
	// over-sized map produced bytes this package's own decoder refuses, so the
	// shard would bootstrap successfully and then fail to start FOREVER: the next
	// StartShard reconstructs membership from that entry, Membership.UnmarshalFrom
	// returns ErrAddressTooLong, and reconstructShardMembership propagates it.
	// proto.Membership.MarshalTo refuses to write such a membership at all,
	// which turns the permanent brick into an error; checking here as well is what
	// turns that error into one naming the replica and the reason, at the call the
	// operator actually made, before anything is persisted.
	if len(members) > proto.MaxMembershipEntries {
		return &InvalidMemberError{
			ShardID: cfg.ShardID,
			Reason: fmt.Sprintf("members map has %d entries; the membership format "+
				"accepts at most %d", len(members), proto.MaxMembershipEntries),
		}
	}
	// The voting-set cap is enforced here because this is the ONLY admission
	// point the bootstrap path passes through. validateAddNode and
	// validateAddWitness apply the same bound to config changes, but neither
	// runs on bootstrap: a 65-member map was accepted, committed as the
	// EntryInitialMembership at index 1, and then panicked rebuildReplicaToBit
	// inside becomeLeader on every subsequent election. The step worker's
	// recover() restarts and the next election panics again, so the shard never
	// elects a leader — and because the membership is committed and durable, a
	// restart does not clear it.
	//
	// Rejecting here means the bad configuration is never written. It cannot be
	// rejected at apply time instead: refusing an already-committed entry would
	// make replicas diverge.
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

	// The same format bounds on cfg.MemberIdentities, which travels the same
	// route: Host.StartShard seeds it into proto.Membership.Identities and it is
	// marshaled into the same committed index-1 entry.
	//
	// The bound is MaxAddressLength and not MaxNodeIdentityLength, because the
	// encoder is what decides: every string map in a Membership goes through the
	// shared putMap/getMap pair, which bounds values at MaxAddressLength
	// regardless of which map they came from. The two constants are equal today,
	// so the distinction costs nothing; it is written this way so that raising
	// MaxNodeIdentityLength alone cannot silently start producing unreadable
	// bootstrap entries.
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

	// Enforce per-node certificate SANs before the shard exists.
	//
	// The transport's in-band authorization (spoofed-SourceAddress clearing,
	// peerEpoch mutation / live-outbound eviction, snapshot-sender binding) all
	// reduce to "is the claimed host in the sender's verified certificate".
	// That is only an authorization if SANs are exclusive per node; a wildcard
	// or shared cluster certificate makes it true for every peer and lets one
	// compromised replica drive cluster-wide replication stalls.
	//
	// This fails CLOSED. A warning is worthless here: the deployment is
	// silently unauthenticated for every one of those decisions, the operator
	// cannot observe the weakness from the running cluster, and the only
	// remediation is re-issuing certificates — something that must happen
	// before the cluster carries data, not after an incident. Refusing to start
	// is also cheap to fix and impossible to miss.
	if err := h.verifyMemberCertificateExclusivity(members); err != nil {
		return err
	}

	// With the certificates known to be per-node, every member must be
	// admitted with the identity that certificate proves. Checked after the
	// SAN rule so a deployment with both defects is told about the one it has
	// to re-issue certificates for first.
	if cfg.RequireMemberIdentities {
		if err := h.validateMemberIdentities(cfg); err != nil {
			return err
		}
	}

	// Resolve this shard's at-rest barrier before anything is written for it.
	//
	// Both failures below are invisible once the shard is running: the log and
	// the snapshots keep being written, just not under the key the operator
	// configured, and every observable signal (the snapshot directory, the
	// absence of errors) says encryption is on. They are also decidable here,
	// where the shardID that selects the barrier first exists, and nothing has
	// been persisted yet.
	//
	//  1. The resolver returned no barrier and there is no host-wide fallback:
	//     this tenant's shard would be written in plaintext alone.
	//  2. The resolved barrier is sealed: nothing for this shard can be
	//     encrypted, and no other route may stand in for it.
	//  3. The LogDB holds this shard's records under some OTHER barrier: the
	//     shard is encrypted, under the wrong tenant's key.
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

	// Hold the shard's import lock for the whole start, before its durable
	// state is read. An import records a snapshot and removes the log beneath
	// it; one that passed its stopped-shard check and is still writing would
	// otherwise commit underneath the node this call is about to build from
	// the OLD log, leaving raft, the log reader, the record and the compaction
	// floor mutually inconsistent. The import re-checks that the shard is not
	// running once it holds this lock, so it sees this start and refuses. It
	// is only TRIED: a start must not queue behind a multi-gigabyte restore,
	// and the caller retries the identical call.
	// Another start of this shard is in progress. It may still fail, so this
	// is not ErrShardAlreadyExists — a caller that reads that as "running"
	// would never start the shard — but the transient ErrShardNotReady: the
	// retry sees the outcome, a running shard or a free one.
	if _, starting := h.startingShards.LoadOrStore(cfg.ShardID, struct{}{}); starting {
		return ErrShardNotReady
	}
	defer h.startingShards.Delete(cfg.ShardID)
	releaseImportLock, importIdle := h.tryAcquireImportLock(cfg.ShardID)
	if !importIdle {
		return ErrImportInProgress
	}
	defer releaseImportLock()

	// A replica ID RemoveData retired never runs again (ReplicaRetiredError).
	// Checked under the shard's import lock, which RemoveData takes too, so a
	// concurrent RemoveData either retired the ID before this check or finds
	// this start published once the lock is released — never one in between
	// that would run a replica whose term, vote and owed vote window are gone.
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

	// Initialize the LogReader's membership with the bootstrap members
	// so the Raft state machine knows about all cluster members for
	// quorum calculations. Without this, each node would think it's a
	// single-node cluster and immediately self-elect. On recovery the
	// membership was already restored from the persisted snapshot above,
	// but applying the bootstrap config on top is safe because the Raft
	// layer reconciles membership from committed config-change entries.
	//
	// For joining nodes (join=true) we also set the membership so that
	// quorum requires more than just the joining node itself. Without this,
	// a joining node with empty membership trivially wins its own election
	// (0 quorum needed) before the existing leader can contact it. Setting
	// the known members here prevents self-election: the joining node needs
	// votes from the existing members, which it cannot get in term 0/1
	// since those members are already in a higher term. The leader's first
	// AppendEntries will update the joining node's term and make it a
	// follower. The membership is later updated to the full cluster config
	// when a snapshot is applied (handleSnapshot).
	//
	// CRITICAL: only seed when recovery restored NOTHING. Membership is
	// replicated state; the recovered snapshot is authoritative and must
	// never be overwritten by the caller's bootstrap map. Config changes
	// at or below the snapshot index are compacted out of the log, and an
	// on-disk state machine's processed marker is seeded to its own
	// lastApplied (see SetProcessedIndex below), so commit-time replay
	// cannot reconstruct them. Overwriting here would drop witnesses,
	// observers and Removed tombstones and silently revert the REPLICATED
	// witness policy to this node's local config — a restarted replica
	// would then compute quorum over a smaller voting set than its peers
	// and two leaders could be elected in the same term.
	recoveredState, recovered := lr.NodeState()
	nothingRecovered := recovered.ConfigChangeID == 0 &&
		len(recovered.Addresses) == 0 &&
		len(recovered.Observers) == 0 &&
		len(recovered.Witnesses) == 0

	// Config-in-log recovery. A restart recovered committed log entries but no
	// snapshot carried the membership (nothingRecovered is true because ReadState
	// returns a membership only alongside a snapshot record). Reconstruct
	// membership from the committed log so both the address routing below and the
	// Raft peer come up with the DURABLE configuration, NOT a re-seed from the
	// caller's members map — the property that makes membership a function of the
	// log across restarts. A genuine first start (no persisted entries) skips
	// this and falls through to the bootstrap seed.
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

	// A witness or an observer must never be seeded through the members map —
	// but ONLY when this call is actually BOOTSTRAPPING the shard.
	//
	// members seeds proto.Membership.Addresses — the FULL VOTER set — and
	// membership is REPLICATED state: every peer seeds the same replica from
	// its own copy of the map, so this replica cannot hold a different role for
	// itself than its peers hold for it. The local Config.IsWitness /
	// Config.IsObserver bit would then contradict the configuration the whole
	// shard agrees on: the leader sends this replica FULL entry payloads
	// (sendAppend, not sendWitnessAppend), it counts as a full replica for the
	// RequireFullReplicaMajority policy and toward every quorum, and the only
	// witness-like behaviour left is this host's local read gate refusing the
	// reads the operator asked for. raft.checkSelfRole latches that
	// disagreement permanently for a witness.
	//
	// Fail closed. Seeding the local node into Membership.Witnesses instead
	// would be worse — a replica-local membership that no peer shares — and
	// suppressing the latch would hide a real misconfiguration. The supported
	// flow is two-step (as for etcd/dragonboat learners): bootstrap the voters,
	// then start this replica with join=true and add it with RequestAddWitness
	// / RequestAddObserver so the role enters membership through a committed
	// config change.
	//
	// # Why this is gated on firstStart
	//
	// The refusal must apply to a BOOTSTRAP and to nothing else. Refusing on a
	// restart left a legitimately-added witness with NO working restart:
	// join=false with itself listed hit this error, join=false without itself
	// hit SelfNotInMembersError, and the only form that returned nil — join=true
	// — set the joining marker, which is cleared in exactly ONE place (the
	// InstallSnapshot adopt branch in raft). A caught-up witness receives
	// Replicate and never InstallSnapshot, so the marker became PERMANENT for
	// the process: Peer.IsMember reports true for every replica ID (per-shard
	// message authorization off), the handler skips SourceAddress binding, and
	// Peer.Tick returns early — no election timer and no CheckQuorum. The gate
	// meant to prevent a misconfiguration was itself producing a far worse one.
	//
	// The discriminator is PERSISTED STATE, not recovered membership. Membership
	// is written only alongside a snapshot record (both LogDB backends set it
	// inside the u.Snapshot.Index > 0 branch), so a caught-up but never
	// snapshotted witness — the common case, since a witness stores no entry
	// payloads and a young shard may never reach SnapshotEntries — recovers no
	// membership and looked exactly like a bootstrap. It hit the refusal, and
	// with it every one of the outcomes above.
	//
	// A restart whose membership WAS recovered is additionally protected by the
	// seeding gate below: the map cannot make this replica a voter because the
	// map is not applied at all. A restart WITHOUT a snapshot has no such
	// protection — the map is still the only membership it has — so the seed
	// does place this replica, and every other non-voter the operator listed,
	// in Addresses. That placement is PROVISIONAL and is repaired by the
	// committed AddWitness / AddNonVoting entries, which are necessarily still
	// in the log (config changes are compacted only behind a snapshot, and a
	// snapshot would have restored the membership instead) and are re-applied
	// at commit time on restart.
	//
	// The repair is the business of raft.membership.apply, which CONVERGES on
	// the role each committed entry names instead of refusing an entry whose
	// target is already a member — see the "Convergent application" comment in
	// internal/raft/membership.go. Nothing here may rely on checkSelfRole for
	// it: that compares only the WITNESS bit, so it does not see an observer
	// seeded as a voter at all, and it is a latch, not a repair — it would
	// leave the membership itself wrong on every replica that restarted this
	// way, and a snapshot taken afterwards would make it durable.
	//
	// Refusing to start instead is not an option: it has no remedy at all.
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

	// A joiner on its FIRST start holds no configuration, so it must be given
	// the replicas it may take its bootstrap snapshot from. An established
	// replica restarted with join=true (a supported convention: its
	// configuration is durable and it needs no bootstrap) is judged on what it
	// recovered, and needs no anchor.
	// The anchor must name a replica OTHER than this one: its own entry is
	// checked by validateMemberIdentities but vouches for no sender. A
	// members map that names other replicas seeds them into the joiner's
	// configuration with their identities and serves the same purpose; one
	// that names only the joiner itself does not, and a joiner with neither
	// would start cleanly and refuse every message and the bootstrap
	// snapshot as unknown_member, forever.
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
		// The members map is about to seed the configuration; every founder
		// in it needs its identity. Asked here and not earlier because on a
		// restart the map is ignored and must not be held to a standard the
		// recovered configuration has since moved past.
		if cfg.RequireMemberIdentities {
			if err := requireFoundersIdentified(cfg, nodesCopy); err != nil {
				return err
			}
		}
		// Seed the replicated witness policy from this node's config. This
		// is the ONLY point where config.WitnessConfig enters the shard:
		// from here on the policy lives in replicated membership state
		// (snapshot + log) and changes only via UpdateWitnessPolicy config
		// changes, so every replica enforces the same policy regardless of
		// local configuration.
		// MemberIdentities is seeded the same way and for the same reason: it is
		// replicated state from here on, and every founder must supply an
		// IDENTICAL map or their index-1 entries diverge (Membership marshals
		// with sorted keys, so identical input gives identical bytes). Behind a
		// transport validateMemberIdentities has already required an entry for
		// every founder; a transportless host may supply none, since nothing
		// crosses the wire there.
		// The session policy is seeded here for exactly the same reason as the
		// witness policy, and this is likewise the ONLY point where
		// config.Config's session limits enter the shard. From here on they
		// live in replicated membership state and the apply path reads them
		// from there, so every replica expires the same sessions at the same
		// log index and admits the same registrations regardless of what any
		// one node is configured with. Founders must agree on them for the
		// same reason they must agree on the witness policy: a differing value
		// produces a divergent index-1 entry.
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

		// Config-in-log bootstrap: a FOUNDING member (join=false) lays the
		// initial configuration down as a committed EntryInitialMembership at
		// index 1, so membership becomes a function of the durable log. On
		// restart it is reconstructed by replaying that entry (no more re-seeding
		// from local config), and a later joiner learns it by ordinary
		// replication of the same entry, ending the joining window
		// deterministically instead of waiting for a snapshot.
		//
		// Every founding member builds a BYTE-IDENTICAL entry from the same
		// bootstrap input (Membership marshals with sorted keys), which is what
		// makes it safe to commit at index 1 on each independently — the etcd
		// bootstrap model. That input is the members map AND the witness policy
		// (MaxWitnesses/RequireFullReplicaMajority, from this node's local
		// Config.Witness): founders configured with a different policy produce a
		// divergent index-1 entry that Log Matching never reconciles, so all
		// founders must be configured identically (see BuildInitialMembershipEntry).
		// A join=true replica must NOT write its own index-1 entry: it would
		// conflict with the authoritative one the leader replicates. It keeps only
		// the in-memory seed and adopts the cluster's entry on arrival.
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

	// Address routing must follow the EFFECTIVE membership, not the
	// bootstrap map: after a restart the recovered membership may contain
	// observers and witnesses the caller never listed, and the transport
	// cannot resolve a replica it has no address for.
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
		// A member recorded without an address (possible for entries that
		// predate address tracking) falls back to the caller's map.
		for id, addr := range nodesCopy {
			if _, ok := effectiveNodes[id]; !ok {
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
	// The read lease is measured in real time, so the peer must know how
	// long the ticks that its election timeout is counted in really are.
	peer.SetTickInterval(h.engine.TickInterval())
	// A joining node starts with incomplete membership. Mark it so
	// per-shard message auth is bypassed until a snapshot delivers
	// the full cluster membership. The Host keeps its own marker: the
	// snapshot receive path needs it to authorize the bootstrap snapshot
	// against the sender's certificate instead of an address registry that
	// is necessarily empty here.
	if join {
		peer.MarkJoining()
	}

	// Create the managed state machine (RSM) from the factory.
	//
	// The on-disk state machine opens its persistent store at snapshotDir and
	// the engine stages snapshot artifacts beneath it. By default this roots
	// under the host NodeHostDir, preserving the historical layout. When the
	// shard supplies an explicit cfg.DataDir, root the shard's on-disk
	// state-machine data there instead so a per-tenant shard can live on the
	// tenant's own volume (dragonboat's "the on-disk SM owns its storage
	// location" behavior). The Raft log/WAL is unaffected: it always remains
	// in the shared host-wide LogDB.
	snapshotDir := shardSnapshotBaseDir(h.cfg.NodeHostDir, cfg.DataDir, cfg.ShardID)

	// Reclaim decrypt scratch files orphaned by a crash BEFORE anything else
	// touches the shard's snapshot tree. A leader killed mid-snapshot-send (or
	// mid-recovery) leaves a COMPLETE PLAINTEXT COPY of the state machine beside
	// the ciphertext; the snapshot directory is retained indefinitely, so without
	// a sweep here the plaintext survives restarts and backups while snapshot.dat
	// next to it still reads as ciphertext. This runs on EVERY start, including
	// the on-disk-state-machine path below that skips snapshot recovery entirely
	// and the case where no snapshot record exists at all.
	engine.SweepStaleDecryptTmpTree(snapshotDir, cfg.ShardID, cfg.ReplicaID)

	// The replica's own snapshot directory is where every plaintext scratch
	// file is created (a send's and a recovery's), so it exists for as long
	// as the replica is loaded. It is created here, once, and never by a
	// scratch writer: a late writer from a stopped incarnation must fail on a
	// removed tree, not recreate it.
	if err := h.snapshotFsys().MkdirAll(engine.ReplicaSnapshotDir(snapshotDir, cfg.ShardID, cfg.ReplicaID), 0o750); err != nil {
		return &HostInitError{Field: "snapshot_dir", Err: err}
	}

	// Reclaim an import staging directory orphaned by a crash. ImportSnapshot
	// writes the artifact PLAINTEXT and re-encrypts it immediately afterwards;
	// a process killed between the two leaves the complete plaintext state
	// machine under a name NOTHING else on the host enumerates (that invisibility
	// is what keeps a half-written import out of snapshot GC and the DEK
	// purge-floor scan), so without this sweep the exposure is permanent.
	//
	// A live staging directory is indistinguishable from an orphaned one by
	// name, so the sweep runs only where no import can be in flight: this
	// call holds the shard's import lock for the whole start (above).
	engine.SweepStaleImportStageDirs(snapshotDir, cfg.ShardID, cfg.ReplicaID)

	// Reclaim an InstallSnapshot receive staging directory orphaned by a crash.
	// A receive removes its own on every error return, so only a kill leaves
	// one, and nothing else ever will: the name cannot parse as
	// "snapshot-<index>", which is exactly what keeps a half-written transfer
	// out of snapshot GC and the DEK purge-floor scan.
	//
	// No lock is needed and none would help. A receive needs the shard's engine
	// node to route to, and this runs before that node exists, so the shard
	// provably has no receive in flight — which is why the receive staging name
	// is distinct from the import one, whose sweeper reclaims under the import
	// lock while the shard is running.
	engine.SweepStaleRecvStageDirs(snapshotDir, cfg.ShardID, cfg.ReplicaID)
	// A local save stages the same way, under its own prefix, and the same
	// argument holds: no node, no save that could still publish.
	engine.SweepStaleSaveStageDirs(snapshotDir, cfg.ShardID, cfg.ReplicaID)

	// The state machine reads the shard's REPLICATED session policy, and falls
	// back to this node's configuration only while none is known — a shard
	// bootstrapped before the policy was replicated, or a join=true replica
	// that has not yet received the shard's configuration. peer.SessionPolicy
	// reads the peer's published membership, which is already seeded here
	// (bootstrap or recovery) and is updated by every later adoption path, so
	// a replica that learns its configuration late converges without a
	// restart.
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

	// Open acquires persistent resources (e.g. a Pebble file lock for an on-disk
	// SM). Close them on EVERY failure path until the node is successfully
	// published into the engine, so a failed StartShard does not leak the open
	// store and brick subsequent retries on the same DataDir with "lock held by
	// current process". Once published (smPublished=true, set after LoadNode),
	// the node's lifecycle — including rsm.Close — is owned by StopShard/Close,
	// and every error path between publication and LoadNode removes the
	// nodeState first, so this never double-closes.
	smPublished := false
	defer func() {
		if !smPublished {
			if cerr := stateMachine.Close(); cerr != nil {
				h.logger.Warn("state machine close failed during StartShard rollback",
					"shard_id", cfg.ShardID, "error", cerr)
			}
		}
	}()

	// recoveredIndex is the highest log index whose effects are durably
	// reflected in the state machine after Open()/recovery. It is used to
	// seed the applied markers below so reads can be served immediately on
	// restart without waiting for a new write. For a fresh start it is 0.
	// recoveredTerm is the Raft term of recoveredIndex (from the recovered
	// snapshot record); it seeds lastAppliedTerm so a snapshot taken right
	// after recovery — before any apply — records a consistent (index, term).
	var recoveredIndex uint64
	var recoveredTerm uint64

	// Recover SM state from a persisted snapshot on restart.
	//
	// There are three cases, distinguished by SM kind and Open()'s result:
	//
	//  1. On-disk SM whose Open() returns lastApplied > 0. Its database is
	//     authoritative and already reflects every applied entry. The local
	//     LogDB snapshot record sits at an OLDER index (an on-disk SM keeps
	//     applying past its last snapshot), so calling RecoverFromSnapshot
	//     would REPLACE its database files and roll it back to the snapshot
	//     index — permanent data loss for entries between the snapshot and
	//     lastApplied. Open() is the sole source of recovered state.
	//
	//  2. On-disk SM whose Open() returns 0 AND a local LogDB snapshot record
	//     exists (ss.Index > 0 && ss.Filepath != ""). This is the
	//     import/bootstrap case: ImportSnapshot wrote snapshot.dat plus a
	//     LogDB snapshot record, expecting StartShard to reconstruct the
	//     on-disk database from it. The SM came up empty (no DB files), so it
	//     is safe — and required — to call RecoverFromSnapshot exactly once.
	//     Without this the SM stays empty while raft believes a snapshot
	//     exists at ss.Index: cross-host restore data loss.
	//
	//  3. In-memory SM. It always starts empty and must have its snapshot
	//     data restored. The raft processed index is initialized to the
	//     snapshot index (in newRaftLog), so entries after the snapshot are
	//     re-applied by the engine.
	//
	//  4. On-disk SM whose Open() returns 0 < lastApplied < ss.Index. The
	//     database is BEHIND the local snapshot record: the replica installed
	//     a snapshot streamed from the leader and stopped before its database
	//     recovered it, or the database lost writes it had already
	//     snapshotted. The entries between lastApplied and ss.Index may be
	//     compacted out of the log and survive only in snapshot.dat, so
	//     seeding the markers from Open() leaves a hole beneath the first entry
	//     the log can offer (ss.Index+1) — refused as ErrApplyGap by the apply
	//     worker, or with no resolvable term for lastApplied at all. The
	//     snapshot is AHEAD of the database, so recovering from it rolls the
	//     database forward, not back: it is case 2, not case 1 (#280).
	//
	// Cases 2, 3 and 4 share the same recovery mechanics, so they are handled
	// by the same local-snapshot path below. Only case 1 skips it.
	onDiskRecovered := stateMachine.IsOnDisk() && lastApplied > 0
	if onDiskRecovered && h.logdb != nil {
		ss, ssErr := h.logdb.GetSnapshot(cfg.ShardID, cfg.ReplicaID)
		if ssErr == nil && ss.Index > lastApplied && ss.Filepath != "" {
			onDiskRecovered = false
		}
	}
	if onDiskRecovered {
		// On-disk SM with durable state: Open() is the sole source of the
		// INDEX, but it knows nothing about raft terms, so the term has to be
		// resolved from the log or the snapshot record. Seeding 0 is not an
		// option: the pair (recoveredIndex, 0) reaches the next snapshot record,
		// and a follower that installs a snapshot with Term 0 has lastTerm() ==
		// 0, which makes isUpToDate accept EVERY candidate — including one whose
		// log is missing entries this replica already applied. That is a Leader
		// Completeness violation, so an unresolvable term fails StartShard.
		// The term is resolved AFTER peer.SetProcessedIndex clamps this index
		// below, not here: the clamp exists to repair an on-disk SM whose
		// applied position outruns the recovered HardState.Commit (a lost state
		// fsync), and resolving against the unclamped value both pairs a term
		// with an index nothing carries and fails a node the clamp would fix.
		recoveredIndex = lastApplied
	} else if h.logdb != nil {
		ss, ssErr := h.logdb.GetSnapshot(cfg.ShardID, cfg.ReplicaID)
		if ssErr == nil && ss.Index > 0 && ss.Filepath != "" {
			dataPath := filepath.Join(ss.Filepath, "snapshot.dat")

			// CONSUMER 2 of 3: verify the artifact against the integrity
			// fields in its snapshot.meta before the state machine sees it.
			// Same check the runtime recovery path applies, on the same
			// as-written bytes, and it must be here too: a shard recovered at
			// STARTUP takes this path, not snapshotPool.recoverSnapshot.
			// Wiring one consumer and not the others reproduces exactly the
			// state the removed Checksum field was in — an integrity control
			// that looks present and is not.
			//
			// A missing snapshot.dat is reported as snapshot_open FIRST, the
			// same answer the open below gives: a directory lost whole has no
			// snapshot.meta either, and the missing artifact is the fact an
			// operator can act on. A missing or unusable snapshot.meta beside
			// a PRESENT artifact fails the integrity check closed.
			if _, statErr := h.snapshotFsys().Stat(dataPath); os.IsNotExist(statErr) {
				return &HostInitError{Field: "snapshot_open", Err: statErr}
			}
			if err := engine.VerifySnapshotArtifact(ss.Filepath, dataPath); err != nil {
				return &HostInitError{Field: "snapshot_integrity", Err: err}
			}

			// And against the FileSize the record claims. The meta hash lives
			// beside the artifact; FileSize lives in LogDB, so it still holds
			// when snapshot.dat and snapshot.meta are replaced together — and
			// without it a snapshot.dat truncated after it was recorded
			// recovers as a SHORTER state machine rather than as an error: the
			// replica comes up at the snapshot's index holding a prefix of its
			// state, diverged from every peer at the same log position with
			// every Raft invariant intact.
			if err := engine.VerifySnapshotFileSize(dataPath, ss.FileSize); err != nil {
				return &HostInitError{Field: "snapshot_integrity", Err: err}
			}

			// snapshot.dat is stored ENCRYPTED whenever the record carries a
			// non-zero epoch, so it must be decrypted before the state machine
			// sees it — feeding it the raw frames either fails recovery or
			// installs corrupt state. Decryption goes to an anonymous scratch
			// file in the replica's own snapshot directory, unlinked before
			// the first plaintext byte is written and read back through its
			// handle, leaving the canonical encrypted artifact untouched and
			// no named plaintext at any crash point; this mirrors the runtime
			// path (snapshotPool.recoverSnapshot) exactly.
			//
			// The barrier is resolved PER SHARD: with WithShardBarriers the
			// snapshot was encrypted under the shard's own key, and the
			// host-wide barrier would be the wrong key.
			//
			// An encrypted snapshot with no usable barrier fails closed. A node
			// that cannot decrypt its own snapshot must say so at startup
			// rather than recover a state machine from ciphertext.
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
					// A snapshot record exists in LogDB but its data file is
					// missing or unreadable. We must NOT silently continue: the
					// raft layer initializes the processed index to the snapshot
					// index, so skipping recovery would bring the state machine up
					// empty and it would never replay the pre-snapshot entries
					// (they are compacted away). This is fatal for BOTH the
					// in-memory path and the on-disk bootstrap path. Fail StartShard.
					return &HostInitError{Field: "snapshot_open", Err: openErr}
				}
				f = plain
			}
			// The reader chain must mirror the chain the ARTIFACT was written
			// through, which the snapshot record carries. Deriving it from
			// cfg instead reads the artifact wrong whenever the setting has
			// changed since the save, or whenever the snapshot was streamed
			// by a leader configured differently. Decompression sits INSIDE
			// decryption: the save path compresses first and encrypts the
			// resulting file, so recovery decrypts first (above) and
			// decompresses here.
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
			// Seed to the snapshot's RAFT LOG INDEX (ss.Index), not its
			// OnDiskIndex. recoveredIndex is the raft processed marker (and
			// the host/engine applied markers below), all of which live in the
			// raft log-index namespace. newRaftLog already initializes
			// processed = ss.Index, and the runtime recovery path
			// (snapshotPool.recoverSnapshot) sets lastApplied/lastSnapshotIndex
			// to logdbSnap.Index — so ss.Index keeps every marker consistent
			// and the engine replays exactly the entries after the snapshot.
			// OnDiskIndex is the SM's own internal applied counter, which
			// RecoverFromSnapshot reconstructs inside the SM; it is not a raft
			// log index and must not seed these markers.
			recoveredIndex = ss.Index
			recoveredTerm = ss.Term
		}
	}

	// For on-disk SMs that recovered durable state via Open(), set the raft
	// peer's processed marker to lastApplied so the engine does not wastefully
	// re-apply entries the SM has already persisted. For the on-disk bootstrap
	// path (case 2) and the in-memory path (case 3), newRaftLog already
	// initialized processed to ss.Index, which equals recoveredIndex; calling
	// SetProcessedIndex(recoveredIndex) there is a consistent no-op-equivalent
	// that keeps the marker explicit. A fresh start leaves processed at 0.
	if recoveredIndex > 0 {
		// Adopt the value raft actually applied. It clamps to the recovered
		// commit index, and the engine markers below MUST agree with it —
		// seeding lastApplied above raft's processed makes the first Commit
		// trip commitUpdate's assertion and take the commit worker down.
		//
		// The error is the fold's FATAL case: a committed config entry in the
		// window this start is skipping does not decode. Abort rather than start.
		// Those bytes are replicated state every peer can read and this binary
		// cannot, so a replica that comes up anyway is computing quorum over a
		// configuration it knows is wrong, and will persist it with its next
		// snapshot. Failing StartShard is also the STRONGEST fail-closed answer
		// available at this point and the one that matches the two paths that see
		// the same input: the commit pipeline faults the shard
		// (processConfigChanges -> failCommit) and membership reconstruction
		// aborts this same call (reconstructShardMembership). Nothing has been
		// published yet — the nodeState is not in h.nodes and the engine node does
		// not exist — so returning here leaves no half-started shard behind, which
		// is why it is an abort and not an Engine.FailShard.
		var setErr error
		recoveredIndex, setErr = peer.SetProcessedIndex(recoveredIndex)
		h.logAppliedClamp(cfg.ShardID, cfg.ReplicaID, peer.AppliedClamp())
		h.logUnfoldedConfig(cfg.ShardID, cfg.ReplicaID, peer.UnfoldedConfig())
		if setErr != nil {
			return &HostInitError{Field: "config_replay", Err: setErr}
		}

		// Resolve the term for the CLAMPED index. The snapshot branches above
		// already paired recoveredTerm with recoveredIndex; the on-disk branch
		// has only an index, and seeding term 0 there stamps it on the next
		// snapshot record — which makes a follower installing that snapshot
		// treat every candidate as up to date (see resolveRecoveredTerm).
		if onDiskRecovered && recoveredIndex > 0 {
			term, termErr := h.resolveRecoveredTerm(lr, cfg.ShardID, cfg.ReplicaID, recoveredIndex)
			if termErr != nil {
				return &HostInitError{Field: "recovered_term", Err: termErr}
			}
			recoveredTerm = term
		}
	}

	// ns.nodes is the shard's MUTABLE routing table, guarded by ns.nodesMu and
	// written by the apply worker (applyMembershipRouting on committed membership
	// entries — the bootstrap entry fires this on the first apply of every shard).
	// It must be a COPY of effectiveNodes, not the same map: StartShard keeps
	// reading effectiveNodes without the lock after LoadNode makes the node live
	// (the registry-registration loop below), so aliasing them races the apply
	// worker's first write.
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

	// Seed the host-side applied marker to the recovered index so that
	// FollowerRead and the OnReadyToRead/OnApplied read-completion gates
	// see lastApplied == raft's processed marker immediately on restart.
	// Without this, a freshly-restarted node with no new writes leaves
	// ns.lastApplied at 0 (OnApplied never fires when there are no entries
	// above processed), so reads poll to their deadline. Stored before the
	// nodeState is published via LoadOrStore so it is visible before any
	// read is served. Harmless when recoveredIndex == 0.
	if recoveredIndex > 0 {
		ns.lastApplied.Store(recoveredIndex)
	}

	// Seed the config-change staleness guard from the membership the peer came
	// up with. ns.configChangeIndex is otherwise advanced ONLY by the
	// incremental OnApplied path when a fresh EntryConfigChange applies; a
	// restart (or a snapshot-adopted / log-reconstructed membership) reflects
	// already-applied config changes WITHOUT re-firing OnApplied, so a fresh
	// nodeState would start at 0. A restarted LEADER would then reject every
	// RequestAddNode(..., N) with StaleConfigChangeError{Expected: 0, Got: N}
	// permanently. peer.GetMembership().ConfigChangeID is the log index of the
	// last applied config change — the exact value SyncGetShardMembership
	// reports and callers pass back — so seeding it makes the guard round-trip.
	if pm := peer.GetMembership(); pm.ConfigChangeID > 0 {
		advanceConfigChangeIndex(ns, pm.ConfigChangeID)
	}

	// Atomically store the nodeState, detecting concurrent StartShard
	// calls for the same shardID.
	if _, loaded := h.nodes.LoadOrStore(cfg.ShardID, ns); loaded {
		return ErrShardAlreadyExists
	}
	h.activeNodes.Add(1)
	// The receive gate's host-side state for this shard — the join window and
	// the operator anchor — is recorded only once the shard is PUBLISHED, so
	// a concurrent StartShard that loses the race above, or a start that
	// fails before this point, leaves nothing behind for a loaded shard to
	// vouch with. The raft-side join flag (peer.MarkJoining) is on the peer
	// this call owns and needs no such care.
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
			// Record where this shard's on-disk tree is rooted. Operations
			// that run while the shard is STOPPED — Host.ImportSnapshot — have
			// no other way to discover it, and writing an artifact outside the
			// tree costs the artifact's DEK at the next purge.
			DataDir: cfg.DataDir,
		}
		if err := h.logdb.SaveBootstrap(cfg.ShardID, cfg.ReplicaID, bootstrap); err != nil {
			// Mark the nodeState stopped BEFORE removing it from h.nodes
			// (matching StopShard/Close ordering): a reader that resolved ns
			// just before the Delete must fail its stopped check rather than
			// call into the state machine the deferred rollback is about to
			// close.
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
	// Seed the engine node's applied/snapshot markers to the recovered
	// index so they match raft's processed marker before the node enters
	// the pipeline. Mirrors the runtime recover path in snapshot_pool.go,
	// which sets node.lastApplied/lastSnapshotIndex after RecoverFromSnapshot.
	engNode.SeedLastApplied(recoveredIndex, recoveredTerm)
	// Seed the STATE MACHINE's own applied marker to the same index. It is a
	// second, independent counter (rsm.StateMachine.lastApplied) and neither
	// RecoverFromSnapshot nor the on-disk Open path above sets it to the value
	// raft actually resumes from, so without this it stays 0 on every restart
	// while raft and the engine node resume at recoveredIndex.
	//
	// The counter is not bookkeeping: it is the LOWER BOUND of the session
	// expiry checkpoint walk in rsm.Apply. A restarted replica starting that
	// walk from 0 replays every checkpoint in (0, recoveredIndex] that its
	// peers already ran, and ExpireSessions is capped at
	// defaultMaxExpirePerCheckpoint deletions per call — so the replay is NOT
	// idempotent and deletes sessions the rest of the shard still holds. From
	// then on this replica answers ResultSessionExpired for entries its peers
	// route into the user state machine: permanent divergence, with every raft
	// invariant intact and nothing logged. It is also the index the next
	// snapshot save is stamped with, which is why the runtime InstallSnapshot
	// path (engine snapshotPool.recoverSnapshot) sets it for the same reason.
	//
	// recoveredIndex is the CLAMPED value peer.SetProcessedIndex returned, so
	// the state machine's marker, the engine node's and raft's processed marker
	// all name the same entry and the contiguity guard in rsm.Apply holds from
	// the first batch.
	stateMachine.SetLastApplied(recoveredIndex)
	h.engine.LoadNode(engNode)
	// The node is now live in the engine; StopShard/Close own its rsm.Close from
	// here, so the StartShard rollback close must not also run.
	smPublished = true

	// Close-vs-Start TOCTOU guard. The entry check above can be passed
	// microseconds before Host.Close sets closed and runs its h.nodes sweep,
	// and Close stops the engine BEFORE that sweep. A shard published into
	// that window is invisible to the sweep and permanently inert: it sits in
	// h.nodes with activeNodes incremented, inside a stopped engine, holding
	// an open state machine nobody will ever close — whose file lock bricks
	// the next host start on the same DataDir. Because both flags are set
	// before the sweep reads h.nodes, a sweep that missed this shard is
	// guaranteed to be visible here (the same reasoning proposeEntry,
	// ReadIndex, RequestSnapshot and requestConfigChange rely on).
	//
	// ns.stopped covers the same window for a per-shard StopShard, which sets
	// no host-wide flag. A StopShard that lands between the h.nodes publication
	// above and LoadNode finds the engine holding NOTHING for the shard, so its
	// UnloadNode is a no-op: it closes the rsm and deletes the nodeState, and
	// this call then hands the live engine a node backed by an ALREADY-CLOSED
	// state machine, absent from h.nodes and therefore unremovable by any later
	// StopShard/Close, with its registry entries orphaned — and returns nil.
	//
	// Losing the ns.stopped CAS means a concurrent Close/StopShard claimed
	// teardown and owns it; this call must not tear the shard down a second
	// time. It must still UNDO its own publication into the engine, because the
	// claimant's UnloadNode ran before LoadNode and could not have removed it.
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

// logRecoveryClamp reports, on the Host's logger, a recovered commit index the
// raft log clamped down to what the recovered log actually holds (see
// raft.RecoveryClamp). The raft package records the clamp instead of logging
// it so the record reaches the sink the operator configured through WithLogger
// or LogConfig. Contained: the logger is user code and must not be able to
// fail or stall StartShard.
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

// logAppliedClamp reports, on the Host's logger, a state machine applied index
// that raft clamped down to the recovered commit index (see raft.AppliedClamp).
// Like logRecoveryClamp it reaches the sink configured through WithLogger, and
// the sink is contained so a faulty handler cannot fail or stall StartShard.
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

// logUnfoldedConfig reports, on the Host's logger, committed config changes
// below the state machine's applied index that SetProcessedIndex could not
// fold (see raft.UnfoldedConfig). Like logRecoveryClamp it reaches the sink
// configured through WithLogger, contained so a faulty handler cannot fail or
// stall StartShard.
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

	// Atomically claim teardown: only the goroutine that flips stopped from
	// false to true performs UnloadNode/rsm.Close/Delete and decrements
	// activeNodes. Without this, two concurrent StopShard calls (or a StopShard
	// racing Close) for the same shard both pass the existence check above and
	// both decrement activeNodes — driving it negative. The counter is later
	// used as a slice capacity (GetNodeHostInfo, HealthCheck), where a negative
	// value panics with "makeslice: cap out of range", and the shard would be
	// double-unloaded and its state machine closed twice (e.g. a Pebble
	// close-after-close).
	if !ns.stopped.CompareAndSwap(false, true) {
		return ErrShardNotFound // another StopShard/Close already owns teardown
	}
	// Wake ExecuteOnLeader callers waiting on this shard's leadership view;
	// they find it stopped and return ErrStopped.
	ns.signalLeaderChange()
	// The state machine's Close error is the shard's LAST durability signal:
	// for an on-disk SM it is the backing store's final flush-and-close, and a
	// failed flush that is only logged reports a clean stop for a shard that
	// lost writes. Teardown itself is unconditional and complete either way —
	// the shard is unloaded, its pending requests failed and its nodeState
	// removed — so the error is carried out rather than aborting the rest.
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

// teardownShard releases every resource a loaded shard holds. The caller
// MUST have claimed teardown by winning ns.stopped's CompareAndSwap, which
// makes this run exactly once per shard incarnation: StopShard, Host.Close,
// and the StartShard close-race rollback all funnel through it, so none of
// them can double-unload a node, double-close a state machine, or drive
// activeNodes negative.
//
// pendingErr is the error delivered to the shard's still-pending proposals,
// reads, and snapshots.
//
// It returns the USER STATE MACHINE's Close error and nothing else. Every other
// step here is unconditional cleanup whose failure has a local remedy; the
// state machine's Close is the embedding application's last chance to learn
// that its store did not flush, so it is returned rather than logged away. The
// teardown always completes: an error return does not mean the shard is still
// loaded.
func (h *Host) teardownShard(ns *nodeState, shardID uint64, pendingErr error) error {
	ns.health.Store(uint32(ShardUnloaded))

	// Signal cancellation to any in-progress DiskStateMachine.Open()
	// or other operations blocking on the open stopper channel.
	ns.openStopOnce.Do(func() { close(ns.openStopper) })

	// Unload the engine node FIRST. This stops the node's participation
	// in the step/commit/apply pipeline and signals the apply worker to
	// close the per-shard channel. The nodeState must remain in h.nodes
	// during this phase so that any in-flight OnApplied callbacks can
	// still locate it and complete proposals normally rather than
	// discarding results.
	h.engine.UnloadNode(shardID)

	// Fail all remaining pending proposals, reads, and snapshots. Any
	// items completed by OnApplied during the drain are already removed
	// from these maps, so only truly orphaned items receive the error.
	h.failPendingRequests(ns, pendingErr)

	// Clean up the registry and the join window BEFORE removing the nodeState
	// from h.nodes. While ns is still present, a concurrent StartShard of the
	// same shardID is turned away with ErrShardAlreadyExists, so it cannot
	// re-register member addresses that this teardown would then wipe. Doing
	// the removal AFTER h.nodes.Delete opened exactly that window: the restart
	// pre-registers the members, and the still-running teardown's registry
	// removal black-holes the leader's Replicate traffic to them.
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

	// Close the state machine to release any resources it holds (e.g.,
	// Pebble file locks). Without this a stopped shard leaves its
	// DiskStateMachine open and a subsequent StartShard on the same data
	// directory fails with "lock held by current process".
	var smCloseErr error
	if ns.rsm != nil {
		if err := ns.rsm.Close(); err != nil {
			smCloseErr = err
			h.logger.Warn("state machine close failed during shard teardown",
				"shard_id", shardID, "error", err)
		}
		// Do NOT nil ns.rsm: a concurrent reader (SyncRead/StaleRead/etc.)
		// may still hold this nodeState and read the field. The node is
		// removed from h.nodes and ns.stopped is set, so no new reader can
		// reach it and existing readers gate on ns.stopped. Writing the
		// field here would be a data race (mirrors Host.Close).
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

// SyncRead performs a linearizable read in a single call. It confirms
// leadership via ReadIndex, waits for the local SM to catch up, then
// executes the query. This provides the strongest consistency guarantee:
// the read is guaranteed to reflect all writes committed before it.
//
// The flow is:
//  1. Submit ReadIndex to confirm leadership via heartbeat quorum
//  2. Wait for the RequestState to resolve (lastApplied >= readIndex)
//  3. Execute SM.Lookup(query) on the now-caught-up state machine
//
// A read the leader cannot serve yet (a new leader that has not committed
// an entry of its term, a leadership transfer in progress, an overloaded
// leader, a role that no longer permits the read) fails fast with
// ErrReadIndexNotReady; see ReadIndex. A forwarded ReadIndex or response
// lost in transit is bounded only by ctx, so callers must pass a deadline.
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

// SyncReadBuf is the pooled-buffer variant of SyncRead for []byte key queries.
// It performs a linearizable read and returns the result in a buffer acquired
// from an internal pool. The caller MUST call release() when done with result,
// after which result MUST NOT be used.
//
// Use SyncReadBuf on hot paths where the result is immediately consumed (e.g.,
// serialized into a response) to eliminate per-call heap allocation. For
// queries that are not []byte or where the underlying SM does not implement
// sm.NALookupInto, the method falls back to a standard allocation.
//
// Example:
//
//	result, release, err := host.SyncReadBuf(ctx, shardID, key)
//	if err != nil { return err }
//	defer release()
//	// use result before returning
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

// StaleReadBuf is the pooled-buffer variant of StaleRead for []byte key queries.
// It reads local SM state without any consensus check; the result may be stale.
// The caller MUST call release() when done with result, after which result MUST
// NOT be used.
//
// See SyncReadBuf for usage details and the pooling contract.
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
	// A context that is ALREADY done must not submit work. Without this the
	// caller's outcome is decided by a select over two ready cases — the
	// result channel and ctx.Done() — which Go resolves uniformly at random,
	// so SyncPropose returned success or ErrCanceled by coin flip whenever the
	// proposal committed before the cancellation was observed. That is how
	// TestSyncPropose_CancelReturnsCanceled failed on a loaded CI runner while
	// passing 40 consecutive local runs.
	//
	// Failing here also means an abandoned request never reaches the log at
	// all. Cancellation racing an in-flight proposal is still inherently
	// undecidable — ErrCanceled never proved the entry did not commit — but
	// that is a different case from one the caller cancelled before asking.
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

	// An empty command is legal and is proposed normally, matching
	// dragonboat (proposalShard.propose types a zero-length cmd as a plain
	// application entry). It commits and advances lastApplied, but the
	// apply path skips empty-Cmd non-session entries before they reach the
	// user state machine — the same treatment the leader's election no-op
	// gets. That makes it a commit barrier: the way to force a
	// current-term committed entry after an election (PhD 6.4 ReadIndex
	// warmup) without inventing a throwaway payload.

	// Witnesses vote but don't store log data; they cannot propose.
	if ns.cfg.IsWitness {
		return nil, ErrInvalidOperation
	}

	// Reject proposals when the WAL disk is full to prevent unbounded
	// log growth and potential data loss.
	if h.diskFull != nil && h.diskFull.Load() {
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

	// Validate entry size. A proposal must not exceed EITHER the
	// MaxProposalPayloadSize (user-facing limit) or MaxEntrySize
	// (transport/storage limit). Use the effective minimum of whichever
	// limits are configured.
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

// ProposeWithSession submits a session-managed proposal with at-most-once
// delivery semantics. The session must have been obtained via GetNewSession
// and must be in the normal proposal state (after registration completes).
//
// The caller MUST call session.ProposalCompleted() after receiving a
// successful result via RequestState.Result() or RequestState.ResultC().
// Failing to do so will cause the session's seriesID to become stale,
// preventing further proposals.
//
// On error, the session state is unchanged and the proposal may be retried
// with the same session.
//
// A session serves ONE proposal at a time. The entry is stamped with the
// session's current series number, and the state machine treats every entry
// with the same (ClientID, SeriesID) as a retransmission of one command —
// which is what makes the retry above safe, and what makes a second
// CONCURRENT proposal on the same session unsafe: only one of the two would
// be applied and both would report success. The series is therefore reserved
// for the returned RequestState, and a second call while it is held returns
// ErrSessionBusy. The reservation is released by ProposalCompleted, by an
// error result, or by RequestState.Release, whichever comes first.
//
// It returns ErrSessionsUnsupportedOnDisk when the shard's state machine is an
// sm.DiskStateMachine and the session is session-managed, for the reason given
// on that error. A NO-OP session (Host.GetNoOPSession) is refused with
// ErrInvalidSession on any shard that gets as far as session validation (a
// closed or draining host, an unknown shard and a witness shard fail earlier
// with their own errors): it carries no session state, so a caller holding
// one proposes through Propose or SyncPropose instead.
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

	// A session-managed proposal on an on-disk state machine is refused HERE,
	// before the entry exists, because the alternative is refusing it at apply
	// time on one replica only. A no-op session carries no session state and is
	// left to ValidForProposal below, which has always rejected it.
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
	if h.diskFull != nil && h.diskFull.Load() {
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

	// Validate entry size. A proposal must not exceed EITHER the
	// MaxProposalPayloadSize (user-facing limit) or MaxEntrySize
	// (transport/storage limit). Use the effective minimum of whichever
	// limits are configured.
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

	// Reserve the series before the entry is built from it, so that the
	// number stamped on the entry is the one this request holds. (A NO-OP
	// session never reaches here: ValidForProposal refused it above.) From
	// here the reservation belongs to proposeEntry: it releases on every
	// failure of its own, before or after the request is bound, so there is
	// exactly one release per reservation — a second one here would clear
	// the reservation a concurrent caller took in between.
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

// SyncProposeWithSession blocks until a session-managed proposal is applied.
// On success, session.ProposalCompleted() is called automatically before
// returning, advancing the session for the next proposal.
//
// On error, the session state is unchanged and the proposal may be retried
// with the same session. It returns ErrSessionsUnsupportedOnDisk on a shard
// backed by an sm.DiskStateMachine (see ProposeWithSession).
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

// proposeEntry is the shared proposal pipeline used by Propose,
// ProposeWithSession, and proposeSession. It handles rate limiting,
// engine node validation, key generation, pending map registration,
// delivery, and queue-full cleanup.
// pendingDeadline returns the deadline to record for a pending request issued
// under ctx. A context WITHOUT a deadline is clamped to
// defaultPendingRequestTimeout from now rather than recorded as the zero time,
// which is what makes the entry sweepable: see defaultPendingRequestTimeout.
func pendingDeadline(ctx context.Context) time.Time {
	return pendingDeadlineWithin(ctx, defaultPendingRequestTimeout)
}

// pendingDeadlineWithin is pendingDeadline with an explicit backstop, for
// request classes whose healthy duration is not bounded by the commit path.
// A caller-supplied deadline always wins: the backstop only fills in for a
// context that carries none.
func pendingDeadlineWithin(ctx context.Context, backstop time.Duration) time.Time {
	if deadline, ok := ctx.Deadline(); ok {
		return deadline
	}
	return time.Now().Add(backstop)
}

// nextRequestKey mints the key a pending request -- a proposal, a read, a
// snapshot -- is tracked under: a random 64-bit value, never zero.
//
// entry.Key is how an applied entry is matched back to the pending
// RequestState that proposed it (hostEngineCallback.OnApplied), and the entry
// carries the key through the raft log to every replica, so the key must not
// repeat across HOSTS -- replica B, holding key 7 for its own request, must
// not complete that caller with replica A's unrelated entry 7 -- and must not
// repeat across INCARNATIONS of one host either. A (replicaID, per-Host
// sequence) key would be unique across hosts by construction, but a restarted
// replica -- a new Host under the same replica ID, its sequence back at zero --
// would mint exactly the keys its previous incarnation had left in the log. A
// snapshot-less restart replays that log through OnApplied, and a replayed old
// entry would complete the fresh proposal registered under its key with the
// old entry's result. A counter cannot be made incarnation-unique without
// persisting something or narrowing the replica half; a random draw needs
// neither, and it is what dragonboat does (keyGenerator.nextKey is
// rand.Uint64). Uniqueness is probabilistic at 2^-64 per pair of keys, the guarantee a UUID gives, over a
// population that is only ever the requests currently pending. The draw comes
// from math/rand/v2's process-wide generator, which is safe for concurrent
// use and seeded from the OS; it is not secret and need not be.
//
// Zero is reserved: an entry with Key 0 is one nothing is waiting on
// (OnCommitted skips it), so a drawn zero is drawn again.
func nextRequestKey() uint64 {
	for {
		if key := mathrand.Uint64(); key != 0 {
			return key
		}
	}
}

func (h *Host) proposeEntry(ctx context.Context, ns *nodeState, shardID uint64, entry proto.Entry, session *Session) (rs *RequestState, err error) {
	// The caller reserved entry.SeriesID on session (nil for a sessionless
	// proposal) and this function owns releasing it on failure. Until the
	// request is bound (bindSession below) nothing else can release it, so a
	// failure before that point releases here; after it, the failure paths
	// call rs.Release, which releases through the binding — and a release for
	// a series the session has already moved past is a no-op, so the two can
	// never double-release.
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

	// Refuse immediately once the shard failure latch has tripped.
	//
	// The latch is terminal: an SM.Apply error or a state-machine panic takes
	// the node offline and the step worker skips it from then on (see
	// engine.Engine.FailShard / Node.IsFailed). Without this gate the proposal is accepted,
	// registered in the pending map, delivered to an inbox nothing drains, and
	// answered only when the sweeper reclaims it at its deadline -- so every
	// call after the failure blocks for its full timeout and reports what looks
	// like a slow shard. shardHealth already reads IsFailed and reports
	// ShardError for exactly this state; the propose path is where a caller
	// finds out without polling health.
	//
	// This is a refusal, not a retry hint: ErrShardFailed is deliberately not
	// in temporaryErrors, because the shard stays failed until it is reloaded
	// or recovered and a retry loop against it would spin forever.
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

	// Generate a unique proposal key for tracking this request through
	// the commit/apply pipeline. The key is set on the entry before
	// delivery and preserved through the raft log, so the OnApplied
	// callback can match applied entries back to pending RequestStates.
	proposalKey := nextRequestKey()
	entry.Key = proposalKey

	// Create request state with the deadline from the context, clamped to
	// defaultPendingRequestTimeout when the caller supplied none so the sweeper
	// can always reclaim this entry.
	deadline := pendingDeadline(ctx)
	rs = newRequestState(ns.requestPool, proposalKey, deadline, h.metrics != nil)
	// Bound to the session BEFORE the request is registered: from the moment
	// it is in the pending map a completion can run, and an error completion
	// is what releases the session's series. Binding after registration left
	// a window where that release found nothing to release.
	if session != nil {
		rs.bindSession(session, entry.SeriesID)
		bound = true
	}

	// Register the proposal in the pending map BEFORE delivering the
	// message to the inbox. This ensures that if the step worker
	// processes and applies the entry extremely fast, the OnApplied
	// callback will find the RS.
	pp := &pendingProposal{
		rs:       rs,
		deadline: deadline,
		floor:    engNode.CommittedIndex(),
	}
	if err := h.registerPendingProposal(ns, engNode, proposalKey, pp); err != nil {
		return nil, err
	}

	// Deliver the proposal to the MPSC proposal queue. The step worker
	// is the only goroutine that touches the Raft state machine, so
	// proposals must be routed through the queue to respect the
	// single-owner invariant on the Peer. Using the dedicated proposal
	// queue avoids channel serialization under high concurrent load.
	if !engNode.DeliverTrackedProposal([]proto.Entry{entry}, &pp.enqueuedAt) {
		// Queue full — remove the RS and let the caller retry. The entry
		// must be CLAIMED with LoadAndDelete: the sweep and
		// failPendingRequests also remove entries with claims, and an
		// unconditional Delete pairing with a sweep that already completed
		// this key would double releaseRef (driving outstanding negative —
		// the RS is permanently lost from the pool) and double
		// completePendingOp (undercounting pendingOps so Drain can close
		// early). Only the claimant rolls back the map reference;
		// rs.Release() is the caller's own reference and is dropped
		// unconditionally.
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
		// ErrDropped, not ErrSystemBusy. ErrSystemBusy is documented as the
		// GLOBAL in-memory log limit (MaxTotalInMemLogSize) and is returned by
		// the rate-limiter check at the top of this function; reporting it
		// here told the caller a limit had been reached that had not, and sent
		// an operator to the wrong metric. This path is a per-shard proposal
		// QUEUE overflow — the condition ErrDropped is documented for, the
		// condition IncProposalDropped counts, and the condition
		// OnProposalDropped announces. Both classify as Temporary, so retry
		// loops are unaffected.
		return nil, ErrDropped
	}

	// Wake the step worker so it processes the proposal queue.
	h.engine.NotifyWork(shardID)

	return rs, nil
}

// registerPendingProposal tracks pp for drain notification and registers it
// under key in ns's pending proposals, and then re-checks the terminal states
// whose drain of that map may already have run. It returns nil once the
// proposal is registered; otherwise the registration has been rolled back,
// the caller's own reference on pp.rs dropped, and the error is the refusal
// to return. Every request that registers in pendingProposals — proposals,
// session operations and config changes — comes through here.
//
// The re-check runs AFTER the store, for the two terminal states that can land
// between the caller's admission checks and the store and would orphan the RS
// in a map nothing drains again — a deadline-less caller would block on
// rs.Result() forever, and one with a deadline would wait it out:
//
//   - Close/stop: the host's pending-request sweeps may have run first.
//   - The failure latch: OnShardFailed may have drained the map first, and a
//     halted shard never completes anything it registers.
//
// Either the registration observes the state or the drain observes the
// entry; there is no interleaving in which neither happens. LoadAndDelete
// makes the rollback exactly-once relative to a concurrent drain. acquireRef
// pins the RS until this map entry is removed, even if the caller releases
// first (e.g. a Sync* timeout).
func (h *Host) registerPendingProposal(ns *nodeState, engNode *engine.Node, key uint64, pp *pendingProposal) error {
	h.addPendingOp()
	pp.node = engNode
	pp.rs.acquireRef()
	// From here a completion or the caller's own wait can fail it, and both
	// classify the failure from this entry's hand-off marker
	// (proposalMayReachRaft, through failClaimedProposal and
	// RequestState.lostWait). The rollback below returns its refusal directly,
	// and that stays definite.
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

// ReadIndex submits a ReadIndex request to confirm linearizability.
// The returned RequestState resolves when the local SM has applied
// up to the confirmed commit index. The result's Value field carries
// the confirmed read index.
//
// Witnesses cannot serve reads (they don't store log data).
// Observers and followers forward the ReadIndex to the leader.
//
// A read that cannot be served yet resolves with ErrReadIndexNotReady
// rather than waiting: on the local replica when it has not seen an entry
// of the current term commit, and on a follower or observer when the
// leader refuses the forwarded read (it has not yet committed an entry of
// its term, a leadership transfer is in progress, it is overloaded, or the
// replica's role no longer permits the read). Retry after a short backoff.
// A forwarded ReadIndex or its response that is lost in transit is not
// answered by anything; that wait is bounded only by ctx.
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
	// A sealed replica serves no linearizable read: the barrier that sealed
	// is the one its log is written under, so it can neither persist nor
	// prove the state the read would be answered from (see
	// nodeState.barrierSealed). SyncRead and SyncReadBuf come through here.
	if ns.barrierSealed() {
		return nil, ErrBarrierSealed
	}

	// Verify the engine node and Peer exist.
	engNode := h.engine.GetNode(shardID)
	if engNode == nil || engNode.Peer() == nil {
		return nil, ErrShardNotReady
	}

	// Refuse a read against a halted shard, exactly as the propose path does.
	//
	// A linearizable read completes only when lastApplied reaches its read
	// index (the OnApplied scan), and a shard whose failure latch has tripped
	// never applies again. Without this gate the read would be accepted,
	// registered, confirmed by a leader that is still perfectly healthy, and
	// then wait for an apply that cannot come — returning at the caller's
	// deadline with ErrTimeout, which points at consensus, the network or a
	// lost leader and not at the Apply that failed earlier, for a read the
	// replica knew immediately it could not serve. SyncRead and SyncReadBuf both come through here, so one gate
	// covers every read.
	//
	// ErrShardFailed is a refusal, not a retry hint: it is deliberately not in
	// temporaryErrors, because the shard stays failed until reloaded.
	if engNode.IsFailed() {
		return nil, ErrShardFailed
	}

	// Generate a unique key for correlating this read request through
	// the raft pipeline. The key is carried in the ReadIndex message's
	// Hint field and returned in ReadyToRead.Key. Uses a dedicated
	// counter separate from proposals and snapshots to avoid contention.
	readKey := nextRequestKey()

	deadline := pendingDeadline(ctx)
	rs := newRequestState(ns.requestPool, readKey, deadline, h.metrics != nil)

	// Track this operation for drain notification.
	h.addPendingOp()

	// Register the pending read BEFORE delivering the message to avoid
	// a race where OnReadyToRead fires before the map entry exists.
	// acquireRef pins the RS until this map entry is removed.
	rs.acquireRef()
	ns.pendingReadsMu.Lock()
	if ns.pendingReads == nil {
		ns.pendingReads = make(map[uint64]*pendingRead)
	}
	ns.pendingReads[readKey] = &pendingRead{rs: rs}
	ns.pendingReadsMu.Unlock()

	// TOCTOU guard for the two TERMINAL states that can land between the
	// checks at the top of ReadIndex and the store above. Either would orphan
	// the RS in a map nothing will ever drain again — a deadline-less caller
	// then blocks on rs.Result() forever, and one with a deadline pays all of
	// it. Re-check after registering and roll back. The delete-under-mutex
	// claim makes the rollback exactly-once relative to a concurrent drain
	// (mirrors proposeEntry's LoadAndDelete pattern).
	//
	//   - Close/stop: the host's pending-request sweeps may have run between
	//     the closed check and the store.
	//   - Failure latch: OnShardFailed may have scanned pendingReads between
	//     the IsFailed check and the store, found it empty, and returned. The
	//     failed node stops processing its queue, so no later callback ever
	//     completes this read. Re-checking here means
	//     either the registration observed the latch or the drain observed
	//     the entry; there is no interleaving in which neither happens.
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

	// Deliver the ReadIndex key to the MPSC ReadIndex queue. The step
	// worker drains all accumulated keys and submits a single batched
	// ReadIndex operation, coalescing N concurrent callers into 1
	// heartbeat quorum round. This is the primary performance
	// optimization for multi-node ReadIndex workloads.
	if !engNode.DeliverReadIndex(readKey) {
		// Claim the entry before rolling back. failPendingRequests and
		// sweepPendingReads remove entries under the same mutex; an
		// unconditional delete here could pair with a concurrent completion
		// that already removed and released this entry, double-counting
		// releaseRef (driving outstanding negative, so tryRecycle's
		// outstanding==0 check never holds again and the object leaks out of
		// the pool) and completePendingOp (undercounting pendingOps so Drain
		// can close drainC with work still in flight). rs.Release() is the
		// caller's own reference and is dropped unconditionally.
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

// QueryLocalNode reads this replica's state machine via SM.Lookup, bypassing
// Raft entirely. No consensus check of any kind is performed: the local SM is
// read as-is, so the result is NOT linearizable and carries no recency
// guarantee. For linearizable reads use SyncRead; for a read that at least
// reflects everything committed when it started, use FollowerRead.
//
// This is the implementation shared with StaleRead, which is the same
// operation named for its consistency rather than for the bypass. Callers
// choosing between them should pick StaleRead unless they specifically mean
// "read this replica's SM directly"; QueryLocalNode exists for API
// compatibility with dragonboat's NodeHost, where it was the original name.
func (h *Host) QueryLocalNode(ctx context.Context, shardID uint64, query interface{}) (interface{}, error) {
	// Reads honour an already-cancelled context before touching the state
	// machine, so cancellation is a strict "do not execute this read"
	// boundary rather than a suggestion. Without this a cancelled read still
	// reached the SM and returned data — and on the byte-key path the SM's
	// Lookup takes no context at all, so nothing downstream could refuse it.
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

// StaleRead reads this replica's state machine directly, without any
// consensus check. It is an eventually-consistent read: the result reflects
// whatever this replica has applied so far and may be arbitrarily stale — a
// partitioned or lagging replica can serve data from an old term indefinitely,
// and reads issued in sequence are not guaranteed to be monotonic. Use it only
// where staleness is acceptable. For linearizable reads use SyncRead.
//
// StaleRead is the preferred name for this operation because it states the
// guarantee (or lack of one) at the call site. It is exactly QueryLocalNode,
// which names the same operation after its mechanism, and delegates to it so
// the two can never drift apart.
func (h *Host) StaleRead(ctx context.Context, shardID uint64, query interface{}) (interface{}, error) {
	return h.QueryLocalNode(ctx, shardID, query)
}

// FollowerRead reads from the local state machine after ensuring the local
// applied index has caught up to the committed index. This allows any node
// (leader or follower) to serve reads without a ReadIndex round-trip,
// providing a consistency guarantee stronger than StaleRead: the result
// reflects all entries committed at the time the read was initiated.
//
// The read waits until lastApplied >= committedIndex. If the node is
// significantly behind, this may block until the SM catches up or the
// context expires.
//
// Compared to SyncRead, FollowerRead does not confirm leadership via a
// heartbeat quorum, so it may return slightly stale data during a network
// partition (at most the data that was committed before the read). For
// strict linearizability, use SyncRead.
func (h *Host) FollowerRead(ctx context.Context, shardID uint64, query interface{}) (interface{}, error) {
	// Reads honour an already-cancelled context before touching the state
	// machine, so cancellation is a strict "do not execute this read"
	// boundary rather than a suggestion. Without this a cancelled read still
	// reached the SM and returned data — and on the byte-key path the SM's
	// Lookup takes no context at all, so nothing downstream could refuse it.
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

	// A halted shard never applies again, so lastApplied is frozen below
	// committed and the catch-up wait below can never end. Refuse before
	// entering it, the way ReadIndex does: this loop checks only stop/close,
	// so without the gate a follower read across an SM.Apply failure would
	// wait for its deadline -- or, with context.Background(), forever.
	if engNode.IsFailed() {
		return nil, ErrShardFailed
	}

	committed := engNode.CommittedIndex()

	// Fast path: SM has already applied up to the committed index.
	if ns.lastApplied.Load() >= committed {
		return ns.rsm.Lookup(ctx, query)
	}

	// Slow path: poll until lastApplied catches up, the shard or host shuts
	// down, or the context expires. Shutdown must terminate the loop on its
	// own: a caller passing a deadline-less context on a partitioned follower
	// would otherwise spin at 1000Hz forever and Host.Close could not
	// interrupt it. The interval backs off after the initial burst so a long
	// catch-up costs a handful of wakeups per second instead of a thousand.
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

// ErrSessionsUnsupportedOnDisk is returned by every client-session entry point
// on a shard whose state machine is an sm.DiskStateMachine. Propose there
// without a session (Propose, SyncPropose) and make the operations idempotent,
// which is the same constraint dragonboat places on IOnDiskStateMachine through
// its NO-OP session.
//
// The client session table is REPLICATED STATE that lives only inside the
// snapshot stream: rsm.StateMachine serializes it ahead of the user state
// machine's bytes and reads it back in RecoverFromSnapshot. An on-disk state
// machine never takes that path on restart — its Open() returns a durable
// applied index, and StartShard therefore SKIPS RecoverFromSnapshot entirely
// because recovering would roll the user's database back to the snapshot index
// — so the replica comes up with the correct data and an EMPTY session table.
// Everything that follows is silent divergence: a session-managed proposal is
// routed into the user state machine by the peers that still hold the session
// and answered with ResultSessionExpired by the restarted replica, which never
// applies the write at all. The client sees success because the leader
// completed the proposal from its own apply.
//
// The rejection is deliberately a LOCAL, API-boundary decision. It reads only
// the state machine's own type — fixed for the life of the shard by the
// sm.CreateFunc the caller passed to StartShard — and never replicated state,
// so it cannot be evaluated differently on two replicas, and it takes effect
// before the entry is anywhere near the log.
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

// GetNewSession creates a new client session for at-most-once proposal
// semantics. The session registration is proposed through Raft consensus
// so all replicas register the session. Blocks until the registration
// is committed and applied, or ctx is cancelled.
//
// It returns ErrSessionsUnsupportedOnDisk when the shard's state machine is an
// sm.DiskStateMachine: an on-disk state machine cannot carry the session table
// across a restart, so a registered session would silently stop existing on any
// replica that restarts. Propose without a session (Propose, SyncPropose) on
// those shards and make the operations idempotent.
//
// The session declares no owner, so it is exempt from the shard's per-owner
// session cap. Use GetNewSessionWithOwner to declare one.
func (h *Host) GetNewSession(ctx context.Context, shardID uint64) (*Session, error) {
	return h.GetNewSessionWithOwner(ctx, shardID, 0)
}

// GetNewSessionWithOwner creates a new client session that declares ownerID as
// the principal holding it, so it counts against — and is limited by — the
// shard's replicated per-owner session cap
// (config.Config.MaxSessionsPerOwner). It is otherwise identical to
// GetNewSession, including the generated client ID.
//
// Registration fails with a *SessionError wrapping ErrInvalidOperation when the
// owner is already at the cap, which is the same failure a shard-wide
// exhaustion produces: the apply path refuses the registration, returns an
// empty result, and the mismatch against the expected client ID is what this
// call observes. It is a REPLICATED refusal — every replica reaches it from the
// log alone — not a leader-local admission decision.
//
// An ownerID of zero declares no owner and is exactly GetNewSession. See
// NewSessionWithOwner for what belongs in this value and why the cap is only as
// trustworthy as the embedder's own authentication.
func (h *Host) GetNewSessionWithOwner(
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

// clientIDRerollLimit bounds the re-roll below. Eight draws of a value that is
// wrong with probability 2^-64 cannot plausibly all fail; exhausting them means
// the entropy source is returning a constant, which must be an error and not a
// silent loop.
const clientIDRerollLimit = 8

// newClientID draws a session client ID from the system CSPRNG, rejecting zero.
//
// Zero is proto.NotSessionManagedClientID — the value that marks an entry as
// carrying NO session. A session minted with it is structurally a no-op
// session, and the failure is silent in both directions: the registration entry
// fails Entry.IsNewSessionRequest, so the state machine skips it entirely and
// returns an empty result, and the caller's own check is
// `result.Value != clientID`, which 0 != 0 satisfies. GetNewSession therefore
// returned SUCCESS and handed back a session whose every later proposal is
// unsessioned — at-most-once delivery silently off for that client, which is
// the one guarantee the API exists to provide. RegisterSession already rejects
// a zero client ID; this closes the same hole on the minting side.
// entropy is a parameter rather than a direct crypto/rand call so the rejection
// can be tested: a value the code exists to refuse cannot be produced by the
// real CSPRNG within the age of the universe, and a test that cannot produce it
// asserts nothing.
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

// ErrEntropyDegenerate is returned when the system CSPRNG produced the same
// rejected value on every attempt, which means it is not producing entropy at
// all. Continuing would mint predictable identifiers, so the operation fails.
var ErrEntropyDegenerate = errors.New(
	"quicraft: the system random source returned a degenerate value repeatedly")

// RegisterSession registers the given session through Raft consensus,
// preserving its existing client ID. Unlike GetNewSession, which generates
// a new random client ID, RegisterSession uses the client ID already set
// on the provided session. This is used by adapters that create sessions
// with caller-specified client IDs (e.g., the dragonboat compatibility
// layer).
//
// The session must have a non-zero client ID and must belong to the
// specified shard. On success the session is prepared for normal proposals.
//
// It returns ErrSessionsUnsupportedOnDisk on a shard backed by an
// sm.DiskStateMachine, for the reason given on that error.
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

// CloseSession closes a client session by proposing unregistration through
// Raft consensus. Returns a RequestState that resolves when the
// unregistration is committed and applied.
//
// It returns ErrSessionsUnsupportedOnDisk on a shard backed by an
// sm.DiskStateMachine, for the reason given on that error. Nothing can have
// registered a session there, so there is nothing to close.
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

// proposeSession proposes a session register or unregister operation through
// Raft consensus. The session's SeriesID must already be set to the
// appropriate sentinel value via PrepareForRegister/PrepareForUnregister.
//
// The pattern mirrors Propose(): generate a key, register in pendingProposals,
// create a proto.Entry with the session fields, deliver via engine inbox,
// and return a RequestState the caller can wait on.
func (h *Host) proposeSession(ctx context.Context, ns *nodeState, session *Session) (*RequestState, error) {
	// Same already-cancelled guard as Propose. Without it a
	// cancelled GetNewSession/RegisterSession/CloseSession still appends its
	// session entry: a session the caller discarded gets registered, a retry
	// fails as a duplicate, or a session the caller believes is live is
	// unregistered — all while the caller was told ErrCanceled.
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

	// Build a session entry: empty Cmd, session fields populated.
	//
	// OwnerID is stamped here, at propose time, which is the only place it can
	// enter replicated state. The apply path reads it back out of the committed
	// entry to decide the per-owner session cap, so it has to be a fact the log
	// carries rather than one this node knows — a replica replaying this entry
	// after a restart has no connection, no caller and no Session object, and
	// must still reach the same verdict. See proto.Entry.OwnerID.
	//
	// It rides on unregister entries too, harmlessly: the apply path reads
	// OwnerID only when registering, and the session's own recorded owner (not
	// this field) is what releases the slot on unregister.
	entry := proto.Entry{
		ClientID:    session.ClientID(),
		SeriesID:    session.SeriesID(),
		RespondedTo: session.RespondedTo(),
		OwnerID:     session.OwnerID(),
	}
	return h.proposeEntry(ctx, ns, session.ShardID(), entry, nil)
}

// GetNoOPSession returns a no-op session for proposals that do not require
// at-most-once semantics. The no-op session has a fixed clientID of 0 and
// carries no session state. Use for idempotent operations only.
//
// A no-op session is not accepted by ProposeWithSession, which refuses it with
// ErrInvalidSession; the operation it stands for is a sessionless Propose or
// SyncPropose. It exists for callers that model a NO-OP session explicitly
// and route it to the sessionless path themselves. On a shard backed by an sm.DiskStateMachine
// that sessionless path is the only way to propose: every session-managed entry
// point returns ErrSessionsUnsupportedOnDisk there.
func (h *Host) GetNoOPSession(shardID uint64) *Session {
	return NewNoOPSession(shardID)
}

// SyncGetShardMembership returns the committed membership of a shard after
// confirming leadership via ReadIndex. This provides a linearizable view of
// the membership configuration: all config changes committed before this call
// are guaranteed to be reflected in the result.
//
// The returned Membership includes the ConfigChangeID that produced it,
// which callers should pass to RequestAddNode / RequestRemoveNode to detect
// stale membership and serialize concurrent config changes.
//
// The context must carry a deadline or timeout to bound the read.
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

// GetShardMembership returns the current membership of a shard from local
// in-memory state. The result may be stale if a config change has been
// committed but not yet observed by this host. For a linearizable view,
// use SyncGetShardMembership.
func (h *Host) GetShardMembership(shardID uint64) (*Membership, error) {
	if h.closed.Load() {
		return nil, ErrClosed
	}
	return h.getMembershipFromPeer(shardID)
}

// getMembershipFromPeer reads the committed membership from the engine node's
// Raft peer and converts it to the public Membership type. The Peer's
// GetMembership method returns deep copies of all maps, so this is safe to
// call from any goroutine.
func (h *Host) getMembershipFromPeer(shardID uint64) (*Membership, error) {
	engNode := h.engine.GetNode(shardID)
	if engNode == nil || engNode.Peer() == nil {
		return nil, ErrShardNotReady
	}

	pm := engNode.Peer().GetMembership()
	return protoMembershipToPublic(&pm), nil
}

// configSessionPolicy reads this node's LOCAL session limits out of a shard
// Config. It is used for two things and no others: seeding the shard's
// replicated policy at bootstrap, and as the state machine's fallback while
// the shard has no replicated policy to read.
//
// Config.SetDefaults has necessarily run by the time this is called (StartShard
// runs it before validation), so MaxTotalSessions is non-zero and the result
// reports IsSet true — which is what makes it usable as a seed. Its
// SessionExpiryEntries may legitimately be zero: that is the operator asking
// for expiry to be disabled, and once seeded it is a replicated, unambiguous
// "disabled" rather than an absent policy.
func configSessionPolicy(cfg config.Config) proto.SessionPolicy {
	return proto.SessionPolicy{
		ExpiryEntries:        cfg.SessionExpiryEntries,
		MaxTotalSessions:     cfg.MaxTotalSessions,
		MaxSessionsPerClient: cfg.MaxSessionsPerClient,
		MaxSessionsPerOwner:  cfg.MaxSessionsPerOwner,
	}
}

// protoMembershipToPublic converts a proto.Membership to the public
// Membership type. The proto type uses map[uint64]bool for Removed,
// while the public type uses map[uint64]struct{} for memory efficiency.
// All maps are copied to prevent aliasing.
func protoMembershipToPublic(pm *proto.Membership) *Membership {
	m := &Membership{
		ConfigChangeID:             pm.ConfigChangeID,
		MaxWitnesses:               pm.MaxWitnesses,
		RequireFullReplicaMajority: pm.RequireFullReplicaMajority,
		SessionExpiryEntries:       pm.SessionExpiryEntries,
		MaxTotalSessions:           pm.MaxTotalSessions,
		MaxSessionsPerClient:       pm.MaxSessionsPerClient,
		MaxSessionsPerOwner:        pm.MaxSessionsPerOwner,
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

// GetShardInfo returns a point-in-time snapshot of the shard's status.
func (h *Host) GetShardInfo(shardID uint64) (*ShardInfo, error) {
	if h.closed.Load() {
		return nil, ErrClosed
	}

	ns, err := h.getNode(shardID)
	if err != nil {
		return nil, err
	}

	return h.buildShardInfo(ns), nil
}

// GetLeaderID returns the current leader's replicaID for the given shard
// and whether a leader is known. The result is from local in-memory state
// and may be stale.
func (h *Host) GetLeaderID(shardID uint64) (uint64, bool, error) {
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

// GetLeaderIDAndTerm returns the current leader's replicaID, the current term,
// whether a leader is known, and any error for the given shard. The pair is
// read through the nodeState's seqlock, so it is a combination that actually
// held — unlike calling GetLeaderID and GetShardInfo separately, where an
// intervening leader update can produce a (leader, term) pair that never
// existed. Both values are still local in-memory state and may be STALE: this
// eliminates torn pairs, not staleness.
func (h *Host) GetLeaderIDAndTerm(shardID uint64) (uint64, uint64, bool, error) {
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

// ShardReadReady reports, without blocking, whether a ReadIndex issued on this
// replica now would get past Raft's read gate: whether the shard's leader for
// the returned term has committed an entry of that term (§6.4 of the Raft
// thesis), so the read is not refused with ErrReadIndexNotReady. It returns the
// leader and term the answer is about, read together with it as one
// combination that held.
//
// It answers from the state the OnLeaderReady and OnLeaderUpdated events
// delivered, so a caller that installs the listeners and then queries cannot
// miss an edge: if the query says not ready, the OnLeaderReady that changes
// that is still to come. Like GetLeaderIDAndTerm it may be STALE — the
// leader can have lost the term already, which the next OnLeaderUpdated
// reports and which a ReadIndex then answers with its own error.
//
// The refusals ReadIndex makes before reaching Raft are returned as the same
// errors: ErrClosed, ErrDraining, an unknown shard, ErrInvalidOperation for a
// witness (it cannot serve reads), ErrShardNotReady before the replica is
// running, and ErrShardFailed once its failure latch has tripped.
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

// RequestLeaderTransfer requests leadership transfer to a specific replica.
// targetReplicaID MUST name a current voting replica. There is no automatic
// target selection: passing 0 does NOT transfer to the most up-to-date
// follower, it returns *InvalidMemberError. A removed replica, an observer
// and a witness are each rejected with *InvalidMemberError, and a replica
// this shard has never seen with *ReplicaNotFoundError. Naming the current
// leader is an idempotent no-op success.
//
// This is otherwise fire-and-forget: once the target validates, the method
// returns immediately and the transfer completes asynchronously via the raft
// state machine. The context is checked for cancellation before delivery.
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

	// Pre-validate the transfer target. The raft layer validates too, but
	// its error surfaces asynchronously in the step worker where it can
	// only be logged — returning nil here for an invalid target would
	// falsely report success to the caller.
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

// PauseShard stops the local replica of shardID without unloading it, so a
// runtime seal can withdraw the shard's barrier key with no log, apply or
// snapshot work of the shard in flight. The replica keeps its log, state
// machine, membership and persisted raft state; ResumeShard restarts it.
//
// While paused the replica:
//
//   - refuses every proposal, session operation, membership change,
//     ExecuteOnLeader, leader transfer, snapshot request, compaction and
//     read (ReadIndex, SyncRead, LeaseRead, StaleRead, follower reads) with
//     ErrShardPaused, which is Temporary;
//   - neither campaigns, votes, replicates nor acknowledges: its election
//     clock stops and every inbound message — proposals and reads forwarded
//     by peers included — is discarded;
//   - applies nothing, including committed entries already handed to the
//     apply goroutine; they apply in log order after ResumeShard;
//   - takes, sends, receives, imports and recovers no snapshot. A due
//     snapshot or recovery runs after ResumeShard, and a refused recovery is
//     never counted toward the shard-failure limit.
//
// A proposal accepted before the pause is never applied while paused: it
// completes after ResumeShard, ends at its ctx deadline, or returns the
// maybe-applied NotLeaderError if leadership moves.
//
// A leader first hands leadership to the reachable voter with the highest
// replicated index, through the RequestLeaderTransfer path, and waits up to
// one election timeout for it to take over. If no voter is reachable or the
// transfer does not finish in time, the pause proceeds and the old leader
// stops heartbeating; the remaining voters elect a leader of their own.
//
// PauseShard returns nil once none of the shard's log, apply or snapshot
// work is in flight. If ctx ends first it returns ctx.Err() and the shard
// STAYS paused: the in-flight work settles on its own, and calling
// PauseShard again waits for it. Pausing a paused shard is idempotent. A
// pause is not persisted: a restarted replica starts unpaused. An unknown
// shard returns ErrShardNotFound; a shard that stops while the pause waits
// returns ErrClosed.
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

	// Refuse new work first, so nothing the host admits from here on can
	// race the pause below. The gate is taken without ctx: a ctx error must
	// mean the shard is paused, and no holder keeps it longer than a
	// leadership transfer.
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

// transferBeforePause hands a leader's shard to the reachable voter with the
// highest replicated index and waits, bounded by ctx and one election
// timeout, for this replica to step down. With no voter to hand to it waits
// out the timeout; PauseShard proceeds either way. The engine node is not paused
// yet, so the step worker still runs the transfer.
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

// pauseGate serializes one shard's pause and resume transitions: the host's
// admission flag, the leadership transfer and the engine node's flag change
// as one step, so a ResumeShard cannot run between a PauseShard's flag and
// its engine pause and leave the two disagreeing. The wait for in-flight
// work is outside it. Each shard has its own gate. The zero value is ready.
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

// ResumeShard restarts a replica PauseShard paused. Call it after the
// shard's barrier is unsealed. The replica rejoins as a follower at its
// current term with a fresh election timeout, applies the committed entries
// held during the pause in log order, and runs any snapshot or recovery the
// pause deferred. The entry points stop returning ErrShardPaused once the
// replica has rejoined. Resuming a shard that is not paused is a no-op. An
// unknown shard returns ErrShardNotFound.
//
// PauseShard and ResumeShard on one shard are serialized: a resume issued
// while a pause is handing leadership over waits for the pause to reach the
// engine, then undoes it, so the shard ends resumed. If ctx ends first,
// ResumeShard returns ctx.Err() and changes nothing.
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
	h.engine.ResumeNode(engNode)
	ns.paused.Store(false)
	ns.pauseGate.unlock()
	return nil
}

// RequestSnapshot requests a snapshot for the given shard with the
// specified options. The returned RequestState resolves when the snapshot
// completes. The result's Value field carries the snapshot index on
// success.
//
// A zero-value SnapshotOption{} uses shard-level defaults, equivalent
// to the previous no-option behavior. When SnapshotOption.ExportPath is
// set, the snapshot is additionally written to the specified directory.
// When SnapshotOption.OverrideCompactionOverhead is true, the provided
// CompactionOverhead replaces the shard-level value for this snapshot's
// log compaction.
func (h *Host) RequestSnapshot(ctx context.Context, shardID uint64, opt SnapshotOption) (*RequestState, error) {
	// Same already-cancelled guard as Propose. Without it a cancelled call
	// would claim the shard's single-flight snapshot slot and enqueue disk
	// work before the wait observed the cancellation, so it would both start
	// expensive I/O the caller had abandoned and make concurrent callers see
	// ErrSnapshotInProgress for a snapshot nobody was waiting on.
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

	// Track this operation for drain notification.
	h.addPendingOp()

	// Register the pending snapshot before submitting to avoid a race
	// where OnSnapshotRequestCompleted fires before the map entry exists.
	// acquireRef pins the RS until this map entry is removed.
	rs.acquireRef()
	ns.pendingSnapshotsMu.Lock()
	ns.pendingSnapshots[snapshotKey] = rs
	ns.pendingSnapshotsMu.Unlock()

	// Close-vs-RequestSnapshot TOCTOU guard: the host may have begun closing
	// (and run its pending-request sweeps) between the top-of-method closed
	// check and the store above, which would orphan the RS in a deleted
	// nodeState's map — a deadline-less caller would then block on
	// rs.Result() forever. Re-check after registering and roll back if
	// shutting down. The delete-under-mutex claim makes the rollback
	// exactly-once relative to a concurrent Close sweep (mirrors
	// proposeEntry's LoadAndDelete pattern).
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
		// Snapshot pool rejected (duplicate or full) -- clean up. Claim the
		// entry first: the deadline sweep and teardown also claim entries from
		// pendingSnapshots, so one may already have completed, removed and
		// released this entry between the Store above and this rollback. An
		// unconditional releaseRef/completePendingOp would then double-count — outstanding
		// goes negative so tryRecycle's outstanding==0 check never holds and
		// the object is permanently lost from the pool, and pendingOps is
		// undercounted so Drain can close drainC with work in flight.
		// rs.Release() is the caller's own reference and is dropped
		// unconditionally.
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

// ErrSnapshotEncryptedBarrierUnavailable indicates that a snapshot encrypted
// at rest (Epoch > 0) cannot be read because the barrier required to decrypt
// it is unavailable (either no barrier is configured for the shard or the
// barrier is sealed).
//
// ExportSnapshot returns it because exported snapshots must be plaintext, so
// emitting the raw ciphertext would corrupt the export -> import round-trip.
// StartShard returns it (wrapped in HostInitError{Field: "snapshot_decrypt"})
// because recovering a state machine from ciphertext would either fail inside
// the state machine or install corrupt state; a node that cannot decrypt its
// own snapshot must fail closed at startup. The runtime recovery path
// (snapshotPool.recoverSnapshot) returns the same sentinel on the same
// predicate, which is why it is declared in the engine and re-exported here:
// both consumers of the artifact must be distinguishable by one errors.Is.
var ErrSnapshotEncryptedBarrierUnavailable = engine.ErrSnapshotEncryptedBarrierUnavailable

// ErrSnapshotBarrierSealed indicates that a snapshot could not be WRITTEN
// because the shard's barrier is configured but sealed. It is the write-side
// counterpart of ErrSnapshotEncryptedBarrierUnavailable: the local save path
// (snapshotPool.saveSnapshot), the receive path (HandleSnapshot) and the
// import (ImportSnapshot) all refuse rather than produce a plaintext
// artifact for a shard that is configured to be encrypted at rest. A shard
// with NO barrier is unaffected — plaintext is its configured outcome.
//
// It is declared in the engine and re-exported here for the same reason
// ErrSnapshotEncryptedBarrierUnavailable is: both packages must fail with one
// error identity.
var ErrSnapshotBarrierSealed = engine.ErrSnapshotBarrierSealed

// snapshotArtifactMagic identifies a QuicRaft exported-snapshot artifact.
// It is written as the first 8 bytes of every export so that ImportSnapshot
// can detect a well-formed artifact (and reject foreign/legacy data).
var snapshotArtifactMagic = [8]byte{'Q', 'R', 'S', 'N', 'A', 'P', '0', '1'}

// snapshotArtifactVersion is the current artifact header version. It is
// bumped when the header layout changes so older/newer readers can detect
// incompatibility instead of misparsing.
// Version 2 added the Compression field: a v1 artifact carries no statement
// of how its data was encoded, so a destination host whose own config differs
// from the exporter's would decode it wrong. Such an artifact is rejected
// rather than assumed uncompressed.
const snapshotArtifactVersion uint32 = 2

// snapshotArtifactFixedHeaderSize is the size of the fixed portion of the
// artifact header that precedes the variable-length membership block:
//
//	magic[8] | version:u32 | index:u64 | term:u64 | onDiskIndex:u64 |
//	compression:u16 | membLen:u32
const snapshotArtifactFixedHeaderSize = 8 + 4 + 8 + 8 + 8 + 2 + 4

// maxSnapshotArtifactMembLen bounds the membership block declared by an
// imported artifact header before it is allocated, so a crafted/corrupt
// membLen cannot trigger a multi-GB allocation (OOM/DoS) ahead of the
// io.ReadFull that would otherwise fail. A real membership — four maps of at
// most proto.MaxMembershipEntries entries, each entry an id plus a bounded address —
// is far below this 16 MiB ceiling; the inner proto decoder enforces the exact
// per-map/per-field bounds.
const maxSnapshotArtifactMembLen = 16 * 1024 * 1024

// snapshotArtifactHeader is the self-describing metadata that precedes the
// plaintext snapshot data in an exported artifact (the data is followed by
// snapshotArtifactTrailer, which binds its length and hash). It carries the original
// logical snapshot identity (Index/Term), the on-disk state machine index,
// and the cluster membership, so that ImportSnapshot can reconstruct the
// SAME logical snapshot on the destination host. Without this, an imported
// snapshot would lose its Raft index/term and membership, corrupting
// recovery (compaction boundary, term, and routing table).
//
// The Epoch is deliberately not carried: exported artifacts are always
// PLAINTEXT (ExportSnapshot decrypts), and imports persist Epoch=0 so the
// recovery path does not attempt to decrypt with a foreign barrier key.
// Compression IS carried: exports are decrypted but never decompressed, so
// the destination host must be told how the payload is encoded. Without it an
// import between hosts whose SnapshotCompressionType differs recovers the
// state machine from bytes in the wrong encoding.
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

// readSnapshotArtifactHeader reads and validates the artifact header from r,
// returning the parsed header and the exact bytes it was parsed from. It
// verifies the magic and version so that malformed, legacy, or foreign streams
// are rejected with a clear error rather than being misinterpreted as snapshot
// data.
//
// The raw bytes are returned because the artifact trailer's hash covers them
// (see snapshotArtifactTrailer): the fields parsed here decide the index, term
// and membership the import is recorded under, and the check that they are the
// ones the exporter wrote runs over the bytes, not over a re-encoding.
func readSnapshotArtifactHeader(r io.Reader) (hdr snapshotArtifactHeader, raw []byte, err error) {
	var fixed [snapshotArtifactFixedHeaderSize]byte
	if _, err := io.ReadFull(r, fixed[:]); err != nil {
		return snapshotArtifactHeader{}, nil, &SnapshotArtifactError{Reason: "read header", Err: err}
	}
	if !bytesEqual(fixed[0:8], snapshotArtifactMagic[:]) {
		return snapshotArtifactHeader{}, nil, &SnapshotArtifactError{Reason: "bad magic"}
	}
	version := binary.LittleEndian.Uint32(fixed[8:12])
	if version != snapshotArtifactVersion {
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
	// Index 0 is "no snapshot", not a snapshot at index zero. It is also the
	// artifact's on-disk NAME: snapshot-00000000000000000000 sorts FIRST among
	// the shard's snapshot directories, so such an import would be the first
	// thing gcSnapshotDirs destroys and would never be chosen by
	// latestSnapshotDir, the recovery fallback. Recovery would additionally
	// read it as an empty compaction boundary.
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

// ExportSnapshot streams a self-describing snapshot artifact for the given
// shard to the provided writer. The shard must have at least one snapshot
// available in LogDB.
//
// The artifact is a versioned header (magic, version, original Index, Term,
// OnDiskIndex, Compression and Membership), the PLAINTEXT snapshot data, and
// a trailer binding that data's length and xxhash64. The
// header preserves the logical snapshot identity so ImportSnapshot can
// reconstruct the SAME snapshot on the destination host: recovery relies on
// Index (compaction boundary), Term, Membership (routing table), and
// OnDiskIndex (on-disk state machine cursor). Emitting only the raw data
// would silently drop membership and reset the Raft index/term, so the
// imported snapshot would be a different logical snapshot.
//
// Exported snapshot data is always PLAINTEXT. When a barrier is configured
// the on-disk snapshot.dat is stored in the encrypted frame format; this
// method decrypts it before writing so the writer receives usable plaintext.
// This matches ImportSnapshot, which stores imported bytes verbatim with
// Epoch=0 (the no-decrypt recovery path), making export -> import a lossless
// round-trip across hosts with independent barrier keys.
//
// The decrypting barrier is resolved per shard via barrierForShard — the
// same resolver the snapshot receive/recover paths use — so a snapshot
// encrypted under a WithShardBarriers per-shard key exports correctly and
// is never fed to the (wrong) host-wide barrier.
//
// When the snapshot is encrypted (Epoch > 0) but the barrier cannot decrypt
// it (no barrier is configured or the barrier is sealed), ExportSnapshot
// returns ErrSnapshotEncryptedBarrierUnavailable rather than silently
// emitting ciphertext.
//
// The on-disk bytes are checked against the length and hash recorded in the
// snapshot's snapshot.meta as they are read, the check recovery makes, and an
// artifact that fails it returns ErrSnapshotCorrupt. The trailer is computed
// over the payload as it is WRITTEN, so damage at rest that the export let
// through would be sealed into a trailer that matches it; an export is the
// last point at which damage at rest can be caught. A
// plaintext export withholds its final bytes (and, for an artifact of at most
// one copy buffer, the header too) until the check passes; an encrypted one
// is authenticated frame by frame before any of its plaintext is written, but
// writes all of it before the recorded check runs.
//
// The trailer is written LAST, and only once every check has passed. An
// export that fails at any point never writes one, and ImportSnapshot refuses
// an artifact without a valid trailer — so what w received on an error can
// never be imported, even by a caller that ignored the error. It is still not
// an artifact and should be discarded.
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

	// Resolve the at-rest barrier for THIS shard. With WithShardBarriers the
	// snapshot was encrypted under the per-shard key, so the host-wide
	// barrier is the wrong (or absent) key for it; barrierForShard mirrors
	// the receive/recover paths.
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

	// Build the self-describing artifact header carrying the original
	// snapshot identity (Index/Term/OnDiskIndex/Membership). ImportSnapshot
	// parses this to persist the SAME logical snapshot.
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

	// When the snapshot was encrypted at rest (Epoch > 0) it must be
	// decrypted to plaintext before writing. Without this the writer would
	// receive raw ciphertext that a later ImportSnapshot stores with
	// Epoch=0, feeding undecryptable bytes into RecoverFromSnapshot.
	//
	// The header is held back until the first frame decrypts successfully
	// (deferredHeaderWriter): a wrong or rotated key fails AEAD on the very
	// first frame, and emitting the header eagerly would leave the caller
	// holding a truncated header-only artifact for a failed export.
	if logdbSnap.Epoch > 0 {
		// Pin the recorded snapshot epoch live for the duration of the read
		// so a concurrent keyRotationLoop -> PurgeEpochsBefore cannot remove
		// its DEK mid-stream and fail AEAD partway through a multi-GB export
		// (GlobalSafePurgeFloor does not account for an in-progress export).
		// This mirrors decryptSnapshotForSend: frames embed their own epoch,
		// and the recorded epoch is the minimum across them, so pinning it
		// protects the oldest DEK any frame may reference.
		barrier.RegisterInFlightEpoch(logdbSnap.Epoch)
		defer barrier.DeregisterInFlightEpoch(logdbSnap.Epoch)

		// The decrypt loop is the LONGER of the two export paths — it reads,
		// authenticates and re-emits every frame of a multi-gigabyte artifact
		// — yet it was the one path that never looked at ctx, so an encrypted
		// export ignored a cancelled caller for its whole duration while a
		// plaintext export of the same artifact stopped promptly. ctxWriter
		// puts the check on the frame boundary, which is the only place the
		// loop yields control anyway.
		hw := &deferredHeaderWriter{w: &ctxWriter{ctx: ctx, w: w}, header: headerBytes}
		pw := newArtifactPayloadWriter(hw, headerBytes)
		if _, err = decryptSnapshotFrames(barrier, shardID, ns.replicaID, logdbSnap.Index, f, pw); err != nil {
			return err
		}
		// Every frame authenticated under this node's key, so the plaintext
		// written is the plaintext that was sealed; the recorded integrity is
		// still checked, so an export refuses exactly what a recovery would.
		// By now every plaintext byte has reached w, so what makes a failure
		// here harmless is the trailer it returns before writing: an artifact
		// without one is refused by ImportSnapshot.
		if err = f.Verify(); err != nil {
			return err
		}
		// The trailer goes through hw, so a frameless (empty-data) stream
		// that never triggered a write still emits the header first: header
		// and trailer alone are a complete, importable artifact.
		_, err = hw.Write(pw.trailer())
		return err
	}

	// Epoch == 0: the on-disk snapshot is already plaintext, so a raw copy
	// is correct — once the bytes match their record. Nothing authenticates a
	// plaintext artifact but that record, so the copy withholds its final
	// bytes until the check over everything read has passed. The header is
	// deferred with them, so an artifact that fits in one copy buffer fails
	// having written nothing at all.
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

// copyVerifiedWithContext copies src to dst like copyWithContext, but holds
// the final bytes back until verify accepts everything read from src: the
// last buffer is written only once src is exhausted and verify has returned
// nil. A verify error is returned with those bytes withheld, so dst never
// receives the whole of an artifact that failed its check.
//
// It reads through two buffers, writing the previous one only once the next
// read proves it was not the last, so the check costs no second pass over
// src and no copy of the data beyond the read itself.
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

// ctxWriter fails a Write once its context is done, so a long producer that
// has no context parameter of its own still stops promptly when the caller
// gives up. It checks BEFORE writing: the point is not to emit the chunk the
// cancellation arrived during.
//
// The error is mapped through mapContextError, so a cancelled export surfaces
// as ErrCanceled / ErrTimeout exactly as the plaintext path's copyWithContext
// does — one identity for one condition, whichever path produced it.
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

// deferredHeaderWriter withholds a header from the underlying writer until
// the first write arrives, so producers that can fail before yielding any
// payload (AEAD decryption with a wrong key) fail before ANY byte reaches the
// destination instead of after a dangling header. ExportSnapshot writes the
// artifact trailer through it, which emits the header of an empty stream.
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

// protoMembershipToLogDB converts a proto.Membership to a logdb.Membership.
// Both types have identical field layouts; all maps are deep-copied to
// prevent aliasing between the decoded header and LogDB storage.
//
// Identities is carried like every other map. It is called out because
// dropping it is silent: the conversion still succeeds, the snapshot record
// still names every member, and the replica that reloads it simply falls
// back to address-based authorization. See logdb.Membership.Identities.
//
// The replicated session policy is carried for the same reason and fails the
// same way: a snapshot written without it recovers as "no policy", the replica
// falls back to local configuration, and its apply path expires sessions at
// indexes its peers do not. See proto.SessionPolicy.
//
// RemovedIdentities rides along for a sharper reason than the rest. Dropping it
// would not lose information so much as revoke a revocation: the replica comes
// back holding the tombstone replica IDs but not the identities they were
// recorded against, so a removed node still carrying its certificate regains
// the ability to evade its tombstone by claiming another replica ID — on
// exactly the restart an operator performs after removing it.
func protoMembershipToLogDB(m proto.Membership) logdb.Membership {
	result := logdb.Membership{
		ConfigChangeID:             m.ConfigChangeID,
		MaxWitnesses:               m.MaxWitnesses,
		RequireFullReplicaMajority: m.RequireFullReplicaMajority,
		SessionExpiryEntries:       m.SessionExpiryEntries,
		MaxTotalSessions:           m.MaxTotalSessions,
		MaxSessionsPerClient:       m.MaxSessionsPerClient,
		MaxSessionsPerOwner:        m.MaxSessionsPerOwner,
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

// importOptions carries an explicit operator assertion about the shard's
// on-disk layout, used to CROSS-CHECK what ImportSnapshot discovers from the
// shard's persisted bootstrap record.
type importOptions struct {
	dataDir       string
	dataDirSet    bool
	membership    map[uint64]string
	membershipSet bool
	identities    map[uint64]string
}

// WithImportMembership REPLACES the membership carried by the artifact with the
// one given, so a shard whose quorum is permanently lost can be reconstituted
// from the replicas that survive.
//
// # When this is the only option left
//
// A 5-voter shard that loses 3 nodes to unrecoverable hardware failure cannot
// make progress and cannot repair itself: every path that would remove the dead
// replicas is a configuration change, and a configuration change is a log entry,
// and committing a log entry needs the quorum that no longer exists. Restoring a
// snapshot does not help either, because the snapshot carries the SAME 5-voter
// configuration. Without this option the data is intact on disk and permanently
// unreachable.
//
// The recovery is: stop every surviving replica, export a snapshot from the most
// up-to-date one, and import it on each survivor with the same membership map
// naming only the survivors. Each then restarts into a configuration whose
// quorum it can actually form.
//
// # This is unsafe, and deliberately so
//
// Rewriting membership discards Raft's safety argument. The excluded replicas
// may hold committed entries this snapshot does not, and once they are gone from
// the configuration nothing will ever reconcile that: committed writes can be
// silently lost, which is a Leader Completeness violation performed on purpose.
// It is the correct trade only when the alternative is losing the shard
// entirely, and only when the operator has established that the excluded
// replicas are permanently gone rather than merely unreachable — a partitioned
// replica that returns to a configuration it was written out of will be told it
// is not a member, while still holding entries the new configuration never saw.
//
// Every surviving replica must be given the SAME map. Importing different
// memberships on different survivors creates two configurations that each
// believe they are authoritative, which is split-brain by construction.
//
// # What it replaces
//
// The map is the complete new VOTING membership: replicaID to raft address.
// Observers and witnesses from the artifact are dropped, because a
// disaster-recovery configuration should name exactly the replicas the operator
// has verified. Shard POLICY carried by the artifact — session limits, witness
// policy — is preserved; it is not membership.
//
// The importing replica must appear in the map. Importing a configuration that
// excludes the very replica doing the import produces a replica that cannot
// participate in the shard it just restored, which is never what was meant.
//
// Identities travel separately (WithImportMemberIdentities). A survivor the
// artifact already holds an identity for keeps it; a survivor it does not —
// a replica that moved to a new keypair as part of the recovery — must be
// given one, because on a host with a transport every member of the restored
// configuration must have an identity or the import is refused.
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

// WithImportMemberIdentities supplies node identities for the imported
// membership — the artifact's own, or the one WithImportMembership rewrites it
// to — replica ID to the value each node reports from Host.NodeIdentity. A
// supplied entry overrides the identity the artifact holds for that replica;
// the artifact's identity is kept for any member left out. It is how an
// artifact exported without identities is completed for a host that requires
// them. An entry for a replica the resulting membership holds no role for is
// refused (ImportMembershipInvalidError), as is one for a replica whose
// identity the artifact's tombstones hold.
func WithImportMemberIdentities(identities map[uint64]string) ImportOption {
	return func(o *importOptions) {
		o.identities = copyIdentityMap(identities)
	}
}

// importMembershipFor materialises the caller's options and resolves the
// membership the imported snapshot should record, then applies the host's
// identity-admission policy to it: on a host with a transport every active
// member of the configuration the shard will restart into must have an
// identity, whether the configuration is the artifact's own or a rewrite.
//
// Split out of ImportSnapshot only to keep that function under the cyclomatic
// limit; it sits at the threshold and this was the branch that tipped it.
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
	// One keypair, one member — the rule every add enforces
	// (ErrDuplicateMemberIdentity) and a rewrite bypasses, so it is asked here
	// whatever the host's posture: a configuration that binds one identity to
	// two voters gives the holder two votes.
	if a, b, dup := duplicateIdentity(ms.Identities); dup {
		return proto.Membership{}, &ImportMembershipInvalidError{
			ShardID: shardID, ReplicaID: replicaID,
			Reason: fmt.Sprintf("replicas %d and %d hold the same node identity; one keypair "+
				"cannot be two members of a shard", a, b),
		}
	}
	// An identity the shard removed stays removed — the rule every add
	// enforces (ErrRemovedMemberIdentity). The tombstones are the artifact's:
	// a rewrite does not carry them into the new configuration, but it must
	// not be the way a removed keypair gets back in either.
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

// refuseStrayIdentities refuses a supplied identity whose replica the
// resulting membership does not name (named reports whether it does). The
// lowest such replica is reported, so the refusal is deterministic across
// runs like every neighbouring check.
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

// resolveImportMembership returns the membership the imported snapshot should
// record: the artifact's own unless the caller overrode it.
//
// The override is validated hard. It is the one input to an import that can
// silently destroy committed data, and every failure mode is an operator typo
// away — an address left off, the local replica omitted, a stale map copied
// from a runbook — so each is refused by name rather than resolved.
func resolveImportMembership(
	shardID, replicaID uint64,
	artifact proto.Membership,
	iopts importOptions,
) (proto.Membership, error) {
	if !iopts.membershipSet {
		if len(iopts.identities) == 0 {
			return artifact, nil
		}
		// Identities supplied without a rewrite complete the ARTIFACT's own
		// membership: an artifact exported by a build that did not require
		// identities has none, and the import is refused behind a transport
		// until they are supplied. The supplied value wins per replica.
		// An entry for a replica the artifact holds no role for is refused
		// rather than dropped: it is the map an operator keyed by the
		// intended NEW replica IDs and then forgot WithImportMembership for,
		// and persisting it would bind a keypair to a non-member — which
		// every later add of that keypair then trips over as a duplicate.
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

	// The same rule for a rewrite: an identity keyed by a replica the new
	// membership does not name is the map keyed by the OLD replica IDs, and
	// dropping it silently would report the survivors as identity-less (or,
	// for a reused ID, import a stale artifact binding) while the operator
	// believes identities were supplied.
	if err := refuseStrayIdentities(shardID, replicaID, iopts.identities,
		func(id uint64) bool { _, ok := iopts.membership[id]; return ok },
		"the rewritten membership does not name"); err != nil {
		return proto.Membership{}, err
	}

	// Policy travels with the shard, membership does not. ConfigChangeID is
	// carried over so the recovered configuration is not ordered BEFORE the one
	// the artifact recorded.
	out := proto.Membership{
		ConfigChangeID:             artifact.ConfigChangeID,
		MaxWitnesses:               artifact.MaxWitnesses,
		RequireFullReplicaMajority: artifact.RequireFullReplicaMajority,
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

// WithImportDataDir asserts that the shard will be started with this
// Config.DataDir. Passing "" asserts that the shard has none and roots its
// snapshot tree under the host's NodeHostDir.
//
// It is not REQUIRED: StartShard records the shard's DataDir in its
// LogDB bootstrap record, and ImportSnapshot reads it back, so the layout is
// discovered rather than declared. When the option IS supplied it must match
// the recorded value exactly; a disagreement is refused with
// *ImportDataDirMismatchError rather than silently resolved, because either
// side being wrong puts the artifact outside the tree snapshot garbage
// collection and the DEK purge-floor scan enumerate — which leaks it and lets
// the next PurgeEpochsBefore destroy the DEK it needs.
//
// The option remains meaningful for a shard whose layout cannot be discovered:
// one with NO bootstrap record (started with join=true, which records none),
// and one whose record PREDATES the DataDir field. Both fail closed with
// ErrImportLayoutUnknown unless the caller states the layout here, and in both
// cases the stated value is trusted because there is nothing to cross-check it
// against.
func WithImportDataDir(dir string) ImportOption {
	return func(o *importOptions) {
		o.dataDir = dir
		o.dataDirSet = true
	}
}

// resolveImportDataDir determines the shard's snapshot-tree root for an import,
// preferring the value StartShard recorded in the bootstrap record over the
// caller's assertion, and refusing to guess.
//
// The previous contract — "a shard configured with Config.DataDir must pass
// WithImportDataDir" — was unenforceable: nothing validated it and
// ImportSnapshot could not detect the omission, so omitting it wrote the
// artifact under NodeHostDir while StartShard pointed the shard's SnapshotDir
// at DataDir/shard-N. listSnapshotDirs then never enumerated the artifact, it
// contributed no floor to the DEK purge, and the next PurgeEpochsBefore
// legitimately destroyed the key it needed: the imported disaster-recovery
// snapshot became permanently undecryptable, silently, behind a caller opt-in.
func (h *Host) resolveImportDataDir(shardID, replicaID uint64, iopts importOptions) (string, error) {
	bs, ok, err := h.logdb.GetBootstrap(shardID, replicaID)
	if err != nil {
		return "", err
	}
	// A record that PREDATES the DataDir field proves nothing about the layout:
	// it decodes the field as "" and that is indistinguishable from a shard
	// that genuinely has none, so it is treated exactly like no record at all.
	// Reading it as proof was wrong in both directions — it silently resolved a
	// DataDir shard's import to the NodeHostDir tree (the DEK destruction this
	// whole mechanism exists to prevent), AND it refused the documented
	// WithImportDataDir escape hatch as a contradiction, leaving the operator no
	// way through.
	if !ok || !bs.DataDirRecorded {
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

// ImportSnapshot imports a snapshot artifact from the reader for the given
// shard. The shard must be stopped (not loaded on this host) before calling
// this method. The artifact is the versioned header, plaintext data and
// trailer produced by ExportSnapshot; the header is parsed first to recover
// the ORIGINAL snapshot identity (Index/Term/OnDiskIndex/Membership), and the
// data is written as snapshot.dat in this host's at-rest form. The snapshot
// is recorded in LogDB so the next StartShard recovers the SAME logical
// snapshot. The context is checked for cancellation at key blocking points.
//
// The data is checked against the length and xxhash64 in the artifact's
// trailer as it is written, and the import fails before anything is adopted —
// before the staged directory is published, recorded in LogDB or seen by raft
// — when they disagree, when the stream ends early, or when there is no
// trailer at all. The refusal is *SnapshotArtifactError; a length or hash
// mismatch also matches ErrSnapshotCorrupt. An artifact exported by a build
// that wrote no trailer is refused the same way: it is not migrated, and the
// remedy is to export it again.
//
// It returns an error wrapping ErrInvalidOperation when the shard is running
// — including one started while this call waited for the shard's import lock
// — and when this host holds data for more than one replica of the shard, in
// which case the stale replica's data must be removed (RemoveData) first: the
// shard-only API cannot tell which replica the operator means. While an import
// runs, StartShard of the same shard is refused with ErrImportInProgress.
//
// The artifact is written to the canonical snapshot path — the same
// shardSnapshotBaseDir + engine.SnapshotSaveDir construction StartShard, the
// snapshot pool and the receive path use — so it is visible to snapshot garbage
// collection and, critically, to the directory scan that computes the DEK purge
// floor.
//
// The shard's snapshot-tree root is DISCOVERED from the Config.DataDir
// StartShard recorded in the LogDB bootstrap record. When there is no such
// record (a shard started with join=true, or one this host has never started —
// the disaster-recovery case), or when the record predates the DataDir field
// and therefore carries none, the layout cannot be discovered and the import
// fails closed with ErrImportLayoutUnknown; pass WithImportDataDir to state it,
// using WithImportDataDir("") to assert the shard has no DataDir. A supplied
// value that contradicts the recorded one is refused with
// ErrImportDataDirMismatch rather than silently resolved.
//
// The on-disk directory is named by the artifact's own snapshot INDEX, the
// same way every other snapshot writer names one: the snapshot tree is ORDERED
// by that name, so an import must take its rightful place among the shard's
// existing snapshots rather than sorting as the oldest.
//
// # Why the artifact is staged and renamed
//
// Naming the directory by the artifact's index means an import can resolve to a
// path it does NOT own: an operator retry of the same artifact, a
// disaster-recovery artifact exported from a peer at an index this replica also
// reached (snapshot indices are largely deterministic), or a concurrent second
// import. So the import never writes through that path directly. It refuses a
// destination that already exists, stages everything in a directory MkdirTemp
// created — one this call provably owns, and the only thing a failure is ever
// allowed to delete — and renames it into place once the artifact is complete
// and durable. Writing in place would truncate a LIVE snapshot.dat before
// anything was validated, leaving the shard's LogDB record describing a file it
// no longer matches: a short FileSize ships a truncated snapshot that erases the
// receiving follower's state, and a stale Epoch makes StartShard fail closed on
// an undecryptable snapshot.
//
// Two refusals follow from that, both raised before the tree is touched:
//
//   - an artifact whose index does not ADVANCE the shard's recorded snapshot is
//     refused with *ImportSnapshotOutOfDateError, which matches
//     logdb.ErrSnapshotOutOfDate — the same answer the LogDB would have given
//     after the damage was done, and
//   - a destination directory that already exists is refused with
//     *ImportSnapshotDirExistsError. Clearing it is an operator decision; an
//     import never removes a directory it did not create.
//
// Concurrent imports of one shard are serialised, so the checks above cannot be
// interleaved with another import's writes.
func (h *Host) ImportSnapshot(ctx context.Context, shardID uint64, r io.Reader, opts ...ImportOption) error {
	replicaID, err := h.importPreflight(ctx, shardID)
	if err != nil {
		return err
	}

	// Parse the self-describing artifact header BEFORE touching disk so a
	// malformed/foreign stream is rejected without creating partial state.
	// The header carries the original logical snapshot identity; the bytes
	// that follow are the plaintext snapshot data.
	header, rawHeader, err := readSnapshotArtifactHeader(r)
	if err != nil {
		return err
	}

	// Resolve the membership the shard will restart into BEFORE anything is
	// staged. A rejected override is an operator typo in a disaster-recovery
	// runbook, and the operator should learn that without first having a
	// partially written snapshot tree to reason about.
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

	// Serialise imports for this shard. Everything from here on reasons about
	// the shard's recorded snapshot and its on-disk tree, and both are resolved
	// from the artifact's index, so a second concurrent import of the same
	// index would otherwise interleave with this one's checks.
	release, err := h.lockStoppedShardForImport(ctx, shardID, replicaID)
	if err != nil {
		return err
	}
	defer release()

	// Refuse an artifact that does not ADVANCE the shard's recorded snapshot,
	// before the tree is touched. Every production LogDB refuses such a save
	// with logdb.ErrSnapshotOutOfDate, so deferring the check to the final
	// SaveSnapshot meant the refusal arrived after the artifact had already
	// been written over the directory the recorded snapshot lives in.
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

	// Record the deepest pre-existing ancestor BEFORE MkdirAll: the directory
	// fsync below must cover every directory this call creates (same
	// bookkeeping as ReencryptSnapshotFile).
	syncRoot := fsutil.DeepestExistingAncestorWith(fsys.Stat, snapDir)
	stageParent := filepath.Dir(snapDir)
	if mkdirErr := fsys.MkdirAll(stageParent, 0o750); mkdirErr != nil {
		return mkdirErr
	}

	// Stage in a directory this call provably owns. The name cannot parse as
	// "snapshot-<index>", so engine.listSnapshotDirs — and with it snapshot GC,
	// the recovery fallback and the DEK purge-floor scan — never sees a
	// half-written import.
	stageDir, tempErr := fsys.MkdirTemp(stageParent, importStageDirPattern)
	if tempErr != nil {
		return tempErr
	}

	// Term is floored at 1 (the minimum valid term) so the Raft engine never
	// treats the imported snapshot as uninitialized. It is computed here, once,
	// because the staged snapshot.meta and the LogDB record below must describe
	// this snapshot identically.
	term := header.Term
	if term == 0 {
		term = 1
	}

	fileSize, epoch, unpin, stageErr := h.stageImportedArtifact(
		ctx, fsys, shardID, replicaID, header, rawHeader, term, stageDir, r)
	// The epoch pin must live until the SaveSnapshot below records it, so it is
	// released on THIS function's return rather than the helper's.
	defer unpin()
	if stageErr != nil {
		return stageErr
	}

	// Make the staged directory's own entries durable before it is published:
	// the rename below makes the whole directory reachable at once, and a
	// directory whose entries are not durable would come back after a crash
	// under the canonical name with files missing.
	//
	// This is DEFENCE IN DEPTH, not the primary guarantee, and the distinction
	// matters to anyone tempted to delete it or to write a test for it. The
	// syncDirChain after the rename fsyncs snapDir FIRST and its parent second,
	// and a rename does not change the inode — so the leaf fsync already covers
	// these same entries, and snapDir's own entry does not become durable until
	// the parent fsync that follows it. No crash point between the two can
	// therefore expose an incomplete published directory, which is why deleting
	// this call leaves the suite (including
	// TestImportSnapshot_CrashAtEveryStepLeavesAConsistentTree, which walks the
	// crash point across every filesystem operation the import performs) green.
	// It is kept for the filesystem that makes a rename durable without covering
	// the entries created beneath the source before it — an ordering POSIX does
	// not forbid — at the cost of one fsync per operator-initiated import.
	if syncErr := syncDir(fsys, stageDir); syncErr != nil {
		discardImportDir(fsys, shardID, stageDir)
		return syncErr
	}

	// Publish. The destination did not exist when this call started and no
	// other import for this shard can have created it since (the per-shard
	// lock above), so this rename adds a name rather than replacing one — and
	// a rename onto a non-empty directory would fail rather than destroy it.
	if renameErr := fsys.Rename(stageDir, snapDir); renameErr != nil {
		discardImportDir(fsys, shardID, stageDir)
		return renameErr
	}

	// The canonical directory now exists and THIS import is what created it —
	// the destination-exists precheck above proved it did not exist, and the
	// per-shard import lock has been held across both. published records that
	// fact explicitly rather than leaving a later cleanup to infer it, because
	// the whole point of the precheck is that an import must never remove a
	// directory it did not create.
	//
	// Every remaining failure must take the publication back down. Leaving it
	// standing turns a transient error — an fsync failure, a cancelled context,
	// a LogDB write error during a disaster-recovery restore — into a permanent
	// lockout: the retry hits the destination-exists precheck and is refused
	// with *ImportSnapshotDirExistsError, whose remedy is a manual rm -rf
	// performed against an error message that says an import never removes a
	// directory it did not create. Before the staging rewrite the retry simply
	// overwrote the half-published directory and succeeded.
	published := true
	defer func() {
		if published {
			discardImportDir(fsys, shardID, snapDir)
		}
	}()

	// Make the directory entries durable BEFORE recording the snapshot in
	// LogDB. The data file is fsynced (both the original write and the
	// reencrypt rename above), but the entries the MkdirAll and the publishing
	// rename added are not durable until each directory in the chain is
	// fsynced; without this a crash right after the import leaves LogDB
	// pointing at a directory that no longer exists, and StartShard then fails
	// with snapshot_open. Same invariant HandleSnapshot enforces.
	if syncErr := syncDirChain(fsys, snapDir, syncRoot); syncErr != nil {
		return syncErr
	}

	// Check context before committing to LogDB.
	if err := ctx.Err(); err != nil {
		return mapContextError(err)
	}

	// Persist the ORIGINAL snapshot metadata from the artifact header so the
	// imported snapshot is the SAME logical snapshot: recovery uses Index as
	// the compaction boundary, Term for the snapshot term, Membership for the
	// routing table, and OnDiskIndex for on-disk state machines. Term is
	// floored at 1 (the minimum valid term) so the snapshot is never treated
	// as uninitialized by the Raft engine.
	//
	// Epoch records what stageImportedArtifact actually did: 0 when the
	// shard has no barrier configured (plaintext deployment, artifact stays
	// as imported), or the DEK epoch it was just encrypted under when a
	// barrier is configured and unsealed. Recovery reads Epoch>0 as "decrypt
	// before use", so this must reflect this host's own at-rest state, not
	// the artifact's. term was floored above and is shared with the on-disk
	// metadata so both descriptions of this snapshot agree.
	// recoveredMembership was resolved before staging: the artifact's own,
	// unless the operator is reconstituting a shard whose quorum is
	// permanently lost.
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
	// Recorded: the directory is now the shard's snapshot and the rollback above
	// must not run. This is set BEFORE the compaction below so a compaction
	// error cannot trip the deferred discardImportDir into removing a snapshot
	// directory the LogDB already points at.
	published = false

	h.gcShardSnapshots(snapBaseDir, shardID, replicaID)

	// Advance the log compaction floor to the imported snapshot index, removing
	// every entry at or below it.
	//
	// A snapshot subsumes all log entries up to its index, so any entry that
	// survives below it is dead weight — but here it is worse than dead weight.
	// The offline disaster-recovery flow bootstraps the shard (StartShard) so an
	// unrecorded shard can be imported into (ImportSnapshot refuses ErrShardNotFound
	// otherwise), and that bootstrap writes an initial configuration entry at
	// index 1. Importing an artifact whose snapshot index is higher then records
	// the snapshot WITHOUT displacing that orphaned index-1 entry. The shard runs
	// on from the snapshot, appends new entries, and — because the default
	// CompactionOverhead (5000) far exceeds the entry count of a freshly restored
	// node — the running snapshotter's compaction never fires to clean it up. On
	// the next restart, log recovery rebuilds the entry index as
	// {1, ...post-restore-tail} whose span (last-first+1) exceeds the entry count,
	// which EntryRange correctly reports as ErrLogGapDetected, and host init fails
	// closed. The node is then unrecoverable without wiping and restoring again.
	//
	// A normal InstallSnapshot advances the compaction floor with the snapshot
	// (see internal/engine snapshot compaction); an import must do the same. The
	// shard is stopped here (an ImportSnapshot precondition), so there is no live
	// LogReader to advance — the LogDB removal is sufficient, and the LogReader is
	// rebuilt from the compacted LogDB on the next StartShard. Unlike the running
	// snapshotter, this retains no CompactionOverhead: a freshly imported node has
	// no follower depending on its pre-snapshot log tail (a follower behind the
	// snapshot is caught up via InstallSnapshot), so entries below the imported
	// snapshot index serve no purpose.
	if rmErr := h.logdb.RemoveEntriesTo(shardID, replicaID, header.Index); rmErr != nil {
		return rmErr
	}
	// Compact only garbage-collects segments the marker RemoveEntriesTo just
	// wrote has already made obsolete, so by this point the restore is COMPLETE
	// and durable: the snapshot is recorded and the compaction floor is set. A
	// failure here reclaims no space but loses no correctness — and returning it
	// would report a successful restore as failed, with no way to retry, because
	// the snapshot index and directory now exist (a re-import of the same artifact
	// is refused). Log it for the operator instead.
	if compactErr := h.logdb.Compact(shardID, replicaID); compactErr != nil {
		h.logger.Warn("imported snapshot recorded and log floor advanced, but WAL "+
			"segment garbage collection failed; the restore is complete and the "+
			"obsolete segments will be reclaimed by a later compaction",
			"shard_id", shardID, "replica_id", replicaID, "error", compactErr)
	}
	return nil
}

// importPreflight refuses an import that must not touch disk at all, and
// resolves the shard's replicaID from the LogDB bootstrap records.
//
// Every check here runs BEFORE any directory, data file or LogDB record
// exists, so a refusal leaves no partial state behind.
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
	if h.diskFull != nil && h.diskFull.Load() {
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
	// The shard must have exactly ONE replica on this host. ListNodeInfo is
	// built from a map, so "the first" of several is a different replica on
	// each call; a shard restarted under a new replica ID without removing
	// the old one's data has two records, and an import would land under one
	// of them at random. Refuse the ambiguity instead: the operator removes
	// the stale replica's data (RemoveData) and imports again.
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

	// A CONFIGURED but SEALED barrier means at-rest encryption is temporarily
	// unavailable for this shard, not that the shard is plaintext. Refuse
	// before any directory, data file, or LogDB record exists, mirroring the
	// same precheck HandleSnapshot enforces on the receive path: an imported
	// artifact's bytes are plaintext (ExportSnapshot decrypts before writing
	// them), so persisting them while sealed would durably downgrade a shard
	// whose data must be encrypted at rest. stageImportedArtifact re-checks
	// before the data file is created, and the streamed encryption covers a
	// seal that lands mid-import.
	//
	// An UNRESOLVED per-shard barrier fails here for the same reason: the
	// artifact would be published in plaintext for a shard the deployment
	// asked to encrypt.
	if _, barrierErr := h.snapshotBarrierForShard(shardID); barrierErr != nil {
		return 0, barrierErr
	}
	return replicaID, nil
}

// gcShardSnapshots reclaims a shard's superseded snapshot directories, keeping
// only the most recent the engine retains.
//
// It must be called only AFTER the new snapshot is durably recorded, so
// retention is computed against the set INCLUDING it; running it earlier counts
// the new directory as absent and can evict the one about to become the shard's
// most recent.
//
// The failure is logged, never returned. ImportSnapshot and the snapshot receive
// path both call it at a point where the snapshot is already recorded and the
// operation has SUCCEEDED, so returning the error would report completed,
// durable work as failed — and for the import there is no way to retry, because
// the snapshot index and directory now exist. Failing to reclaim disk loses no
// correctness and the next snapshot runs the GC again.
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

// importStageDirPattern names the directory an import stages its artifact in
// before publishing it under the canonical, index-derived name.
//
// The leading dot and the "import" word make an orphan (a process killed
// mid-import) self-identifying, and the name deliberately cannot parse as
// "snapshot-<index>": engine.listSnapshotDirs skips it, so a half-written
// import is invisible to snapshot GC, to latestSnapshotDir — the recovery
// fallback — and to the DEK purge-floor scan.
//
// That same invisibility is why the pattern is NOT defined here: an orphaned
// staging directory holds the imported state machine in PLAINTEXT and nothing
// else on the host will ever enumerate it, so the name a writer creates must be
// the name engine.SweepStaleImportStageDirs reclaims. The constant is shared
// with the sweeper for exactly the reason the decrypt scratch prefixes are.
const importStageDirPattern = engine.ImportStageDirPattern

// checkImportAdvancesSnapshot refuses an artifact whose index does not advance
// the shard's recorded snapshot.
//
// This is the SAME condition memdb.SaveSnapshot and waldb.DB.SaveSnapshot
// enforce with logdb.ErrSnapshotOutOfDate; running it here makes the refusal
// arrive BEFORE the shard's snapshot tree is touched. Snapshot indices are
// largely deterministic (a save is triggered at processed - lastSnapshotIndex
// >= SnapshotEntries), so an artifact exported from a peer routinely carries an
// index this replica also reached, and an idempotent operator retry carries the
// same index by construction — the collision is the expected case, not a rare
// one.
//
// A shard with no recorded snapshot imposes no constraint. Any error other than
// logdb.ErrNoSnapshot is returned: an import must not proceed on an
// indeterminate view of what it might be about to replace.
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

// importSnapshotDir resolves the canonical on-disk directory an imported
// artifact must be written to.
//
// The path is built exactly as every other snapshot writer builds one: the
// shard's canonical base directory plus the engine's save-dir construction.
// Hand-rolling it here produced a tree one level shallower than the canonical
// one, so listSnapshotDirs never saw the import: snapshot GC leaked it, and —
// far worse — the imported snapshot's epoch never reached the DEK purge floor
// (which is computed by SCANNING these directories), so the next
// PurgeEpochsBefore legitimately dropped the DEK and StartShard then failed
// closed on an undecryptable snapshot: total loss of a disaster-recovery
// restore.
//
// index is the artifact's OWN snapshot index and names the directory, exactly
// as every other snapshot writer names one. That name is not decoration:
// listSnapshotDirs PARSES it and ORDERS by it, gcSnapshotDirs deletes the
// lowest-ordered directories, and latestSnapshotDir — the recovery fallback —
// picks the highest. Naming an import by a per-host sequence number instead
// made a disaster-recovery restore sort as the OLDEST snapshot on a node that
// already had some: first in line for GC, and never chosen by the recovery
// fallback, which would prefer a stale but higher-numbered directory.
// It returns the shard's snapshot BASE directory alongside the destination.
// The base is what engine.GCSnapshotDirs takes, and deriving it back out of the
// destination path would re-hard-code the very layout this function exists to
// keep in one place.
func (h *Host) importSnapshotDir(shardID, replicaID, index uint64, opts []ImportOption) (baseDir, snapDir string, err error) {
	var iopts importOptions
	for _, opt := range opts {
		opt(&iopts)
	}
	// The layout comes from what StartShard RECORDED, not from what the caller
	// remembered to declare. resolveImportDataDir fails closed when it can
	// prove neither.
	importDataDir, err := h.resolveImportDataDir(shardID, replicaID, iopts)
	if err != nil {
		return "", "", err
	}
	base := shardSnapshotBaseDir(h.cfg.NodeHostDir, importDataDir, shardID)
	return base, engine.SnapshotSaveDir(base, shardID, replicaID, index), nil
}

// writeImportedSnapshotData writes the artifact's payload — r is everything
// after the header, the payload followed by its trailer — to the snapshot data
// file in its AT-REST form and makes it durable. The payload is checked
// against the trailer as it streams, and an artifact whose payload does not
// match its trailer, or that has none, fails here. The file is always closed;
// on error the caller discards the whole directory.
//
// With a barrier the payload is streamed straight through
// encryptSnapshotFrames, so the data file only ever receives CIPHERTEXT: the
// artifact's plaintext is never materialized under any name, and a crash or
// power loss at any point of the import leaves nothing readable on disk.
// Writing the plaintext first and re-encrypting it afterwards left a complete,
// fsynced plaintext copy of the state machine in the staging directory until
// the re-encryption renamed over it — and for good if the process died in
// between, since the staging sweep runs only when the shard starts again.
// Without a barrier (a plaintext deployment) the payload is written as it is,
// which is that shard's at-rest form; epoch is then 0.
//
// It returns the recorded (minimum) epoch and every epoch a frame was sealed
// under, PINNED: the imported shard is STOPPED, so it contributes no floor to
// the DEK purge scan and these pins are its only protection until the LogDB
// record naming the epoch is durable — the caller releases them after that.
// On error every pin has already been released and pinned is nil.
func (h *Host) writeImportedSnapshotData(ctx context.Context, fsys snapshotFS, shardID, replicaID, index uint64,
	dataPath string, header []byte, r io.Reader, barrier Barrier) (epoch uint64, pinned []uint64, err error) {
	f, createErr := fsys.OpenFile(dataPath, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0o600)
	if createErr != nil {
		return 0, nil, createErr
	}
	// closeFile returns the Close error as well as logging it. On the error
	// paths the caller already has a better error and discards the whole
	// directory, so the return is ignored there; on the SUCCESS path it must be
	// checked — see the write_complete call below.
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

	// Bound the copy by the maximum snapshot size. An unbounded copy lets any
	// artifact — a malformed stream, or one produced by a host with a larger
	// limit — fill the volume before anything notices. The bound is on the
	// PLAINTEXT, as it always was.
	//
	// The shard is stopped by definition here, so its per-shard
	// Config.MaxSnapshotSize is not available; the package default is the
	// applicable ceiling.
	// The copy is ctx-aware. This is the single longest-running step of an
	// import — it writes the whole artifact, which for a restore is measured
	// in gigabytes — and an uninterruptible copy made an operator who aborted
	// a wrong-artifact restore wait for it to finish writing before the
	// staging directory was discarded.
	//
	// r is the artifact after its header: the payload, then the trailer that
	// binds the payload's length and the hash of header and payload together.
	// header is the raw header already read from the artifact, so a flipped
	// Index, Term or Membership byte fails the same check as a flipped payload
	// byte. artifactPayloadReader withholds the trailer and checks it when the
	// payload ends, INSIDE this copy — so a truncated, damaged, re-headed or
	// trailerless artifact fails the write of the staged data file
	// and the caller discards the stage before anything is synced, published,
	// recorded in LogDB or seen by raft. The limit admits the trailer on top
	// of the largest permitted payload.
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
	// The Close is part of the durability chain, not cleanup. Sync flushes what
	// this process wrote; Close is where a file system reports a write failure
	// it deferred (a full or quota-exceeded volume, a network file system's
	// late error), and on some platforms it is the only place that error is
	// ever delivered. Dropping it meant the import went on to SaveSnapshot —
	// recording this artifact as the shard's durable snapshot — and then to
	// RemoveEntriesTo, which DELETES the log entries the snapshot subsumes,
	// over a snapshot file that may be short. The entries are gone and the
	// snapshot cannot replace them. Returning it lands the caller on the
	// staging-directory discard, so the operator can simply retry the import.
	// Same idiom as HandleSnapshot's receive path.
	if closeErr := closeFile("write_complete"); closeErr != nil {
		unpinAll()
		return 0, nil, closeErr
	}
	return epoch, pinned, nil
}

// importPayloadReader is the imported artifact's payload as the import reads
// it: cancelled with ctx, and refused with *SnapshotTooLargeError once it
// yields more than maxSize bytes. r must be limited to maxSize+1 bytes, so an
// artifact exactly at the limit still imports while an oversized one is
// detected without reading the rest of it.
//
// It is a reader rather than a copy loop because the encrypted import hands it
// to the frame sealer, which does the copying.
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

// stageImportedArtifact writes the artifact's payload into the import's staging
// directory and makes that directory a complete, self-describing snapshot: the
// data file, encrypted under this host's own barrier, and the snapshot.meta
// recording the epoch it actually used. It returns the on-disk size the LogDB
// record must carry, that epoch, and the pin holding the epoch's DEK alive.
//
// It is separate from ImportSnapshot because it is a coherent phase with a
// single postcondition — "the staging directory now holds a publishable
// snapshot" — and because every one of its failures has the same remedy: the
// import has not been published or recorded, so the staging directory is
// discarded and the operator retries.
//
// unpin is never nil, so the caller can defer it unconditionally; it must be
// held until the LogDB record naming the epoch is durable.
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

	// Encrypt the imported snapshot under THIS host's own barrier so it is
	// encrypted at rest with the same key every other on-disk snapshot for the
	// shard uses. The artifact's bytes are plaintext (ExportSnapshot decrypts
	// before writing, so an import never carries ciphertext), and they are
	// sealed as they are read — the same streaming form HandleSnapshot's
	// receive uses — so no plaintext copy is ever written. A nil barrier leaves
	// the snapshot plaintext (epoch 0); a barrier sealed after the precheck in
	// importPreflight fails closed here, before the data file exists, rather
	// than durably recording a plaintext artifact, as does a per-shard barrier
	// that stopped resolving since the precheck.
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

	// Write the snapshot metadata file beside the artifact, recording the epoch
	// the encryption above actually used. Its integrity fields hash the data
	// file as stored — the ciphertext when a barrier is configured.
	//
	// Without it the directory is anonymous to every scan: engine's purge-floor
	// computation reads snapshot.meta for each snapshot directory and treats a
	// MISSING file as "GC'd, imposes no floor", so the imported snapshot's DEK
	// is legitimately dropped by the next PurgeEpochsBefore and the restored
	// state becomes permanently undecryptable — StartShard then fails closed.
	// For a disaster-recovery import that is total loss of the restore.
	if metaErr := writeReceivedSnapshotMetadata(fsys, stageDir, header.Index, term,
		shardID, replicaID, epoch, header.Compression); metaErr != nil {
		discardImportDir(fsys, shardID, stageDir)
		return 0, 0, unpin, metaErr
	}

	// Determine the data file size for recovery's InstallSnapshot path. This
	// is measured on the file as written so FileSize always means "size of the
	// file at Filepath", matching the framed-ciphertext size when a barrier
	// is configured rather than the artifact's plaintext size. It is read
	// from the staging path, which is the same file the rename below publishes.
	// A failed Stat must NOT fall through with FileSize 0: recovery records that
	// size in the snapshot record, and a (non-empty Filepath, zero FileSize)
	// pair makes this replica stream a metadata-only snapshot once it becomes
	// leader — erasing the receiving follower's state. The import has not been
	// published or recorded in LogDB yet, so the staging directory is discarded
	// and the operator can retry.
	fi, statErr := fsys.Stat(dataPath)
	if statErr != nil {
		discardImportDir(fsys, shardID, stageDir)
		return 0, 0, unpin, statErr
	}
	return uint64(fi.Size()), epoch, unpin, nil
}

// discardImportDir removes an import's STAGING directory when the import will
// never be recorded in LogDB. A removal failure is logged and otherwise
// ignored: the caller is already returning the error that caused the discard.
//
// dir must always be a directory the failing import itself created: the staging
// directory (fsys.MkdirTemp), or the canonical destination once the publishing
// rename has moved that staging directory onto it. It was once called with the
// canonical, index-named destination UNCONDITIONALLY, which an import does not
// necessarily own: a failed import at a colliding index then deleted the shard's
// PRE-EXISTING snapshot outright while LogDB still referenced it. What makes the
// post-publish call safe is that the caller has PROVED ownership — the
// destination-exists precheck and the rename ran under one hold of the
// per-shard import lock — and tracks it in an explicit flag rather than
// inferring it from the path.
func discardImportDir(fsys snapshotFS, shardID uint64, dir string) {
	if rmErr := fsys.RemoveAll(dir); rmErr != nil {
		slog.Debug("snapshot import directory cleanup failed",
			"shard", shardID,
			"dir", dir,
			"error", rmErr)
	}
}

// clampNonNegative converts an activeNodes count to a slice capacity, flooring
// at 0. Teardown is claimed exactly once (CompareAndSwap in StopShard/Close), so
// activeNodes should never be negative; this is defense-in-depth so a monitoring
// call can never panic with "makeslice: cap out of range".
func clampNonNegative(n int64) int {
	if n < 0 {
		return 0
	}
	return int(n)
}

// GetNodeHostInfo returns a snapshot of the host's state for monitoring.
func (h *Host) GetNodeHostInfo(opt NodeHostInfoOption) *NodeHostInfo {
	info := &NodeHostInfo{
		RaftAddress: h.cfg.RaftAddress,
	}

	info.ShardInfoList = make([]ShardInfo, 0, clampNonNegative(h.activeNodes.Load()))
	h.nodes.Range(func(_, value any) bool {
		ns, ok := value.(*nodeState)
		if !ok {
			h.logger.Error("GetNodeHostInfo: nodes map value type assertion failed",
				"error", &TypeAssertionError{
					Context:      "GetNodeHostInfo nodes range",
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
func (h *Host) DiagPipelineState(shardID uint64) (commitPending bool, proposalQLen int, inboxLen int, pendingProposals int, err error) {
	engNode := h.engine.GetNode(shardID)
	if engNode == nil {
		return false, 0, 0, 0, ErrShardNotReady
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
	return d.CommitPending, d.ProposalQLen, d.InboxLen, pp, nil
}

// DiagShardGates reports the snapshot gates that suspend a shard's state
// machine work, together with the number of apply closures still in flight.
//
// While recovering is true the shard's state machine is being replaced by a
// received snapshot: it applies nothing and extracts no committed entries,
// though it keeps voting and replicating. While saving is true a snapshot
// capture has quiesced applies. applyInFlight is the drain counter both
// gates wait on before touching the state machine; a non-zero value with a
// gate up means the drain has not completed yet.
//
// Both states are also surfaced through ShardInfo.Health as ShardRecovering
// and ShardSnapshotting.
func (h *Host) DiagShardGates(shardID uint64) (recovering, saving bool, applyInFlight int64, err error) {
	engNode := h.engine.GetNode(shardID)
	if engNode == nil {
		return false, false, 0, ErrShardNotReady
	}
	d := engNode.DiagState()
	return d.Recovering, d.Saving, d.ApplyInFlight, nil
}

// DiagRaftState returns diagnostic raft state for the given shard.
// Used for debugging pipeline stalls.
func (h *Host) DiagRaftState(shardID uint64) (term, committed, processed, lastIdx uint64, msgs int, state string, err error) {
	engNode := h.engine.GetNode(shardID)
	if engNode == nil {
		return 0, 0, 0, 0, 0, "", ErrShardNotReady
	}
	t, c, p, li, m, s := engNode.DiagRaftState()
	return t, c, p, li, m, s, nil
}

// DiagRemotes returns diagnostic remote state for debugging commit stalls.
func (h *Host) DiagRemotes(shardID uint64) (string, error) {
	engNode := h.engine.GetNode(shardID)
	if engNode == nil {
		return "", ErrShardNotReady
	}
	return engNode.DiagRemotes(), nil
}

// Close shuts down the host, stopping all shards and releasing resources.
// Close is idempotent. Close calls Drain() first to allow in-flight work
// to complete gracefully before tearing down subsystems.
//
// Shutdown always runs to completion; the returned error is the JOIN
// (errors.Join, so errors.Is reaches each cause) of everything that failed
// along the way: an incomplete Drain, each shard state machine's Close, the
// LogDB Close and the Seal of every barrier the host was given. This is the last durability signal the
// embedding application receives — for an on-disk state machine, the Close
// error is its backing store's final flush — so a nil return means every one
// of those succeeded, not merely that shutdown was attempted.
//
// A concurrent or repeated Close BLOCKS until the shutdown that is already
// running has finished, and returns that same error. Returning nil to the
// second caller — which is what the closed-flag CAS alone did — reported a
// clean shutdown before any of it had happened, and reported it to precisely
// the caller most likely to act on it: the deferred Close in a supervisor that
// races the explicit one in the shutdown path.
func (h *Host) Close() error {
	// Only the first caller fires the event; the CAS is what elects it. The
	// closed flag also fails new work fast, so it is set before any of the
	// (potentially slow) shutdown work below begins.
	if h.closed.CompareAndSwap(false, true) {
		if el := h.cfg.EventListener; el != nil && el.OnNodeHostShuttingDown != nil {
			el.OnNodeHostShuttingDown()
		}
	}

	// sync.Once.Do does not return until f has completed, for EVERY caller —
	// that is what makes the join below correct for the losers of the CAS as
	// well as the winner, and what publishes h.closeErr to them safely.
	var closeErr error
	h.stopOnce.Do(func() {
		// Drain in-flight work first. If drain times out, proceed with
		// shutdown anyway — but REPORT it. A drain that timed out abandoned
		// in-flight work, and returning nil from Close after that tells the
		// embedding application its shutdown was clean when proposals it
		// accepted were dropped.
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

		// Stop the engine FIRST. The engine's step workers are the only
		// producers of snapshot sends (transportSender.SendSnapshot, which
		// does snapshotWg.Add(1)). engine.Stop() closes stepStopC and joins
		// the step workers, after which no new snapshotWg.Add can occur.
		// Stopping the engine before ts.Wait() eliminates the illegal
		// "WaitGroup.Add concurrent with Wait" race (a producer could call
		// Add(1) while Close was already blocked in Wait()).
		if h.engine != nil {
			h.engine.Stop()
		}

		// Drain in-flight snapshot sends that the (now-stopped) step workers
		// launched before exiting. The transport is still up, so these sends
		// complete normally. Because no new sends can be started after
		// engine.Stop(), this Wait() races with no concurrent Add.
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

		// Stop all nodes. Complete any remaining pending proposals,
		// reads, and snapshots with ErrClosed so callers are not left
		// blocking forever on channels that will never be written.
		//
		// Collect nodeState pointers so a second sweep can catch any
		// proposals registered between the first sweep visiting a shard
		// and Propose/ReadIndex finishing its registration (TOCTOU race
		// between h.closed.Load() in Propose and this sweep).
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

		// Second sweep: catch proposals or reads that were registered
		// after the first sweep visited their shard but before the
		// engine fully stopped. Goroutines that passed h.closed.Load()
		// and getNode() before the first sweep may have registered new
		// pending entries between the first sweep and now.
		// RequestState.complete is CAS-guarded, so re-completing
		// already-completed entries is a safe no-op.
		for _, ns := range stoppedNodes {
			h.failPendingRequests(ns, ErrClosed)
		}

		// Close each node's state machine to release resources such as
		// on-disk file locks (e.g., Pebble). Without this, a stopped
		// shard would leave its DiskStateMachine open, causing "lock
		// held by current process" errors on Host restart.
		//
		// Each failure is JOINED into closeErr rather than only logged. For an
		// on-disk state machine this call is the backing store's final
		// flush-and-close, so it is the last durability signal the embedding
		// application ever receives; a Close that returns nil over a failed
		// flush reports a clean shutdown for a host that lost writes. Every
		// shard is still closed — one failure must not strand another shard's
		// file lock — so the loop collects instead of returning early.
		for _, ns := range stoppedNodes {
			if ns.rsm != nil {
				if err := ns.rsm.Close(); err != nil {
					h.logger.Warn("state machine close failed during host close",
						"shard_id", ns.shardID, "error", err)
					closeErr = errors.Join(closeErr, err)
				}
				// Do NOT nil ns.rsm here. An in-flight reader (SyncRead/
				// StaleRead) may still hold this nodeState and read the rsm
				// field concurrently; writing it would be a data race. The
				// node was already removed from h.nodes (sweep 1) so no new
				// caller can reach it, and Host.Close is sync.Once-guarded so
				// the SM is closed exactly once — nil-ing added nothing but
				// the race. Readers gate on ns.stopped (set in sweep 1).
			}
		}

		// Close LogDB.
		if h.logdb != nil {
			if err := h.logdb.Close(); err != nil {
				closeErr = errors.Join(closeErr, err)
			}
		}

		// Seal every barrier the host was given: the host-wide one and each
		// per-shard barrier the resolver returned. They are sealed last, after
		// every shard, snapshot and the LogDB that encrypt under them have
		// stopped. A per-shard barrier that IS the host-wide barrier (pointer
		// identity, logdb.SameBarrier) was sealed with it; one that cannot be
		// identified is sealed again, and its ErrBarrierSealed is benign.
		closeErr = errors.Join(closeErr, sealOnClose(h.barrier))
		h.shardBarriersGiven.each(func(b Barrier) {
			if !logdb.SameBarrier(b, h.barrier) {
				closeErr = errors.Join(closeErr, sealOnClose(b))
			}
		})

		h.logger.Info("host closed", "id", h.id)
		h.closeErr = closeErr
	})

	return h.closeErr
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

// Drain transitions the host into drain mode as part of graceful shutdown.
// Once called, new proposals and ReadIndex requests are permanently rejected
// with ErrDraining. In-flight proposals, reads, and snapshots are allowed to
// complete. Drain blocks until all in-flight work has completed or the
// configured ShutdownTimeout expires. If the timeout expires before all work
// completes, ErrShutdownTimeout is returned.
//
// Drain is irreversible: the draining state cannot be cleared without stopping
// and restarting the host. It is intended to be called immediately before Close
// as the first step of a graceful shutdown sequence. Callers that need to
// resume accepting proposals must create a new Host instance.
//
// Drain is idempotent, and every caller gets the SAME answer: there is one
// drain attempt, and a concurrent second caller blocks until it has finished
// and then returns its result. This is what makes Drain a join barrier for
// Close, which calls it first: a Close that raced an in-progress Drain and
// got nil back immediately would tear the engine down while that drain was
// still waiting and fail the in-flight work with ErrClosed — driving
// pendingOps to zero and releasing the FIRST caller with nil as well — so
// both would report a clean graceful shutdown after accepted work had been
// aborted. A call after the attempt has completed returns its recorded result
// without waiting.
//
// The implementation is event-driven: instead of polling at fixed intervals,
// it uses an atomic counter (pendingOps) decremented by operation completion
// callbacks. When the counter reaches 0, a notification channel is closed.
func (h *Host) Drain() error {
	// The flag goes up before the Once so that new work is refused from the
	// instant ANY caller asks for a drain, not from when the elected attempt
	// gets scheduled. It is never cleared: see the irreversibility note.
	h.draining.Store(true)

	// sync.Once.Do does not return until f has completed, for EVERY caller —
	// the losers of the election wait here for the winner's attempt and then
	// read the drainErr it published.
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

// addPendingOp increments the outstanding pending operations counter.
// Called when a proposal, read, or snapshot operation is registered.
func (h *Host) addPendingOp() {
	h.pendingOps.Add(1)
}

// completePendingOp decrements the outstanding pending operations counter.
// When the counter reaches 0 and the host is draining, the drain
// notification channel is closed to unblock Drain().
//
// The CAS loop prevents the counter from going negative on double-completion.
// A RequestState can be completed twice when both the normal completion path
// (OnApplied) and the sweep/timeout path race. Without this guard, each
// extra completion would push the counter below zero, causing Drain() to
// close drainC prematurely on a future addPendingOp/completePendingOp cycle.
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

// failPendingRequests completes all pending proposals, reads, and snapshots
// on the given nodeState with the provided error. It is for teardown — the
// shard is going away, so nothing pending can ever complete. Leadership
// changes must use failPendingClientRequests instead, which leaves local
// snapshot work alone.
//
// This is safe to call multiple times; RequestState.complete uses CAS so
// duplicate completions are no-ops.
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

// failPendingClientRequests completes the pending requests whose outcome
// depends on LEADERSHIP, or on the shard still running — proposals (session
// operations and config changes included) and reads — with the provided error,
// and leaves pending snapshots untouched. OnShardFailed uses it for a halted
// shard.
//
// A proposal gets err reported as maybe-applied (failClaimedProposal): it was
// registered, so it has been or will be handed to raft. A read gets err as
// given: it appends nothing that could commit.
//
// Saving a snapshot is a purely LOCAL operation: it reads this replica's own
// state machine and writes this replica's own disk, and it proceeds regardless
// of who leads the shard. Failing those requests on every leadership change
// (elections are routine) completed the caller with NotLeaderError while the
// save kept running, so the retry it prompted hit ErrSnapshotInProgress against
// the still-running save. Only teardown, where the save really is abandoned,
// fails them — see failPendingRequests.
//
// Safe to call multiple times; RequestState.complete is CAS-guarded.
func (h *Host) failPendingClientRequests(ns *nodeState, err error) {
	h.failPendingProposals(ns, err, false)
	h.failPendingReads(ns, err)
}

// failLeaderChangeRequests fails the requests a leadership change strands:
// every pending read, and every proposal already handed to raft — taken from
// the proposal queue, so appended at the old leader or forwarded to it, where
// it can still commit (maybe-applied). A proposal the step worker has not yet
// taken is left pending: it has not reached raft, and the step worker resolves
// it — appended or forwarded under the new leader, or refused definitely
// through OnProposalRejected — so failing it here would hand it a
// maybe-applied result it does not deserve, racing that refusal.
func (h *Host) failLeaderChangeRequests(ns *nodeState, err error) {
	h.failPendingProposals(ns, err, true)
	h.failPendingReads(ns, err)
}

// failPendingProposals completes the pending proposals of ns with err. Each
// is classified BEFORE it is claimed, by whether it may have reached raft
// (proposalMayReachRaft): if so the failure is maybe-applied, otherwise —
// never taken from a queue that is now closed — definite. With
// onlyHandedOff, a proposal not yet taken from the queue is left pending.
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
		// Claim the entry atomically. pendingProposals is a lock-free
		// sync.Map shared with OnApplied/OnProposalFailed (which use
		// LoadAndDelete). A plain Range+Delete+releaseRef would double-count
		// the ref when a concurrent completion removes the same key first,
		// driving outstanding negative and recycling the RequestState early.
		// Only the goroutine whose CompareAndDelete actually removes this
		// entry completes and releases it.
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

// keyRotationLoop periodically rotates the barrier epoch. It runs as a
// background goroutine and exits when keyRotStopC is closed. After each
// rotation, it logs the new epoch. The loop is a simple ticker-based
// design; callers handle the complexity of coordinating WAL segment
// rotation externally.
//
// In addition to the scheduled interval, a faster threshold-check ticker
// detects when the AEAD invocation counter has reached its rotation threshold
// and triggers an immediate rotation to prevent exceeding NIST SP 800-38D limits.
//
// Every rotation and purge the loop starts runs under a context cancelled when
// keyRotStopC closes, so a barrier whose sealer blocks in a provider call
// cannot hold the host's shutdown open past the provider's own honouring of
// the cancellation.
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

// endedBy reports whether err was caused by ctx ending: ctx has ended and err
// matches the reason. A failure of its own that merely happens while ctx is
// ending — a missing state persister, a persist error, a superseded barrier —
// does not match, and is reported at its normal level.
func endedBy(ctx context.Context, err error) bool {
	ended := ctx.Err()
	return ended != nil && errors.Is(err, ended)
}

// rotateBarrierKey performs a barrier key rotation and optionally purges old
// DEKs when MaxRetainedEpochs is configured. Extracted from keyRotationLoop
// to avoid duplication between scheduled and threshold-triggered rotations.
//
// A rotation or purge cut short by ctx — the loop's lifetime context, cancelled
// when the host closes — is logged at DEBUG: nothing was persisted and the
// current epoch stays active. Any other failure keeps its normal level even
// when it surfaces during shutdown.
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
		// Rotate fails closed when the barrier has no state persister: a
		// rotation that is not persisted would leave every record written
		// under the new epoch undecryptable after a restart. The condition is
		// a permanent misconfiguration, not a transient failure, so the
		// operator has to be told exactly what is broken and what it costs.
		// It is NOT fatal to the host: reads and writes continue under the
		// current epoch until the AEAD invocation limit is reached.
		h.logger.Error("at-rest key rotation is DISABLED: the barrier rejected the rotation; "+
			"configure a barrier state persister to enable it, otherwise the current epoch key "+
			"is never replaced and writes will eventually fail at the AEAD invocation limit",
			"error", err)
		return
	}
	h.logger.Info("key rotation completed", "epoch", newEpoch)

	if h.cfg.MaxRetainedEpochs > 0 && newEpoch > h.cfg.MaxRetainedEpochs {
		purgeThreshold := newEpoch - h.cfg.MaxRetainedEpochs

		// Clamp the retention-based threshold to the GLOBAL safe-purge
		// floor: the minimum epoch still needed by ANY shard's retained
		// encrypted snapshot or any live WAL record. Without this clamp,
		// rotation alone could purge a DEK still required to decrypt a
		// persisted snapshot or WAL, permanently bricking encrypted data.
		//
		// Fail safe: when the floor cannot be determined (errors scanning
		// snapshot dirs / MinLiveEpoch error, or nothing encrypted is
		// retained), skip purging entirely rather than purge on uncertainty.
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

// pendingSweepLoop periodically scans all node states and completes
// pending proposals, reads, and snapshots whose deadlines have expired.
// This prevents orphaned entries from accumulating indefinitely when
// entries are lost due to leader crashes or network partitions. Callers
// using context.Background() with async Propose would otherwise block
// forever on an entry that will never be applied.
//
// The sweeper runs every pendingSweepInterval and exits when sweepStopC
// is closed. It is started by NewHost and stopped by Close.
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

// pendingSweepLoopOnce runs the pending sweep loop body and recovers
// from panics. Returns true if the loop panicked, false if it returned
// normally (stop channel closed). The interval parameter controls the
// sweep ticker period.
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

// sweepExpiredPending iterates all loaded node states and completes
// pending proposals, reads, and snapshots whose RS deadline has passed.
// Each expired entry is completed with ErrTimeout and removed from its
// pending map. The method is safe to call concurrently with proposal
// registration and completion; it acquires the per-map mutex for each
// scan. RequestState.complete uses CAS, so racing with other completion
// paths is a harmless no-op.
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
		// Classified before the claim: it lost its wait, and unless its node
		// can provably no longer hand it to raft the timeout is maybe-applied.
		handedOff := proposalMayReachRaft(pp)
		// Claim atomically before completing/releasing: pendingProposals is a
		// lock-free sync.Map shared with OnApplied/OnProposalFailed. Without
		// the LoadAndDelete guard a concurrent completion of the same key
		// would cause a double releaseRef (outstanding goes negative, the
		// RequestState is recycled early). Only the remover acts.
		if _, removed := ns.pendingProposals.LoadAndDelete(key); !removed {
			return true
		}
		h.failClaimedProposal(pp, ErrTimeout, handedOff)
		return true
	})
}

// sweepPendingReads expires read requests whose deadline has passed.
//
// When an expired entry is a batch key with coalesced aliases, its
// UNCONFIRMED aliases are expired with it: an unconfirmed alias is
// reachable ONLY through the batch key's pendingRead (OnReadyToRead
// looks up the batch key and walks pr.aliases), so once the batch entry
// is removed a deadline-less alias could never complete and its
// RequestState would never return to the pool. Confirmed aliases are
// left in place — confirmation (which happens for the batch key and all
// aliases atomically under pendingReadsMu) makes each alias
// independently completable by the OnApplied read scan.
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

// EmergencySeal immediately seals the barrier without waiting for in-flight
// operations to complete. All in-flight operations will fail with
// ErrBarrierSealed. This is for security incident response where speed
// matters more than graceful shutdown.
func (h *Host) EmergencySeal() error {
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

// HealthCheck returns a point-in-time health assessment of the host.
// The returned HealthStatus includes barrier state, disk condition,
// shard count, per-shard health information, and the process-wide count of
// library log records dropped because the slog handler was not taking them.
//
// Status values:
//   - "sealed"    if a barrier is configured and sealed (HTTP 503)
//   - "unhealthy" if no shards are loaded (HTTP 503)
//   - "degraded"  if any shard has ShardError health (HTTP 503)
//   - "ok"        if all shards are healthy (HTTP 200)
func (h *Host) HealthCheck() *HealthStatus {
	sealed := h.barrier != nil && h.barrier.IsSealed()
	diskOK := h.diskFull == nil || !h.diskFull.Load()

	shards := make([]ShardInfo, 0, clampNonNegative(h.activeNodes.Load()))
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
		shards = append(shards, *h.buildShardInfo(ns))
		return true
	})

	status := &HealthStatus{
		BarrierSealed:      sealed,
		DiskOK:             diskOK,
		ShardCount:         len(shards),
		Shards:             shards,
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
	}

	return status
}

// NodeIdentity returns this host's own stable node identity: the value peers
// must have committed for it before they will count its quorum-bearing
// messages under the identity binding.
//
// It is derived from the leaf certificate this host presents — the SHA-256 of
// its SubjectPublicKeyInfo — so it is stable across certificate RENEWAL on the
// same keypair and changes only when the keypair does. An operator collects it
// from each node (it is safe to log, print and copy: it is a hash of a public
// key) and passes it to the shard through Config.MemberIdentities at bootstrap
// or RequestAddNodeWithIdentity afterwards.
//
// It returns "" when this host has no transport (WithoutTransport), which has
// no certificate and therefore no identity.
func (h *Host) NodeIdentity() string {
	if h.transport == nil {
		return ""
	}
	return h.transport.LocalNodeIdentity()
}

// RequestAddNode proposes adding a replica to the shard via a Raft
// config change. The configChangeIndex should be obtained from
// SyncGetShardMembership().ConfigChangeID to ensure ordered config
// changes. Pass 0 to skip the ordering check.
//
// It commits NO node identity for the replica. On a host with a transport that
// is refused with ErrMemberIdentityRequired unless the configuration already
// holds an identity for the replica (a role change), because a member is
// authorized by its identity and by nothing else; use
// RequestAddNodeWithIdentity. It remains the form for a transportless host,
// where nothing crosses the wire.
func (h *Host) RequestAddNode(ctx context.Context, shardID, replicaID uint64, addr string, configChangeIndex uint64) (*RequestState, error) {
	return h.RequestAddNodeWithIdentity(ctx, shardID, replicaID, addr, "", configChangeIndex)
}

// RequestAddNodeWithIdentity proposes adding a replica together with the stable
// NODE IDENTITY the shard should commit for it — the value that node returns
// from Host.NodeIdentity.
//
// Once committed, every replica of the shard authorizes that replica's
// quorum-bearing messages by comparing this identity against the authenticated
// identity of the connection they arrive on, and stops using its address as an
// authorization input entirely. That is what makes a wildcard or shared-SAN
// certificate useless for asserting this replica's msg.From, and what lets the
// replica move to a new address without its votes and acks going uncounted.
//
// An empty identity means "commit no identity", which is exactly
// RequestAddNode, with the same refusal behind a transport.
//
// CONSIDERED AND REJECTED: capturing the identity from the joining node's own
// connection instead of taking it as an argument. It reads as the more
// convenient design and it is unsound in precisely the threat model identities
// exist to close. The capture would have to decide WHICH connection speaks for
// the replica being added, and the only evidence available is the certificate's
// address coverage — so a peer holding a promiscuous certificate connects,
// claims the target's address, and has ITS OWN identity committed for that
// replica ID. That is worse than no identity at all: the attacker becomes the
// replica permanently, and the legitimate node is locked out by the very
// mechanism meant to protect it. It is also not usually possible — a node is
// customarily added BEFORE it is started, so there is no connection to capture.
// The identity must therefore come from outside the cluster, from the operator
// or the provisioning system that issued the certificate.
func (h *Host) RequestAddNodeWithIdentity(ctx context.Context, shardID, replicaID uint64,
	addr, identity string, configChangeIndex uint64) (*RequestState, error) {

	return h.requestConfigChange(ctx, shardID, proto.ConfigChange{
		ConfigChangeID: configChangeIndex,
		Type:           proto.AddNode,
		ReplicaID:      replicaID,
		Address:        addr,
		NodeIdentity:   identity,
	})
}

// SyncRequestAddNodeWithIdentity adds a replica together with its committed
// node identity and blocks until the membership change is committed. See
// RequestAddNodeWithIdentity for what the identity buys and why it is an
// argument rather than something the leader discovers.
//
// The context must carry a deadline or timeout to bound the wait.
func (h *Host) SyncRequestAddNodeWithIdentity(ctx context.Context, shardID, replicaID uint64,
	addr, identity string, configChangeIndex uint64) error {

	rs, err := h.RequestAddNodeWithIdentity(ctx, shardID, replicaID, addr, identity, configChangeIndex)
	return awaitConfigChange(ctx, rs, err)
}

// awaitConfigChange is the body every Sync* membership call shares: it takes
// the (RequestState, error) pair its asynchronous form returned, waits for the
// change to commit, and maps the context's own expiry to ErrTimeout /
// ErrCanceled. A submission error is returned as it was.
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

// SyncRequestAddNode adds a replica to the shard's membership and blocks
// until the membership change is committed. This is a convenience wrapper
// around RequestAddNode for callers that prefer a synchronous API.
//
// The context must carry a deadline or timeout to bound the wait.
func (h *Host) SyncRequestAddNode(ctx context.Context, shardID, replicaID uint64, addr string, configChangeIndex uint64) error {
	rs, err := h.RequestAddNode(ctx, shardID, replicaID, addr, configChangeIndex)
	return awaitConfigChange(ctx, rs, err)
}

// SyncRequestDeleteNode removes a replica from the shard's membership and
// blocks until the membership change is committed. This is a convenience
// wrapper around RequestRemoveNode for callers that prefer a synchronous API.
//
// The context must carry a deadline or timeout to bound the wait.
func (h *Host) SyncRequestDeleteNode(ctx context.Context, shardID, replicaID uint64, configChangeIndex uint64) error {
	rs, err := h.RequestRemoveNode(ctx, shardID, replicaID, configChangeIndex)
	return awaitConfigChange(ctx, rs, err)
}

// SyncRequestAddObserver adds a non-voting observer to a shard and blocks
// until the membership change is committed.
//
// The context must carry a deadline or timeout to bound the wait.
func (h *Host) SyncRequestAddObserver(ctx context.Context, shardID, replicaID uint64, addr string, configChangeIndex uint64) error {
	rs, err := h.RequestAddObserver(ctx, shardID, replicaID, addr, configChangeIndex)
	return awaitConfigChange(ctx, rs, err)
}

// SyncRequestAddObserverWithIdentity is SyncRequestAddObserver carrying the
// observer's node identity; see RequestAddNodeWithIdentity.
func (h *Host) SyncRequestAddObserverWithIdentity(ctx context.Context, shardID, replicaID uint64,
	addr, identity string, configChangeIndex uint64) error {

	rs, err := h.RequestAddObserverWithIdentity(ctx, shardID, replicaID, addr, identity, configChangeIndex)
	return awaitConfigChange(ctx, rs, err)
}

// SyncRequestAddWitness adds a voting witness to a shard and blocks
// until the membership change is committed.
//
// The context must carry a deadline or timeout to bound the wait.
func (h *Host) SyncRequestAddWitness(ctx context.Context, shardID, replicaID uint64, addr string, configChangeIndex uint64) error {
	rs, err := h.RequestAddWitness(ctx, shardID, replicaID, addr, configChangeIndex)
	return awaitConfigChange(ctx, rs, err)
}

// SyncRequestAddWitnessWithIdentity is SyncRequestAddWitness carrying the
// witness's node identity; see RequestAddNodeWithIdentity.
func (h *Host) SyncRequestAddWitnessWithIdentity(ctx context.Context, shardID, replicaID uint64,
	addr, identity string, configChangeIndex uint64) error {

	rs, err := h.RequestAddWitnessWithIdentity(ctx, shardID, replicaID, addr, identity, configChangeIndex)
	return awaitConfigChange(ctx, rs, err)
}

// RequestAddObserver requests adding a non-voting observer to a shard.
// Observers receive log entries but do not participate in quorum.
// The configChangeIndex should be obtained from
// SyncGetShardMembership().ConfigChangeID. It commits no identity, which a
// host with a transport refuses as RequestAddNode does; use
// RequestAddObserverWithIdentity there.
func (h *Host) RequestAddObserver(ctx context.Context, shardID, replicaID uint64, addr string, configChangeIndex uint64) (*RequestState, error) {
	return h.RequestAddObserverWithIdentity(ctx, shardID, replicaID, addr, "", configChangeIndex)
}

// RequestAddObserverWithIdentity adds a non-voting observer together with the
// node identity the shard should commit for it. See
// RequestAddNodeWithIdentity for what the identity buys.
//
// An observer's messages are quorum-bearing too — it acks replication, and its
// ReplicateResp advances a match index the leader reads — so it is bound by the
// same gate as a voter.
func (h *Host) RequestAddObserverWithIdentity(ctx context.Context, shardID, replicaID uint64,
	addr, identity string, configChangeIndex uint64) (*RequestState, error) {

	return h.requestConfigChange(ctx, shardID, proto.ConfigChange{
		ConfigChangeID: configChangeIndex,
		Type:           proto.AddNonVoting,
		ReplicaID:      replicaID,
		Address:        addr,
		NodeIdentity:   identity,
	})
}

// RequestAddWitness requests adding a voting witness to a shard.
// Witnesses participate in quorum but do not store a state machine.
// The configChangeIndex should be obtained from
// SyncGetShardMembership().ConfigChangeID. It commits no identity, which a
// host with a transport refuses as RequestAddNode does; use
// RequestAddWitnessWithIdentity there.
func (h *Host) RequestAddWitness(ctx context.Context, shardID, replicaID uint64, addr string, configChangeIndex uint64) (*RequestState, error) {
	return h.RequestAddWitnessWithIdentity(ctx, shardID, replicaID, addr, "", configChangeIndex)
}

// RequestAddWitnessWithIdentity adds a voting witness together with the node
// identity the shard should commit for it. See RequestAddNodeWithIdentity for
// what the identity buys.
//
// A witness votes and acks, so its forged messages buy an attacker exactly what
// a voter's do; committing its identity is no less important for holding no
// state machine.
func (h *Host) RequestAddWitnessWithIdentity(ctx context.Context, shardID, replicaID uint64,
	addr, identity string, configChangeIndex uint64) (*RequestState, error) {

	return h.requestConfigChange(ctx, shardID, proto.ConfigChange{
		ConfigChangeID: configChangeIndex,
		Type:           proto.AddWitness,
		ReplicaID:      replicaID,
		Address:        addr,
		NodeIdentity:   identity,
	})
}

// RequestUpdateWitnessPolicy proposes replacing the shard's replicated
// witness policy (config.WitnessConfig semantics: MaxWitnesses caps the
// witness count, RequireFullReplicaMajority keeps the full replicas able
// to form a quorum on their own). The change goes through Raft like any
// membership change, so every replica adopts the new policy atomically at
// the same log position. The new policy must be satisfied by the current
// membership: tightening a policy the shard already violates is rejected
// with ErrTooManyWitnesses or ErrBreaksFullReplicaMajority — reshape the
// membership first, then tighten. configChangeIndex carries the usual
// staleness guard (0 to skip).
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

// SyncRequestSnapshot requests a snapshot of the shard with the
// specified options and blocks until the snapshot is created or the
// context is canceled. Returns the snapshot index on success.
//
// A zero-value SnapshotOption{} uses shard-level defaults.
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

// RemoveData removes all LogDB data for the given shard/replica pair and
// retires the replica ID on this host: StartShard refuses it from then on
// (ReplicaRetiredError), across restarts. The shard must be
// stopped on this host before calling RemoveData. This is used to clean up
// persistent state after a node has been permanently removed from a shard; a
// replica that is to rejoin does so under a new replica ID, as Raft requires.
func (h *Host) RemoveData(shardID, replicaID uint64) error {
	if h.closed.Load() {
		return ErrClosed
	}

	// Take the shard's import lock, which StartShard holds for the whole of a
	// start: the checks below and the removal then cannot interleave with a
	// start that checked the replica was not retired and has not yet
	// published its node (see StartShard's retirement check). Only tried, as
	// StartShard does; the caller retries a transient refusal.
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
		// Not loaded here: assume the replica was already retired via
		// RemoveData (which writes the tombstone and runs segment GC), and
		// reclaim whatever is still reclaimable. Returning success keeps the
		// canonical StopShard -> RemoveData -> RequestCompaction sequence
		// working instead of failing on a step whose work already happened.
		//
		// Deliberately NOT dragonboat's CompactEntriesTo(MaxUint64): this
		// host cannot distinguish "removed" from "merely stopped", and a
		// stopped replica's log is still live data it needs on restart —
		// compacting it to MaxUint64 would destroy a healthy replica.
		// RemoveEntriesTo is also unsafe post-RemoveData: it would re-create
		// the compactedTo and metaSegments entries that RemoveNodeData just
		// deleted, pinning a segment for a node that no longer exists.
		// Segment GC alone honors the existing markers and cannot lose data.
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

// GetLogReader returns a read-only view of the Raft log for the given
// shard. The returned ReadonlyLogReader provides access to log range
// and node state without permitting mutations.
func (h *Host) GetLogReader(shardID uint64) (ReadonlyLogReader, error) {
	if h.closed.Load() {
		return nil, ErrClosed
	}

	engNode := h.engine.GetNode(shardID)
	if engNode == nil {
		return nil, ErrShardNotFound
	}

	return &logReaderAdapter{reader: engNode.LogReader()}, nil
}

// LogRange represents the valid index range of a shard's Raft log.
// FirstIndex is the first available entry index (inclusive) and
// LastIndex is the last available entry index (inclusive).
type LogRange struct {
	FirstIndex uint64
	LastIndex  uint64
}

// QueryRaftLog returns Raft log entries in the half-open range
// [firstIndex, lastIndex) for the given shard, up to maxSize bytes
// total. This is a local operation that reads directly from the
// host's LogDB and does not require leadership.
//
// Returns ErrClosed if the host is closed, ErrShardNotFound if the
// shard does not exist on this host, and ErrInvalidRange if
// firstIndex >= lastIndex or maxSize is zero.
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

	// GetRangeCopy handles the compacted/unavailable checks and returns entries
	// the caller fully owns, deep-copied UNDER the reader lock. QueryRaftLog runs
	// on an arbitrary API-caller goroutine, so it must NOT use GetRange — that
	// hands out a buffer the step worker retains past the lock, and copying it
	// here (after the lock) would race a concurrent step-worker GetRange.
	result, err := lr.GetRangeCopy(firstIndex, lastIndex, maxSize)
	if err != nil {
		return nil, err
	}
	return result, nil
}

// GetLogRange returns the available log index range for the given shard.
// The returned LogRange contains the first and last available entry indices
// (both inclusive). Returns ErrClosed if the host is closed, or
// ErrShardNotFound if the shard does not exist on this host.
func (h *Host) GetLogRange(shardID uint64) (LogRange, error) {
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

// requestConfigChange is the shared implementation for all membership
// operations. It serializes the ConfigChange, wraps it in an
// EntryConfigChange entry, and delivers it via the engine inbox.
//
//nolint:gocyclo // one funnel for every membership change: role validation, witness-policy gating, cert exclusivity, claim/rollback and the pending-op bookkeeping; splitting it would move validation away from the rollback that pairs with it, which is exactly where this path has produced defects
func (h *Host) requestConfigChange(ctx context.Context, shardID uint64, cc proto.ConfigChange) (*RequestState, error) {
	// A context that is ALREADY done must not submit a QUORUM CHANGE. This is
	// the same guard Propose has, applied to the path where the
	// consequence is worst: without it the caller receives ErrCanceled while
	// the membership change commits and applies, so it cannot tell whether the
	// voting set changed. A retry after that answer issues a duplicate or
	// conflicting change against a configuration it has mis-read.
	//
	// Cancellation racing an in-flight config change remains undecidable — see
	// MaybeAppliedError — but a context that was done BEFORE the call is not a
	// race, and it neither submits nor reports success.
	if err := ctx.Err(); err != nil {
		return nil, mapContextError(err)
	}
	configChangeIndex := cc.ConfigChangeID
	if h.closed.Load() {
		return nil, ErrClosed
	}
	// Drain is a one-way transition into shutdown: it stops accepting NEW work
	// and waits only for what is already in flight. Propose, ReadIndex and the
	// session path all refuse here; a config change did not, so a membership
	// change submitted during drain either raced the drain deadline (accepted,
	// never waited for, host torn down mid-commit) or was completed with
	// ErrClosed by the Close sweep — in both cases the caller cannot tell
	// whether the voting set changed. Refuse it for the same reason a proposal
	// is refused, and for a quorum change the reason is stronger.
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

	// Validate ConfigChangeID staleness. When the caller provides a non-zero
	// configChangeIndex (obtained from SyncGetShardMembership), verify it
	// matches the shard's current configuration version. This prevents
	// stale config changes from being proposed when two clients race to
	// modify the membership (PhD thesis Section 4.1).
	//
	// NOTE: This check is intentionally conservative. The atomic load of
	// configChangeIndex is not linearizable with the Raft log, so a
	// concurrent config change that commits between the caller's
	// SyncGetShardMembership and this check may cause a false rejection
	// (false positive). This is safe: the caller simply re-reads the
	// membership and retries. False negatives (accepting a truly stale
	// index) cannot occur because configChangeIndex is monotonically
	// updated on apply and this check requires an exact match.
	if configChangeIndex != 0 {
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

	// Pre-validate the config change against the current membership before
	// delivering it to the engine. The Host proposal path reaches
	// membership.apply on commit (Peer.Commit -> processConfigChanges),
	// which deliberately does NOT re-check the voting-member cap so that
	// committed changes always replay. Without this pre-check an invalid
	// change (cap exceeded, duplicate add, last-voter removal) would either
	// have its apply error swallowed — leaving the caller's RequestState to
	// hang until deadline — or, for a 65th voting member, pass apply and then
	// panic rebuildReplicaToBit's invariant. Returning the validation error
	// here gives the caller a prompt, actionable failure instead.
	if err := engNode.Peer().ValidateConfigChange(cc); err != nil {
		return nil, err
	}

	// Re-run the certificate-exclusivity precondition for the address this
	// change introduces.
	//
	// StartShard checks it against the INITIAL members map, which leaves the
	// grow-by-join flow completely unchecked: the seed node starts with only
	// itself (so the peer list is empty after self-filtering) and every joiner
	// starts with an empty map, so no node ever evaluates it. Under a wildcard
	// or shared cluster certificate that means the deployment the check exists
	// to refuse comes up clean, and every in-band authorization the transport
	// performs (SourceAddress clearing, peerEpoch mutation and live-outbound
	// eviction, snapshot-sender binding) is satisfied by every peer for every
	// other peer's address.
	//
	// A membership add is the point at which a new peer host first becomes
	// known to this node, so it is where the precondition can be evaluated at
	// all. Failing here is also the least damaging place to fail: the shard
	// keeps running with its current membership and the operator gets the
	// error on the API call rather than at the next restart.
	if cc.Address != "" {
		if err := h.verifyMemberCertificateExclusivity(map[uint64]string{
			cc.ReplicaID: cc.Address,
		}); err != nil {
			return nil, err
		}
	}

	// Pre-register the address in the transport registry for Add operations.
	// Config changes take effect on commit, and the leader adds the new
	// remote (syncRaftRemotes in Peer.Commit) and starts replicating to it
	// the moment the entry commits. The transport must be able to resolve
	// the new node's address at that point, not later when the entry is
	// applied to the state machine. Without this, the first Replicate
	// messages are silently dropped (registry miss), causing the new node
	// to never receive entries when no subsequent proposals create new log
	// entries to trigger resends.
	//
	// Register overwrites (Swap), so the prior mapping is captured first:
	// AddNode on an EXISTING observer (observer -> voter promotion) targets a
	// replicaID that already has a live registry entry, and an unconditional
	// Remove on rollback would delete the address of a replica that is still
	// a member and still being replicated to. A follower/observer never
	// initiates traffic, so learnSourceAddress cannot heal the deletion and
	// the replica would fall behind until the next committed config change,
	// snapshot recovery, or host restart.
	preRegistered := cc.Address != "" && (cc.Type == proto.AddNode || cc.Type == proto.AddNonVoting || cc.Type == proto.AddWitness)
	var priorAddr string
	var hadPrior bool
	if preRegistered {
		if addr, resolveErr := h.registry.Resolve(shardID, cc.ReplicaID); resolveErr == nil && addr != "" {
			priorAddr, hadPrior = addr, true
		}
		h.registry.Register(shardID, cc.ReplicaID, cc.Address)
	}
	// Roll back the speculative registration if the proposal never reaches
	// the log or never commits. Leaving a stale (replicaID -> addr) mapping
	// for a config change that was never applied could misdirect later
	// traffic to a node that was never actually added. The rollback restores
	// the prior mapping when there was one rather than deleting it.
	//
	// The rollback acts ONLY if the mapping still holds the address this call
	// speculatively wrote: a later config change — or the apply-path
	// registration of THIS change after it committed — must win over a late
	// rollback from this superseded attempt. The compare and the mutation are a
	// single atomic step (CompareAndRegister / CompareAndRemove): a
	// Resolve-then-Remove would have a check-to-act gap in which a concurrent
	// apply could register the committed member between the Resolve and the
	// Remove, and the unconditional Remove would then delete that committed member's
	// mapping, silently black-holing the leader's Replicate traffic to it.
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
	if err := h.registerPendingProposal(ns, engNode, proposalKey, pp); err != nil {
		rollbackPreRegister()
		return nil, err
	}

	entry := proto.Entry{
		Type: proto.EntryConfigChange,
		Key:  proposalKey,
		Cmd:  buf,
	}
	if !engNode.DeliverTrackedProposal([]proto.Entry{entry}, &pp.enqueuedAt) {
		// Queue full — claim the entry before rolling back. The sweep and
		// failPendingRequests remove entries with LoadAndDelete-style
		// claims; an unconditional Delete here could pair with a sweep that
		// already completed the entry, double-counting releaseRef (driving
		// outstanding negative) and completePendingOp. Only the claimant
		// rolls back; rs.Release() is the caller's own reference and is
		// dropped unconditionally.
		if _, removed := ns.pendingProposals.LoadAndDelete(proposalKey); removed {
			rs.releaseRef()
			h.completePendingOp()
		}
		rs.Release()
		rollbackPreRegister()
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
		Paused:               ns.paused.Load(),
		Replicas:             h.replicaProgress(ns.shardID),
	}
}

// shardIsObserver reports whether this replica is currently a non-voting
// observer of the shard. The role follows the committed membership — an
// observer whose promotion has been applied here is a voter without a restart —
// so it is read from the Raft peer's published membership. Config.IsObserver is
// only the initial hint, and is what a shard whose peer is not yet published
// reports.
func (h *Host) shardIsObserver(ns *nodeState) bool {
	engNode := h.engine.GetNode(ns.shardID)
	if engNode == nil || engNode.Peer() == nil {
		return ns.cfg.IsObserver
	}
	return engNode.Peer().IsObserver()
}

// replicaProgress converts the engine's published follower progress into the
// public form. It returns nil for a follower, for an unknown shard, and on the
// first call for a shard, because publication is opt-in and the next tick has
// not run yet.
//
// It reads the snapshot the step worker publishes rather than the Peer, so it
// takes no raft lock: GetShardInfo is an operator call and must not be able to
// queue behind a SaveState.
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

// shardLaggingThreshold is the number of committed-but-unapplied entries at
// which a loaded, led shard is reported as ShardLagging. It is deliberately
// generous: a healthy shard under load carries a small apply backlog, so the
// threshold only trips when the state machine is falling meaningfully behind.
const shardLaggingThreshold uint64 = 1000

// shardHealth derives the reported health of a loaded shard. ns.health only
// records the coarse lifecycle state (healthy once started, unloaded once
// stopped); the failure, no-leader, and lagging conditions are live and are
// derived here from the engine node so that a wedged shard is not reported
// as healthy. Only a stored ShardHealthy is refined — ShardUnloaded is a
// terminal lifecycle state and must not be overridden.
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
	// A snapshot gate is the most specific answer available: while it is up
	// the replica applies nothing, serves no up-to-date local read, and (for
	// recovery) extracts no committed entries at all. Reporting such a
	// replica as healthy is what makes a snapshot-recovery outage so hard to
	// diagnose — it looks fine from every angle the API exposes. Checked
	// before the leader and lag conditions, which are consequences of it.
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

// buildTransportConfig maps a HostConfig into the internal
// transport.Config, combining the host-level ListenAddress
// and DeploymentID with the user-facing config.TransportConfig tuning
// parameters.
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
			// Fail closed by default: the checker enforces unless the operator
			// explicitly opted into soft-fail. The internal field carries the
			// same name and sense as the public one, so this is a copy rather
			// than a negation that could be dropped or inverted by mistake.
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

// maxMembershipReconstructBytes bounds each chunk read during config-in-log
// membership reconstruction, so a committed log holding large data entries
// between config changes is not pulled into memory whole. limitEntrySize returns
// at least one entry per read and the loop advances by the last index seen, so
// the bound cannot stall progress.
const maxMembershipReconstructBytes = 4 << 20 // 4 MiB

// reconstructShardMembership recovers a shard's INITIAL membership from its
// committed log for a restart whose membership was not carried by a snapshot
// (config-in-log).
//
// It reads exactly ONE entry: the bootstrap EntryInitialMembership at
// BootstrapMembershipIndex. That is deliberate: it is O(1), where scanning the
// whole committed log for config entries is not, and it loses nothing.
//
// # Why one entry is enough
//
// The bootstrap entry is the authoritative BASE — the founding voter set that
// exists nowhere else in the log. Every membership change ABOVE it is an ordinary
// committed EntryConfigChange, and a restarting replica re-applies its committed
// entries through the normal pipeline (Peer.Commit -> processConfigChanges, and
// replayConfigChangesTo for an on-disk state machine that skips ahead), which
// folds each one into the membership. So the base comes from here and the deltas come from replay; nothing is lost.
//
// # Why not scan the whole log
//
// A shard with SnapshotEntries=0 never compacts, so nothingRecovered is true on
// EVERY restart and a full scan would read the ENTIRE committed log from disk —
// every data entry, just to find the rare config entries — on every restart.
// Under sustained write load that delays StartShard enough that a restarted
// replica cannot catch up (TestSoak_RollingRestartUnderLoad exercises this).
//
// It returns ok=true only when the bootstrap entry is present. ok=false means
// the entry is simply NOT AVAILABLE — compacted away, or below the reader's
// range — and that case always has a snapshot whose membership the caller uses
// instead; the caller falls back to the bootstrap seed plus commit-time repair.
//
// An index that IS readable and holds something OTHER than the bootstrap entry
// is a different thing entirely and fails closed with *BootstrapEntryMissingError.
// Config-in-log is the only bootstrap path this code base creates, so on a shard
// it made that condition means the log is damaged, and falling back
// would re-seed membership from the caller's members map — dropping witnesses,
// observers, Removed tombstones, identities and the replicated policies.
//
// The returned entry's Cmd is decoded before returning, so the reader's reusable
// buffer is not retained.
func (h *Host) reconstructShardMembership(lr *logdb.LogReader, commit uint64) (proto.Membership, bool, error) {
	first, last := lr.IndexRange()
	if first == 0 || commit < first {
		return proto.Membership{}, false, nil
	}
	// The bootstrap entry sits at a KNOWN index. If the log no longer starts at or
	// below it, it was compacted away and a snapshot carries the membership.
	if first > raft.BootstrapMembershipIndex || last < raft.BootstrapMembershipIndex ||
		commit < raft.BootstrapMembershipIndex {
		return proto.Membership{}, false, nil
	}
	entries, err := lr.GetRange(raft.BootstrapMembershipIndex,
		raft.BootstrapMembershipIndex+1, maxMembershipReconstructBytes)
	if err != nil {
		// ErrCompacted / ErrUnavailable mean the entry is simply not available:
		// benign, fall back to the seed + commit-time repair. Any OTHER error is a
		// genuine read/corruption failure and must FAIL CLOSED and abort StartShard,
		// exactly like every other step in recoverLogReader, rather than silently
		// letting the caller re-seed membership from its local members map (which
		// drops witnesses, observers, Removed tombstones and the replicated witness
		// policy — the split this reconstruction exists to prevent).
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
		// The log HAS an index 1 and it is not the founding configuration.
		//
		// Config-in-log is the only bootstrap this code base performs, and
		// every shard it creates lays an EntryInitialMembership down at
		// BootstrapMembershipIndex before any other entry can be appended. On a shard this code made, an ordinary
		// entry sitting at index 1 therefore means the log is DAMAGED.
		//
		// Returning "not found" here sends StartShard to the nothingRecovered
		// seed, which rebuilds membership from the CALLER'S members map and so
		// drops the witnesses, observers, Removed tombstones, committed
		// identities and the replicated witness and session policies the shard
		// agreed on — a replica that computes quorum over a different set than
		// its peers, silently. Damaged durable state has no safe reconstruction,
		// so fail closed and let the operator restore or re-provision.
		//
		// A log whose first index is ABOVE BootstrapMembershipIndex never
		// reaches here: it is compacted, and the snapshot that compacted it
		// carries the membership (the early return above).
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

// resolveRecoveredTerm returns the Raft term of index, which an on-disk state
// machine reported as its durably-applied position.
//
// The term is resolved from the log first and from the snapshot record second,
// and an index neither covers is an error rather than a zero. Seeding term 0
// is not a harmless default: it is stamped on the next snapshot record, and a
// follower that installs that snapshot reports lastTerm() == 0, which makes
// isUpToDate grant a vote to EVERY candidate — including one missing entries
// this replica holds committed. Failing the shard start is the safe outcome.
//
// Callers must pass the CLAMPED index (the value peer.SetProcessedIndex
// returned), not the raw lastApplied from Open. An on-disk SM whose applied
// position outruns the recovered HardState.Commit — a lost state fsync — is a
// state the clamp exists to repair; resolving against the unclamped index both
// pairs a term with an index nothing carries and fails a node that is fine.
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

// recoverLogReader initializes a LogReader from persisted LogDB state on
// restart. It applies the latest snapshot (setting the compaction boundary
// and membership), restores the hard state, and sets the entry range so
// the raft layer can see all pre-existing log entries.
//
// On first boot (no persisted data) every step is a no-op and the
// LogReader remains in its default empty state.
//
// It reports whether the LogDB held ANY durable state for the replica, which is
// what tells a RESTART from a shard's first start. See the persisted variable
// for why recovered membership cannot answer that question.
func (h *Host) recoverLogReader(lr *logdb.LogReader, shardID, replicaID uint64) (bool, error) {
	if h.logdb == nil {
		return false, nil
	}

	// persisted reports whether the LogDB holds ANY durable Raft state for this
	// replica: a snapshot record, a hard state, or log entries. It is the
	// discriminator between the shard's FIRST start on this replica and a
	// RESTART, and membership cannot serve that purpose: membership is persisted
	// ONLY alongside a snapshot record (memdb.applyUpdate and waldb both set it
	// inside the u.Snapshot.Index > 0 branch), so a replica that has been
	// running — even one fully caught up — recovers no membership at all until
	// it snapshots, and is otherwise indistinguishable from a bootstrap.
	var persisted bool

	// Step 1: Restore snapshot metadata. This sets markerIndex,
	// markerTerm, and membership on the LogReader.
	ss, err := h.logdb.GetSnapshot(shardID, replicaID)
	if err != nil && err != logdb.ErrNoSnapshot {
		return false, err
	}
	if err == nil && ss.Index > 0 {
		persisted = true
		// Resolve the snapshot data file path and size so the raft
		// layer can send InstallSnapshot to followers after recovery.
		// The LogDB stores the snapshot directory; the data file is
		// always "snapshot.dat" inside that directory.
		var snapFilePath string
		var snapFileSize uint64
		if ss.Filepath != "" {
			dataPath := filepath.Join(ss.Filepath, "snapshot.dat")
			// Fail closed when the artifact cannot be MEASURED, exactly as the
			// receive paths do (HandleSnapshot's re-delivery and
			// ImportSnapshot's staging). Swallowing the error left snapFilePath
			// EMPTY, and an empty Filepath is how the send path recognises the one
			// legitimate METADATA-ONLY snapshot (sendSnapshot ships a single empty
			// chunk): an unreadable artifact silently turned this replica into a
			// state eraser for whichever follower it later streamed to.
			//
			// A file that is simply ABSENT must NOT clear snapFilePath either.
			// StartShard deliberately tolerates a compacted-away artifact for an
			// on-disk state machine that recovered through Open() (its database is
			// authoritative), and recoverLogReader runs long before NewStateMachine
			// so it cannot tell the two SM kinds apart. Recording the path
			// unconditionally is what keeps that tolerance from leaking into the
			// SEND path: the record stays "a snapshot with a data file", the send
			// path Opens it, gets ENOENT and fails the transfer with
			// SnapshotSendError instead of shipping the empty chunk that erases the
			// receiving follower's state. Only the SIZE is left at 0, and the send
			// path never trusts a declared size it has not verified against disk.
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
			// The durable record's full description of the artifact, not just
			// its position. This LogReader snapshot is what the SEND path reads
			// when this replica becomes leader and ships the snapshot on, so a
			// field dropped here is a field the FOLLOWER records wrong: a
			// snappy artifact announced as uncompressed is recovered as garbage.
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
		// If ReadState returned a membership (from the latest snapshot
		// in waldb), apply it. This is the same membership that
		// ApplySnapshot already set, but ReadState may return it even
		// when there is no separate snapshot record.
		if membership.ConfigChangeID > 0 || len(membership.Addresses) > 0 {
			lr.SetMembership(logdbMembershipToProto(membership))
		}
	}

	// Step 3: Restore the tracked entry range so LogReader knows which
	// indices are available in LogDB. This is O(1) -- it does not load
	// entry payloads.
	firstIndex, entryCount, err := h.logdb.EntryRange(shardID, replicaID)
	if err != nil {
		// SELF-HEAL: a stale prefix left below a recorded snapshot.
		//
		// A replica whose log floor was never advanced past a snapshot it
		// recorded holds {1..k} U {N+1..M} — a hole EntryRange correctly reports
		// as ErrLogGapDetected, which otherwise makes StartShard fail FOREVER:
		// nothing compacts it (SnapshotEntries has no default) and
		// RequestCompaction cannot run on an unloaded shard. The way in is a
		// crash inside ImportSnapshot between recording the snapshot and
		// advancing the floor — a window that exists by construction — or a
		// data directory written by a receive path that did not advance the
		// floor.
		//
		// The snapshot subsumes every index at or below it, so removing that
		// prefix loses nothing and is exactly what the writer should have done.
		// Repairing here rescues replicas ALREADY wedged in the field, which a
		// write-path fix alone cannot do.
		//
		// Strictly conditional: only when a snapshot is recorded (ss.Index > 0),
		// only on ErrLogGapDetected. Any other error, or a gap with no snapshot
		// covering it, still fails closed — a hole with nothing to subsume it is
		// real data loss and must not be silently papered over.
		if !errors.Is(err, logdb.ErrLogGapDetected) || ss.Index == 0 {
			return false, err
		}
		h.logger.Warn("log entry index holed below a recorded snapshot; removing the "+
			"subsumed prefix to repair recovery",
			"shard_id", shardID, "replica_id", replicaID, "snapshot_index", ss.Index)
		if rmErr := h.logdb.RemoveEntriesTo(shardID, replicaID, ss.Index); rmErr != nil {
			// Report why the REPAIR failed, not the gap that prompted it.
			// Returning the stale ErrLogGapDetected here would present a storage
			// failure (a write-gate rejection, a failed fsync, a closed DB) as a
			// permanent log hole, sending the operator after data corruption that
			// did not happen while hiding the fault that actually blocked recovery.
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

// logdbMembershipToProto converts a logdb.Membership to a proto.Membership.
// Both types have identical field layouts; this function performs a deep copy
// of all maps to prevent aliasing between the LogDB storage layer and the
// in-memory raft state.
//
// This is the direction a restarting replica recovers its configuration in
// (recoverLogReader), so Identities has to survive it or the shard comes back
// authorizing by address alone. See logdb.Membership.Identities. The session
// policy travels the same path and for the same reason: this is where a
// restarting replica relearns the limits its apply path must use instead of
// its own Config. See proto.SessionPolicy.
func logdbMembershipToProto(m logdb.Membership) proto.Membership {
	result := proto.Membership{
		ConfigChangeID:             m.ConfigChangeID,
		MaxWitnesses:               m.MaxWitnesses,
		RequireFullReplicaMajority: m.RequireFullReplicaMajority,
		SessionExpiryEntries:       m.SessionExpiryEntries,
		MaxTotalSessions:           m.MaxTotalSessions,
		MaxSessionsPerClient:       m.MaxSessionsPerClient,
		MaxSessionsPerOwner:        m.MaxSessionsPerOwner,
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

// BootstrapEntryMissingError reports that a shard's durable log holds an entry
// at the bootstrap membership index that is NOT the founding configuration.
//
// Config-in-log is the only bootstrap this code base performs, so every shard
// it creates commits an EntryInitialMembership at that index before any other
// entry can be appended. Anything else there means the log was damaged, and the
// only alternative to failing — rebuilding membership from the caller's members
// map — silently drops the witnesses, observers, Removed tombstones, committed
// identities and replicated policies the shard agreed on.
//
// The remedy is operational: restore this replica's data directory from a
// backup, or wipe it and re-add the replica with join=true so it receives the
// configuration by replication.
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

// releaseRemovedMemberConn drops the outbound QUIC connection to a replica that
// a COMMITTED configuration change has removed, once no other shard still needs
// that peer.
//
// Without this the connection lives for the process lifetime: quic-go's
// KeepAlivePeriod (10s) is below MaxIdleTimeout (30s), so the connection is
// PINGed indefinitely and never idle-closes, and the transport's cleanup only
// reaps connections whose context is already dead. A removed peer that keeps
// running therefore pins a connection, its pooled streams, its epoch records,
// and a 10s keep-alive heartbeat forever.
//
// Two conditions make this safe, and both are load-bearing:
//
//   - It is driven by a COMMITTED removal, never by a sweep over the registry.
//     "No registry entry" does not mean "not a member": entries are legitimately
//     absent under a live, healthy connection while a shard is being torn down
//     and restarted, while a speculative pre-registration is rolled back, and
//     while a joining replica has not yet learned its membership. A periodic
//     sweep would close healthy connections in exactly those windows — including
//     the rolling-restart window, where an extra handshake is most costly.
//   - A connection is keyed by ADDRESS and multiplexed across shards (each shard
//     is a stream index on the same connection), so it may only be closed when
//     no other (shard, replica) still resolves to that address. CloseTarget
//     additionally declines while a snapshot transfer is in flight.
//
// Shard teardown deliberately does NOT come here: stopping a shard locally says
// nothing about the peer's membership elsewhere.
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

// leaderAndTerm returns the cached leader ID, term, and this replica's own
// leadership flag TOGETHER, as a combination that actually co-existed. Any
// caller reporting more than one of them must use this rather than loading the
// fields separately, which can straddle an updateNodeLeader and yield a
// (leader, term) pair that never held.
//
// Safe for concurrent access from any goroutine.
func (ns *nodeState) leaderAndTerm() (leaderID, term uint64, isLeader bool) {
	leaderID, term, isLeader, _ = ns.leaderTermReady()
	return leaderID, term, isLeader
}

// leaderTermReady is leaderAndTerm plus the pair's read readiness, all four
// read as one combination that co-existed.
//
// Safe for concurrent access from any goroutine.
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
	ns.signalLeaderChange()
}

// markLeaderReady records that the cached (leaderID, term) pair is read-ready
// and reports whether it did. A pair that is no longer the cached one is
// refused: readiness belongs to the pair it was observed for, never to its
// successor. The engine delivers OnLeaderReady behind its own pair's
// OnLeaderUpdated on the one dispatcher goroutine, so the check holds by
// construction; it is kept because a mirror that could claim readiness for
// the wrong term would promise a read the leader refuses.
//
// Called only from the dispatcher goroutine, the single writer of the seqlock.
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

// leaderChanged returns the channel the next change to the leadership view
// closes. A waiter takes it BEFORE reading the view and waits on it only if
// what it read did not satisfy it: a change that lands between the two has
// either closed the channel it holds or replaced it before it was taken, and
// then the read already saw the change. Nothing is missed.
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

// buildWALOptions constructs waldb.Option values from the HostConfig.
// When MaxWALDiskSize is set and/or EventListener disk callbacks are
// configured, a DiskMonitor is attached to the WAL database. The
// diskFullFlag is atomically set to true when disk full is detected
// and cleared when usage drops below the full threshold. shards reports
// DiskInfo.Shards when a disk callback fires; nil leaves it empty.
func buildWALOptions(cfg config.HostConfig, diskFullFlag *atomic.Bool, shards func() []uint64) []waldb.Option {
	var opts []waldb.Option

	// Disable fsync when NoSyncWAL is set (testing only — never production).
	if cfg.NoSyncWAL {
		opts = append(opts, waldb.WithNoSync(true))
	}

	// Only configure disk monitoring if there is a size limit or
	// listener callbacks that need disk usage events.
	hasCallbacks := cfg.EventListener != nil &&
		(cfg.EventListener.OnLogDBDiskWarning != nil || cfg.EventListener.OnLogDBDiskFull != nil)

	if cfg.MaxWALDiskSize > 0 || hasCallbacks {
		maxBytes := cfg.MaxWALDiskSize
		// If callbacks are set but no size limit, we still need a
		// maxBytes to compute thresholds. Default to 0 which
		// effectively disables the monitor in NewDiskMonitor.
		if maxBytes == 0 {
			return opts
		}

		var monOpts []waldb.DiskMonitorOption

		// Always wire a recovery callback to clear diskFullFlag when
		// disk usage drops below the full threshold.
		monOpts = append(monOpts, waldb.WithOnRecovery(func(_, _ uint64) {
			diskFullFlag.Store(false)
		}))

		if cfg.EventListener != nil {
			walDir := cfg.WALDir
			if cfg.EventListener.OnLogDBDiskWarning != nil {
				warningCb := cfg.EventListener.OnLogDBDiskWarning
				monOpts = append(monOpts, waldb.WithOnWarning(func(used, maxBytes uint64) {
					warningCb(config.DiskInfo{
						WALDir:    walDir,
						UsedBytes: used,
						MaxBytes:  maxBytes,
						Shards:    diskShards(shards),
					})
				}))
			}
			if cfg.EventListener.OnLogDBDiskFull != nil {
				fullCb := cfg.EventListener.OnLogDBDiskFull
				monOpts = append(monOpts, waldb.WithOnFull(func(used, maxBytes uint64) {
					diskFullFlag.Store(true)
					fullCb(config.DiskInfo{
						WALDir:    walDir,
						UsedBytes: used,
						MaxBytes:  maxBytes,
						Shards:    diskShards(shards),
					})
				}))
			} else {
				// No user callback but disk monitoring is active.
				// Still set the diskFull flag for HealthCheck.
				monOpts = append(monOpts, waldb.WithOnFull(func(_, _ uint64) {
					diskFullFlag.Store(true)
				}))
			}
		} else {
			// No event listener but disk monitoring is active.
			// Set the diskFull flag for HealthCheck.
			monOpts = append(monOpts, waldb.WithOnFull(func(_, _ uint64) {
				diskFullFlag.Store(true)
			}))
		}

		opts = append(opts, waldb.WithDiskMonitorOpts(maxBytes, monOpts...))
	}

	return opts
}

// diskShards calls shards when it is set, for DiskInfo.Shards.
func diskShards(shards func() []uint64) []uint64 {
	if shards == nil {
		return nil
	}
	return shards()
}

// walShardIDs returns, in ascending order and without duplicates, the IDs of
// the shards db holds data for: a stopped shard's data stays until RemoveData
// purges it, so it is listed, and a removed one is not. It returns nil when db
// is nil (a disk event during waldb.Open, before the handle exists) or cannot
// list its nodes.
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

// effectiveMaxPayload returns the effective maximum proposal payload size
// given the two configurable limits. If both are non-zero, the minimum is
// used because a proposal must satisfy BOTH limits: MaxProposalPayloadSize
// (the user-facing cap) and MaxEntrySize (the transport/storage cap). If
// only one is set, that value is used.
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

// copyWithContext copies from src to dst while periodically checking the
// context for cancellation. The context is checked every 1 MB of data
// copied to balance throughput (fewer syscalls) against cancellation
// responsiveness. Returns mapContextError(ctx.Err()) on cancellation,
// or any I/O error encountered during the copy.
func copyWithContext(ctx context.Context, dst io.Writer, src io.Reader) error {
	_, err := copyWithContextN(ctx, dst, src)
	return err
}

// copyWithContextN is copyWithContext, additionally reporting how many bytes
// reached dst. Callers that enforce a size limit need the count: an
// io.LimitReader can only stop the copy, it cannot say whether the limit was
// the reason, so the byte total is what distinguishes an artifact that ended
// exactly at the limit from one that was truncated by it.
//
// The count is the bytes WRITTEN, and it is accurate on the error paths too,
// so a caller can discard exactly what it wrote.
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

// recordedSnapshotPath returns the cleaned snapshot directory LogDB records for
// the node: "" with a nil error when there is no record (or no LogDB), and an
// error when the record cannot be read.
//
// It exists so the snapshot GC and a receive's publish can refuse to delete
// the directory the durable record points at, and an unreadable record is
// therefore NOT "no record". Mapping a read error to "" made both fail open —
// deleting the recorded snapshot, whose log below it may already be
// compacted, on one transient read error. A caller that cannot learn the
// record must not remove anything the record might name.
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

// StateMachineHash returns a hash of this replica's applied state machine
// state, together with the applied index it was taken at.
//
// It exists to detect the one failure Raft cannot: a state machine that is not
// a deterministic function of the log. Consensus keeps agreeing on the entry
// sequence while replicas silently derive different state from it, and the
// first symptom is usually a linearizable read returning different values
// depending on which replica served it.
//
// ok is false when the user state machine does not implement sm.Hasher; that
// is not an error, only an absence of the facility. Implementations must make
// the hash ORDER-INDEPENDENT (both reference stores in pkg/sm/kv sort before
// hashing) or it reports divergence that does not exist.
//
// Compare across replicas at the SAME applied index — the returned index is
// what makes that possible. Two replicas at different indexes are expected to
// differ, so a comparison that ignores it is meaningless.
//
// This is a diagnostic, not a hot path: it hashes the whole state machine.
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
	// The index is read FIRST so it can never be newer than the state the hash
	// covers: a concurrent apply between the two reads then makes the pair
	// conservative (an older index for newer state), which a comparison
	// rejects as "not at the same index" rather than accepting as a false
	// divergence.
	appliedIndex = ns.rsm.LastApplied()
	hash, ok, err = ns.rsm.GetHash()
	if err != nil {
		return 0, 0, false, err
	}
	return hash, appliedIndex, ok, nil
}
