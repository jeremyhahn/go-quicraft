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

// Adversarial / property-based coverage for the raft state machine.
//
// The defect class this file targets is the one soak testing structurally
// cannot find: a WELL-FORMED but hostile message that a peer is authenticated
// to send. Two shipped criticals were of exactly this shape -- a forged wire
// InstallSnapshot carrying Term 0 and a full Membership, and a term-less
// local-control type accepted from the wire -- and both were found by reading
// code, not by running it under fault injection.
//
// pkg/internal/raft is a pure deterministic state machine (messages in,
// messages out, no I/O), which makes it directly fuzzable. This file holds
// the harness and the safety-invariant checker; the go-native fuzz targets
// that drive it live in fuzz_test.go behind the quicraft_fuzz build tag, and
// the seed corpus below is replayed as an ordinary unit test so the same
// assertions run in CI on every `go test ./pkg/...`.
//
// THE ATTACKER MODEL IS THE ORIGIN STAMP. Every message the harness steps is
// proto.OriginRemote, because that is the only thing a wire peer can produce:
// proto.MessageOrigin is process-local state that the wire format cannot
// express, so OriginLocal is unreachable from the network by construction.
// Everything else in the message -- type, term, indices, entries, and the
// snapshot's entire Membership -- is chosen by the fuzzer.

package raft

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"math"
	mathrand "math/rand"
	"sort"
	"strings"
	"testing"

	"github.com/jeremyhahn/go-quicraft/pkg/config"
	"github.com/jeremyhahn/go-quicraft/pkg/proto"
)

// --------------------------------------------------------------------------
// Fuzzer input grammar
//
// The byte stream a fuzzer hands us is decoded into a sequence of operations
// by fuzzStream. The grammar is deliberately INVERTIBLE (see the fuzzEncoder
// below) so that a seed corpus entry can be written as readable Go rather
// than as a hex blob, and so a reported crash can be decoded back into the
// message sequence that produced it.
// --------------------------------------------------------------------------

// fuzzStream decodes a fuzzer-supplied byte slice into raft operations.
// Reads past the end of the slice yield zero, so a truncated input decodes
// into a shorter but still well-defined operation sequence.
type fuzzStream struct {
	b   []byte
	pos int
}

// done reports whether the stream has been fully consumed.
func (s *fuzzStream) done() bool { return s.pos >= len(s.b) }

// next returns the next byte, or 0 once the stream is exhausted.
func (s *fuzzStream) next() byte {
	if s.pos >= len(s.b) {
		s.pos++
		return 0
	}
	v := s.b[s.pos]
	s.pos++
	return v
}

// fuzzExtremeValues are the uint64 boundary values worth reaching often.
// Overflow at the log-index arithmetic boundaries (prevLogIndex+1+i in
// tryAppend, next-1 in sendAppend) is only reachable from up here.
var fuzzExtremeValues = [...]uint64{
	0,
	1,
	math.MaxUint64,
	math.MaxUint64 - 1,
	1 << 63,
	1<<32 - 1,
	1 << 32,
	math.MaxInt64,
}

// u64 decodes one uint64. The encoding is biased toward the small dense
// values the state machine actually distinguishes (terms and indices in a
// short synthetic log) while still reaching byte-scale, boundary, and fully
// arbitrary 64-bit values.
//
// Encoding (see fuzzEncoder.u64 for the inverse):
//
//	tag 0x00       -> 0
//	tag 0x01 b     -> uint64(b)
//	tag 0x02 b0..7 -> big-endian uint64
//	tag 0x03 b     -> fuzzExtremeValues[b % len]
//	tag >= 0x04    -> uint64(tag - 4), i.e. 0..251
func (s *fuzzStream) u64() uint64 {
	tag := s.next()
	switch tag {
	case 0x00:
		return 0
	case 0x01:
		return uint64(s.next())
	case 0x02:
		var raw [8]byte
		for i := range raw {
			raw[i] = s.next()
		}
		return binary.BigEndian.Uint64(raw[:])
	case 0x03:
		return fuzzExtremeValues[int(s.next())%len(fuzzExtremeValues)]
	default:
		return uint64(tag - 4)
	}
}

// fuzzMessageTypes is every message type the step path dispatches on, plus
// the local-only and unused types. A wire peer can name ANY of these -- the
// decoder validates only that the type is in the known range -- so the fuzzer
// must be able to name them too.
var fuzzMessageTypes = [...]uint64{
	proto.LocalTick,
	proto.Election,
	proto.LeaderHeartbeat,
	proto.ConfigChangeEvent,
	proto.NoOP,
	proto.Ping,
	proto.Pong,
	proto.Propose,
	proto.SnapshotStatus,
	proto.Unreachable,
	proto.CheckQuorum,
	proto.BatchedReadIndex,
	proto.Replicate,
	proto.ReplicateResp,
	proto.RequestVote,
	proto.RequestVoteResp,
	proto.InstallSnapshot,
	proto.Heartbeat,
	proto.HeartbeatResp,
	proto.ReadIndex,
	proto.ReadIndexResp,
	proto.Quiesce,
	proto.SnapshotReceived,
	proto.LeaderTransfer,
	proto.TimeoutNow,
	proto.RequestPreVote,
	proto.RequestPreVoteResp,
	proto.RateLimit,
}

// fuzzReplicaIDs is the replica-ID pool. It covers the configured members
// (1..3), plausible non-members (4, 5), the reserved zero, and values that
// exercise the map lookups with no matching remote.
var fuzzReplicaIDs = [...]uint64{1, 2, 3, 4, 5, 0, 99, math.MaxUint64}

// fuzzEntryTypes is the entry-type pool, mirroring proto's base types.
var fuzzEntryTypes = [...]uint64{
	proto.EntryNormal,
	proto.EntryConfigChange,
	proto.EntryEncodedEntry,
	proto.EntryMetadata,
}

// Operation kinds. A byte b selects kind b%8; step dominates so that most of
// the fuzzer's budget goes into the message path rather than into ticking.
const (
	fuzzOpStep    = 0 // 0..5 all mean step
	fuzzOpTick    = 6
	fuzzOpPersist = 7
)

// fuzzMaxOps bounds the operation count decoded from one input. Without it a
// large corpus entry turns into an unbounded run and the fuzzer's throughput
// collapses.
const fuzzMaxOps = 64

// fuzzMaxEntries bounds entries per message; fuzzMaxCmd bounds a payload.
// Both are small on purpose: the interesting behaviour is in the index/term
// arithmetic, not in payload size, and small values keep each execution fast.
const (
	fuzzMaxEntries = 4
	fuzzMaxCmd     = 8
)

// fuzzMaxTickBurst bounds a single tick operation. One tick per operation is
// not enough to cross an election timeout inside the operation budget, so the
// election, quiesce, snapshot-ack-wait, and rate-limit timers would never
// fire. A burst crosses them in one operation.
const fuzzMaxTickBurst = 32

// fuzzLiveState is the replica's own view at the moment a message is decoded.
// It is what the align byte (see message) snaps fields onto.
//
// Without it the fuzzer is a pure black box and essentially every Replicate
// is refused by the log-matching check, so the ACCEPT paths -- append,
// truncate, commit, elect -- stay unreachable and the invariants are checked
// only over refusals. A real attacker observes the exchange and constructs
// messages that fit; the align byte gives the fuzzer that same capability.
type fuzzLiveState struct {
	term       uint64
	committed  uint64
	commitTerm uint64
	lastIndex  uint64
	lastTerm   uint64
}

// liveState captures the replica's current view for the align byte.
func liveState(r *raft) fuzzLiveState {
	lastTerm, _ := r.log.lastTerm()
	commitTerm, _ := r.log.term(r.log.committed)
	return fuzzLiveState{
		term:       r.term,
		committed:  r.log.committed,
		commitTerm: commitTerm,
		lastIndex:  r.log.lastIndex(),
		lastTerm:   lastTerm,
	}
}

// Align byte bits. Each snaps one or more message fields onto the replica's
// live state, turning an otherwise-refused message into one that reaches the
// handler bodies.
const (
	alignTermCurrent = 0x01 // Term = current term
	alignTermNext    = 0x02 // Term = current term + 1
	alignLogToLast   = 0x04 // (LogIndex, LogTerm) = log tail
	// alignLogToCommit anchors the append at the commit frontier. It sets the
	// term too: without a matching prevLogTerm the append is refused by the
	// log-matching check, and the TRUNCATION path -- where a conflicting
	// batch replaces the uncommitted tail -- is then unreachable. That path
	// is where a stale commit index turns into a State Machine Safety
	// violation, so it has to be reachable.
	alignLogToCommit = 0x08

	alignCommitCurrent = 0x10 // Commit = commit index
	alignCommitToBatch = 0x20 // Commit = LogIndex + len(Entries)
	alignFromLeader    = 0x40 // From = replica 2 (the shard's other voter)
	alignSnapshotAhead = 0x80 // Snapshot.Index = lastIndex + 1
)

// message decodes one proto.Message. The returned message is always stamped
// OriginRemote: see the file comment for why that is the whole attacker model.
func (s *fuzzStream) message(shardID, selfID uint64, st fuzzLiveState) proto.Message {
	var m proto.Message

	typeByte := s.next()
	if typeByte >= 0xF0 {
		// Escape hatch: a fully arbitrary type value, including ones outside
		// the range proto's decoder accepts. Reaching step() with such a type
		// requires a caller that skipped decoding, but the state machine must
		// still be total over it.
		m.Type = s.u64()
	} else {
		m.Type = fuzzMessageTypes[int(typeByte)%len(fuzzMessageTypes)]
	}

	m.From = fuzzReplicaIDs[int(s.next())%len(fuzzReplicaIDs)]
	m.Term = s.u64()
	m.LogTerm = s.u64()
	m.LogIndex = s.u64()
	m.Commit = s.u64()
	m.Hint = s.u64()

	flags := s.next()
	m.Reject = flags&0x01 != 0
	coherent := flags&0x02 != 0
	attachSnapshot := flags&0x04 != 0

	align := s.next()
	if align&alignTermCurrent != 0 {
		m.Term = st.term
	}
	if align&alignTermNext != 0 {
		m.Term = st.term + 1
	}
	if align&alignLogToLast != 0 {
		m.LogIndex, m.LogTerm = st.lastIndex, st.lastTerm
	}
	if align&alignLogToCommit != 0 {
		m.LogIndex, m.LogTerm = st.committed, st.commitTerm
	}
	if align&alignCommitCurrent != 0 {
		m.Commit = st.committed
	}
	if align&alignFromLeader != 0 {
		m.From = 2
	}

	n := int(s.next()) % (fuzzMaxEntries + 1)
	if n > 0 {
		m.Entries = make([]proto.Entry, n)
		for i := range m.Entries {
			idx := s.u64()
			term := s.u64()
			if coherent {
				// A batch the append path can actually accept: contiguous
				// from prevLogIndex+1 and at the message's term. Without
				// this mode nearly every Replicate is rejected by the
				// contiguity guard and the accept path is never explored.
				idx = m.LogIndex + 1 + uint64(i)
				term = m.Term
			}
			et := s.next()
			entryType := fuzzEntryTypes[int(et)%len(fuzzEntryTypes)]
			if et&0x80 != 0 {
				entryType |= proto.EntryCompressFlag
			}
			cmdLen := int(s.next()) % (fuzzMaxCmd + 1)
			var cmd []byte
			if cmdLen > 0 {
				cmd = make([]byte, cmdLen)
				for j := range cmd {
					cmd[j] = s.next()
				}
			}
			m.Entries[i] = proto.Entry{
				Index: idx,
				Term:  term,
				Type:  entryType,
				Cmd:   cmd,
			}
		}
	}

	if align&alignCommitToBatch != 0 {
		m.Commit = m.LogIndex + uint64(len(m.Entries))
	}

	if attachSnapshot || m.Type == proto.InstallSnapshot {
		m.Snapshot = s.snapshot(shardID)
		if align&alignSnapshotAhead != 0 {
			m.Snapshot.Index = st.lastIndex + 1
			m.Snapshot.Term = m.Term
		}
	}

	// Fields the transport/engine layer fills in before step() ever sees the
	// message. The fuzzer does not control them: a batch whose ShardID does
	// not match is routed elsewhere (and ShardID==0 is dropped outright in
	// recv.go), and the engine dispatches by To.
	m.ShardID = shardID
	m.To = selfID
	m.Origin = proto.OriginRemote
	return m
}

// snapshot decodes a proto.Snapshot including its full Membership. The
// membership is the payload that made the forged-InstallSnapshot finding
// critical: it is the exact set quorum() is computed over.
func (s *fuzzStream) snapshot(shardID uint64) proto.Snapshot {
	ss := proto.Snapshot{
		ShardID: shardID,
		Index:   s.u64(),
		Term:    s.u64(),
	}
	ss.Membership.ConfigChangeID = s.u64()
	ss.Membership.MaxWitnesses = s.u64()
	flags := s.next()
	ss.Membership.RequireFullReplicaMajority = flags&0x01 != 0
	ss.Imported = flags&0x02 != 0
	ss.Witness = flags&0x04 != 0
	ss.Membership.Addresses = s.idMap()
	ss.Membership.Observers = s.idMap()
	ss.Membership.Witnesses = s.idMap()
	return ss
}

// idMap decodes a replica-ID -> address map of up to 3 entries.
func (s *fuzzStream) idMap() map[uint64]string {
	n := int(s.next()) % 4
	if n == 0 {
		return nil
	}
	m := make(map[uint64]string, n)
	for i := 0; i < n; i++ {
		id := fuzzReplicaIDs[int(s.next())%len(fuzzReplicaIDs)]
		m[id] = fmt.Sprintf("addr%d", id)
	}
	return m
}

// --------------------------------------------------------------------------
// Seed-corpus encoder (the inverse of fuzzStream)
// --------------------------------------------------------------------------

// fuzzEncoder builds a byte stream fuzzStream decodes back to the same
// operations. It exists so the seed corpus -- which must include the two
// known exploits -- can be written as readable Go instead of hex.
type fuzzEncoder struct {
	b []byte
}

func (e *fuzzEncoder) byteVal(v byte) { e.b = append(e.b, v) }

// u64 emits the shortest encoding fuzzStream.u64 accepts for v.
func (e *fuzzEncoder) u64(v uint64) {
	switch {
	case v <= 251:
		e.b = append(e.b, byte(v+4))
	case v <= 255:
		e.b = append(e.b, 0x01, byte(v))
	default:
		var raw [8]byte
		binary.BigEndian.PutUint64(raw[:], v)
		e.b = append(e.b, 0x02)
		e.b = append(e.b, raw[:]...)
	}
}

// start emits the mandatory header byte selecting the replica's starting
// role. It must be the first thing written.
func (e *fuzzEncoder) start(asLeader bool) {
	if asLeader {
		e.byteVal(0x01)
	} else {
		e.byteVal(0x00)
	}
}

// tick emits a tick burst of n ticks (n is clamped to 1..fuzzMaxTickBurst).
func (e *fuzzEncoder) tick(n int) {
	if n < 1 {
		n = 1
	}
	if n > fuzzMaxTickBurst {
		n = fuzzMaxTickBurst
	}
	e.byteVal(fuzzOpTick)
	e.byteVal(byte(n - 1))
}

func (e *fuzzEncoder) persist() { e.byteVal(fuzzOpPersist) }

// msgTypeIndex returns the fuzzMessageTypes position of t.
func msgTypeIndex(t uint64) byte {
	for i, v := range fuzzMessageTypes {
		if v == t {
			return byte(i)
		}
	}
	panic("adversarial seed: unknown message type")
}

// replicaIDIndex returns the fuzzReplicaIDs position of id.
func replicaIDIndex(id uint64) byte {
	for i, v := range fuzzReplicaIDs {
		if v == id {
			return byte(i)
		}
	}
	panic("adversarial seed: replica id not in the fuzz pool")
}

// seedMessage is the readable form of one step operation in a seed.
type seedMessage struct {
	Type     uint64
	From     uint64
	Term     uint64
	LogTerm  uint64
	LogIndex uint64
	Commit   uint64
	Hint     uint64
	Reject   bool
	// Coherent makes the entries contiguous from LogIndex+1 at Term.
	Coherent bool
	// Entries is the entry count when Coherent is set.
	Entries int
	// Align is the align byte: it snaps fields onto the replica's live state
	// (see the align* constants).
	Align byte
	// Snapshot, when non-nil, is attached to the message.
	Snapshot *seedSnapshot
}

// seedSnapshot is the readable form of an attached snapshot.
type seedSnapshot struct {
	Index                      uint64
	Term                       uint64
	ConfigChangeID             uint64
	MaxWitnesses               uint64
	RequireFullReplicaMajority bool
	Voters                     []uint64
	Observers                  []uint64
	Witnesses                  []uint64
}

// step appends one step operation.
func (e *fuzzEncoder) step(m seedMessage) {
	e.byteVal(fuzzOpStep)
	e.byteVal(msgTypeIndex(m.Type))
	e.byteVal(replicaIDIndex(m.From))
	e.u64(m.Term)
	e.u64(m.LogTerm)
	e.u64(m.LogIndex)
	e.u64(m.Commit)
	e.u64(m.Hint)
	var flags byte
	if m.Reject {
		flags |= 0x01
	}
	if m.Coherent {
		flags |= 0x02
	}
	if m.Snapshot != nil && m.Type != proto.InstallSnapshot {
		flags |= 0x04
	}
	e.byteVal(flags)
	e.byteVal(m.Align)
	e.byteVal(byte(m.Entries))
	for i := 0; i < m.Entries; i++ {
		e.u64(0)     // index (overridden by Coherent)
		e.u64(0)     // term  (overridden by Coherent)
		e.byteVal(0) // entry type -> EntryNormal
		e.byteVal(0) // cmd length 0
	}
	if m.Snapshot != nil {
		e.snapshot(*m.Snapshot)
	}
}

func (e *fuzzEncoder) snapshot(ss seedSnapshot) {
	e.u64(ss.Index)
	e.u64(ss.Term)
	e.u64(ss.ConfigChangeID)
	e.u64(ss.MaxWitnesses)
	var flags byte
	if ss.RequireFullReplicaMajority {
		flags |= 0x01
	}
	e.byteVal(flags)
	e.idMap(ss.Voters)
	e.idMap(ss.Observers)
	e.idMap(ss.Witnesses)
}

func (e *fuzzEncoder) idMap(ids []uint64) {
	e.byteVal(byte(len(ids)))
	for _, id := range ids {
		e.byteVal(replicaIDIndex(id))
	}
}

// --------------------------------------------------------------------------
// Safety-invariant checker
// --------------------------------------------------------------------------

// raftObservation is the subset of raft state the invariants are stated over.
type raftObservation struct {
	term      uint64
	vote      uint64
	committed uint64
	processed uint64
	lastIndex uint64
	leaderID  uint64
	role      roleState
	// config is a deterministic fingerprint of everything quorum() is
	// computed over, plus the replicated witness policy and the two
	// configuration-derived safety latches.
	config string
	quorum int
}

// observe captures the current raft state.
func observe(r *raft) raftObservation {
	return raftObservation{
		term:      r.term,
		vote:      r.vote,
		committed: r.log.committed,
		processed: r.log.processed,
		lastIndex: r.log.lastIndex(),
		leaderID:  r.leaderID,
		role:      r.state,
		config:    configFingerprint(r),
		quorum:    quorum(r.remotes, r.witnesses),
	}
}

// configFingerprint renders the shard configuration as seen by this replica.
// Two states with the same fingerprint agree on every input to quorum(), on
// the replicated witness policy, and on the self-role latches.
func configFingerprint(r *raft) string {
	var sb strings.Builder
	writeIDs := func(label string, m map[uint64]*remote) {
		ids := make([]uint64, 0, len(m))
		for id := range m {
			ids = append(ids, id)
		}
		sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
		sb.WriteString(label)
		for _, id := range ids {
			fmt.Fprintf(&sb, "%d,", id)
		}
		sb.WriteByte(';')
	}
	writeIDs("v=", r.remotes)
	writeIDs("o=", r.observers)
	writeIDs("w=", r.witnesses)
	fmt.Fprintf(&sb, "maxw=%d;rfm=%t;selfRemoved=%t;roleMismatch=%t",
		r.maxWitnesses, r.requireFullReplicaMajority, r.selfRemoved, r.roleMismatch)
	return sb.String()
}

// raftInvariants tracks the cross-step safety properties. Each is a Raft
// safety invariant that must hold no matter what an authenticated peer sends.
type raftInvariants struct {
	// prev is the observation from the previous step.
	prev raftObservation
	// baseConfig is the configuration the replica started with. Under a
	// wire-only message sequence it must never change: membership mutates
	// only on a COMMITTED config change (Peer.Commit -> processConfigChanges,
	// which this harness never drives) or on a snapshot raft legitimately
	// adopted -- and a wire InstallSnapshot is refused outright.
	baseConfig string
	// ledTerms records the terms in which THIS replica held leadership. It is
	// the local witness to Election Safety: combined with vote stability it
	// says the replica never leads a term it voted away, and it is asserted
	// against the leader-side branch of check. It deliberately does NOT drive
	// a "second leader in this term" assertion -- see check for why that one
	// is outside Raft's failure model.
	ledTerms map[uint64]bool
	// votes records the vote cast in each term.
	votes map[uint64]uint64
}

func newRaftInvariants(r *raft) *raftInvariants {
	inv := &raftInvariants{
		prev:     observe(r),
		ledTerms: make(map[uint64]bool),
		votes:    make(map[uint64]uint64),
	}
	inv.baseConfig = inv.prev.config
	if inv.prev.vote != noNode {
		inv.votes[inv.prev.term] = inv.prev.vote
	}
	if inv.prev.role == leader {
		inv.ledTerms[inv.prev.term] = true
	}
	return inv
}

// check asserts every safety invariant against the post-step state and folds
// the observation into the tracked history. label identifies the operation
// that produced the state, so a failure names the exact step.
func (inv *raftInvariants) check(tb testing.TB, r *raft, label string) {
	tb.Helper()
	cur := observe(r)
	prev := inv.prev

	fatal := func(format string, args ...any) {
		tb.Helper()
		msg := fmt.Sprintf(format, args...)
		tb.Fatalf("%s: %s\n  before: %+v\n  after:  %+v", label, msg, prev, cur)
	}

	// Raft Figure 2: currentTerm is monotonically non-decreasing. A term
	// regression re-opens every term the replica has already voted in.
	if cur.term < prev.term {
		fatal("term regressed %d -> %d", prev.term, cur.term)
	}

	// The commit index only ever advances. A regression un-commits an entry
	// the replica has already acknowledged as committed.
	if cur.committed < prev.committed {
		fatal("commit index regressed %d -> %d", prev.committed, cur.committed)
	}

	// A replica must never claim to have committed an entry it does not
	// hold: getConflictIndex treats every index at or below committed as
	// unable to conflict, so a commit index above lastIndex turns into a
	// NON-rejecting ReplicateResp for entries this replica has nothing for,
	// and the leader counts that phantom match toward quorum.
	//
	// The exception is exact and narrow: raftLog.restore fast-forwards
	// committed to the snapshot index while the LogReader has not yet been
	// told about the snapshot (the engine does that at the persistence
	// edge). While a snapshot is pending, lastIndex() still reports the old
	// LogDB range. The harness closes that window itself after every step
	// (see settlePendingSnapshot), so this only tolerates the in-step state.
	if cur.committed > cur.lastIndex && r.log.pendingSnapshot() == nil {
		fatal("commit index %d exceeds last index %d", cur.committed, cur.lastIndex)
	}

	// The apply frontier can never pass the commit frontier, and never moves
	// backwards: either would hand the state machine an entry that is not
	// committed, or replay one it has already applied.
	if cur.processed > cur.committed {
		fatal("processed %d exceeds committed %d", cur.processed, cur.committed)
	}
	if cur.processed < prev.processed {
		fatal("processed regressed %d -> %d", prev.processed, cur.processed)
	}

	// A pending snapshot is committed by construction.
	if ss := r.log.pendingSnapshot(); ss != nil && cur.committed < ss.Index {
		fatal("commit index %d below pending snapshot index %d", cur.committed, ss.Index)
	}

	// THE membership invariant. Nothing a wire peer sends may change the set
	// quorum() is computed over, the replicated witness policy, or the
	// self-role latches. This is the direct guard for the forged
	// InstallSnapshot finding: that message carried a full Membership.
	if cur.config != inv.baseConfig {
		fatal("configuration changed from a wire message\n  base:   %s\n  now:    %s",
			inv.baseConfig, cur.config)
	}
	if cur.quorum < 1 {
		fatal("quorum collapsed to %d", cur.quorum)
	}

	// Election Safety, in the form a single replica can actually enforce.
	//
	// "No two leaders in one term" is a statement about the CLUSTER, and a
	// replica cannot check it directly: a peer that claims leadership at a
	// term it did not win is outside Raft's failure model, and every
	// reference implementation (etcd/raft stepFollower, dragonboat) adopts
	// m.From as the leader unconditionally. What a replica CAN enforce is
	// the local half -- it never holds leadership of a term it did not win
	// for itself:
	//
	//   leader => leaderID == self, AND leader => vote == self.
	//
	// Combined with vote stability below, that is exactly "this replica
	// never leads a term it voted away", which is the local precondition
	// the cluster-wide property is built out of.
	//
	// Note that LOSING leadership within a term is legal and expected:
	// CheckQuorum steps a leader that has lost contact with a quorum down to
	// follower at the SAME term (Raft thesis p.42, etcd/raft
	// becomeFollower(r.Term, None)), so role is not an invariant of the term.
	if cur.role == leader {
		if cur.leaderID != r.replicaID {
			fatal("leader records leaderID %d, not self %d", cur.leaderID, r.replicaID)
		}
		if cur.vote != r.replicaID {
			fatal("leader at term %d voted for %d, not self %d", cur.term, cur.vote, r.replicaID)
		}
		// A term this replica led is a term whose vote is settled forever:
		// it cannot later be spent on anyone else, in any role.
		if prior, ok := inv.votes[cur.term]; ok && prior != r.replicaID {
			fatal("replica leads term %d after voting for %d", cur.term, prior)
		}
		inv.ledTerms[cur.term] = true
	} else if inv.ledTerms[cur.term] && cur.vote != r.replicaID {
		fatal("replica led term %d but its vote for that term is now %d",
			cur.term, cur.vote)
	}

	// Vote safety (Raft §5.2): at most one vote per term, and it is never
	// retracted within that term.
	if prior, ok := inv.votes[cur.term]; ok {
		if cur.vote != prior {
			fatal("vote in term %d changed %d -> %d", cur.term, prior, cur.vote)
		}
	} else if cur.vote != noNode {
		inv.votes[cur.term] = cur.vote
	}

	// Outbound messages must be ones a peer will actually accept, and must
	// not overstate what this replica holds.
	inv.checkOutbound(tb, r, label)

	inv.prev = cur
}

// checkOutbound asserts the properties of the messages produced by the step.
func (inv *raftInvariants) checkOutbound(tb testing.TB, r *raft, label string) {
	tb.Helper()
	for i := range r.msgs {
		m := &r.msgs[i]
		if m.From != r.replicaID {
			tb.Fatalf("%s: outbound %s has From=%d, want %d",
				label, proto.MessageTypeName(m.Type), m.From, r.replicaID)
		}
		if m.ShardID != r.shardID {
			tb.Fatalf("%s: outbound %s has ShardID=%d, want %d",
				label, proto.MessageTypeName(m.Type), m.ShardID, r.shardID)
		}
		// Every peer drops a term-less protocol message (step's zero-term
		// gate), so emitting one starves the exchange it belongs to. send()
		// fills Term from r.term, so this fires only if r.term is 0 at a
		// point where the replica answers a peer.
		if m.Term == 0 {
			tb.Fatalf("%s: outbound %s carries no term; every peer will drop it",
				label, proto.MessageTypeName(m.Type))
		}
		// A non-rejecting ReplicateResp is the leader's evidence that this
		// replica HOLDS everything through LogIndex; the leader counts it
		// toward the commit quorum. Acknowledging an index the log cannot
		// produce is the phantom-match defect.
		if m.Type == proto.ReplicateResp && !m.Reject && m.LogIndex > 0 {
			if m.LogIndex > r.log.lastIndex() && r.log.pendingSnapshot() == nil {
				tb.Fatalf("%s: acknowledged index %d beyond last index %d",
					label, m.LogIndex, r.log.lastIndex())
			}
			if _, err := r.log.term(m.LogIndex); errors.Is(err, ErrUnavailable) {
				tb.Fatalf("%s: non-rejecting ReplicateResp acknowledges index %d the log does not hold",
					label, m.LogIndex)
			}
		}
	}
}

// --------------------------------------------------------------------------
// Harness
// --------------------------------------------------------------------------

// adversarialShardID and adversarialSelfID identify the replica under test.
const (
	adversarialShardID = 1
	adversarialSelfID  = 1
)

// adversarialMembers is the configuration the replica under test starts in:
// a plain 3-voter shard. It is the configuration the membership invariant is
// stated against.
func adversarialMembers() map[uint64]string {
	return map[uint64]string{1: "addr1", 2: "addr2", 3: "addr3"}
}

// newAdversarialRaft builds the replica under test together with the
// LogReader backing it. The log starts with a short committed prefix so the
// compaction, conflict, and commit paths are all reachable from the first
// message rather than only after the fuzzer has grown a log.
func newAdversarialRaft(tb testing.TB) (*raft, *testLogReader) {
	tb.Helper()
	cfg := raftTestCfg(adversarialShardID, adversarialSelfID)
	cfg.PreVote = config.BoolPtr(true)
	cfg.CheckQuorum = config.BoolPtr(true)

	entries := makeEntries(1, 4, 1) // indices 1..3 at term 1
	lr := newTestLogReader(entries, proto.State{Term: 1, Commit: 2, Vote: 0})
	lr.membership = proto.Membership{Addresses: adversarialMembers()}

	r, err := newRaft(cfg, lr)
	if err != nil {
		tb.Fatalf("newRaft: %v", err)
	}
	return r, lr
}

// settlePendingSnapshot mirrors what the engine does at the persistence edge
// (Peer.Commit): a snapshot raft accepted is registered with the LogReader
// and then acknowledged, which clears it from the in-memory log. Without this
// the harness would leave the log in the transient state where committed is
// ahead of the LogReader's range indefinitely, and the invariant that catches
// a phantom commit index would be permanently suppressed.
func settlePendingSnapshot(r *raft, lr *testLogReader) {
	ss := r.log.pendingSnapshot()
	if ss == nil {
		return
	}
	if err := lr.ApplySnapshot(*ss); err != nil {
		return
	}
	r.log.commitUpdate(proto.UpdateCommit{StableSnapshotTo: ss.Index})
}

// fuzzPersist mirrors the engine's fsync ack: the leader's own match index is
// its DURABLE index, so without this no commit can ever advance and the whole
// commit path stays unreachable. It is the local-capability half of the
// harness; everything the fuzzer controls arrives over the wire.
func fuzzPersist(r *raft) {
	last := r.log.lastIndex()
	if last == 0 || last <= r.log.inmem.savedTo {
		return
	}
	term, err := r.log.term(last)
	if err != nil {
		return
	}
	r.log.inmem.savedLogTo(last, term)
	if r.state == leader && r.tryCommit() {
		r.broadcastAppend()
	}
}

// quietLogs silences the state machine's slog output for the duration of the
// test. The step path logs a warning for every refused message, which at fuzz
// throughput dominates the runtime.
func quietLogs(tb testing.TB) {
	tb.Helper()
	prev := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(io.Discard, &slog.HandlerOptions{
		Level: slog.LevelError + 1,
	})))
	tb.Cleanup(func() { slog.SetDefault(prev) })
}

// electSelf drives the replica to leadership through the ordinary protocol:
// its election timer fires, and its two configured peers grant the pre-vote
// and the vote. Nothing here is privileged -- the grants are wire messages of
// exactly the kind a peer sends -- but the sequence is far too specific for a
// black-box byte stream to hit reliably, and without it the entire leader-side
// half of the state machine (handleAppendResp, tryCommit, ReadIndex quorum,
// snapshot status, leader transfer) is never exercised.
func electSelf(r *raft) {
	for i := 0; i < 4*len(fuzzExtremeValues)*8 && r.state != leader; i++ {
		switch r.state {
		case follower:
			r.tickFunc(r)
		case preCandidate:
			for _, from := range []uint64{2, 3} {
				_ = r.step(proto.Message{
					Type: proto.RequestPreVoteResp, From: from, To: r.replicaID,
					ShardID: r.shardID, Term: r.term, Origin: proto.OriginRemote,
				})
			}
			if r.state == preCandidate {
				r.tickFunc(r)
			}
		case candidate:
			for _, from := range []uint64{2, 3} {
				_ = r.step(proto.Message{
					Type: proto.RequestVoteResp, From: from, To: r.replicaID,
					ShardID: r.shardID, Term: r.term, Origin: proto.OriginRemote,
				})
			}
			if r.state == candidate {
				r.tickFunc(r)
			}
		case leader:
		}
	}
	r.drainMessages()
}

// harnessCoverage counts the raft behaviours a run actually reached. A
// harness that never elects a leader, never accepts an append and never
// advances a commit index proves nothing no matter how many inputs it runs,
// and that failure mode is SILENT -- every assertion passes. It is asserted
// against in TestAdversarialHarnessReachesRaftBehaviour.
type harnessCoverage struct {
	roles             map[roleState]int
	committedAdvanced int
	appendsAccepted   int
	appendsRejected   int
	votesGranted      int
	votesDenied       int
	snapshotsRefused  int
	logsTruncated     int
}

func newHarnessCoverage() *harnessCoverage {
	return &harnessCoverage{roles: make(map[roleState]int)}
}

// record folds one post-step observation into the counters.
func (c *harnessCoverage) record(r *raft) {
	if c == nil {
		return
	}
	c.roles[r.state]++
	for i := range r.msgs {
		m := &r.msgs[i]
		switch {
		case m.Type == proto.ReplicateResp && m.Reject:
			c.appendsRejected++
		case m.Type == proto.ReplicateResp:
			c.appendsAccepted++
		case (m.Type == proto.RequestVoteResp || m.Type == proto.RequestPreVoteResp) && m.Reject:
			c.votesDenied++
		case m.Type == proto.RequestVoteResp || m.Type == proto.RequestPreVoteResp:
			c.votesGranted++
		}
	}
}

// runAdversarialStepSequence is the body shared by the fuzz target and the
// seed-corpus unit test. It decodes data into an operation sequence, applies
// it to a freshly built replica, and asserts every safety invariant after
// each operation.
//
// The first byte selects the starting role. Half the inputs therefore attack
// a follower and half attack a leader; the two halves reach disjoint handler
// sets, and the leader half is where a forged response turns into a commit.
func runAdversarialStepSequence(tb testing.TB, data []byte) {
	tb.Helper()
	runAdversarialStepSequenceCov(tb, data, nil)
}

// runAdversarialStepSequenceCov is runAdversarialStepSequence with optional
// behaviour accounting.
func runAdversarialStepSequenceCov(tb testing.TB, data []byte, cov *harnessCoverage) {
	tb.Helper()
	r, lr := newAdversarialRaft(tb)

	s := &fuzzStream{b: data}
	if s.next()&0x01 != 0 {
		electSelf(r)
	}
	inv := newRaftInvariants(r)
	startCommit := r.log.committed
	startLast := r.log.lastIndex()

	for op := 0; op < fuzzMaxOps && !s.done(); op++ {
		switch s.next() % 8 {
		case fuzzOpTick:
			n := 1 + int(s.next())%fuzzMaxTickBurst
			for i := 0; i < n; i++ {
				r.tickFunc(r)
			}
			settlePendingSnapshot(r, lr)
			inv.check(tb, r, fmt.Sprintf("op %d: tick x%d", op, n))
		case fuzzOpPersist:
			fuzzPersist(r)
			settlePendingSnapshot(r, lr)
			inv.check(tb, r, fmt.Sprintf("op %d: persist", op))
		default:
			m := s.message(adversarialShardID, adversarialSelfID, liveState(r))
			label := fmt.Sprintf("op %d: step %s(from=%d term=%d logTerm=%d logIndex=%d commit=%d hint=%d reject=%t entries=%d snapIndex=%d snapTerm=%d ccid=%d)",
				op, proto.MessageTypeName(m.Type), m.From, m.Term, m.LogTerm,
				m.LogIndex, m.Commit, m.Hint, m.Reject, len(m.Entries),
				m.Snapshot.Index, m.Snapshot.Term, m.Snapshot.Membership.ConfigChangeID)
			before := r.log.lastIndex()
			// The step path returns typed errors for refused input; a refusal
			// is a valid outcome, the invariants are what matter.
			err := r.step(m)
			settlePendingSnapshot(r, lr)
			inv.check(tb, r, label)
			if cov != nil {
				if r.log.lastIndex() < before {
					cov.logsTruncated++
				}
				if m.Type == proto.InstallSnapshot && errors.Is(err, ErrWireLocalMessage) {
					cov.snapshotsRefused++
				}
			}
		}
		cov.record(r)
		// Drain so the outbound buffer cannot grow without bound; the
		// invariant check above has already inspected it.
		r.drainMessages()
	}
	if cov != nil {
		if r.log.committed > startCommit {
			cov.committedAdvanced++
		}
		if r.log.lastIndex() < startLast {
			cov.logsTruncated++
		}
	}
}

// --------------------------------------------------------------------------
// Seed corpus
// --------------------------------------------------------------------------

// adversarialSeed pairs a corpus entry with the reason it exists, so a
// regression names the finding it re-introduced.
type adversarialSeed struct {
	name string
	data []byte
}

// adversarialSeeds is the seed corpus shared by the fuzz target and the
// unit test. The first two entries are the shipped criticals.
//
// Every seed is emitted twice, once against a follower and once against a
// pre-elected leader, because the two roles dispatch on disjoint message sets
// and a message the follower ignores may be the one the leader acts on.
func adversarialSeeds() []adversarialSeed {
	var seeds []adversarialSeed
	add := func(name string, build func(e *fuzzEncoder)) {
		for _, asLeader := range []bool{false, true} {
			var e fuzzEncoder
			e.start(asLeader)
			build(&e)
			role := "follower"
			if asLeader {
				role = "leader"
			}
			seeds = append(seeds, adversarialSeed{name: name + "/" + role, data: e.b})
		}
	}

	// FINDING 1 (critical, shipped): a forged wire InstallSnapshot carrying
	// Term 0 rewrote the shard's membership. Term 0 was the local-injection
	// escape hatch, so the message bypassed every term comparison, and
	// handleSnapshot's non-matching branch feeds ss.Membership straight into
	// rebuildRemotesFromMembership -- the set quorum() is computed over. The
	// huge ConfigChangeID makes the forged configuration look newest.
	add("forged-install-snapshot-term0", func(e *fuzzEncoder) {
		e.step(seedMessage{
			Type: proto.InstallSnapshot,
			From: 2,
			Term: 0,
			Snapshot: &seedSnapshot{
				Index:                      9,
				Term:                       1,
				ConfigChangeID:             math.MaxUint64,
				MaxWitnesses:               7,
				RequireFullReplicaMajority: true,
				Voters:                     []uint64{4, 5},
				Witnesses:                  []uint64{3},
			},
		})
	})

	// The same forgery at a PLAUSIBLE term. The zero-term gate does not
	// apply here, so this is the variant that has to be stopped by the
	// process-local origin check alone.
	add("forged-install-snapshot-live-term", func(e *fuzzEncoder) {
		e.step(seedMessage{
			Type: proto.InstallSnapshot,
			From: 2,
			Term: 5,
			Snapshot: &seedSnapshot{
				Index:          9,
				Term:           5,
				ConfigChangeID: math.MaxUint64,
				Voters:         []uint64{4, 5},
				Observers:      []uint64{99},
			},
		})
	})

	// FINDING 2 (critical, shipped): a term-less local-control type accepted
	// from the wire. Nothing forwards these between replicas, so a peer
	// naming one is claiming to be this host's own transport or admin API,
	// and no term, log, or quorum check constrains them.
	add("wire-local-control-types", func(e *fuzzEncoder) {
		for _, t := range []uint64{
			proto.SnapshotStatus, proto.Unreachable, proto.LeaderTransfer, proto.InstallSnapshot,
		} {
			e.step(seedMessage{Type: t, From: 2, Term: 0})
			e.step(seedMessage{Type: t, From: 2, Term: 1})
			e.step(seedMessage{Type: t, From: 2, Term: 9})
		}
	})

	// Term 0 across every wire type: the escape hatch must be closed for all
	// of them, not only the four process-local ones.
	add("zero-term-every-type", func(e *fuzzEncoder) {
		for _, t := range fuzzMessageTypes {
			e.step(seedMessage{Type: t, From: 2, Term: 0, LogIndex: 3, LogTerm: 1, Commit: 9})
		}
	})

	// A Replicate claiming a commit index far past anything the replica
	// holds. handleAppend takes min(msg.Commit, lastIdx), and the phantom
	// commit index is what turns into a false quorum acknowledgement.
	add("replicate-commit-beyond-log", func(e *fuzzEncoder) {
		e.step(seedMessage{
			Type: proto.Replicate, From: 2, Term: 4,
			LogIndex: 3, LogTerm: 1, Commit: math.MaxUint64,
		})
		e.step(seedMessage{
			Type: proto.Replicate, From: 2, Term: 4,
			LogIndex: 3, LogTerm: 1, Commit: math.MaxUint64,
			Coherent: true, Entries: 2,
		})
		e.persist()
	})

	// prevLogIndex at the uint64 ceiling: prevLogIndex+1+i overflows in
	// tryAppend's contiguity check.
	add("replicate-index-overflow", func(e *fuzzEncoder) {
		e.step(seedMessage{
			Type: proto.Replicate, From: 2, Term: 4,
			LogIndex: math.MaxUint64, LogTerm: 1, Commit: math.MaxUint64,
			Coherent: true, Entries: 3,
		})
		e.step(seedMessage{
			Type: proto.Replicate, From: 2, Term: 4,
			LogIndex: math.MaxUint64 - 2, LogTerm: 4, Commit: math.MaxUint64,
			Coherent: true, Entries: 3,
		})
	})

	// Win an election, then have a peer report a match beyond the leader's
	// own log. Accepting it would push rm.match past lastIndex and let
	// tryCommit advance over entries that do not exist.
	add("elect-then-overstated-match", func(e *fuzzEncoder) {
		// Time out into PreVote, collect two pre-vote grants, then two real
		// vote grants. Align the response terms to the live term so the seed
		// does not depend on the randomized election timeout.
		e.tick(32)
		e.step(seedMessage{Type: proto.RequestPreVoteResp, From: 2, Align: alignTermCurrent})
		e.step(seedMessage{Type: proto.RequestPreVoteResp, From: 3, Align: alignTermCurrent})
		e.step(seedMessage{Type: proto.RequestVoteResp, From: 2, Align: alignTermCurrent})
		e.step(seedMessage{Type: proto.RequestVoteResp, From: 3, Align: alignTermCurrent})
		e.persist()
		e.step(seedMessage{Type: proto.ReplicateResp, From: 2, LogIndex: math.MaxUint64, Align: alignTermCurrent})
		e.step(seedMessage{Type: proto.ReplicateResp, From: 3, LogIndex: math.MaxUint64, Align: alignTermCurrent})
		e.persist()
		// And an honest ack, so the commit path runs to completion.
		e.step(seedMessage{Type: proto.ReplicateResp, From: 2, Align: alignTermCurrent | alignLogToLast})
		e.step(seedMessage{Type: proto.ReplicateResp, From: 3, Align: alignTermCurrent | alignLogToLast})
		e.persist()
	})

	// Vote solicitation from a non-member and from the reserved zero ID, at
	// a term far in the future. Counting a grant from outside the
	// configuration is a two-leaders-in-one-term defect.
	add("votes-from-outside-configuration", func(e *fuzzEncoder) {
		e.step(seedMessage{Type: proto.RequestVote, From: 4, Term: math.MaxUint64, LogIndex: math.MaxUint64, LogTerm: math.MaxUint64})
		e.step(seedMessage{Type: proto.RequestVoteResp, From: 4, Term: math.MaxUint64})
		e.step(seedMessage{Type: proto.RequestVoteResp, From: 0, Term: math.MaxUint64})
		e.step(seedMessage{Type: proto.RequestVoteResp, From: 99, Term: math.MaxUint64})
		e.step(seedMessage{Type: proto.RequestVote, From: 2, Term: math.MaxUint64, Hint: 2})
		e.step(seedMessage{Type: proto.RequestVote, From: 3, Term: math.MaxUint64, Hint: 3})
	})

	// A leadership-transfer vote hint (Hint == From) is exempt from both the
	// CheckQuorum drop and the known-leader veto, so it is the highest-value
	// field for a vote-stealing peer.
	add("transfer-hint-vote-storm", func(e *fuzzEncoder) {
		e.step(seedMessage{Type: proto.Heartbeat, From: 2, Term: 3, Commit: 2})
		for term := uint64(4); term < 10; term++ {
			e.step(seedMessage{Type: proto.RequestVote, From: 3, Term: term, Hint: 3, LogIndex: 3, LogTerm: 1})
			e.step(seedMessage{Type: proto.RequestVote, From: 2, Term: term, Hint: 2, LogIndex: 3, LogTerm: 1})
		}
	})

	// Snapshot at or below the committed index, and a snapshot whose term
	// matches the log at that point: both branches of handleSnapshot.
	add("snapshot-boundary-indices", func(e *fuzzEncoder) {
		for _, idx := range []uint64{0, 1, 2, 3, 4, math.MaxUint64} {
			e.step(seedMessage{
				Type: proto.InstallSnapshot, From: 2, Term: 3,
				Snapshot: &seedSnapshot{Index: idx, Term: 1, Voters: []uint64{1, 2, 3}},
			})
		}
	})

	// TimeoutNow from a peer at every plausible term: it is the campaign
	// entry point that bypasses startElection's gates.
	add("timeout-now-storm", func(e *fuzzEncoder) {
		for term := uint64(0); term < 6; term++ {
			e.step(seedMessage{Type: proto.TimeoutNow, From: 2, Term: term})
		}
		e.persist()
	})

	// A plain healthy exchange, so the corpus contains at least one input
	// that drives the accept paths end to end.
	add("healthy-replication", func(e *fuzzEncoder) {
		e.step(seedMessage{Type: proto.Heartbeat, From: 2, Term: 2, Commit: 3})
		for i := 0; i < 4; i++ {
			e.step(seedMessage{
				Type: proto.Replicate, From: 2, Coherent: true, Entries: 3,
				Align: alignTermCurrent | alignLogToLast | alignCommitToBatch,
			})
			e.persist()
		}
		e.step(seedMessage{Type: proto.Heartbeat, From: 2, Commit: math.MaxUint64, Align: alignTermCurrent})
	})

	// A conflicting append that truncates the uncommitted tail, followed by a
	// commit over the replacement. Truncation is where a stale commit index
	// turns into a State Machine Safety violation.
	add("conflicting-append-truncation", func(e *fuzzEncoder) {
		e.step(seedMessage{
			Type: proto.Replicate, From: 2, Coherent: true, Entries: 4,
			Align: alignTermCurrent | alignLogToLast,
		})
		e.persist()
		e.step(seedMessage{
			Type: proto.Replicate, From: 3, Coherent: true, Entries: 4,
			Align: alignTermNext | alignLogToCommit | alignCommitToBatch,
		})
		e.persist()
		e.step(seedMessage{Type: proto.Heartbeat, From: 3, Commit: math.MaxUint64, Align: alignTermCurrent})
	})

	return seeds
}

// TestAdversarialStepSeeds replays the seed corpus through the invariant
// harness. This is what makes the fuzz target meaningful in CI: `go test`
// without -fuzz runs only the seeds, and the two shipped criticals are among
// them, so a regression fails the ordinary test run.
func TestAdversarialStepSeeds(t *testing.T) {
	quietLogs(t)
	for _, seed := range adversarialSeeds() {
		t.Run(seed.name, func(t *testing.T) {
			runAdversarialStepSequence(t, seed.data)
		})
	}
}

// sweepInput builds the i'th deterministic sweep input. Sharing it between
// the sweep and the reachability test guarantees the reachability numbers
// describe the inputs the sweep actually runs.
func sweepInput(rng *mathrand.Rand, buf []byte) []byte {
	n := 16 + rng.Intn(len(buf)-16)
	chunk := buf[:n]
	for j := range chunk {
		chunk[j] = byte(rng.Intn(256))
	}
	return chunk
}

// TestAdversarialHarnessReachesRaftBehaviour is the anti-vacuity gate.
//
// An adversarial harness whose inputs are all refused at the front door
// asserts nothing: every invariant holds trivially over a state machine that
// never changed state. That failure is invisible -- the suite is green -- so
// the reachability of each interesting behaviour is asserted explicitly here,
// and a change to the grammar, the config, or the step path that stops
// reaching them fails this test instead of silently hollowing out the fuzzer.
func TestAdversarialHarnessReachesRaftBehaviour(t *testing.T) {
	quietLogs(t)
	cov := newHarnessCoverage()
	for _, seed := range adversarialSeeds() {
		runAdversarialStepSequenceCov(t, seed.data, cov)
	}
	rng := mathrand.New(mathrand.NewSource(0x5EED_1A57))
	buf := make([]byte, 512)
	for i := 0; i < 3000; i++ {
		runAdversarialStepSequenceCov(t, sweepInput(rng, buf), cov)
	}

	// Each threshold is deliberately far below the observed value: the point
	// is to catch a collapse to zero (or near it), not to pin a number that
	// legitimate changes would churn.
	checks := []struct {
		name string
		got  int
		min  int
	}{
		{"follower observations", cov.roles[follower], 1000},
		{"pre-candidate observations", cov.roles[preCandidate], 100},
		{"candidate observations", cov.roles[candidate], 100},
		{"leader observations", cov.roles[leader], 100},
		{"appends accepted", cov.appendsAccepted, 50},
		{"appends rejected", cov.appendsRejected, 50},
		{"votes granted", cov.votesGranted, 50},
		{"votes denied", cov.votesDenied, 50},
		{"runs that advanced the commit index", cov.committedAdvanced, 50},
		{"log truncations", cov.logsTruncated, 5},
		{"wire InstallSnapshot refusals", cov.snapshotsRefused, 50},
	}
	for _, c := range checks {
		if c.got < c.min {
			t.Errorf("harness reached %s only %d times, want >= %d: the adversarial sweep is not exercising this path and its invariants are vacuous",
				c.name, c.got, c.min)
		}
	}
	t.Logf("reachability: roles=%v appendsAccepted=%d appendsRejected=%d votesGranted=%d votesDenied=%d commitAdvanced=%d truncations=%d wireSnapshotRefusals=%d",
		cov.roles, cov.appendsAccepted, cov.appendsRejected, cov.votesGranted,
		cov.votesDenied, cov.committedAdvanced, cov.logsTruncated, cov.snapshotsRefused)
}

// TestAdversarialStepSweep is the deterministic property test: a fixed-seed
// pseudo-random sweep of the same input grammar. It runs in CI on every
// commit and covers the space between the hand-written seeds without
// requiring -fuzz.
func TestAdversarialStepSweep(t *testing.T) {
	quietLogs(t)
	const iterations = 3000
	rng := mathrand.New(mathrand.NewSource(0x5EED_1A57))
	buf := make([]byte, 512)
	for i := 0; i < iterations; i++ {
		n := 16 + rng.Intn(len(buf)-16)
		chunk := buf[:n]
		for j := range chunk {
			chunk[j] = byte(rng.Intn(256))
		}
		t.Run(fmt.Sprintf("iter%d", i), func(t *testing.T) {
			runAdversarialStepSequence(t, chunk)
		})
	}
}
