# Seal and barrier: architecture

> **Status: NORMATIVE** · Scope: go-quicraft `pkg/seal` (and the `pkg/crypto`
> compatibility shim) · Last verified: 2026-10-13
> Conflicts with this document are defects. Fix them here first.

## The problem

A consensus engine writes everything it is told to a log and to snapshots, and
those artifacts sit on disk long after the process that wrote them. Encrypting
them needs a key; a key needs somewhere to live; and "somewhere" turns out to be
a decision with hardware, cloud and threshold-custody variants that a Layer-1
library must not know about.

The barrier keeps those two problems fully separate, so they compose freely:

| Layer | Responsibility |
|---|---|
| **`Barrier`** | derives DEKs from a root key via HKDF-SHA256, encrypts and decrypts every at-rest byte with an AEAD, manages epoch rotation, and enforces the AEAD safety bounds |
| **`SealingStrategy`** | protects the root key at rest. Anything that can wrap and unwrap 32 bytes qualifies. |

A TPM-wrapped root key in a Raft-backed store, a passphrase-wrapped key in a
file, an HSM-wrapped key in a KMS: all the same code path.

**`pkg/seal` is the Layer-1 encryptor only.** It holds the `Barrier` (alias
`EpochBarrier`: epoch DEKs, HKDF, the AEAD data path, rotation), the `None` and
`Software` (passphrase) sealing strategies, the `SealingStrategy` interface and
its optional `ProtectedSealingStrategy` sibling, and the
`ProtectedShareSplitter` / `ProtectedShareAccumulator` interfaces. Shamir
custody, M-of-N quorum, recovery, rekey, root tokens and the Security-Officer
ceremony live in go-qrdb; this repository holds only the GF(256) primitive (see
[`../shamir-primitive/architecture.md`](../shamir-primitive/architecture.md)).

## The shape chosen

```
┌──────────────────────────── Node ────────────────────────────┐
│                                                              │
│   WAL records      Snapshot frames      (SM files)           │
│        │                  │                  │               │
│        └──────────┬───────┴──────────────────┘               │
│                   ▼                                          │
│            ┌─────────────┐   epoch DEK = HKDF(root, salt,    │
│            │   Barrier   │      deploymentID, epoch, alg)    │
│            │  epoch DEKs │   AEAD = AES-GCM | ChaCha20-Poly  │
│            └──────┬──────┘                                   │
│                   ▼                                          │
│          ┌──────────────────┐  wraps the ROOT KEY only       │
│          │ SealingStrategy  │  passphrase | none  (here)     │
│          │                  │  tpm2/pkcs11/kms    (go-xkms)  │
│          └──────────────────┘  shamir              (go-qrdb) │
└──────────────────────────────────────────────────────────────┘
```

**Per-node, not clustered.** Each node manages its own barrier and root key.
QUIC/mTLS protects data in transit; the barrier protects it at rest. Nodes cannot
decrypt each other's at-rest data: a snapshot sent between nodes is decrypted
for transport and re-encrypted by the receiver under its own key.

**The barrier writes nothing to disk.** It holds sealed state in memory and
hands it to a caller-supplied `StatePersister` when that state changes.
Durability, exclusivity, permissions and backup of the sealed blob belong to the
embedder. That is the Layer-1 boundary: `pkg/seal` is the encryptor, go-qrdb is
the custodian.

## Epochs: rotation without re-encryption

1. One root key, generated at `Initialize`, never changing.
2. An epoch counter starting at 1, incremented by `Rotate`.
3. `DEK(epoch) = HKDF-SHA256(rootKey, salt, "quicraft/barrier/v1/" ‖
   deploymentID ‖ epoch [‖ algID ‖ keyLen])`.
4. Writes always use the current epoch's DEK.
5. Reads take the epoch from the ciphertext header and look up that epoch's DEK.
6. Old data is **never re-encrypted**. It ages out when compaction deletes it.

Ciphertext frame:

```
[Epoch:8 big-endian | AlgID:1 | Nonce:N | Ciphertext | Tag:16]
```

The epoch and algorithm ID are unencrypted because both must be known before any
key material is touched: the epoch selects the DEK, the algorithm ID gives the
nonce length. `N` is the algorithm's nonce size (12 for the AES-GCM family, 24
for XChaCha20-Poly1305), so the header is not a fixed width.

The endianness split is deliberate: the epoch is big-endian in the header, for
readability when inspecting raw ciphertext, and little-endian in the AAD. They
are not interchangeable.

### Rejected: re-encrypting on rotation

Rotating by rewriting every WAL segment and snapshot gives forward secrecy
against a key compromise, at the cost of downtime, I/O amplification
proportional to the data set, and a partially-rewritten state to recover from.
Epoch derivation gets zero-downtime rotation for the price of retaining old
DEKs, which are 32 bytes each and re-derivable from the root key anyway.

### Rejected: a cluster-wide DEK

One key shared by every replica would let a follower read a leader's at-rest
data and would make key rotation a consensus operation. Per-node barriers keep
key custody a local decision and keep the blast radius of one compromised node
to that node.

## Two safety bounds, and why both exist

Nonces are random, not counters: there is no `[SegmentID|WriteEpoch|RecordSeq]`
construction anywhere in the barrier, the WAL or the snapshot pipeline. For
AES-GCM the 12 nonce bytes are drawn inside the FIPS module by
`cipher.NewGCMWithRandomNonce` and prepended by the AEAD itself; FIPS 140-3
(SP 800-38D §8.2.2, CMVP IG C.H) prohibits an IV generated in application code,
and Go refuses it outright under `GODEBUG=fips140=only`.

Random 96-bit nonces make *collision*, not repetition, the concern, so:

| Bound | What it is | Coverage |
|---|---|---|
| **Invocation cap** | `InvocationCounter` refuses encryption past 2^32 per epoch (`DefaultInvocationLimit`) | the whole epoch. A hard collision bound, not advice. |
| **Nonce-reuse tracker** | `InMemoryNonceTracker` records nonces and rejects a repeat | a **prefix**: the first `DefaultMaxTrackedNonces` (4Mi, ~160 MB) of the epoch, then it saturates |

The tracker is a bounded-memory backstop against a defective RNG, **not** a
proof of uniqueness across an epoch. With the default limit it covers 0.0977% of
the permitted invocations; the remainder rests on the SP 800-38D random-nonce
argument. It reports its own state honestly through `Saturated()` and `Count()`
rather than silently disabling itself.

A GCM nonce collision yields keystream XOR and recovery of the GHASH
authentication key, which means arbitrary forgery of at-rest records. That is why the cap
fails closed rather than warning.

One consequence of module-generated nonces: on the AES-GCM path the tracker runs
*after* the `Seal`, because the nonce is not knowable before it. A reused nonce
still fails closed and the ciphertext never reaches the caller, but the AEAD
invocation has already happened. The alternative is a caller-chosen IV, which is
the construction FIPS forbids.

## AAD binding: epoch, then position

**The baseline is epoch-only.** `Encrypt`/`Decrypt` bind the 8-byte
little-endian epoch as AAD, which defeats cross-epoch splicing. It does not defeat
same-epoch reorder, splice or truncation: two frames under one DEK authenticate
identically wherever they sit.

**Positional binding is the opt-in layer.** `aadForEpoch(epoch, contextAAD)`
appends a caller context after the fixed 8-byte epoch prefix, so the boundary is
unambiguous and no context can alias the epoch bytes. Both in-repo consumers
pass one:

| Consumer | Context | Layout |
|---|---|---|
| Snapshot frames | 40 bytes (`engine.SnapshotFrameAADLen`) | `"QRSNAP1"(7) ‖ ShardID:8 ‖ ReplicaID:8 ‖ SnapshotIndex:8 ‖ ChunkSeq:8 ‖ Final:1`, big-endian |
| WAL records | 30 bytes | `"QRWAL1"(6) ‖ WALShardIdx:8 ‖ SegmentID:8 ‖ RecordSeq:8`, big-endian |

Both are magic-tagged, fixed-width and delimiter-free, so no two distinct
positions can produce the same bytes. The magic is a scheme tag, not a version
byte: it is part of the authenticated context rather than a byte on disk, so
a frame sealed under a future layout can never authenticate against this one and
there is no unauthenticated marker for an attacker to rewrite.

**There is no epoch-only fallback, and there must never be one.** A reader that
fails to authenticate a frame must not retry without the context; that retry
would be the vulnerability.

`WALShardIdx` is required rather than decorative: each WAL shard is its own
directory with an independent `%06d.log` ID space and all of them share one
barrier, so `(SegmentID, RecordSeq)` alone is not unique.

### Tail truncation: detected for snapshots, a rollback for the WAL

Positional AAD alone does not name how many frames an artifact holds, so every
surviving frame of a shortened artifact still authenticates at its own ordinal.
The two consumers close that differently, because they are different kinds of
artifact.

**Snapshots carry a terminal flag.** The last byte of every snapshot frame's
context is `Final`: 1 on exactly one frame, the artifact's last, and 0 on every
other. The frame writer holds a full chunk back until more plaintext proves it
is not the last, so `Close` always emits a terminal frame (an empty one when
the plaintext was empty or ended on a chunk boundary). The one reader,
`engine.OpenSnapshotFrames`, reads a frame ahead to decide which frame it is
authenticating as the last. So:

- **Truncated**: the frame the artifact now ends on was sealed with `Final=0`
  and fails authentication when read as the last.
- **Extended**: an artifact's terminal frame followed by frames spliced in from
  a longer artifact of the same identity (whose ordinals line up) fails on the
  terminal frame, read as non-terminal.
- **Truncated to nothing**: no frame is left to authenticate, so a sealed
  artifact with no frames is refused structurally as `ErrSnapshotCorrupt`.

A barrier that cannot bind caller context (a delegated encryptor without AAD
support, or plaintext mode) gets no positional binding and therefore no
truncation detection beyond the structural zero-frame rule; that capability is
resolved once per deployment, never per artifact.

**The WAL's case is a rollback, and no in-file format can detect it.** A WAL
legitimately loses its unacknowledged tail in a crash, and the recovery rules
(`GenStart`, the sync mark) exist precisely to tell that apart from lost
acknowledged data. An attacker with write access to the disk who removes
trailing records *and* the sync mark after them produces bytes identical to a
crash inside the writeback window, so recovery truncates as it would for the
crash. Telling the two apart needs a trusted, monotonic record of the
acknowledged end kept somewhere the attacker cannot rewrite: a hardware
monotonic counter (a TPM NV index, via go-xkms), or confirmation from peers.
Both are outside this layer, so **at-rest rollback of the WAL by someone who can
write the disk is not detected**. The consequence is not bounded by Raft: its safety argument assumes
stable storage, and a replica rolled back past what it acknowledged may have
forgotten entries it counted toward a commit, or a vote it cast in the current
term. Treat write access to a replica's data directory as equivalent to
compromising that replica.

**Cross-artifact splicing is accepted on the same grounds.** A snapshot frame's
context carries nothing unique to one artifact, so two artifacts sealed under
one key at the same `(shard, replica, index)` (a rebuild re-save, or a local
save and a receive at one index) can exchange frames, and a spliced file still
authenticates frame by frame. It needs write access to the data directory and a
retained copy of the other artifact, which is the rollback threat above.
*Rejected: a per-artifact random value folded into the context.* It would close
this case, but the attacker it stops already has the rollback capability, which
no in-file value can stop.

## Delegated (hardware) mode is total, by design

`SealingStrategy.BarrierEncryptor` decides which of two very different
deployments you are in:

| Returns | Hardware protects | Barrier's data path |
|---|---|---|
| `(nil, nil)` | the **root key only**; the barrier derives epoch DEKs and encrypts itself | fully active |
| a non-nil `SymmetricEncrypter` | the **entire data path**; the root key need never enter process memory | entirely bypassed |

`(nil, nil)` is correct and intended, not an unimplemented stub. Every
non-hardware strategy returns it, including this repository's `SoftwareStrategy`
and `NoneStrategy`.

Delegation is total. A non-nil encryptor bypasses epoch DEK derivation, the
`[epoch][algID]` header, the nonce-reuse tracker, the SP 800-38D invocation
counter, and the load-generation fence; `Rotate` and `PurgeEpochsBefore` return
`ErrHardwareMode`. None of that is an oversight: the delegated encryptor owns
its key and nonce management, and the barrier cannot count invocations against a
key it does not control. Every barrier setup logs this at WARN, naming exactly
what is delegated.

The usual hardware deployment does not delegate: the strategy wraps the root key
in the device and returns a nil encryptor, keeping the barrier's full AEAD stack,
positional AAD included. The delegated path exists and is tested; choosing it is
a deliberate act.

### The AAD capability contract, and why it is measured

A delegated encryptor that can authenticate caller AAD end to end declares it by
also implementing `AADSymmetricEncrypter`. **Do not implement it on an encryptor
that accepts an `aad` argument and drops it**: advertising a binding that does
not exist is strictly worse than not advertising one. The failure is silent:
`SupportsContextAAD()` reports true, callers stop suppressing their context,
`RequireContextAAD` passes at startup, the context is discarded on every write,
and nothing can retroactively bind ciphertext already at rest.

So the declaration is measured, not trusted. At `Initialize` and `Unseal` only
(never on the data path), `verifyAADBinding` runs one encrypt and two
decrypts with random 32-byte values: seal a probe under `aadA`, require it to
open under `aadA` returning the same bytes, and require it to **fail** under an
independently drawn `aadB`. An encryptor that drops the argument passes the
first two and fails the third. Both values are random so a known probe cannot be
special-cased, and `aadB` is an independent draw rather than a mutation of
`aadA` so a length- or prefix-sensitive implementation cannot pass by accident.

A failing probe logs at ERROR and downgrades the encryptor to
`ContextBindingUnsupported`, so `RequireContextAAD` then refuses the deployment
rather than accepting it. The probe is a backstop, not a licence to implement the
interface speculatively.

## Sealed-state integrity: three mechanisms for three different holes

The wrapping AEAD covers only `SealedRootKey.Ciphertext`, with the constant
strategy name as AAD. Every other field sits in plaintext beside it, bound to
nothing.

### `StateMAC`: authenticating the metadata

HMAC-SHA256 over a canonical, length-prefixed encoding, keyed by
`HKDF-SHA256(rootKey, info="quicraft/barrier/state-mac/v1")`. Every
variable-length field carries a fixed 8-byte big-endian length prefix, so no
field's contents can imitate another's boundary.

Covered: `Strategy`, `Version`, the barrier's `BarrierConfig.DeploymentID`
(not a blob field), `Epoch`, `LoadGeneration`, `Salt`, `KnownEpochs`,
`HKDFVersion`, `Algorithm`, `Ciphertext`, `Metadata`, `Nonce`,
`HardwarePayload`. `Ciphertext` is included even though it
authenticates itself, so genuine metadata from one blob cannot be paired with a
genuine ciphertext from another.

Deliberately **not** covered: `FailedAttempts` and `LastFailedUnixNano`, written
after a *failed* unseal when no root key is available to re-MAC with; and
`CreatedAt`, `ShamirThreshold`, `ShamirTotal`, which are higher-layer custody
bookkeeping go-qrdb owns; and `KDFVersion`, which `pkg/seal` neither reads nor
writes (the software strategy records its KDF version in `Metadata[0]`, which is
covered).

The field that matters most is `Epoch`: rewriting it downward resurrects a spent
DEK alongside a zeroed invocation counter and an empty nonce tracker: two
independent random-nonce sequences under one key.

A MAC failure is deliberately not counted as a failed unseal attempt. The
credential was correct and the state is not, and driving brute-force backoff
from a file an attacker can write would hand them a lockout primitive.

The covered-field encoding is versioned by a leading byte, currently 2, which
covers `LoadGeneration` because the barrier refuses to encrypt on the strength of
that number. A tag over the version-1 field set (which lacks `LoadGeneration`) is
recognized only when `LoadGeneration == 0`, accepted once at WARN, and restamped
at version 2. That is not a downgrade: forging either version needs the same
HKDF-derived MAC key, which comes from the root key the strategy just recovered.

A blob with no MAC is refused with `ErrSealedStateUnauthenticated`; accepting an
absent tag would make the mechanism optional at the attacker's discretion. `AllowUnauthenticatedSealedState` is the explicit one-start
migration hatch, and does not apply to a tag that is present but wrong.

### `MinEpoch`: refusing replay

A MAC cannot detect replay: an attacker with write access can restore an older,
genuinely-MACed copy. Only state the attacker cannot roll back in the same step
catches that, and the barrier holds none; the embedder does.
`BarrierConfig.MinEpoch` is that floor, and the effective floor is
`max(MinEpoch, b.epoch)`. Set it to the highest epoch any live WAL segment or
retained snapshot still references; the engine already computes exactly this as
its global safe-purge floor.

### The load-generation fence: detecting a competing claim

`LoadGeneration` counts how many times a blob has been claimed. `Initialize`
stamps 1; every `Unseal` that can persist writes `LoadGeneration+1` before the
barrier will encrypt. A barrier later handed a blob naming a higher generation
marks itself permanently superseded and refuses `Encrypt`, `Rotate`,
`PurgeEpochsBefore` and `Reset`; `Decrypt` is unaffected, and it cannot launder
the state by sealing and unsealing again. The winner's state is deliberately not
adopted: picking up the winner's epoch would make this instance a second writer
of the winner's DEK, which is the failure being prevented.

The ordering is the point: the claim must be durable before this barrier
encrypts, or a process that dies between encrypting and persisting leaves a
generation the next loader increments without ever learning a DEK was used under
it.

**It is not a lock:**

- Not continuous: compared at load, enforced at write or use. The barrier never
  re-reads the blob on its own, because that would put a disk read on the
  encrypt hot path. A barrier superseded while running keeps encrypting until
  the embedder hands it the newer state.
- Two loaders that both read before either writes claim the same number and
  neither can tell.
- Inert in delegated and no-encryption mode, and inert without a
  `StatePersister` (logged at WARN).

A generation that goes *backwards* is a rollback, not a competing claim: logged
at ERROR, execution continues, because refusing there would let anyone who can
restore an old backup stop a node from writing. That case is `MinEpoch`'s.

**Exclusivity remains the embedder's obligation.** Every DEK input is a function
of the blob and the forced unseal rotation is `epoch+1`, so two barriers loaded
from one blob derive the *same* DEK, each with a zeroed counter and an empty
tracker. Only an exclusive data-directory lock or a compare-and-swap persister
makes single-writer a guarantee.

## Trust boundaries

- **The root key** lives in an `enclave.SealedEnclave` (see
  [`../memory-enclave/architecture.md`](../memory-enclave/architecture.md)),
  opened into a locked enclave only for derivation, export and unseal.
  `Initialize` generates it directly into locked memory, so on the initialize
  path it is never an ordinary heap slice at all. `Unseal` cannot do the same:
  `SealingStrategy.UnsealRootKey` returns a `[]byte`, so the key does reach the
  heap. `Unseal` copies it into locked memory and wipes the heap copy at once,
  before MAC verification, encryptor resolution or DEK derivation, and a deferred
  wipe covers every other return.
- **Epoch DEKs are protected the same way and are never opened.** Each is held in
  its own `SealedEnclave` so a DEK can be re-derived, but neither `Encrypt` nor
  `Decrypt` touches one: both run off the `cipher.AEAD` cached per epoch in
  `b.ciphers`. Opening a key is a setup, rotation and rekey cost, not a per-frame
  one. The AEAD cache is what makes the DEK's protection free at steady state,
  and also what makes it incomplete, which is the next bullet.
- **Live `cipher.AEAD`s cannot be wiped, and they are the only un-erasable
  copy.** Each holds a copy of the DEK in its key schedule, and neither the
  standard library nor `x/crypto` offers a wipe API. Dropping the barrier's
  reference on seal or purge makes it GC-eligible, and that is the only bound: a
  core dump taken before collection contains every live and recently purged epoch
  DEK. The sealed copies beside them are wiped: `SealedEnclave.Destroy` zeroes
  the ciphertext.
- **A purge revokes capability; it is not crypto-shredding.** After
  `PurgeEpochsBefore` returns, the barrier can no longer decrypt the epoch, and
  the epoch's sealed DEK has been wiped, but the cached AEAD's copy of it
  remains until garbage collection.
- **`Decrypt` returns one generic error** (`ErrDecryptionFailed`) for every
  failure mode (unknown epoch, authentication failure, corruption), so the error
  type cannot be used as an oracle.
- **Metadata leaks.** The outer WAL framing (`CRC32`, `Length`, `Type`) and the
  `Epoch` are unencrypted, so a reader of the file sees record size
  distribution, record type, and rotation epoch. Accepted: an attacker with
  local file read access is already high-privilege.
- **Compress-then-encrypt** (Snappy → AEAD) when both are on. CRIME/BREACH-style
  compression oracles are not practical for Raft because proposals originate
  from trusted application code.

## Component boundaries

- **`pkg/seal`**: the whole barrier (epoch DEKs, the AEAD data path, the two
  strategies, the safety trackers, the sealed-state format, the AAD binding
  probe) and the `StrategyID` type.
- **`pkg/crypto`**: five type aliases (`Barrier`, `BarrierConfig`,
  `SealingStrategy`, `Credentials`, `SealedRootKey`) and three forwarding
  functions (`NewBarrier`, `MustNewBarrier`, `NewCredentials`). No cipher code.
- **`pkg.Barrier`** (in `pkg/host.go`): the interface the `Host` accepts,
  declared there to break an import cycle. `*seal.Barrier` satisfies it.
- **`github.com/jeremyhahn/go-enclave`** (Layer 0): memory protection for all
  key material. This repository implements none of it and declares no
  protected-memory type of its own.
- **Consumers in this repo**: `pkg/logdb/waldb` through its own `WALBarrier`
  interface, and `pkg/internal/engine`'s snapshot pool. Neither imports
  `pkg/seal` directly.

### How the host routes barriers

The host-wide choice and the per-shard choice are two independent axes
(`WithBarrier` / `WithoutEncryption` for the first, `WithShardBarriers` for the
second). Exactly one host-wide statement is required, and `WithoutEncryption`
conflicts only with `WithBarrier` (`ErrAtRestEncryptionConflict`), so
`WithShardBarriers` combines with either:

- A shard whose resolver returns a barrier is encrypted under it, snapshots
  and log alike, in every host-wide posture, including `WithoutEncryption` and
  a `WithBarrier` barrier still in plaintext mode.
- A shard whose resolver returns `nil` takes the host-wide route: the
  `WithBarrier` barrier, or plaintext when `WithoutEncryption` declared it.
  With neither, `StartShard` fails closed with `ErrShardBarrierUnresolved`.
- A resolved barrier that is sealed fails `StartShard` with `ErrBarrierSealed`.
- The LogDB must actually hold the shard under that barrier
  (`ErrShardBarrierLogDBMismatch`); a LogDB that would store barriered shards'
  log in plaintext is refused at `NewHost` (`ErrShardBarriersPlaintextWAL`,
  `ErrBarrierPlaintextLogDB`). The contract, including the `logdb.AtRestHostRouted`
  report a multiplexing LogDB makes, is in
  [`../multi-group-host/api.md`](../multi-group-host/api.md).
- Snapshot encrypt/decrypt, transport epoch protection and DEK purge dispatch
  per shard, and barriers are matched by pointer identity, so purging one
  tenant's barrier never removes epochs live under another's. Rotation of a
  per-shard barrier is the embedder's; the host rotates only the `WithBarrier`
  one. `Host.Close` seals each distinct barrier once.

## Runtime seal: pausing a shard first

Sealing a running shard's barrier by itself leaves the engine mid-flight:
admission refuses new client work, but a log write already extracted from Raft
fails and is held until unseal (the sealed-write hold), and an apply or a
snapshot under way meets `ErrBarrierSealed`. That is safe, but untidy for an
embedder (go-qrdb) whose seal is routine. `Host.PauseShard` lets it quiesce the
shard first, so the seal finds nothing of the shard in flight:

```
PauseShard(ctx, shard) → barrier.Seal() → … → barrier.Unseal() → ResumeShard(ctx, shard)
```

What the pause stops, and where:

| Layer | Mechanism |
|---|---|
| Host admission | `nodeState.paused`, one atomic load per call, is checked beside the sealed check, so every client entry point returns `ErrShardPaused` (`Temporary`) |
| Step worker | A paused node is not ticked, and its inbox is discarded each pass: no campaign, vote, append, ack, forwarded proposal or forwarded read. A snapshot install it was handed is cancelled, not run |
| Apply | The apply worker parks a batch at the gate before the state machine, after releasing its in-flight registration, and continues it in log order on resume |
| Snapshot pool | A request taken off the queue for a paused shard is refused without running. A recovery is re-armed without counting an attempt, so a pause can never exhaust recovery into a shard failure; a dropped automatic save is re-requested on resume |
| Transport | A snapshot send or receive registers with the node, and is refused once paused |

`PauseNode` sets the flag first, waits for the step worker's iteration in
progress to end, then waits until no log batch is being persisted, no apply is
running, and no snapshot save, recovery or transfer is running or queued. Each
party that can start work registers before it reads the flag (the Dekker
order), so the wait cannot miss work that started concurrently. On `ctx`
expiry the shard stays paused and the work still settles.

A paused leader first hands off through the ordinary leader-transfer path to
the active voter with the highest match index, bounded by one election timeout;
otherwise it simply stops heartbeating. `ResumeShard` makes the replica a
follower at its current term with a fresh, randomised election timeout; the
term is unchanged, so nothing is persisted for it. No message type, field, or
format version was added.

A pause lives in memory only. A barrier sealed without a pause keeps the
sealed-write hold described in [user.md](user.md#serve-requests-while-sealed).

## What this feature deliberately does not do

- **No TPM, HSM, PKCS#11, cloud-KMS or Shamir strategy.** `pkg/seal` names
  exactly two `StrategyID` values, `StrategyPassphrase` and `StrategyNone`, and
  implements only those. `tpm2`, `pkcs11`, `awskms`, `gcpkms`, `azurekv` and
  `vault` belong to go-xkms; `shamir` to go-qrdb, which also owns the
  `StrategyID` namespace and the barrier registry.
- **No strategy catalog, preference order, or auto-ranking.** `Available()` is a
  startup diagnostic, not a selection mechanism. Selection is explicit at
  barrier or tenant setup.
- **No storage.** No file, no KV, no fsync. See `StatePersister`.
- **No `StorageBarrier`, `BarrierRegistry`, `TenantBarrier` or
  `StrategyRegistry`.** All are go-qrdb's.
- **No re-encryption of existing data** on rotation or compaction.
- **No transport encryption.** That is mTLS on the QUIC transport; see
  [`../quic-transport/architecture.md`](../quic-transport/architecture.md).
  Snapshots travel as plaintext over the mTLS session and are re-encrypted at
  rest by the receiver.
- **No hardware enclave.** go-enclave is `mlock`'d process memory and in-process
  AEAD, not SGX/SEV/TrustZone.
