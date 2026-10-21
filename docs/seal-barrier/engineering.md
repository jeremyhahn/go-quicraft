# Seal and barrier: engineering contract

> **Status: NORMATIVE** · Scope: go-quicraft `pkg/seal` ·
> Last verified: 2026-10-13
> Conflicts with this document are defects. Fix them here first.

Import path: `github.com/jeremyhahn/go-quicraft/pkg/seal`

## 1. Exported surface

### Construction and lifecycle

```go
func NewBarrier(config BarrierConfig) (*Barrier, error)
func MustNewBarrier(config BarrierConfig) *Barrier   // panics; constant configs only

// Aliases for readability at call sites: the same type, and FUNCTIONS that
// call the originals. Not `var NewEpochBarrier = NewBarrier`: an exported
// function variable in a crypto package is writable by every importer, so an
// init() in a transitive dependency could substitute the constructor for the
// barrier that holds the root key.
type EpochBarrier = Barrier
func NewEpochBarrier(config BarrierConfig) (*Barrier, error)
func MustNewEpochBarrier(config BarrierConfig) *Barrier

func (b *Barrier) Initialize(ctx context.Context, strategy SealingStrategy, creds Credentials) error
func (b *Barrier) SetSealedRootKey(sealed *SealedRootKey) error
func (b *Barrier) Unseal(ctx context.Context, strategy SealingStrategy, creds Credentials) error
func (b *Barrier) Seal() error
func (b *Barrier) Reset() error   // first-boot rollback only; see §2
```

### Data path

```go
func (b *Barrier) Encrypt(dst, plaintext []byte) ([]byte, error)
func (b *Barrier) EncryptWithContext(dst, plaintext, contextAAD []byte) ([]byte, error)
func (b *Barrier) EncryptWithEpoch(dst, plaintext []byte) ([]byte, uint64, error)
func (b *Barrier) EncryptWithEpochContext(dst, plaintext, contextAAD []byte) ([]byte, uint64, error)
func (b *Barrier) EncryptWithEpochPinned(dst, plaintext []byte) ([]byte, uint64, error)
func (b *Barrier) EncryptWithEpochPinnedContext(dst, plaintext, contextAAD []byte) ([]byte, uint64, error)

func (b *Barrier) Decrypt(dst, ciphertext []byte) ([]byte, error)
func (b *Barrier) DecryptWithContext(dst, ciphertext, contextAAD []byte) ([]byte, error)
func (b *Barrier) DecryptForEpoch(dst, ciphertext []byte, epoch uint64) ([]byte, error)
func (b *Barrier) DecryptForEpochContext(dst, ciphertext []byte, epoch uint64, contextAAD []byte) ([]byte, error)
```

### Epoch management

```go
func (b *Barrier) Rotate(ctx context.Context) (uint64, error)
func (b *Barrier) PurgeEpochsBefore(ctx context.Context, minEpoch uint64) (int, error)
func (b *Barrier) RegisterInFlightEpoch(epoch uint64)
func (b *Barrier) DeregisterInFlightEpoch(epoch uint64)
func (b *Barrier) CurrentEpoch() uint64
func (b *Barrier) ShouldRotateKey() bool
```

### Introspection

```go
func (b *Barrier) Config() BarrierConfig
func (b *Barrier) IsInitialized() bool
func (b *Barrier) IsSealed() bool
func (b *Barrier) IsSuperseded() bool
func (b *Barrier) IsHardwareMode() bool
func (b *Barrier) IsPlaintextMode() bool
func (b *Barrier) SupportsContextAAD() bool
func (b *Barrier) ContextBinding() ContextBinding
func (b *Barrier) ContextBindingResolved() bool
func (b *Barrier) LoadGeneration() uint64
func (b *Barrier) InvocationCounter() InvocationCounter
func (b *Barrier) NonceTracker() NonceTrackerProvider
func (b *Barrier) SealedRootKeyData() *SealedRootKey
func (b *Barrier) StampSealedStateMAC(ctx context.Context, sealed *SealedRootKey) error
func (b *Barrier) RootKey(ctx context.Context) ([]byte, error)
func (b *Barrier) RootKeyProtected(ctx context.Context) (enclave.ByteAccess, error)
```

The three methods that open the sealed root key take the caller's context and
pass it to go-enclave's `Sealed.Open`, which may reach a blocking
provider. `Rotate` and `PurgeEpochsBefore` take one too, as do the `Host`-facing
and snapshot-pool interfaces below; their background callers pass a context
cancelled when the host's key-rotation loop or the snapshot pool stops. The rotation
`Unseal` forces when an epoch's invocation budget cannot be recovered runs on
`context.WithoutCancel` of the unseal's context, because the barrier is already
unsealed when it starts and a rotation cut short would only leave encryption
refused.

### Interfaces this repository declares

```go
type SealingStrategy interface {
    ID() StrategyID
    Available() bool
    HardwareBacked() bool
    SealRootKey(ctx context.Context, rootKey []byte, creds Credentials) (*SealedRootKey, error)
    UnsealRootKey(ctx context.Context, sealed *SealedRootKey, creds Credentials) ([]byte, error)
    BarrierEncryptor(ctx context.Context, rootKey []byte) (SymmetricEncrypter, error)
    Close() error
}

type SymmetricEncrypter interface {
    Encrypt(plaintext []byte) ([]byte, error)
    Decrypt(ciphertext []byte) ([]byte, error)
}

type AADSymmetricEncrypter interface {          // OPTIONAL capability
    SymmetricEncrypter
    EncryptWithAAD(plaintext, aad []byte) ([]byte, error)
    DecryptWithAAD(ciphertext, aad []byte) ([]byte, error)
}

type AEADProvider interface {
    NewAEAD(alg SymmetricAlgorithm, key []byte) (cipher.AEAD, error)
}

type InvocationCounter interface{ /* SP 800-38D counting */ }
type NonceTrackerProvider interface{ /* nonce-reuse detection */ }

type StorageBackend interface {                 // shape only; the barrier never calls it
    Get(ctx context.Context, key string) ([]byte, error)
    Put(ctx context.Context, key string, data []byte) error
    Delete(ctx context.Context, key string) error
    List(ctx context.Context, prefix string) ([]string, error)
    Exists(ctx context.Context, key string) (bool, error)
    Scan(ctx context.Context, prefix string, fn func(key string, value []byte) error) error
    PutIfAbsent(ctx context.Context, key string, data []byte) (bool, error)
    CompareAndDelete(ctx context.Context, key string, expected []byte) (bool, error)
    Close() error
}

type StatePersister func(*SealedRootKey) error
```

### Strategies shipped here: exactly two

```go
type StrategyID string
const StrategyPassphrase StrategyID = "passphrase"
const StrategyNone       StrategyID = "none"

func NewNoneStrategy() *NoneStrategy

func NewSoftwareStrategy(ctx context.Context, passphrase []byte) (*SoftwareStrategy, error)
func NewSoftwareStrategyFIPS(ctx context.Context, passphrase []byte, iterations int) (*SoftwareStrategy, error)
func NewSoftwareStrategyWithParams(ctx context.Context, passphrase []byte, time, memory uint32, threads uint8) (*SoftwareStrategy, error)
func NewDeferredSoftwareStrategy() *SoftwareStrategy
func NewDeferredSoftwareStrategyWithParams(version KDFVersion, params KDFParams) (*SoftwareStrategy, error)

func (s *SoftwareStrategy) KDFVersion() KDFVersion
func (s *SoftwareStrategy) KDFParams() KDFParams
func (s *SoftwareStrategy) DeferredPassphrase() bool
```

`KDFParams` and `NewDeferredSoftwareStrategyWithParams` exist as a pair, so a
consumer can preserve a work factor across a strategy replacement rather than
reset it. One reads the factor off the strategy in hand, the other carries it
into the replacement. See `api.md` ("Strategies") for why a sealed blob cannot
supply the factor instead.

### Algorithms, KDF, FIPS

```go
type SymmetricAlgorithm byte
const AlgAES128GCM        SymmetricAlgorithm = 0x01
const AlgAES192GCM        SymmetricAlgorithm = 0x02
const AlgAES256GCM        SymmetricAlgorithm = 0x03
const AlgChaCha20Poly1305 SymmetricAlgorithm = 0x04
const AlgXChaCha20Poly1305 SymmetricAlgorithm = 0x05

func SelectOptimalAlgorithm(isHardwareBacked bool) SymmetricAlgorithm
func HasAESNI() bool
func FIPSEnabled() bool

type KDFVersion uint8
const KDFArgon2id      KDFVersion = 1
const KDFPbkdf2SHA256  KDFVersion = 2
func DefaultKDFVersion() KDFVersion
func DefaultKDFParams() KDFParams
func DefaultFIPSKDFParams() KDFParams
func DeriveKey(passphrase, salt []byte, version KDFVersion, params KDFParams) ([]byte, error)
func AllowWeakKDFParams() func()   // TEST HELPER; returns the restore func

type HKDFVersion uint8
const HKDFV2 HKDFVersion = 2

const DefaultInvocationLimit int64 = 1 << 32
const MaxKnownEpochs = 4096
func StampSealedStateMACWithRootKey(rootKey []byte, deploymentID uint64, sealed *SealedRootKey) error
func BoolPtr(v bool) *bool
```


### The `Host`-facing interface

`pkg.Barrier` in `pkg/host.go` is the narrower interface the `Host` accepts,
declared there to break an import cycle:

```go
type Barrier interface {
    IsSealed() bool
    Seal() error
    Encrypt(dst, plaintext []byte) ([]byte, error)
    Decrypt(dst, ciphertext []byte) ([]byte, error)
    DecryptWithContext(dst, ciphertext, contextAAD []byte) ([]byte, error)
    EncryptWithEpoch(dst, plaintext []byte) ([]byte, uint64, error)
    EncryptWithEpochContext(dst, plaintext, contextAAD []byte) ([]byte, uint64, error)
    DecryptForEpoch(dst, ciphertext []byte, epoch uint64) ([]byte, error)
    IsHardwareMode() bool
    SupportsContextAAD() bool
    Rotate(ctx context.Context) (uint64, error)
    PurgeEpochsBefore(ctx context.Context, minEpoch uint64) (int, error)
    RegisterInFlightEpoch(epoch uint64)
    DeregisterInFlightEpoch(epoch uint64)
    CurrentEpoch() uint64
    ShouldRotateKey() bool
    EncryptWithEpochPinnedContext(dst, plaintext, contextAAD []byte) ([]byte, uint64, error)
}
```

`EncryptWithEpochPinnedContext` is required. It seals a frame and registers its
epoch in-flight under one lock, and every at-rest snapshot frame is sealed
through it. A custom `Barrier` must implement it atomically: sealing and then
registering in two steps leaves a window in which `Rotate` plus
`PurgeEpochsBefore` can drop the DEK of a frame already sealed into the
artifact.

## 2. Invariants

### Lifecycle

```
Uninitialized ──Initialize()──▶ Unsealed ──Seal()──▶ Sealed
      ▲                             ▲                   │ │
      │                             └───── Unseal() ────┘ │
      └──────────────────── Reset() ──────────────────────┘
```

| Method | Precondition | Postcondition |
|---|---|---|
| `Initialize` | Uninitialized | root key + salt generated, epoch-1 DEK derived, `HKDFVersion`/`Algorithm`/`LoadGeneration=1`/`StateMAC` stamped. **Does not call `StatePersister`.** |
| `SetSealedRootKey` | any | blob validated and adopted; Sealed if it was Uninitialized |
| `Unseal` | Sealed | root key unwrapped, `StateMAC` verified **before deriving any DEK**, blob claimed (`LoadGeneration+1`), DEKs re-derived for every known epoch, invocation budget re-established |
| `Seal` | Unsealed | every key reference dropped; delegated encryptor cleared; `binding` reset to `ContextBindingUnresolved` |
| `Reset` | Sealed, not superseded, **an uncompleted first boot** (root key from this process's `Initialize`, no `SetSealedRootKey` since, epoch never advanced past 1), no epoch pinned in flight | Uninitialized: any remaining key enclave destroyed; in-memory sealed blob, salt, epoch, `KnownEpochs`, DEK/AEAD maps, load generation and backoff counters cleared; invocation counter reset and nonce tracker cleared. **Does not call `StatePersister`**; any persisted blob is the caller's to remove. `Initialize` may then run again and produces an unrelated root key and salt. |

**`Reset` is the rollback for a failed first boot.** An embedder whose
initialization cannot complete after `Initialize` returned (for example, a
Shamir share distribution that fails) seals and resets the same instance rather
than building a new one, because the WAL and host already hold it. Nothing encrypted
under the discarded key decrypts under the next one: epoch 1 is reused, but its
DEK derives from a different root key and salt, so the AEAD fails closed with
`ErrDecryptionFailed`.

**`Reset` refuses anything but a first boot** (`ErrResetNotFirstBoot`).
`SetSealedRootKey`'s in-process rollback floor is the epoch the barrier holds,
and `Reset` discards it. On a barrier that had loaded a blob or reached epoch
N > 1, a `Reset` would let an older authentic blob load, and `Unseal`'s forced
rotation would re-derive an epoch whose DEK was already used, with the
invocation counter and nonce tracker freshly reset: nonce and DEK reuse. It also
refuses a superseded barrier (`ErrBarrierSuperseded`), so `Reset` plus
`Initialize` cannot shed supersession in-process.
A first boot never derived any epoch but 1, so the only blob naming its key
also names epoch 1, and the forced rotation on loading it derives epoch 2,
which never existed. The `firstBoot` field is set by `Initialize` and cleared
by `SetSealedRootKey`, by any committed rotation (`Rotate` or `Unseal`'s
forced one) and by `Reset` itself; an in-process `Seal`/`Unseal` cycle that
stays at epoch 1 keeps it.

**`Unseal` writes.** It persists the load-generation claim before the barrier
will encrypt, and it usually rotates. Neither happens in delegated or
no-encryption mode, which return earlier.

`Barrier.Seal()` is exported, but for a barrier wired into a host (host-wide or
per-shard) production code should go through `Host.Close()`, which drains the
pipeline before sealing so the engine does not take an `ErrBarrierSealed` storm.

### Persist-then-activate (`Rotate`)

Order is load-bearing:

1. Derive the new DEK and AEAD. Nothing has changed on error.
2. Clone the sealed state, set `Epoch` and `KnownEpochs`, **re-stamp
   `StateMAC`** (both fields are covered; a blob MACed over the old values would
   correctly be rejected by the next `Unseal`), and persist.
3. Only on a nil return from the persister: commit in memory, infallibly, under
   the held write lock.
4. Reset the trackers, **last and fail-safe**.

A persister error aborts the rotation entirely: no DEK installed, no
`knownEpochs` mutation, no tracker reset, no change to `b.sealed`. The old epoch
stays fully active and `Rotate` returns `(0, *StatePersistError{Op: "rotate"})`.

**Why the trackers reset last.** They are shared in-place state with no rollback
API. Resetting before the commit could hand a still-active old key a freshly
zeroed invocation counter: a second 2^32 budget beyond its safety limit. After
the commit both failure modes are safe: a non-reset counter only makes the new
key rotate sooner, a non-cleared nonce set only yields conservative
false-positive rejections. A failure is logged at WARN and `Rotate` still
returns `(newEpoch, nil)`; returning an error alongside a committed epoch is
avoided so a caller cannot retry into a second rotation.

**Root-key borrows.** The root key is held between operations as a
`Sealed`. `Rotate` opens it once into a locked `ByteAccess`, whose
destruction is deferred, and reads that through go-enclave's scoped
`ByteAccess.WithBytes` (go-enclave v0.8.0-alpha removed the unscoped
`MemoryEnclave.Open`). It borrows twice, each time for the one call that reads it: the epoch-cipher
derivation, then the `StateMAC` re-stamp. Neither borrow spans the sealing of
the new DEK, which may block on a provider, and no view of the key outlives its
borrow. Cleanup paths destroy a `Sealed` through one helper that logs a
failed `Destroy` at WARN rather than discarding it; go-enclave documents that
error as always nil today.

Check order in `rotateLocked`: sealed → superseded → hardware → nil sealed data
→ no persister.

### Persist-then-drop (`PurgeEpochsBefore`)

- The current epoch is **never** purged.
- An epoch with a non-zero in-flight refcount is **never** purged.
- The reduced `KnownEpochs` is persisted (and the MAC re-stamped) **before any
  key is dropped**. A restart after an unpersisted purge would resurrect the
  epochs and re-derive the DEKs the purge retired.
- A purge that would drop nothing needs no persister and returns `(0, nil)`.
- A persister error aborts: every DEK stays usable, `(0,
  *StatePersistError{Op: "purge"})`.
- Crash safety follows from the order: dying after the persist leaves a blob
  that simply omits the epochs already decided retired.

### Invocation budget across restarts

`resolveInvocationBudget` picks one of these at `Unseal`:

| Condition | Action |
|---|---|
| counting disabled (`InvocationLimit == -1`) | nothing |
| `InvocationCounter` injected | nothing; the injected counter owns persistence and seeding |
| this process created the epoch (`Initialize`, or a `Rotate` since) | nothing; a seal/unseal cycle inside one process must not be disturbed |
| `StatePersister` set | **rotate to a fresh epoch**, inside `Unseal`, under the same write lock |
| otherwise | seed the counter to the **full** limit: every `Encrypt` fails `ErrInvocationLimitExceeded`, `Decrypt` unaffected |

The rotation happens inside `Unseal` rather than in an embedder's rotation loop
because a loop reacting a tick later leaves a window in which writes land on the
exhausted DEK, and an embedder without such a loop leaves it open forever.

**Side effect to plan for.** The forced rotation grows `KnownEpochs` by one per
restart, and `SetSealedRootKey` refuses a blob naming more than `MaxKnownEpochs`
(4096). Nothing reconciles the two: the bound is a DoS guard against a forged
blob and it also caps legitimate growth. An embedder that never calls
`PurgeEpochsBefore`, or one in a crash loop (which reaches 4096 in minutes, not
years), ends up with a node that cannot start at all and needs bespoke tooling
to recover. The barrier logs at ERROR once the set passes half the bound.

### `SetSealedRootKey` validation order

Non-nil blob → `Epoch > 0` → rollback floor (`max(MinEpoch, b.epoch)`) →
32-byte `Salt` → `len(KnownEpochs) <= MaxKnownEpochs` → no epoch `0` and none
above `Epoch` in `KnownEpochs` → `Epoch` present in `KnownEpochs` →
`HKDFVersion` equal to `HKDFV2` → `Algorithm` agreement → the
load-generation fence.

Epoch `0` is rejected because accepting it is not inert: `Unseal` would derive
and install a full epoch-0 DEK, the forced rotation would persist it durably, no
purge floor could ever retire it, and `b.ciphers[0]` is exactly what a
zero-filled ciphertext header decodes to.

### Concurrency

| Operation | Lock | Frequency |
|---|---|---|
| `Encrypt` / `Decrypt` | `RLock` after an `atomic.Bool` pre-filter | hot path |
| `Rotate` | exclusive `Lock` (~1–2 ms) | rare |
| `Seal` / `Unseal` / `Reset` | exclusive `Lock`; `Reset` then takes `inFlightMu`, in the `PurgeEpochsBefore` order | lifecycle |

The `atomic.Bool` avoids `RLock` contention when sealed; the crypto operation
always holds `RLock` so `Seal` cannot zero the cipher mid-operation, and the
re-check under the lock closes the TOCTOU.

**`StatePersister` runs with the write lock held and must not re-enter the same
barrier** (`Encrypt`, `Rotate`, `SealedRootKeyData`, any of them), or it
deadlocks. If your storage layer is itself backed by this barrier, break the
cycle before you get here. The argument is a private deep copy the persister may
retain or mutate.

### Capability reporting

`SupportsContextAAD()` is `ContextBinding().Binds()`:

| `ContextBinding` | Meaning | `Binds()` |
|---|---|---|
| `ContextBindingUnresolved` | not unsealed; no encryptor installed | false |
| `ContextBindingBarrier` | software path | true |
| `ContextBindingDelegated` | delegated encryptor implementing `AADSymmetricEncrypter` that passed the probe | true |
| `ContextBindingUnsupported` | delegated encryptor with no AAD capability | false |
| `ContextBindingPlaintext` | `StrategyNone` | false |

**Safe to ask at any time, unsafe to remember.** False means either "cannot
bind" or "cannot say yet", and those demand opposite handling. A consumer that
caches must gate on `ContextBinding().Resolved()` (or
`Barrier.ContextBindingResolved()`, for consumers that assert the barrier
structurally and cannot import the `ContextBinding` type) and refuse to record an
unresolved answer. The capability is valid only while the barrier
stays unsealed and can legitimately change across a seal/unseal cycle.
`IsHardwareMode()` is true for both `Unsupported` and `Plaintext` and cannot
tell them apart; `IsPlaintextMode()` separates them.

### Algorithm is a property of the data

`Decrypt` selects the AEAD by **epoch**, not from the header's `algID` byte, and
under HKDFV2 the algorithm is an HKDF domain separator. A deployment therefore
uses one algorithm for the life of its data, and nothing in the WAL or a
snapshot names it in a form a reader can act on. `Initialize` records the choice
in `SealedRootKey.Algorithm`; `SetSealedRootKey` rejects a mismatch with
`*AlgorithmMismatchError` naming both. A zero `Algorithm` means "unrecorded" and
loads under whatever the barrier is configured with.

**Pin `BarrierConfig.Algorithm`.** The default reads the local CPU, which is a
trap for any data directory that outlives the host that wrote it.

### HKDF info layout

| Version | Length | Layout |
|---|---|---|
| `HKDFV2` = 2 | 38 | `prefix:20 ‖ deploymentID:8 ‖ epoch:8 ‖ algID:1 ‖ keyLen:1` |

`prefix` is the 20 ASCII bytes `quicraft/barrier/v1/`, a fixed string unrelated
to `HKDFVersion`. `deploymentID` and `epoch` are big-endian.

**Why v2.** HKDF is a stream: under the retired v1 layout (36 bytes, no suffix) the 16-byte AES-128 DEK for a
(deployment, epoch) pair is the first 16 bytes of that pair's 32-byte AES-256
DEK, and ChaCha20-Poly1305 and AES-256-GCM share a key outright, so the
unauthenticated `algID` header byte could select a key that is a prefix of
another. The `[algID][keyLen]` suffix makes an algorithm change yield unrelated
keys.

`Initialize` always stamps v2. `SetSealedRootKey` rejects every other value
with `ErrUnsupportedHKDFVersion`: 1 is retired and never reallocated, and 0 is
refused rather than read as v1. The version applies to every epoch of a
barrier; there is no per-epoch mixing. A blob from before v2 is wiped and
resealed.

### Brute-force backoff

After 5 consecutive failed `Unseal` attempts (`bruteForceThreshold`), a doubling
backoff gates the next: 1s, 2s, 4s, 8s, 16s, 32s, then 60s
(`bruteForceMaxDelay`). Failures at or above 20 (`bruteForceErrorThreshold`) log
at ERROR rather than WARN. The counter resets on success. A failure writes it
into the in-memory blob's `FailedAttempts` / `LastFailedUnixNano` and does
**not** call `StatePersister` (only the load claim, `Rotate` and
`PurgeEpochsBefore` do), so backoff survives a restart only if the embedder
persists `SealedRootKeyData()` after a failed `Unseal`; `SetSealedRootKey`
restores it from whatever blob it is given.

Backoff is measured against the wall clock, so a backward clock step could
otherwise lock a node out permanently; `Unseal` treats negative elapsed time as
satisfying the delay and re-anchors `lastFailedAt` to now.

Four things deliberately do not count as a failed attempt: a `StateMAC`
verification failure, a superseded barrier, in no-encryption mode the empty
root key that would otherwise trip the 32-byte size gate every restart, and a
cancelled or timed-out `Unseal`. An already-ended context is refused before the
strategy runs; a strategy error matching `ctx.Err()` is returned without
touching the counter or the persisted backoff, unless it also matches
`enclave.ErrSealedAuthentication` or `ErrDecryptionFailed`, which still count —
go-enclave joins an authentication verdict with a cancellation that arrives at
the same moment, and cancelling must not try a credential for free.

The primary defence is the KDF's cost, not the backoff: roughly 93 ms per
attempt at the Argon2id defaults on fast 2025 desktop hardware.

## 3. Error semantics

### Sentinels

| Error | Meaning |
|---|---|
| `ErrBarrierSealed` | encrypt/decrypt while sealed |
| `ErrBarrierNotInit` | never initialized |
| `ErrBarrierAlreadyInit` | `Initialize` on an initialized barrier |
| `ErrBarrierUnsealed` | `Unseal`/`Initialize` while already unsealed |
| `ErrDecryptionFailed` | **generic for every decrypt failure**: unknown epoch, auth failure, corruption |
| `ErrInvalidRootKeySize` | root key is not 32 bytes |
| `ErrInvalidPassphrase` | empty passphrase to `SoftwareStrategy` |
| `ErrCiphertextTooShort` | below the minimum frame length |
| `ErrUnsealBackoff` | brute-force backoff active |
| `ErrPurgeCurrentEpoch` | `PurgeEpochsBefore` with `minEpoch` above the current epoch; a floor equal to it is accepted and still keeps the current epoch |
| (no error) | an epoch with in-flight references is **skipped** by `PurgeEpochsBefore`, not refused; the caller deregisters and purges again later |
| `ErrEpochsInFlight` | `Reset` while any epoch holds an in-flight reference |
| `ErrResetNotFirstBoot` | `Reset` on a barrier that loaded a blob via `SetSealedRootKey` or whose epoch advanced past 1 |
| `ErrNoStatePersister` | `Rotate`, or a purging `PurgeEpochsBefore`, with no persister |
| `ErrUnsupportedHKDFVersion` | blob names a DEK-derivation layout this build lacks |
| `ErrInvocationLimitExceeded` | SP 800-38D budget reached for the epoch |
| `ErrNonceReuse` | tracker has already recorded this nonce for the epoch |
| `ErrPlaintextTooLarge` | frame exceeds the algorithm's maximum message size |
| `ErrBarrierSuperseded` | another loader claimed the blob; encrypt/rotate/purge refuse permanently |
| `ErrSealedStateUnauthenticated` | blob has no `StateMAC`, or it does not verify |
| `ErrSealedEpochRollback` | blob `Epoch` below the effective floor |
| `ErrTooManyKnownEpochs` | `KnownEpochs` exceeds `MaxKnownEpochs` |
| `ErrKnownEpochAboveCurrent` | `KnownEpochs` names an epoch above `Epoch` |
| `ErrInvalidSealedEpoch` / `ErrInvalidSealedSalt` / `ErrInvalidSealedEpochs` | structural validation in `SetSealedRootKey` |
| `ErrNilSealedData` | operation needs sealed state and there is none |
| `ErrHardwareMode` | `Rotate`/`PurgeEpochsBefore` in delegated mode, or a non-empty context on an encryptor without AAD capability |
| `ErrNoBarrierEncryptor` | a strategy that must supply an encryptor returned nil |
| `ErrContextAADUnsupported` | `RequireContextAAD` set and the barrier cannot bind |
| `ErrAADProbeRoundTrip` / `ErrAADProbeNotBound` | the behavioural AAD probe failed |
| `ErrUnsupportedAlgorithm` / `ErrAlgorithmNotFIPSApproved` / `ErrFIPSUnavailable` | `BarrierConfig.Validate` failures |
| `ErrKDFNotFIPSApproved` / `ErrKDFUnsupportedVersion` / `ErrKDFWeakParams` / `ErrKDFInvalidSalt` / `ErrKDFInvalidPassphrase` | KDF selection and work-factor failures |
| `ErrNoRootKeyAvailable` | `RootKey(ctx)` in no-encryption mode |
| `ErrInvalidKeySize` | key size does not match the algorithm |
| `ErrInvocationBudgetUnknown` | logged, never returned: the cause recorded when a restart cannot re-establish a provable budget |
| `ErrInvalidSealedKey` | a strategy was handed a nil or structurally invalid `SealedRootKey` |
| `ErrStrategyMismatch` | a strategy was asked to unseal a blob whose `Strategy` names another |
| `ErrStrategyClosed` | the strategy's key material was released by `Close`; re-create it |
| `ErrEmptyCredentials` | a deferred-passphrase strategy received a zero `Credentials` |
| `ErrNoneStrategyEncryptedData` | `StrategyNone` asked to decrypt a frame with a non-zero epoch, i.e. data a real strategy wrote |
| `ErrShamirThresholdInvalid`, `ErrShamirNilStorage`, `ErrShamirSplitFailed`, `ErrShamirCombineFailed`, `ErrShamirSerializationFailed`, `ErrShamirStorageFailed`, `ErrShamirShareNotFound`, `ErrShamirNoSharesFound`, `ErrShamirVerificationFailed`, `ErrShamirQuorumIncomplete` | declared for the Shamir custody layer, which go-qrdb owns; nothing in this module returns them |

### Typed errors

| Type | Fields |
|---|---|
| `*KeyDerivationError` | `Epoch`, `Err` |
| `*StatePersistError` | `Op` (`"rotate"`/`"purge"`/`"claim"`), `Epoch`, `Err` |
| `*SupersededError` | `Observed`, `Claimed`; unwraps to `ErrBarrierSuperseded` |
| `*AlgorithmMismatchError` | `Sealed`, `Configured` |
| `*AADBindingProbeError` | `Stage`, `Err` |
| `*TrackerError` | `Tracker`, `Op`, `Err` |
| `*CipherError` | `Err` |
| `*UnsealError`, `*Error` | `Strategy`, `Err` |

Every typed error except `*AlgorithmMismatchError` implements `Unwrap`; match
that one with `errors.As`. Match the rest with `errors.Is` / `errors.As`.

### Fail-closed paths

- **`Decrypt` never discriminates.** One error for every failure mode, to deny
  an error oracle.
- **Nonce read failure** on a caller-nonce AEAD returns an error rather than
  falling back to a weaker entropy source.
- **`ErrPlaintextTooLarge` is checked before allocating or sealing**, because
  both GCM and ChaCha20-Poly1305 answer an over-long plaintext with a panic
  rather than an error.
- **Invocation limit** refuses to encrypt rather than exceeding the budget.
- **Nonce reuse** returns the error and the ciphertext never reaches the caller.
- **`RequireContextAAD`** refuses at `Initialize`/`Unseal`, not at first write.
- **`RequireFIPS`** refuses at `NewBarrier`.
- **A missing `StateMAC`** refuses the load.
- **A superseded barrier** refuses to encrypt permanently, and cannot launder
  the state by re-sealing.
- **An unpersisted rotation or purge** aborts rather than committing.
- **`NewSnapshotAAD`** asks `SupportsContextAAD` first and fails the write
  closed rather than silently dropping the binding.

The one path that fails open without configuration: FIPS detection is
environmental, so `FIPSEnabled() == false` means either "not requested" or
"requested but this binary has no module", and the second quietly wraps the
root key with Argon2id and may encrypt with ChaCha20-Poly1305. `RequireFIPS`
turns that into a refusal. Set it if you need FIPS.

## 4. Configuration

All on `BarrierConfig`. `DefaultBarrierConfig(deploymentID)` fills the defaults;
`Validate()` (called by `NewBarrier`) enforces them.

| Field | Type | Default | Notes |
|---|---|---|---|
| `Algorithm` | `SymmetricAlgorithm` | `SelectOptimalAlgorithm(false)` via `DefaultBarrierConfig`; reads the local CPU | **Pin it.** See §2. |
| `DeploymentID` | `uint64` | none | bound into HKDF info; two clusters sharing a root key get independent DEKs |
| `NonceTracking` | `*bool` | nil ⇒ true | a `*bool` so the zero value means "unset", not "off"; with a plain `bool` every hand-built config would silently get no tracker. Read via `GetNonceTracking()`; disable with `BoolPtr(false)`. |
| `InvocationLimit` | `int64` | 0 ⇒ 2^32; `-1` disables | SP 800-38D for random 96-bit nonces |
| `RotationThreshold` | `float64` | 0 or out of range ⇒ 95.0 (`DefaultRotationThreshold`) | percentage of the limit at which `ShouldRotateKey()` turns true |
| `StatePersister` | `StatePersister` | nil | **required** for `Rotate`, for a purging `PurgeEpochsBefore`, and for every post-restart `Unseal` |
| `InvocationCounter` | `InvocationCounter` | nil ⇒ in-memory | an injected counter owns cross-restart persistence and seeding; seeding too low is exactly as unsound as not counting |
| `NonceTrackerProvider` | `NonceTrackerProvider` | nil ⇒ `InMemoryNonceTracker` | keys on the full nonce verbatim, with no truncation to a 12-byte prefix, so 24-byte XChaCha20 nonces sharing a prefix are never falsely rejected |
| `AEADProvider` | `AEADProvider` | nil ⇒ `SoftwareAEADProvider` | injected code runs on the plaintext root key during derivation, which is why `rotateLocked` defers destruction of the root-key buffer: a panic in third-party provider code that some caller recovers would otherwise strand an `mlock`'d buffer holding the root key in the clear for the life of the process |
| `RequireContextAAD` | `bool` | false | fail closed at setup if positional binding is unavailable, `StrategyNone` included |
| `RequireFIPS` | `bool` | false | fail closed at `NewBarrier` without the FIPS module |
| `MinEpoch` | `uint64` | 0 (disabled) | replay floor; set to the engine's minimum live epoch |

**Nonce-tracking memory.** The in-memory tracker admits at most
`DefaultMaxTrackedNonces` (4Mi, ~160 MB) per epoch and then saturates: memory
stays flat and repeats of the retained prefix are still rejected. Read the
coverage state from `Saturated()` and `Count()`, not from `IsEnabled()`, which
reports only the operator's switch. Raise the ceiling with
`InMemoryNonceTracker.SetMaxTrackedNonces` (via `NonceTrackerProvider`) when the
epoch is guaranteed to rotate well before memory matters. `NewBarrier` warns
when tracking is on with neither a `StatePersister` nor an `InvocationCounter`,
the combination that cannot rotate and so can never release the tracked set.

`Clear()` (called by `Rotate`) resets the tracker with one atomic pointer swap
of a whole epoch: nonce set, count and saturation flag together. Each
`CheckAndRecordNonce` loads the epoch once and works only against it, so a call
racing `Clear` lands wholly in the old epoch or wholly in the new one. Resetting
the fields separately let a racing call insert into the new set while counting
against the old (breaking `Count()` and the ceiling), or saturate the fresh
epoch from the old one's ceiling crossing. The tracker stays lock-free: a mutex
over admission measured 6x slower on the contended path.

### KDF selection

| Build mode | KDF | Parameters |
|---|---|---|
| Standard | Argon2id | time=4, memory=128 MB, threads=4, keyLen=32, saltLen=32 |
| FIPS (`GOFIPS140`) | PBKDF2-SHA256 | iterations=600000, keyLen=32, saltLen=32 |

The Argon2id defaults exceed OWASP 2024 minimums (time=3, memory=64 MB) because
the sealed root key is a high-value offline target; one derivation costs about
93 ms on a 2025 desktop CPU, once at startup.

`DeriveKey` enforces a work-factor floor and returns `ErrKDFWeakParams` below
it: Argon2id `time >= 2`, `memory >= 32 MiB`, `threads >= 1`; PBKDF2-SHA256
`iterations >= 100000`; and for any KDF `KeyLen >= 16` with a salt of at least
16 bytes.

The KDF version is stored in `SealedRootKey.Metadata[0]`, enabling cross-version
unseal: when the stored version matches the strategy's, the strategy's params
are used; when they differ, defaults for the *stored* version are. A sealed
root key whose metadata is shorter than the version byte, or whose version byte
names no known KDF, is refused with `ErrKDFUnsupportedVersion` (wrapped in
`UnsealError`) before any key derivation runs; there is no fallback to the
strategy's own KDF. A sealed root key whose `Strategy` field is empty is
refused with `ErrStrategyMismatch`, exactly like one naming another strategy:
every blob the strategy writes names it. The remedy for both is to wipe the
data directory and reseal.

### FIPS: runtime mode vs. build tag

`FIPSEnabled()` reads `crypto/fips140.Enabled()` with `GOFIPS140` as a fallback
(the literal `off` counts as disabled). It constrains the KDF
(`DefaultKDFVersion` selects PBKDF2-SHA256, `DeriveKey` refuses Argon2id) and
the data-path cipher (`SelectOptimalAlgorithm` returns AES-256-GCM
unconditionally, `Validate` rejects the ChaCha20 family with
`ErrAlgorithmNotFIPSApproved`).

A FIPS build (`-tags fips`) is the stronger form: the non-approved algorithms
are not compiled in at all:

| Removed | File | Effect |
|---|---|---|
| ChaCha20-Poly1305, XChaCha20-Poly1305 (and the `x/crypto/chacha20poly1305` dependency) | `pkg/seal/algorithm_chacha.go` | absent from `algorithmRegistry`; `Valid()` false, sizes zero, `newAEADForAlgorithm` and `Validate` return `ErrUnsupportedAlgorithm`; `SelectOptimalAlgorithm` falls back to AES-256-GCM on a CPU without AES-NI |
| Argon2id (and `x/crypto/argon2`) | `pkg/seal/kdf_argon2.go` | absent from `kdfDispatchers`; `DeriveKey(…, KDFArgon2id, …)` returns `ErrKDFUnsupportedVersion`; `DefaultKDFVersion` returns `KDFPbkdf2SHA256` whatever `GOFIPS140` says |

The wire identifiers `AlgChaCha20Poly1305`, `AlgXChaCha20Poly1305` and
`KDFArgon2id` remain defined in every build: they are values that appear in
data already on disk, and a FIPS build must be able to read the byte and say
what it means rather than mis-attribute it.

**The exported API does not change with the tag either.**
`NewSoftwareStrategyWithParams` exists in every build and compiles at every call
site; in a FIPS build it simply cannot succeed, returning
`ErrKDFUnsupportedVersion`. go-quicraft is a Layer-1 dependency, and an exported
symbol that vanishes under a build tag breaks every consumer that merely
mentions it, including one whose call is already guarded by its own runtime
FIPS check. Removing the cipher is the compliance requirement; removing the
signature would be collateral damage.
`pkg/seal/strategy_params_api_fips_test.go` is an external test package for that
reason: it consumes `pkg/seal` the way a downstream module does, so the FIPS run
stops building if the symbol ever disappears.

**Capability lost in a FIPS build**: cannot unseal a root key wrapped
under Argon2id, cannot seal a new one under it, cannot read ChaCha20-family
data. A FIPS-*mode* process already refused all three; what changes is the error
and the fact that no configuration can undo it. Migrate by unsealing once with a
non-FIPS build and re-sealing with `NewSoftwareStrategyFIPS`, which stamps
`KDFPbkdf2SHA256` and unseals identically in either build.

**The FIPS boundary.** `pkg/internal/transport/revocation` does not pass
`GODEBUG=fips140=only`, and this tag does not change that: OCSP's `CertID` is
SHA-1 by protocol definition (RFC 6960 §4.1.1), it lives in
`golang.org/x/crypto/ocsp`, and it cannot be made approved from here. It can be
avoided: revocation `Mode: "crl"` never reaches it.

`pkg/internal/transport` itself does pass `fips140=only`: `quic-go` (v0.60.0 and
later) builds packet-protection AEADs through Go's TLS 1.3 AES-GCM rather than
the general `cipher.NewGCM`.

So **`-tags fips` makes go-quicraft FIPS at rest and on the wire**, with one
configurable exemption on the OCSP path. Three make targets keep the claims
apart, and none speaks for the others:

| Target | Does | Proves |
|---|---|---|
| `make build-fips` | `go build` (± `-tags fips`) under `GOFIPS140` | the tree compiles, nothing more: this module has no `main`, so no artifact is produced, and `GOFIPS140` alone never executes a cryptographic operation. |
| `make verify-fips` | `GOFIPS140=latest GODEBUG=fips140=only go test -tags fips` over every package except `pkg/internal/transport/revocation/...` | everything inside the boundary, the QUIC transport included, runs under approved-only enforcement. The exclusion is a deny-list of that one tree, so a package added later is inside the gate by default. |
| `make verify-fips-exempt` | `GOFIPS140=latest GODEBUG=fips140=on go test -tags fips` over exactly `pkg/internal/transport/revocation/...` | the excluded tree is still gated, at `on` rather than `only`: the exemption buys a weaker mode, not an unmeasured one. |

Do not read `build-fips` as a compliance gate or a shippable FIPS artifact.

All three run in CI, and `test/buildgates/fips_gate_test.go` fails if the mode
is weakened from `only`, if the exclusion list grows, or if the excluded tree
stops being gated. The exemption itself (which primitives, under which RFCs, and
what it means for FedRAMP/PCI/HIPAA) is recorded in
[`../quic-transport/architecture.md`](../quic-transport/architecture.md#fips-140-3-exemption-record).

## 5. Persistence

The barrier persists nothing itself. It hands `*SealedRootKey` to
`StatePersister` and waits.

```go
type SealedRootKey struct {
    Strategy    StrategyID          // NOT a string
    Version     int                 // sealed-blob format version
    Epoch       uint64
    Salt        []byte              // 32 bytes, not secret
    Ciphertext  []byte              // the wrapped root key
    Metadata    []byte              // strategy-specific; [0] is the KDF version
    KnownEpochs []uint64            // ascending, for DEK re-derivation
    KDFVersion  int
    HKDFVersion HKDFVersion         // HKDFV2; any other value is refused
    Algorithm   SymmetricAlgorithm

    LoadGeneration uint64
    StateMAC       []byte           // HMAC-SHA256 over the covered fields

    FailedAttempts     int64        // backoff, survives restart if re-persisted, NOT MACed
    LastFailedUnixNano int64        // NOT MACed
    // ... plus Nonce, CreatedAt and the hardware/Shamir fields; see
    //     type SealedRootKey in pkg/seal/strategy.go
}
```

**`StatePersister` contract**, in the order it matters:

1. **Returning nil must mean the blob is durable**: readable back verbatim
   after a crash. The barrier activates the new epoch on a nil return, so a
   premature nil re-opens the data-loss window the hook exists to close. In
   practice: temp file → fsync file → rename → fsync directory. Or a store whose
   write is already synchronous.
2. The argument is a private deep copy; retain or mutate it freely.
3. It runs with the write lock held and must not re-enter the barrier.
4. Returning an error aborts the operation with the previous epoch intact.

Called by: `Rotate` (always), `PurgeEpochsBefore` (only when something would
actually be purged), `Unseal` (twice: to claim the blob, and again if it
rotates). Not called by `Initialize` or `Reset`: the embedder persists
`SealedRootKeyData()` itself at bootstrap, and removes it itself after a
`Reset`.

**`KnownEpochs` is what preserves epoch history.** Without it `Unseal` would
derive only the current epoch's DEK and prior-epoch data would be unreadable.
`Initialize` sets `[1]`; each `Rotate` rebuilds it ascending (so persisted blobs
are reproducible) before activating; each purging `PurgeEpochsBefore` reduces it
before dropping a key; `Unseal` re-derives a DEK and cipher for every entry.

**`StorageBackend`** is a context-aware KV shape satisfied by go-qrdb's
`storage.Storage` through duck typing, so neither package imports the other. The
barrier never calls it; it exists so an embedder that already has such a store
can adapt it to a `StatePersister` in a few lines. `PutIfAbsent` is the
primitive that turns the load-generation fence into something closer to a lock:
it reports `stored=true` only when this call created the key, and a non-nil
error means the outcome is unknown; do not assume either way. Atomicity is per
consistency domain.

`CompareAndDelete` is its release half, and the two are not independent: a
guarded create is only as strong as its release. Pairing `PutIfAbsent` with a
plain `Delete` lets two releasers both read holder `A`, the first delete, `B`
legitimately claim the now-absent key, and the second remove `B`'s value.
Re-reading before the delete does not close that window, because the window is
between the read and the delete; only a delete conditioned on the value the
caller last observed, evaluated where the store serializes writes, does.

Two obligations for implementers:

- An encrypting backend must guard on the ciphertext it read, not on a fresh
  encryption of `expected`. With `cipher.NewGCMWithRandomNonce` the same
  plaintext encrypts to different bytes, so a re-encrypted guard refuses every
  release and silently turns each into a no-op.
- A refused delete is a normal outcome, reported as `deleted=false`, not an
  error: erroring on a lost guard stalls consensus over a deterministic
  consequence of valid input. A failed guard read is the opposite and must
  refuse rather than decide.

### Consumer frame formats

| Artifact | On-disk shape |
|---|---|
| WAL encrypted chunk | `[CRC32:4 ‖ Length:2 ‖ Type:1 ‖ DurSeq:4 ‖ GenStart:4 ‖ Epoch:8 ‖ AlgID:1 ‖ Nonce:N ‖ Ciphertext ‖ Tag:16]` |
| Encrypted `snapshot.dat` | a sequence of `[ChunkLen:4 little-endian ‖ EncryptedChunk]`, 64 KiB plaintext per chunk |

CRC32-C protects against accidental corruption only; an adversary with disk
access recomputes it trivially. Tamper protection is the AEAD's. The split is
deliberate: CRC32-C runs at ~30 GB/s hardware-accelerated where HMAC-SHA256 runs
at ~1–3 GB/s, so the CRC is a cheap first-pass detector that keeps garbage out
of the decryption path.

Snapshot encryption uses write-to-temp → fsync → rename → fsync the directory.
The last step matters because without it a crash can roll the directory entry
back to the deleted temp name even though the encrypted bytes were fsynced.
Decryption writes to a scratch file and never replaces `snapshot.dat`.
*Rejected: in-place decryption.* A crash after the rename would leave plaintext
on disk while LogDB still records `Epoch > 0`, so every restart would re-attempt
decryption on plaintext and fail permanently, losing a valid snapshot.

The `epoch` parameter of the decrypt helper is advisory: each frame is
decrypted with the epoch in its own header. `shardID`, `replicaID` and `index`
must be the identity the frames were sealed under, because they reproduce the
positional AAD; a mismatch fails authentication and there is no weaker retry.

## 6. Test strategy

Targets: `make test-seal` (`./pkg/seal/...`), `make bench-seal`, `make fuzz-seal`.

**Unit**: `pkg/seal/*_test.go`, 47 files. Notable ones and what they pin:

| File | Pins |
|---|---|
| `barrier_test.go` | the lifecycle state machine, encrypt/decrypt, rotation |
| `barrier_reset_test.go` | `Reset`: first-boot only, superseded and in-flight refusals, an unrelated key afterwards |
| `barrier_context_aad_test.go` | positional binding: right context opens, wrong context fails |
| `barrier_hardware_aad_test.go`, `aad_binding_test.go` | the `verifyAADBinding` probe, including an encryptor that drops the AAD |
| `load_generation_fence_test.go` | claim, supersession, permanence, the backwards-generation case, refusal of a version-1 tag |
| `barrier_stamp_state_mac_test.go` | MAC coverage |
| `barrier_bounds_regression_test.go`, `nonce_bound_regression_test.go` | the tracker ceiling and saturation |
| `legacy_format_test.go` | checked-in ciphertext bytes from a caller-supplied-nonce writer decode under the current reader, proving `cipher.NewGCMWithRandomNonce` produces the same frame format |
| `strategy_params_api_fips_test.go` | an external package, so the FIPS run fails to build if an exported symbol disappears under the tag |
| `algorithm_table_fips_test.go` / `_nofips_test.go` | the registry contents in each build |
| `barrier_hardening_test.go` | hardening regression tests |

**Fuzz**: `pkg/seal/fuzz_test.go`: `FuzzBarrierEncryptDecryptRoundTrip`,
`FuzzBarrierDecryptGarbage`, `FuzzBarrierDecryptWrongEpoch`,
`FuzzBarrierEncryptVariableSizes`. The garbage and wrong-epoch targets exist to
keep `Decrypt` from ever growing a discriminating error.

**Fault-injection seams**: `AEADProvider`, `InvocationCounter`,
`NonceTrackerProvider` and `StatePersister` are all injectable, which is how a
persister failure, an exhausted budget, and a lying encryptor are tested without
touching the crypto.

**Test helper**: `AllowWeakKDFParams()` relaxes the work-factor floor and
returns the restore function. It exists so tests do not pay 93 ms per
derivation. It must never be called from production code.

## 7. Observability

All events go through `log/slog`. There is no custom callback system;
embedders configure their handler at startup. The lifecycle events:

```go
slog.Info("barrier initialized", "epoch", epoch, "algorithm", alg)
slog.Info("barrier unsealed", "epoch", epoch, "algorithm", alg)
slog.Info("barrier sealed")
slog.Info("barrier reset to uninitialized")
slog.Warn("barrier unseal failed", "attempt", n)  // from the 5th failure; ERROR from the 20th
slog.Info("key rotated", "from_epoch", old, "to_epoch", new, "algorithm", alg)
slog.Info("old DEK purged", "epoch", epoch)
```

`Decrypt` failures are returned, not logged.

Worth alerting on rather than merely collecting:

| Level | Event | Why |
|---|---|---|
| ERROR | sealed root key was claimed by another loader | two processes on one data directory; the barrier has stopped encrypting |
| ERROR | AEAD invocation budget unrecoverable, encryption refused | no `StatePersister`; the node is read-only until the epoch rotates |
| ERROR | the known-epoch set is approaching the load-time bound | `PurgeEpochsBefore` is not being called, or the node is crash-looping; the blob will become unloadable |
| ERROR | delegated encryptor declares `AADSymmetricEncrypter` but does not authenticate the AAD | the probe caught a false capability claim |
| ERROR | key rotation aborted: sealed state not persisted | the previous epoch remains active |
| ERROR | epoch purge aborted: reduced sealed state not persisted | DEKs retained |
| WARN | barrier sealing is delegated to an external encryptor | once per setup, naming what is bypassed |
| WARN | barrier performs NO encryption | `StrategyNone` |
| WARN | no `StatePersister`, so the load-generation fence is inert | |

**No key material, no nonce and no plaintext appears in any log line.**

Metrics for the barrier are exposed through the host's `SecurityMetrics`
collector rather than from this package; see
[`../observability/engineering.md`](../observability/engineering.md).
