# Seal and barrier: API

> **Status: NORMATIVE** · Scope: go-quicraft `pkg/seal`, `pkg/crypto` ·
> Last verified: 2026-10-13
> Conflicts with this document are defects. Fix them here first.

## Transport coverage

go-quicraft is a library, and the Go API is the only API.

| Transport | This feature |
|---|---|
| Embedded (Go call) | the whole surface below |
| Unix socket | n/a: this repository serves no socket |
| REST | n/a: no HTTP handler |
| gRPC | n/a: no gRPC service |
| QUIC | n/a. The QUIC listener carries Raft messages only; the barrier is at-rest, the transport is in-transit, and snapshots cross it as plaintext under mTLS. See [`../quic-transport/api.md`](../quic-transport/api.md). |
| MCP | n/a: no MCP tools |

These rows are not gaps. The barrier and strategy types are consumed by
implementing the interfaces and re-exporting the types one layer up, not by
calling a client. A barrier endpoint in this repository would be a defect.

## Go API

Import: `github.com/jeremyhahn/go-quicraft/pkg/seal`

### Construct a barrier

```go
func DefaultBarrierConfig(deploymentID uint64) BarrierConfig
func NewBarrier(config BarrierConfig) (*Barrier, error)
func MustNewBarrier(config BarrierConfig) *Barrier
func (c *BarrierConfig) Validate() error
func (c *BarrierConfig) GetNonceTracking() bool
```

| | |
|---|---|
| Request | a `BarrierConfig`; see [`engineering.md`](engineering.md) §4 for every field |
| Response | `*Barrier` in the Uninitialized state |
| Errors | `ErrUnsupportedAlgorithm`, `ErrAlgorithmNotFIPSApproved` or `ErrFIPSUnavailable`, the only failures `Validate` returns |
| Permission | none; in-process call |

`MustNewBarrier` panics instead of returning an error and is for
compile-time-constant configs only. `NewEpochBarrier` / `MustNewEpochBarrier`
are functions that call the same constructors (not `var` aliases, which every
importer could reassign), and `EpochBarrier` is a type alias of `Barrier`.

### First boot: create the root key

```go
func (b *Barrier) Initialize(ctx context.Context, strategy SealingStrategy, creds Credentials) error
func (b *Barrier) SealedRootKeyData() *SealedRootKey
```

| | |
|---|---|
| Request | a strategy and its credentials |
| Response | barrier is Unsealed at epoch 1 |
| Errors | `ErrBarrierAlreadyInit`, `ErrBarrierUnsealed`, `ErrContextAADUnsupported`, strategy errors (`*Error`) |
| Permission | none in this library; the credential is the authorization |

**`Initialize` does not call `StatePersister`.** Persist
`SealedRootKeyData()` yourself, once, at bootstrap.

### Roll back a failed first boot

```go
func (b *Barrier) Reset() error
```

| | |
|---|---|
| Request | none; the barrier must be Sealed |
| Response | barrier is Uninitialized and accepts `Initialize` again |
| Errors | `ErrBarrierNotInit`, `ErrBarrierUnsealed`, `ErrBarrierSuperseded`, `ErrResetNotFirstBoot`, `ErrEpochsInFlight` |
| Permission | none; in-process call on the concrete barrier |

For an embedder whose bootstrap fails after `Initialize` returned, and which
cannot swap the barrier instance because the WAL and host already hold it:
`Seal`, then `Reset`, then `Initialize` again. Every key enclave is destroyed
and the epoch, DEK maps, load generation and invocation accounting are cleared.
**`Reset` does not touch persisted state**: delete or replace any blob you
already saved. The next `Initialize` draws a new root key and salt, so data
sealed under the discarded key fails with `ErrDecryptionFailed`.

`Reset` rolls back only a first boot: a root key this process created with
`Initialize`, with no `SetSealedRootKey` applied since and no rotation
(`Rotate`, or the forced rotation inside `Unseal`) past epoch 1. Anything else
returns `ErrResetNotFirstBoot` and leaves the barrier untouched, because
discarding a later epoch would drop `SetSealedRootKey`'s rollback floor and let
an older blob re-derive an already-used DEK with fresh nonce and invocation
counters.

`Reset` is a method of `*Barrier` only. The host-facing `Barrier` interface and
the WAL's `WALBarrier` carry no lifecycle methods, and gain none.

### Restart: adopt and unseal persisted state

```go
func (b *Barrier) SetSealedRootKey(sealed *SealedRootKey) error
func (b *Barrier) Unseal(ctx context.Context, strategy SealingStrategy, creds Credentials) error
```

| | |
|---|---|
| Request | the persisted blob, then the strategy and credentials |
| Response | Unsealed, with a DEK re-derived for every known epoch |
| Errors (`SetSealedRootKey`) | `ErrNilSealedData`, `ErrInvalidSealedEpoch`, `ErrSealedEpochRollback`, `ErrInvalidSealedSalt`, `ErrTooManyKnownEpochs`, `ErrKnownEpochAboveCurrent`, `ErrInvalidSealedEpochs`, `ErrUnsupportedHKDFVersion`, `*AlgorithmMismatchError`, `ErrBarrierSuperseded` |
| Errors (`Unseal`) | `ErrUnsealBackoff`, `ErrSealedStateUnauthenticated`, `ErrContextAADUnsupported`, `*UnsealError`, `*StatePersistError{Op:"claim"}` |
| Permission | the credential |

**`Unseal` writes**: it claims the blob (`LoadGeneration+1`) before the barrier
will encrypt, and usually rotates. Both need a `StatePersister`.

### Encrypt at rest

```go
func (b *Barrier) Encrypt(dst, plaintext []byte) ([]byte, error)
func (b *Barrier) EncryptWithEpoch(dst, plaintext []byte) ([]byte, uint64, error)
func (b *Barrier) EncryptWithEpochPinned(dst, plaintext []byte) ([]byte, uint64, error)
func (b *Barrier) EncryptWithContext(dst, plaintext, contextAAD []byte) ([]byte, error)
func (b *Barrier) EncryptWithEpochContext(dst, plaintext, contextAAD []byte) ([]byte, uint64, error)
func (b *Barrier) EncryptWithEpochPinnedContext(dst, plaintext, contextAAD []byte) ([]byte, uint64, error)
```

| | |
|---|---|
| Request | `dst` (may be nil), `plaintext`, and for the `*Context` forms the positional context bytes |
| Response | `[Epoch:8 BE ‖ AlgID:1 ‖ Nonce:N ‖ Ciphertext ‖ Tag:16]`; the `*Epoch*` forms also return the epoch used |
| Errors | `ErrBarrierSealed`, `ErrBarrierSuperseded`, `ErrInvocationLimitExceeded`, `ErrNonceReuse`, `ErrPlaintextTooLarge`, `ErrHardwareMode` (non-empty context on an encryptor without AAD capability), `*CipherError` |
| Permission | none |

The `Pinned` forms seal the frame and register its epoch in-flight under one
lock, closing the window in which a concurrent `Rotate` + `PurgeEpochsBefore`
could drop the DEK of a frame already written into an artifact.

A nil or empty `contextAAD` produces byte-identical, epoch-only AAD, so
`EncryptWithContext(dst, pt, nil)` and `Encrypt(dst, pt)` are interoperable.

### Decrypt at rest

```go
func (b *Barrier) Decrypt(dst, ciphertext []byte) ([]byte, error)
func (b *Barrier) DecryptWithContext(dst, ciphertext, contextAAD []byte) ([]byte, error)
func (b *Barrier) DecryptForEpoch(dst, ciphertext []byte, epoch uint64) ([]byte, error)
func (b *Barrier) DecryptForEpochContext(dst, ciphertext []byte, epoch uint64, contextAAD []byte) ([]byte, error)
```

| | |
|---|---|
| Request | ciphertext, and the exact context bytes used at encrypt time |
| Response | plaintext |
| Errors | `ErrBarrierSealed`, `ErrCiphertextTooShort`, and `ErrDecryptionFailed` for everything else |
| Permission | none |

`ErrDecryptionFailed` is deliberately generic: unknown epoch, authentication
failure, corruption and wrong context all return it, so the error type cannot be
used as an oracle.

The context is not stored in the ciphertext; the caller reproduces it. **Never
retry without the context on failure**: that retry is the vulnerability the
binding exists to prevent.

### Rotate and purge epochs

```go
func (b *Barrier) Rotate(ctx context.Context) (uint64, error)
func (b *Barrier) PurgeEpochsBefore(ctx context.Context, minEpoch uint64) (int, error)
func (b *Barrier) CurrentEpoch() uint64
func (b *Barrier) ShouldRotateKey() bool
func (b *Barrier) RegisterInFlightEpoch(epoch uint64)
func (b *Barrier) DeregisterInFlightEpoch(epoch uint64)
```

| | |
|---|---|
| `Rotate` response | the new epoch |
| `Rotate` errors | `ErrBarrierSealed`, `ErrBarrierSuperseded`, `ErrHardwareMode`, `ErrNilSealedData`, `ErrNoStatePersister`, then a root-key open error, `*KeyDerivationError` or the `AEADProvider`'s own error (building the new epoch's cipher), `*Error{Strategy:"rotate"}` (sealing the new DEK), `*Error{Strategy:"state-mac"}` (re-stamping the state MAC) and `*StatePersistError{Op:"rotate"}`, in that order |
| `PurgeEpochsBefore` response | the number of epochs dropped |
| `PurgeEpochsBefore` errors | `ErrBarrierSealed`, `ErrBarrierSuperseded`, `ErrHardwareMode`, `ErrPurgeCurrentEpoch` (`minEpoch` above the current epoch), then, only when something would be purged, `ErrNilSealedData`, `ErrNoStatePersister`, a root-key open error, `*Error{Strategy:"state-mac"}` and `*StatePersistError{Op:"purge"}` |
| Permission | none |

Both persist before committing. `PurgeEpochsBefore` never drops the current
epoch or an epoch with a non-zero in-flight refcount, and returns `(0, nil)`
when there is nothing to purge.

`RegisterInFlightEpoch` / `DeregisterInFlightEpoch` are a refcount the snapshot
receive path holds across a transfer.

### Seal

```go
func (b *Barrier) Seal() error
```

Destroys (wipes) the sealed root key and every sealed DEK, drops the AEADs,
clears any delegated encryptor, and resets the context binding to
`ContextBindingUnresolved`. Each dropped `cipher.AEAD` still holds a copy of its
DEK in its key schedule until the GC reclaims it; Go offers no key-wipe API.
Returns `ErrBarrierSealed` when the barrier is not unsealed. For a barrier wired into a host,
host-wide or per-shard, prefer `Host.Close()`, which drains the pipeline before
sealing so the engine does not take an `ErrBarrierSealed` storm.

### Hand the barrier to a host

`quicraft.WithBarrier(b)` (host-wide), `quicraft.WithShardBarriers(fn)`
(per-shard, `fn func(shardID uint64) Barrier`) and `quicraft.WithoutEncryption()`
are `NewHost` options; `*seal.Barrier` satisfies the host's `Barrier`
interface. `WithShardBarriers` combines with either host-wide choice; a `nil`
from the resolver takes the host-wide route, or fails with
`ErrShardBarrierUnresolved` when none was declared. Routing and the LogDB checks
are in [`architecture.md`](architecture.md) and
[`../multi-group-host/api.md`](../multi-group-host/api.md).

### Introspect state and capability

```go
func (b *Barrier) IsInitialized() bool
func (b *Barrier) IsSealed() bool
func (b *Barrier) IsSuperseded() bool
func (b *Barrier) IsHardwareMode() bool
func (b *Barrier) IsPlaintextMode() bool
func (b *Barrier) SupportsContextAAD() bool
func (b *Barrier) ContextBinding() ContextBinding
func (b *Barrier) ContextBindingResolved() bool
func (b *Barrier) LoadGeneration() uint64
func (b *Barrier) Config() BarrierConfig
func (b *Barrier) InvocationCounter() InvocationCounter
func (b *Barrier) NonceTracker() NonceTrackerProvider
```

`SupportsContextAAD()` is safe to ask at any time and unsafe to remember; see
[`engineering.md`](engineering.md) §2. `IsHardwareMode()` is
true for both `ContextBindingUnsupported` and `ContextBindingPlaintext`;
`IsPlaintextMode()` separates them.

### Root-key access (rare)

```go
func (b *Barrier) RootKey(ctx context.Context) ([]byte, error)
func (b *Barrier) RootKeyProtected(ctx context.Context) (*enclave.Memory, error)
func (b *Barrier) StampSealedStateMAC(ctx context.Context, sealed *SealedRootKey) error
func StampSealedStateMACWithRootKey(rootKey []byte, deploymentID uint64, sealed *SealedRootKey) error
```

`RootKeyProtected` is the form to prefer. It returns a frozen enclave that
`Destroy` wipes: a key to read and destroy, not a buffer to edit, so a write
through a slice retained past the borrow faults instead of silently rewriting
the caller's copy. Only the caller's copy is at stake, because each call
decrypts into a fresh enclave. **Do not zero the borrowed bytes yourself before
`Destroy`**: that is a write to a read-only region and faults; `Destroy` is the
release. `RootKey` returns an ordinary slice the caller must zero itself.
Both return `ErrNoRootKeyAvailable` in no-encryption mode.

All three open the sealed root key, and `ctx` bounds that open: go-enclave hands
it to the process sealer's provider, which may block. A context that has ended
fails the call with an error matching both `enclave.ErrOpenFailed` and
`ctx.Err()`.

### Interfaces you implement

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
```

Implementer obligations:

- Every method takes a `context.Context` where it may block on an external
  device, and `Credentials` by value.
- `Credentials.Secret` is a `*enclave.Sealed` (built by
  `NewCredentials`, which wipes the caller's slice). The caller owns it; a
  strategy opens it, uses the plaintext and destroys the opened copy, and never
  destroys the credential itself. Each strategy interprets it differently: the
  software strategy as an Argon2id or PBKDF2 passphrase, TPM2 possibly as an
  authorization value, cloud KMS typically ignoring it in favour of ambient
  credentials.
- `UnsealRootKey` returns a slice the caller owns and must zero.
- A strategy may also implement `ProtectedSealingStrategy` to take and return
  the root key in locked memory; see
  [`../shamir-primitive/api.md`](../shamir-primitive/api.md#go-api-the-protected-sealing-pair).
- `Close` must be idempotent.
- **`BarrierEncryptor` returning `(nil, nil)` is correct and intended.** It
  means "I seal only the root key; the barrier derives epoch DEKs and does all
  bulk encryption itself." Every non-hardware strategy returns it, including
  this repository's `SoftwareStrategy` and `NoneStrategy`. A non-nil return
  delegates the entire data path; see [`architecture.md`](architecture.md).
- `Available()` is a startup diagnostic, not an auto-selection mechanism. There
  is no default strategy and no preference order anywhere in `pkg/seal`;
  selection is always explicit at barrier or tenant setup.

```go
type SymmetricEncrypter interface {
    Encrypt(plaintext []byte) ([]byte, error)
    Decrypt(ciphertext []byte) ([]byte, error)
}

type AADSymmetricEncrypter interface {   // OPTIONAL: a promise, not a convenience
    SymmetricEncrypter
    EncryptWithAAD(plaintext, aad []byte) ([]byte, error)
    DecryptWithAAD(ciphertext, aad []byte) ([]byte, error)
}
```

**Implement `AADSymmetricEncrypter` only if the `aad` bytes are authenticated
end to end** such that `DecryptWithAAD` fails when a different `aad` is
supplied. An encryptor that cannot bind AAD must implement **only**
`SymmetricEncrypter`; the barrier then refuses a non-empty context with
`ErrHardwareMode` (loudly, at the write) and `RequireContextAAD` refuses the
deployment at setup. The declaration is verified behaviourally at `Initialize`
and `Unseal`.

```go
type AEADProvider interface {
    NewAEAD(alg SymmetricAlgorithm, key []byte) (cipher.AEAD, error)
}
type StatePersister func(*SealedRootKey) error
```

`StatePersister`'s four-point contract is in
[`engineering.md`](engineering.md) §5 and must be read before writing one.

### Strategies shipped here

```go
const StrategyPassphrase StrategyID = "passphrase"
const StrategyNone       StrategyID = "none"

func NewSoftwareStrategy(ctx context.Context, passphrase []byte) (*SoftwareStrategy, error)
func NewSoftwareStrategyFIPS(ctx context.Context, passphrase []byte, iterations int) (*SoftwareStrategy, error)
func NewSoftwareStrategyWithParams(ctx context.Context, passphrase []byte, time, memory uint32, threads uint8) (*SoftwareStrategy, error)
func NewDeferredSoftwareStrategy() *SoftwareStrategy
func NewDeferredSoftwareStrategyWithParams(version KDFVersion, params KDFParams) (*SoftwareStrategy, error)
func NewNoneStrategy() *NoneStrategy

func (s *SoftwareStrategy) KDFVersion() KDFVersion  // which algorithm
func (s *SoftwareStrategy) KDFParams() KDFParams    // which work factor
func (s *SoftwareStrategy) DeferredPassphrase() bool
```

- The three bound constructors take a `context.Context` because they seal a
  copy of the passphrase into a `Sealed`, which go-enclave may hand to a
  blocking provider. The deferred constructors hold no passphrase and take none.
- `NewSoftwareStrategy` auto-selects the KDF from `FIPSEnabled()`.
- `NewSoftwareStrategyWithParams` names Argon2id explicitly and is refused with
  `ErrKDFNotFIPSApproved` in FIPS mode; in a `-tags fips` build it exists and
  compiles but cannot succeed, answering `ErrKDFUnsupportedVersion`.
- `NewDeferredSoftwareStrategy` takes no passphrase at construction and reads it
  from `Credentials.Secret` at seal/unseal time, for flows where the strategy is
  built before an operator has supplied anything. It derives with the defaults.
- `NewDeferredSoftwareStrategyWithParams` is that constructor with an explicit
  work factor, and the only way to get both deferred binding and a tuned work
  factor.
- Sealed format (`SealedRootKey.Ciphertext`):
  `[KDFSalt:32 ‖ Nonce:12 ‖ EncryptedRootKey ‖ Tag:16]`.

**Preserving a work factor across a strategy replacement.** `KDFParams()` reads
the factor off the strategy in hand; `NewDeferredSoftwareStrategyWithParams`
carries it into the replacement. They are designed to compose in one line, with
no branch on the algorithm:

```go
replacement, err := seal.NewDeferredSoftwareStrategyWithParams(old.KDFVersion(), old.KDFParams())
```

This is not optional hardening. **A sealed blob records the KDF version byte and
never the params**, so a blob sealed at a non-default factor can only be unsealed
by a strategy carrying the same factor; the live strategy object is the only
place that number exists. A replacement built at the defaults silently moves a
hardened deployment toward the weaker setting, on the credential that wraps the
root key. Taking `(version, params)` together, rather than mirroring the bound
constructors' split signatures, stops a caller that is merely copying settings
from picking the wrong algorithm and failing on the FIPS leg only.

**`tpm2`, `pkcs11`, `awskms`, `gcpkms`, `azurekv` and `vault` are go-xkms's;
`shamir` is go-qrdb's.** This repository defines the `StrategyID` type and
exactly two values.

### Algorithms and helpers

```go
func SelectOptimalAlgorithm(isHardwareBacked bool) SymmetricAlgorithm
func HasAESNI() bool
func FIPSEnabled() bool
func DeriveKey(passphrase, salt []byte, version KDFVersion, params KDFParams) ([]byte, error)
func DefaultKDFVersion() KDFVersion
func DefaultKDFParams() KDFParams
func DefaultFIPSKDFParams() KDFParams
func NewInvocationCounter(enabled bool, limit int64, rotationThreshold float64) *InMemoryInvocationCounter
func NewNonceTracker(enabled bool) *InMemoryNonceTracker
func NewSoftwareAEADProvider() *SoftwareAEADProvider
func BoolPtr(v bool) *bool
```

| Algorithm | ID | Key | Nonce | Auto-selected |
|---|---|---|---|---|
| `AlgAES128GCM` | 0x01 | 16 | 12 | never |
| `AlgAES192GCM` | 0x02 | 24 | 12 | never |
| `AlgAES256GCM` | 0x03 | 32 | 12 | yes, with hardware AES or under FIPS |
| `AlgChaCha20Poly1305` | 0x04 | 32 | 12 | yes, without hardware AES and outside FIPS mode (absent in a FIPS build) |
| `AlgXChaCha20Poly1305` | 0x05 | 32 | 24 | never (absent in a FIPS build) |

## `pkg/crypto`: compatibility shim

```go
type Barrier         = seal.Barrier
type BarrierConfig   = seal.BarrierConfig
type Credentials     = seal.Credentials
type SealedRootKey   = seal.SealedRootKey
type SealingStrategy = seal.SealingStrategy
func NewBarrier(config BarrierConfig) (*Barrier, error)   // calls seal.NewBarrier
func MustNewBarrier(config BarrierConfig) *Barrier        // calls seal.MustNewBarrier
func NewCredentials(ctx context.Context, secret []byte) (Credentials, error) // calls seal.NewCredentials, consuming secret
```

Type aliases and forwarding functions only (functions, not `var`s holding
function values, for the reason given at `NewEpochBarrier`). No cipher code
lives there. New code should import `pkg/seal`.

## Versioning and deprecation

- **Ciphertext frame**: no version discriminator, and none is needed: the epoch
  and `algID` are in the header. Module-generated and caller-supplied nonces
  produce the same frame layout, which `legacy_format_test.go` checks against
  checked-in bytes.
- **`SealedRootKey.Version`**: the sealed-blob format version.
- **`HKDFVersion`**: current is `HKDFV2`. `0` means pre-versioning and derives
  as v1; anything above the current version is refused with
  `ErrUnsupportedHKDFVersion`.
- **`StateMAC` covered-field encoding**: version byte `2`. A tag over the
  version-1 field set is accepted once, at WARN, only when
  `LoadGeneration == 0`, and is restamped at version 2.
- **`KDFVersion`**: `KDFArgon2id` (1) and `KDFPbkdf2SHA256` (2). Both identifiers
  stay defined in every build because they appear in data already on disk.
- **Removed in 0.17.0**: `ProtectedCredentials` and `NewProtectedCredentials`,
  the deprecated aliases for `Credentials` and `NewCredentials`. `EpochBarrier`, `NewEpochBarrier` and
  `MustNewEpochBarrier` are supported aliases, not legacy names.
- A signature change to `SealingStrategy`, `SymmetricEncrypter` or
  `AADSymmetricEncrypter` breaks go-xkms and go-qrdb implementations. Sequence
  such changes bottom-up.
