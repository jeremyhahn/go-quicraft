# Seal & Barrier — API

> **Status: NORMATIVE** · Scope: go-quicraft `pkg/seal`, `pkg/crypto` ·
> Last verified: 2026-08-19
> Conflicts with this document are defects. Fix them here first.

## Transport coverage

go-quicraft is a library. **The Go API is the API.**

| Transport | This feature |
|---|---|
| **Embedded (Go call)** | the whole surface below. This is the transport. |
| Unix socket | n/a — this repository serves no socket |
| REST | n/a — this repository exposes no HTTP handler |
| gRPC | n/a — this repository defines no gRPC service |
| QUIC | n/a. The repository's QUIC listener carries Raft messages only; the barrier is at-rest, the transport is in-transit, and snapshots cross it as **plaintext** under mTLS. See [`../quic-transport/api.md`](../quic-transport/api.md). |
| MCP | n/a — this repository registers no MCP tools |

These are not missing-capability rows. In the portfolio capability register the
go-quicraft rows for `Barrier` / `SealingStrategy` / `EpochBarrier` /
`StrategyID` and for the `none` + `passphrase` strategies are consumed by
**implementing the interfaces and re-exporting the types**, not by calling a
client — the register records the re-export path through go-qrdb's `types_seal.go`
and `types_seal_strategy.go` rather than a transport tier. A barrier endpoint in
this repository would be the defect, not the fix.

## Go API

Import: `github.com/jeremyhahn/go-quicraft/pkg/seal`

### Capability: construct a barrier

```go
func DefaultBarrierConfig(deploymentID uint64) BarrierConfig
func NewBarrier(config BarrierConfig) (*Barrier, error)
func MustNewBarrier(config BarrierConfig) *Barrier
func (c *BarrierConfig) Validate() error
func (c *BarrierConfig) GetNonceTracking() bool
```

| | |
|---|---|
| Request | a `BarrierConfig` — see [`engineering.md`](engineering.md) §4 for every field |
| Response | `*Barrier` in the Uninitialized state |
| Errors | `ErrUnsupportedAlgorithm`, `ErrAlgorithmNotFIPSApproved`, `ErrFIPSUnavailable`, and the other `Validate` failures |
| Permission | none — in-process call |

`MustNewBarrier` panics instead of returning an error and is for
compile-time-constant configs only. `NewEpochBarrier` / `MustNewEpochBarrier`
are `var` aliases of the same functions, and `EpochBarrier` is a type alias of
`Barrier`.

### Capability: first boot — create the root key

```go
func (b *Barrier) Initialize(ctx context.Context, strategy SealingStrategy, creds Credentials) error
func (b *Barrier) SealedRootKeyData() *SealedRootKey
```

| | |
|---|---|
| Request | a strategy and its credentials |
| Response | barrier is Unsealed at epoch 1 |
| Errors | `ErrBarrierAlreadyInit`, `ErrBarrierUnsealed`, `ErrContextAADUnsupported`, strategy errors (`*Error`) |
| Permission | none in this library; the credential **is** the authorization |

**`Initialize` does not call `StatePersister`.** Persist
`SealedRootKeyData()` yourself, once, at bootstrap.

### Capability: restart — adopt and unseal persisted state

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

### Capability: encrypt at rest

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

The `Pinned` forms seal the frame **and** register its epoch in-flight under one
lock, closing the window in which a concurrent `Rotate` + `PurgeEpochsBefore`
could drop the DEK of a frame already written into an artifact.

A nil or empty `contextAAD` produces byte-identical, epoch-only AAD, so
`EncryptWithContext(dst, pt, nil)` and `Encrypt(dst, pt)` are interoperable.

### Capability: decrypt at rest

```go
func (b *Barrier) Decrypt(dst, ciphertext []byte) ([]byte, error)
func (b *Barrier) DecryptWithContext(dst, ciphertext, contextAAD []byte) ([]byte, error)
func (b *Barrier) DecryptForEpoch(dst, ciphertext []byte, epoch uint64) ([]byte, error)
func (b *Barrier) DecryptForEpochContext(dst, ciphertext []byte, epoch uint64, contextAAD []byte) ([]byte, error)
```

| | |
|---|---|
| Request | ciphertext, and the **exact** context bytes used at encrypt time |
| Response | plaintext |
| Errors | `ErrBarrierSealed`, `ErrCiphertextTooShort`, and `ErrDecryptionFailed` for **everything else** |
| Permission | none |

**`ErrDecryptionFailed` is deliberately generic** — unknown epoch,
authentication failure, corruption and wrong context all return it, so error
type cannot be used as an oracle.

The context is **not stored** in the ciphertext; the caller reproduces it.
**Never retry without the context on failure** — that retry is the vulnerability
the binding exists to prevent.

### Capability: rotate and purge epochs

```go
func (b *Barrier) Rotate() (uint64, error)
func (b *Barrier) PurgeEpochsBefore(minEpoch uint64) (int, error)
func (b *Barrier) CurrentEpoch() uint64
func (b *Barrier) ShouldRotateKey() bool
func (b *Barrier) RegisterInFlightEpoch(epoch uint64)
func (b *Barrier) DeregisterInFlightEpoch(epoch uint64)
```

| | |
|---|---|
| `Rotate` response | the new epoch |
| `Rotate` errors | `ErrBarrierSealed`, `ErrBarrierSuperseded`, `ErrHardwareMode`, `ErrNilSealedData`, `ErrNoStatePersister`, `*StatePersistError{Op:"rotate"}`, `*KeyDerivationError` — checked in that order |
| `PurgeEpochsBefore` response | the number of epochs dropped |
| `PurgeEpochsBefore` errors | `ErrBarrierSealed`, `ErrBarrierSuperseded`, `ErrHardwareMode`, `ErrPurgeCurrentEpoch`, `ErrNoStatePersister` (only when something would be purged), `*StatePersistError{Op:"purge"}` |
| Permission | none |

Both persist **before** committing. `PurgeEpochsBefore` never drops the current
epoch or an epoch with a non-zero in-flight refcount, and returns `(0, nil)`
when there is nothing to purge.

`RegisterInFlightEpoch` / `DeregisterInFlightEpoch` are a refcount the snapshot
receive path holds across a transfer.

### Capability: seal

```go
func (b *Barrier) Seal() error
```

Drops every key reference, clears any delegated encryptor, and resets the
context binding to `ContextBindingUnresolved`. Prefer `Host.Seal()` /
`Host.Close()`, which quiesce the pipeline first so the engine does not take an
`ErrBarrierSealed` storm.

### Capability: introspect state and capability

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

`SupportsContextAAD()` is safe to **ask** at any time and unsafe to
**remember** — see [`engineering.md`](engineering.md) §2. `IsHardwareMode()` is
true for both `ContextBindingUnsupported` and `ContextBindingPlaintext`;
`IsPlaintextMode()` separates them.

### Capability: root-key access (rare)

```go
func (b *Barrier) RootKey() ([]byte, error)
func (b *Barrier) RootKeyProtected() (*enclave.ProtectedBuffer, error)
func (b *Barrier) StampSealedStateMAC(sealed *SealedRootKey) error
func StampSealedStateMACWithRootKey(rootKey []byte, deploymentID uint64, sealed *SealedRootKey) error
```

`RootKeyProtected` is the form to prefer: it returns a wiped-on-`Destroy`
buffer. `RootKey` returns an ordinary slice the caller must zero itself, and
returns `ErrNoRootKeyAvailable` in no-encryption mode.

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
  device, and `Credentials` **by value**.
- `Credentials.Secret` is a `[]byte`, not a `string`, so callers can wipe it.
  Each strategy interprets it differently: the software strategy as an Argon2id
  or PBKDF2 passphrase, TPM2 possibly as an authorization value, cloud KMS
  typically ignoring it in favour of ambient credentials.
- `UnsealRootKey` returns a slice **the caller owns and must zero**.
- `Close` must be idempotent.
- **`BarrierEncryptor` returning `(nil, nil)` is correct and intended.** It
  means "I seal only the root key; the barrier derives epoch DEKs and does all
  bulk encryption itself." Every non-hardware strategy returns it, including
  this repository's own `SoftwareStrategy`. A non-nil return delegates the
  entire data path — see [`architecture.md`](architecture.md).
- `Available()` is a startup diagnostic, not an auto-selection mechanism. There
  is no preference order anywhere in `pkg/seal`.

```go
type SymmetricEncrypter interface {
    Encrypt(plaintext []byte) ([]byte, error)
    Decrypt(ciphertext []byte) ([]byte, error)
}

type AADSymmetricEncrypter interface {   // OPTIONAL — a promise, not a convenience
    SymmetricEncrypter
    EncryptWithAAD(plaintext, aad []byte) ([]byte, error)
    DecryptWithAAD(ciphertext, aad []byte) ([]byte, error)
}
```

**Implement `AADSymmetricEncrypter` only if the `aad` bytes are authenticated
end to end** such that `DecryptWithAAD` fails when a different `aad` is
supplied. An encryptor that cannot bind AAD must implement **only**
`SymmetricEncrypter`; the barrier then refuses a non-empty context with
`ErrHardwareMode` — loud, at the write — and `RequireContextAAD` refuses the
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

func NewSoftwareStrategy(passphrase []byte) (*SoftwareStrategy, error)
func NewSoftwareStrategyFIPS(passphrase []byte, iterations int) (*SoftwareStrategy, error)
func NewSoftwareStrategyWithParams(passphrase []byte, time, memory uint32, threads uint8) (*SoftwareStrategy, error)
func NewDeferredSoftwareStrategy() *SoftwareStrategy
func NewNoneStrategy() *NoneStrategy
```

- `NewSoftwareStrategy` auto-selects the KDF from `FIPSEnabled()`.
- `NewSoftwareStrategyWithParams` names Argon2id explicitly and is refused with
  `ErrKDFNotFIPSApproved` in FIPS mode; in a `-tags fips` **build** it exists
  and compiles but cannot succeed, answering `ErrKDFUnsupportedVersion`.
- `NewDeferredSoftwareStrategy` takes no passphrase at construction and reads it
  from `Credentials.Secret` at seal/unseal time — for flows where the strategy
  is built before an operator has supplied anything.
- Seal format: `[KDFSalt:32 ‖ Nonce:12 ‖ EncryptedRootKey ‖ Tag:16]`.

**`tpm2`, `pkcs11`, `awskms`, `gcpkms`, `azurekv` and `vault` are go-xkms's;
`shamir` is go-qrdb's.** This repository defines the `StrategyID` *type* and
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
| `AlgChaCha20Poly1305` | 0x04 | 32 | 12 | yes, without hardware AES (absent in a FIPS build) |
| `AlgXChaCha20Poly1305` | 0x05 | 32 | 24 | never (absent in a FIPS build) |

## `pkg/crypto` — compatibility shim

```go
type Barrier         = seal.Barrier
type BarrierConfig   = seal.BarrierConfig
type Credentials     = seal.Credentials
type SealedRootKey   = seal.SealedRootKey
type SealingStrategy = seal.SealingStrategy
var  NewBarrier      = seal.NewBarrier
var  MustNewBarrier  = seal.MustNewBarrier
```

Aliases only. No cipher code lives there. New code should import `pkg/seal`.

## Versioning and deprecation

- **Ciphertext frame**: no version discriminator, and none is needed — the epoch
  and `algID` are in the header, and the module-nonce and hand-written-nonce
  constructions produce byte-identical output. `legacy_format_test.go` proves
  that against checked-in pre-migration bytes.
- **`SealedRootKey.Version`**: the sealed-blob format version.
- **`HKDFVersion`**: current is `HKDFV2`. `0` means pre-versioning and derives
  as v1; anything above the current version is refused with
  `ErrUnsupportedHKDFVersion`.
- **`StateMAC` covered-field encoding**: version byte `2`. A v1 blob is accepted
  once, at WARN, only when `LoadGeneration == 0`, and is restamped at v2.
- **`KDFVersion`**: `KDFArgon2id` (1) and `KDFPbkdf2SHA256` (2). Both identifiers
  stay defined in every build because they appear in data already on disk.
- Nothing here is deprecated. `EpochBarrier`, `NewEpochBarrier` and
  `MustNewEpochBarrier` are supported aliases, not legacy names.
- A signature change to `SealingStrategy`, `SymmetricEncrypter` or
  `AADSymmetricEncrypter` breaks go-xkms and go-qrdb implementations. Sequence
  such changes bottom-up.
