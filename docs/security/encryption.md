# At-Rest Encryption

At-rest encryption in QuicRaft is built on a **per-node Barrier** that manages AEAD encryption with epoch-based key rotation. All encryption is opt-in. The algorithm is automatically selected based on hardware acceleration availability: AES-256-GCM when AES-NI is detected, ChaCha20-Poly1305 as a portable fallback.

**Key packages:**
- `pkg/crypto/` — AES-GCM and ChaCha20-Poly1305 AEAD with automatic hardware detection
- `pkg/seal/` — Barrier/seal/unseal pattern for at-rest key protection
- `pkg/crypto/shamir/` — Shamir's Secret Sharing for key management
- `pkg/enclave/` — Secure enclave integration

**Source:** `pkg/seal/barrier.go`, `pkg/seal/barrier_config.go`, `pkg/seal/strategy.go`, `pkg/seal/errors.go`

## Overview

```
┌─────────────────────────────────────────────┐
│                  Node                       │
│                                             │
│  ┌──────────┐   ┌──────────┐   ┌──────────┐ │
│  │ WAL      │   │ Snapshot │   │ SM Files │ │
│  │ Encryptor│   │ Encryptor│   │ Encryptor│ │
│  └────┬─────┘   └────┬─────┘   └────┬─────┘ │
│       └───────────┬───┘             │       │
│              ┌────┴────┐            │       │
│              │ Barrier │────────────┘       │
│              │ (DEK)   │                    │
│              └────┬────┘                    │
│              ┌──────────────┐               │
│              │ SealingStrat.│               │
│              │ (root wrap)  │               │
│              └──────────────┘               │
│         (software|HSM|KMS)                  │
└─────────────────────────────────────────────┘
```

The system separates two concerns:

| Layer | Responsibility | Configurable |
|-------|----------------|--------------|
| **Barrier** | Derives DEKs via HKDF-SHA256, encrypts/decrypts data with AEAD, manages epoch rotation, auto-detects optimal algorithm | Algorithm, deployment ID, AEAD safety trackers, and the `StatePersister` via `BarrierConfig` |
| **SealingStrategy** | Protects the root key at rest (wrapping/unwrapping) | Yes—any backend (software, HSM, TPM2, cloud KMS) |

**The embedder owns durable storage.** The barrier holds sealed state in memory and hands it to a caller-supplied `StatePersister` when that state changes; it never writes a file. Custody of the sealed blob belongs to the host application (go-qrdb), per the Layer-1 boundary.

**Per-node encryption, NOT clustered DEK.** Each node independently manages its own barrier and root key. QUIC provides in-transit encryption via mTLS; the barrier encrypts data at-rest. Nodes cannot decrypt each other's at-rest data.

## Barrier Interface

The Barrier is defined to break import cycles:

```go
type Barrier interface {
    IsSealed() bool
    Seal() error
    Encrypt(dst, plaintext []byte) ([]byte, error)
    Decrypt(dst, ciphertext []byte) ([]byte, error)
    EncryptWithEpoch(dst, plaintext []byte) ([]byte, uint64, error)
    DecryptForEpoch(dst, ciphertext []byte, epoch uint64) ([]byte, error)
    Rotate() (uint64, error)
    PurgeEpochsBefore(minEpoch uint64) (int, error)
    RegisterInFlightEpoch(epoch uint64)
    DeregisterInFlightEpoch(epoch uint64)
    CurrentEpoch() uint64
    ShouldRotateKey() bool
}
```

## Supported Algorithms

| Algorithm | Key Size | Hardware Acceleration | Default When |
|-----------|----------|----------------------|---------------|
| **AES-256-GCM** | 32 bytes | x86-64 (AES-NI), ARM64 (crypto ext) | **Yes, when hardware detected** |
| **ChaCha20-Poly1305** | 32 bytes | Software (no hardware acceleration) | **Yes, when no hardware AES** |
| **XChaCha20-Poly1305** | 32 bytes | Software (large nonce variant) | Manual override only |

Algorithm detection is automatic via `pkg/crypto.SelectOptimalAlgorithm()`. Users can override via `BarrierConfig.Algorithm`.

## Barrier State Machine

**Source:** `pkg/seal/barrier.go`

```go
type Barrier struct {
    mu       sync.RWMutex
    unsealed atomic.Bool  // fast-path check

    state    barrierState
    rootKey  *enclave.ProtectedKey  // 32 bytes, encrypted at-rest in memory
    salt     []byte                 // 32 bytes, used for HKDF

    epoch      uint64
    currentDEK *enclave.ProtectedKey  // derived via HKDF
    cipher     cipher.AEAD             // current epoch's AEAD cipher
    deks       map[uint64]*enclave.ProtectedKey // epoch → DEK for reading old data
    ciphers    map[uint64]cipher.AEAD

    knownEpochs    map[uint64]struct{}  // all epochs ever used
    sealed         *SealedRootKey       // persisted sealed root key
    failedAttempts int64                // brute-force protection
    lastFailedAt   time.Time
    inFlightMu     sync.Mutex           // separate lock for in-flight tracking
    inFlightEpochs map[uint64]int64     // epoch → refcount
}
```

Key material is held in `enclave.ProtectedKey` (`pkg/enclave`), which wraps a
memguard enclave: the plaintext key exists only inside a guarded, mlock'd
buffer for the duration of a single operation and is encrypted at rest in
process memory the rest of the time. `Seal()` drops every key reference via
`ProtectedKey.Clear()`; note that `Clear()` releases the reference rather than
zeroing the enclave's own ciphertext, so residual encrypted material remains
until garbage collection.

### Lifecycle Transitions

```
Uninitialized ──Initialize()──> Unsealed ──Seal()──> Sealed
                                   ^                    │
                                   └──── Unseal() ──────┘
```

| Method | Precondition | Action | Postcondition |
|--------|-------------|--------|---------------|
| `Initialize(strategy)` | Uninitialized | Generate root key + salt, derive epoch-1 DEK | Unsealed |
| `Unseal(strategy)` | Sealed | Decrypt root key, re-derive all known epoch DEKs | Unsealed |
| `Seal()` | Unsealed | Zero root key + all DEKs from memory | Sealed |

## Key Derivation

Each epoch produces a cryptographically independent DEK using HKDF-SHA256. The DEK length is the configured algorithm's key size (16 for AES-128-GCM, 24 for AES-192-GCM, 32 for AES-256-GCM and both ChaCha20 variants).

```go
func deriveDEK(rootKey, salt []byte, deploymentID, epoch uint64, alg SymmetricAlgorithm, version HKDFVersion) ([]byte, error) {
    keySize := alg.KeySize()

    var info [38]byte
    copy(info[:20], "quicraft/barrier/v1/")              // domain separation
    binary.BigEndian.PutUint64(info[20:], deploymentID)  // deployment isolation
    binary.BigEndian.PutUint64(info[28:], epoch)         // epoch

    infoLen := 36
    switch version {
    case 0, HKDFV1:                                      // legacy layout
    case HKDFV2:
        info[36] = byte(alg)                             // algorithm separation
        info[37] = byte(keySize)                         // key-size separation
        infoLen = 38
    default:
        return nil, &KeyDerivationError{Epoch: epoch, Err: ErrUnsupportedHKDFVersion}
    }

    hkdfReader := hkdf.New(sha256.New, rootKey, salt, info[:infoLen])
    dek := make([]byte, keySize)
    _, err := io.ReadFull(hkdfReader, dek)
    return dek, err
}
```

**Key properties:**

- **Deterministic:** Same root key + salt + deploymentID + epoch + algorithm always produces the same DEK. DEKs can be re-derived on demand after `Unseal()`.
- **Independent:** Each epoch's info string differs, so DEKs are cryptographically independent.
- **Deployment isolation:** The `deploymentID` ensures different deployments produce entirely different DEKs.
- **Algorithm isolation (v2):** The algorithm identifier and key length are bound into the info string.
- **Salt purpose:** 32-byte random salt provides additional node-level separation.

### HKDF Info Versioning

The info layout is versioned by `SealedRootKey.HKDFVersion` (type `HKDFVersion uint8`), persisted with the sealed root key so data keeps deriving under the layout it was written with.

| Version | Info length | Layout |
|---------|-------------|--------|
| `HKDFV1` = 1 | 36 bytes | `[prefix:20 \| deploymentID:8 \| epoch:8]` |
| `HKDFV2` = 2 | 38 bytes | `[prefix:20 \| deploymentID:8 \| epoch:8 \| algID:1 \| keyLen:1]` |

- `prefix` is the 20 ASCII bytes `quicraft/barrier/v1/` — a fixed string, unrelated to `HKDFVersion`.
- `deploymentID` and `epoch` are big-endian `uint64`.
- `algID` is the `SymmetricAlgorithm` byte (`0x01` AES-128-GCM, `0x02` AES-192-GCM, `0x03` AES-256-GCM, `0x04` ChaCha20-Poly1305, `0x05` XChaCha20-Poly1305); `keyLen` is the DEK length in bytes.

**Why v2.** HKDF is a stream: under v1 the 16-byte AES-128 DEK for a given (deployment, epoch) is the first 16 bytes of that pair's 32-byte AES-256 DEK, and ChaCha20-Poly1305 and AES-256-GCM share a key outright. The unauthenticated `algID` header byte could therefore select a key that is a prefix of another. v2's `[algID][keyLen]` suffix makes a change of `BarrierConfig.Algorithm` yield unrelated keys.

**Version handling:**

- `Initialize` stamps `currentHKDFVersion` (= `HKDFV2`) into the sealed blob. New deployments are always v2.
- `SetSealedRootKey` treats `HKDFVersion == 0` — a blob persisted before the field existed — as `HKDFV1`, so pre-upgrade data stays derivable.
- `SetSealedRootKey` rejects any version above `currentHKDFVersion` with `ErrUnsupportedHKDFVersion` rather than failing later with an opaque decryption error; `deriveDEK` rejects the same with a `KeyDerivationError` wrapping it.
- The version applies to every epoch of a barrier. There is no per-epoch layout mixing and no in-place upgrade of an existing v1 blob.

## Epoch-Based Key Rotation

Epoch rotation changes encryption keys without re-encrypting existing data and without downtime.

### How It Works

1. **Single root key** — generated once at `Initialize()`, never changes
2. **Epoch counter** — starts at 1, incremented on each `Rotate()` call
3. **DEK per epoch** — derived deterministically: `DEK(epoch) = HKDF(rootKey, salt, "quicraft/barrier/v1/" + deploymentID + epoch [+ algID + keyLen])`
4. **New writes use current epoch** — `Encrypt()` always uses the current DEK
5. **Old reads use historical DEKs** — `Decrypt()` reads the epoch from the ciphertext header, looks up the corresponding DEK
6. **No re-encryption** — old data stays encrypted with its original epoch's DEK until compaction deletes it

### Rotate()

```go
func (b *Barrier) Rotate() (uint64, error) {
    b.mu.Lock()
    defer b.mu.Unlock()

    persist := b.config.StatePersister
    if persist == nil {
        return 0, ErrNoStatePersister
    }

    newEpoch := b.epoch + 1

    // 1. Derive. Nothing has changed yet on error.
    dek, aead, err := b.deriveEpochCipher(rootBuf.Bytes(), b.salt, newEpoch)
    if err != nil {
        wipeBytes(dek)
        return 0, err
    }

    // 2. Persist the new epoch + KnownEpochs BEFORE activating them. On error
    //    nothing is committed and the OLD epoch stays fully active.
    candidate := cloneSealedRootKey(b.sealed)
    candidate.Epoch = newEpoch
    candidate.KnownEpochs = sortedEpochsWith(b.knownEpochs, newEpoch)
    if perr := persist(cloneSealedRootKey(candidate)); perr != nil {
        wipeBytes(dek)
        return 0, &StatePersistError{Op: "rotate", Epoch: newEpoch, Err: perr}
    }

    // 3. Commit — in-memory, infallible, under the held write lock.
    b.epoch = newEpoch
    b.currentDEK = dekPK
    b.cipher = aead
    b.deks[newEpoch] = dekPK
    b.ciphers[newEpoch] = aead
    b.knownEpochs[newEpoch] = struct{}{}
    b.sealed = candidate

    // 4. Reset the AEAD safety trackers for the new key. Failures here are
    //    fail-safe: logged, never reverting the commit.
    ...
    return newEpoch, nil
}
```

The write lock blocks concurrent `Encrypt`/`Decrypt` calls during rotation. Since rotation is fast (~1-2ms for HKDF + map update), this pause is negligible.

**Persist-then-activate.** A rotated epoch that is not durably recorded is a data-loss bug: the write path stamps the new epoch into WAL segments and snapshots immediately, while the read path fails closed on an epoch the restored blob does not name — so every byte written after an unpersisted rotation is permanently undecryptable once the process restarts. `Rotate` therefore hands the new sealed state (`Epoch` + `KnownEpochs`) to `BarrierConfig.StatePersister` and installs the new key **only** after the persister returns nil. A persister error aborts the rotation: no DEK installed, no `knownEpochs` mutation, no tracker reset, no change to `b.sealed`; the old epoch remains fully active and usable, and `Rotate` returns `(0, *StatePersistError{Op: "rotate"})`. With no persister configured, `Rotate` returns `ErrNoStatePersister` and changes nothing.

**Tracker resets are fail-safe and run last.** The invocation-counter `Reset()` and nonce-tracker `Clear()` happen *after* the epoch commit and never revert it; a failure is logged at `slog.Warn` and `Rotate` still returns `(newEpoch, nil)`. The trackers are shared in-place state with no rollback API — resetting before the commit could hand a still-active old key a freshly zeroed invocation counter, i.e. a second 2^32 budget beyond its AEAD safety limit. After the commit, both failure modes are safe: a non-reset counter only makes the new key re-rotate sooner, and a non-cleared nonce set only yields conservative false-positive reuse rejections. Returning an error alongside a committed epoch is deliberately avoided so a caller cannot retry into a second rotation.

Rotation is rejected with `ErrBarrierSealed` unless the barrier is unsealed, and with `ErrHardwareMode` in hardware mode.

On restart, `SetSealedRootKey` restores the persisted known epochs and `Unseal()` re-derives DEKs for all of them.

### Durable State: StatePersister

**Source:** `pkg/seal/barrier_config.go`

```go
type StatePersister func(*SealedRootKey) error
```

The barrier owns no storage. Per the Layer-1 boundary, `pkg/seal` is the encryptor only — the **embedder** (go-qrdb, or whichever component loads the blob at boot via `SetSealedRootKey`) owns the file/KV record holding the sealed root key and its fsync. `StatePersister` is the seam: the barrier hands over the state it needs made durable and waits for confirmation.

**Contract:**

- Returning nil MUST mean the blob is durable — readable back verbatim after a crash. The barrier activates the new epoch on a nil return, so a premature nil re-opens the data-loss window the hook exists to close.
- The argument is a private deep copy; the persister may retain or mutate it.
- The persister runs **with the barrier's write lock held**. It MUST NOT call back into the same `Barrier` (`Encrypt`, `Rotate`, `SealedRootKeyData`, …) — doing so deadlocks.
- Returning an error aborts the operation with the previous epoch intact.

**Required for:** `Rotate`, and any `PurgeEpochsBefore` that would actually drop an epoch. Both change state that must survive a restart. `Initialize` does **not** call the persister — the embedder persists `SealedRootKeyData()` directly at bootstrap.

**Consequence of leaving it nil.** At-rest key rotation is disabled: `Rotate` fails closed with `ErrNoStatePersister` on every call. Encryption continues under the current epoch, but only until the AEAD invocation limit. The default `InvocationLimit` is `DefaultInvocationLimit` = 2^32 (NIST SP 800-38D for random 96-bit nonces); `Encrypt` increments the counter on every call and returns `ErrInvocationLimitExceeded` once the limit would be exceeded, failing every subsequent encrypt until the key is rotated. That end is reached quickly after any restart: with the default in-memory counter, `Unseal` cannot know how many invocations the current epoch already performed before the restart, so it pessimistically seeds the counter to the rotation threshold (default 95%), leaving only the ~5% headroom (≈2×10^8 invocations) before the hard limit. A deployment that leaves `StatePersister` nil therefore burns that headroom and then stops accepting writes.

### Ciphertext Format

Every encrypted output carries its epoch and algorithm:

```
[Epoch:8 (big-endian) | AlgID:1 | Nonce:N | EncryptedData | Tag:16]
```

The epoch prefix and algorithm ID are **unencrypted**: the epoch selects the
DEK and the algorithm ID gives the nonce length, both of which must be known
before any key material is touched. `Nonce:N` is the AEAD's own nonce size —
12 bytes for the AES-GCM algorithms, 24 for XChaCha20-Poly1305 — so the header
is not a fixed width. `decryptLocked` validates the declared nonce size
against the selected AEAD before slicing, so a tampered algorithm ID cannot
cause an out-of-range read.

Note the deliberate endianness split: the epoch is written **big-endian** in
this header (for readability when inspecting raw ciphertext) but
**little-endian** in the AAD below. The two encodings serve different purposes
and are not interchangeable.

### Epoch AAD Binding

Every `Encrypt` and `Decrypt` operation binds the ciphertext to its epoch using Additional Authenticated Data (AAD). The epoch is encoded as a fixed 8-byte little-endian uint64:

```go
func epochAAD(epoch uint64) []byte {
    var aad [8]byte
    binary.LittleEndian.PutUint64(aad[:], epoch)
    return aad[:]
}
```

Both `encryptLocked` and `decryptLocked` pass this AAD to `aead.Seal()` and `aead.Open()` respectively. The AAD prevents cross-epoch ciphertext splicing attacks — an attacker cannot take a ciphertext encrypted under epoch N and present it as epoch M, because the AAD embedded in the authentication tag will not match.

**Baseline binding is epoch-only.** The default `Encrypt`/`Decrypt` (and `EncryptWithEpoch`, `EncryptWithEpochPinned`, `DecryptForEpoch`) bind ONLY the epoch. Within a single epoch, two frames sealed under the same DEK authenticate identically regardless of their position or artifact, so epoch-only AAD does **not** by itself defeat same-epoch reorder/splice/truncation by an at-rest attacker. Positional protection is provided by the optional context-AAD below, and only when a caller opts into it.

### Optional Context-AAD (positional binding)

The barrier exposes an additive, backward-compatible API that folds a caller-supplied **context** into the AAD alongside the epoch:

```go
func (b *Barrier) EncryptWithContext(dst, plaintext, contextAAD []byte) ([]byte, error)
func (b *Barrier) EncryptWithEpochContext(dst, plaintext, contextAAD []byte) ([]byte, uint64, error)
func (b *Barrier) EncryptWithEpochPinnedContext(dst, plaintext, contextAAD []byte) ([]byte, uint64, error)
func (b *Barrier) DecryptWithContext(dst, ciphertext, contextAAD []byte) ([]byte, error)
func (b *Barrier) DecryptForEpochContext(dst, ciphertext []byte, epoch uint64, contextAAD []byte) ([]byte, error)
```

The AAD is composed by `aadForEpoch(epoch, contextAAD)`:

```go
// AAD = [epoch:8 (little-endian)] || contextAAD
func aadForEpoch(epoch uint64, contextAAD []byte) []byte {
    if len(contextAAD) == 0 {
        return epochAAD(epoch) // exactly the epoch-only AAD
    }
    aad := make([]byte, 8+len(contextAAD))
    binary.LittleEndian.PutUint64(aad[:8], epoch)
    copy(aad[8:], contextAAD)
    return aad
}
```

Because the epoch always occupies the fixed 8-byte prefix and the context is appended after it, the epoch/context boundary is unambiguous: no context value can shift or alias the epoch bytes.

- **Opt-in only.** A nil/empty context yields byte-identical, epoch-only AAD, so `EncryptWithContext(_, _, nil)` and `Encrypt` are interoperable and all existing ciphertext and callers are unaffected. Positional binding exists **only** when the caller passes a context that uniquely identifies the frame.
- **The context is not stored** in the ciphertext (the wire format is unchanged). The caller must reproduce the exact same context bytes at decrypt time; a mismatched or omitted context fails authentication with the generic `ErrDecryptionFailed`.
- **Intended contexts.** Snapshot chunks pass `SnapshotIndex || ChunkSeq`; WAL records pass `SegmentID || RecordSeq`. When threaded, this binds each frame to its position and artifact, so same-epoch reorder, splice, and truncation fail authentication.
- **Delegated (hardware) mode.** The plain `SymmetricEncrypter` seam takes no AAD. A delegated encryptor that can authenticate AAD end to end declares it by also implementing `AADSymmetricEncrypter`; the barrier then routes the context through `EncryptWithAAD`/`DecryptWithAAD` using the SAME `aadForEpoch(epoch, contextAAD)` bytes the software path builds, so hardware deployments keep their positional binding. An encryptor that does NOT implement it returns `ErrHardwareMode` for a non-empty context (fail-closed) rather than silently dropping the binding; a nil/empty context is delegated unchanged in both cases.
- **Capability reporting.** `Barrier.SupportsContextAAD()` is what callers gate on (true for software and for a capable delegated encryptor). `Barrier.ContextBinding()` and `Barrier.IsPlaintextMode()` separate "nothing to bind because nothing is encrypted" (`StrategyNone`) from "binding unavailable because the encryptor cannot carry AAD" — `IsHardwareMode()` is true for both and cannot tell them apart.
- **Startup enforcement.** `BarrierConfig.RequireContextAAD` makes the requirement a deployment property: `Initialize`/`Unseal` fail with `ErrContextAADUnsupported` when the barrier cannot bind context, including `StrategyNone` (requiring authenticated positional binding while selecting no encryption is a misconfiguration). Default `false` preserves existing behavior.
- **What delegated mode bypasses.** Delegation is total: epoch DEK derivation, the epoch ciphertext header, the nonce-reuse tracker and the NIST SP 800-38D invocation counter are all skipped because the delegated encryptor owns its own key and nonce management. Every barrier setup logs this at WARN, naming what is delegated.

## Encrypt / Decrypt

**Source:** `pkg/seal/barrier.go`

### Two-Phase Fast Path

Both `Encrypt()` and `Decrypt()` use a two-phase pattern to minimize contention:

```
Phase 1: atomic.Bool check (no lock)
  └─ if sealed → return ErrBarrierSealed immediately

Phase 2: RLock + re-check (under lock)
  └─ re-verify unsealed (prevents TOCTOU with Seal)
  └─ perform AES-GCM operation under RLock
```

The `atomic.Bool` pre-filter avoids `RLock` contention when the barrier is sealed. The actual crypto operation always holds `RLock` to prevent `Seal()` from zeroing the cipher mid-operation.

### Nonce Generation

Every ciphertext frame uses a **fresh random nonce**, not a deterministic counter. `encryptLocked` reads the AEAD's full nonce size (12 bytes for the AES-GCM/ChaCha20 algorithms, 24 for XChaCha20-Poly1305) directly from `crypto/rand.Reader` on every call. There is no `[SegmentID|WriteEpoch|RecordSeq]` or `[AttemptSalt|ChunkSeq]` construction anywhere in the barrier, the WAL, or the snapshot pipeline — those layers call `barrier.Encrypt`/`EncryptWithEpoch`, which produce random nonces.

If `crypto/rand.Read` fails (entropy exhaustion), encryption returns an error rather than falling back to a weaker source. This fail-closed behavior prevents encrypting with a predictable nonce.

Random 96-bit nonces make nonce collision — not a repeated counter — the safety concern, so the barrier bounds invocations per DEK:

- **NIST SP 800-38D invocation cap.** `InvocationCounter` counts `Encrypt` calls per epoch and returns `ErrInvocationLimitExceeded` once the limit (`DefaultInvocationLimit` = 2^32 for random 96-bit nonces) would be exceeded, failing closed until the key is rotated. This is a hard collision-safety bound, not advisory; `Unseal` seeds it pessimistically after a restart (see the StatePersister section).
- **Nonce-reuse tracker.** When enabled, every generated nonce is recorded and a repeat is rejected outright (see below).

Because a rotated epoch derives a fresh DEK that has never been used with any nonce, the `(DEK, nonce)` pair is unique across epochs even if a random nonce were ever to repeat within the 2^32 budget.

### Nonce-Reuse Detection

**Source:** `pkg/seal/aead_tracking.go`

When nonce tracking is enabled, every generated nonce is recorded by
`InMemoryNonceTracker` and a repeat returns `ErrNonceReuse` (nonce reuse is
catastrophic for AEAD ciphers). The tracker keys on the **full nonce verbatim**
— the entire byte slice, of whatever length, is used as the `sync.Map` string
key. There is **no truncation to a 12-byte prefix**, so the 24-byte
XChaCha20-Poly1305 nonces that share a common 12-byte prefix are tracked
exactly and are never falsely rejected as reuse, while genuine reuse of an
identical nonce is always detected. `Clear()` (called by `Rotate`) swaps in a
fresh map via an atomic pointer swap, avoiding a TOCTOU race with concurrent
`CheckAndRecordNonce` calls. Memory grows linearly with encrypt operations
per epoch, so plan key rotation to bound it.

### Methods

**Encrypt:**
```go
func (b *Barrier) Encrypt(dst, plaintext []byte) ([]byte, error)
```
Output: `[Epoch:8 | Nonce:12 | EncryptedData | Tag:16]`

**EncryptWithEpoch:**
```go
func (b *Barrier) EncryptWithEpoch(dst, plaintext []byte) ([]byte, uint64, error)
```
Same as `Encrypt` but also returns the epoch used.

**Decrypt:**
```go
func (b *Barrier) Decrypt(dst, ciphertext []byte) ([]byte, error)
```
Reads the epoch from the first 8 bytes, looks up the corresponding cipher, decrypts. Returns a single generic `ErrDecryptionFailed` for ALL failure modes — unknown epoch, authentication failure, corrupted ciphertext. This prevents error oracle attacks.

**DecryptForEpoch:**
```go
func (b *Barrier) DecryptForEpoch(dst, ciphertext []byte, epoch uint64) ([]byte, error)
```
Decrypts using a specific epoch's DEK. Used when reading old WAL segments where the epoch is known from the segment header.

### Concurrency Profile

| Operation | Lock | Frequency | Overhead |
|-----------|------|-----------|----------|
| `Encrypt` / `Decrypt` | `RLock` | Hot path (100K+ ops/sec) | ~10-15ns per RLock/RUnlock |
| `Rotate` | `Lock` (exclusive) | Rare (admin/schedule) | Blocks Encrypt/Decrypt for ~1-2ms |
| `Seal` / `Unseal` | `Lock` (exclusive) | Lifecycle only | Blocks everything |

With 16 commit workers at 100K entries/sec, total RLock overhead is ~1-1.5ms/sec — acceptable.

## WAL Encryption

**Source:** `pkg/logdb/waldb/db.go`

When a barrier is active, WAL records are encrypted before writing to disk. The WAL subsystem has its own `WALBarrier` interface that mirrors the barrier methods needed, avoiding direct dependency:

```
Plaintext record:  [CRC32:4 | Length:4 | Type:1 | Data:N]
Encrypted record:  [CRC32:4 | Length:4 | Type:1 | Epoch:8 | Nonce:12 | Ciphertext:N | Tag:16]
```

- `CRC32` covers `epoch + nonce + ciphertext + tag`
- `Type` is unencrypted (entry batch, state, snapshot)
- `Epoch` is unencrypted (routing — tells decryptor which DEK)

### WAL Write Integration

Encryption is applied transparently in the `walShard.writeRecord` method:

```go
func (s *walShard) writeRecord(data []byte) error {
    writeData := data
    if s.barrier != nil {
        encrypted, err := s.barrier.Encrypt(nil, data)
        if err != nil {
            return &WALEncryptError{Err: err}
        }
        writeData = encrypted
    }
    _, err := s.activeSegment.Write(writeData)
    return err
}
```

### WAL Recovery Integration

During shard recovery, each record is decrypted before replay:

```go
for _, rec := range records {
    replayData := rec
    if s.barrier != nil {
        decrypted, derr := s.barrier.Decrypt(nil, rec)
        if derr != nil {
            return &WALDecryptError{SegmentID: segID, Err: derr}
        }
        replayData = decrypted
    }
    s.replayRecord(replayData, segID)
}
```

Decryption failures are fatal — silently skipping records would cause undetected data loss.

### Nonce Construction in WAL

The WAL does **not** construct its own nonces. `writeRecord` calls `barrier.Encrypt` (above), so each record's 12-byte nonce is fresh `crypto/rand` output produced by the barrier, and the `Nonce:12` in the encrypted-record layout is that random nonce. There is no `[SegmentID | WriteEpoch | RecordSeq]` deterministic-counter scheme in the write path.

Crash-recovery nonce safety therefore does not rely on a per-segment counter: because every nonce is random, a recovered segment that resumes appending cannot reproduce a pre-crash nonce by construction. Collision safety is instead bounded by the barrier's per-DEK NIST SP 800-38D invocation cap (2^32) and, when enabled, the nonce-reuse tracker — see [Nonce Generation](#nonce-generation). A `Rotate()` further guarantees uniqueness across epochs: the new DEK has never been used with any nonce.

### Positional binding for WAL records (optional)

Random nonces and epoch-only AAD do not, on their own, bind a WAL record to its position, so an at-rest attacker could in principle reorder or splice same-epoch records. To bind position, the WAL write/read path can pass a per-record context — `SegmentID || RecordSeq` — to the barrier's context-AAD API (`EncryptWithEpochContext` / `DecryptForEpochContext`; see [Optional Context-AAD](#optional-context-aad-positional-binding)). This is opt-in: it protects records only once the WAL threads that context on both encrypt and decrypt.

## Snapshot Encryption

Snapshots are encrypted as a stream of 64KB chunks. Each chunk is a standard barrier ciphertext frame produced by `barrier.Encrypt`/`EncryptWithEpoch`:

```
Chunk: [Epoch:8 | AlgID:1 | Nonce:N | Ciphertext:N | Tag:16]
```

### Nonce Construction

Each chunk's nonce is a **fresh random nonce** from the barrier (12 bytes for AES-GCM), exactly as for any other `Encrypt` call. There is no `[AttemptSalt | ChunkSeq]` deterministic construction; the snapshot pipeline does not synthesize nonces. Collision safety is bounded by the barrier's per-DEK invocation cap and reuse tracker — see [Nonce Generation](#nonce-generation).

### AAD (Additional Authenticated Data)

By default each chunk binds **only its epoch** (`epochAAD`). Epoch-only AAD does not bind a chunk to its position, so an at-rest attacker could in principle reorder chunks, substitute chunks from another snapshot at the same epoch, or truncate the stream without failing per-chunk authentication.

To bind position, the snapshot path can pass a per-chunk context — `SnapshotIndex || ChunkSeq` — to the barrier's context-AAD API (`EncryptWithEpochPinnedContext` / `DecryptForEpochContext`; see [Optional Context-AAD](#optional-context-aad-positional-binding)):

```
context = [SnapshotIndex:8 | ChunkSeq:4]   →   AAD = [Epoch:8] || context
```

When threaded on both encrypt and decrypt, this makes:
- **Chunk reordering** — a wrong `ChunkSeq` fails authentication.
- **Cross-snapshot substitution** — a wrong `SnapshotIndex` fails authentication.

This binding is opt-in and exists only where the caller supplies the context. Stream **truncation** is additionally guarded at the application layer by verifying `TotalChunkCount` agreement across all chunks.

### Snapshot Encryption: Atomic Write Pattern

**Source:** `pkg/internal/engine/snapshot_pool.go`

The `encryptSnapshotFile` function encrypts a plaintext snapshot data file in-place using an atomic write-to-temp-then-rename pattern. This ensures crash safety — a crash mid-encryption leaves the original plaintext file intact.

```
1. Read entire plaintext file into memory
2. Create temporary file (.encrypt-*.tmp) in same directory
3. For each 64KB chunk of plaintext:
   a. barrier.Encrypt(nil, chunk) → [Epoch:8 | Nonce:12 | Ciphertext | Tag:16]
   b. Write frame: [ChunkLen:4 (little-endian) | EncryptedChunk]
4. fsync the temporary file
5. Atomic rename: temp file → original path
```

The on-disk format is a sequence of length-prefixed frames:

```
[ChunkLen:4 | EncryptedChunk₁][ChunkLen:4 | EncryptedChunk₂]...
```

Each `EncryptedChunk` carries its own epoch and nonce in the standard barrier ciphertext format, so the file is self-describing for decryption.

### Snapshot Decryption: Epoch-Aware Recovery

**Source:** `pkg/internal/engine/snapshot_pool.go`

The `decryptSnapshotFile` function decrypts an encrypted snapshot using a specific epoch's DEK. The epoch is read from the snapshot metadata file (not from the ciphertext frames), enabling multi-epoch support — snapshots encrypted at different epochs coexist on disk.

```
1. Read encrypted file into memory
2. Create temporary file (.decrypt-*.tmp) in same directory
3. For each frame:
   a. Read 4-byte little-endian frame length
   b. Read encrypted chunk of that length
   c. barrier.DecryptForEpoch(nil, frame, epoch) → plaintext chunk
   d. Write plaintext to temp file
4. fsync the temporary file
5. Atomic rename: temp file → original path
```

The recovery path in `snapshotPool.recoverSnapshot` checks the epoch from LogDB snapshot metadata. When the epoch is non-zero and a barrier is available, it calls `decryptSnapshotFile` before opening the file for state machine recovery:

```go
if logdbSnap.Epoch > 0 && p.barrier != nil && !p.barrier.IsSealed() {
    decryptErr := p.decryptSnapshotFile(dataPath, logdbSnap.Epoch)
    // ...
}
```

This design means the state machine always sees plaintext data during `RecoverFromSnapshot()`, regardless of whether encryption is enabled.

### Snapshot Transport Between Nodes

Snapshots are **NOT encrypted at the application layer during transport**. The flow for a single-barrier setup is:

```
Leader:
  1. State machine writes plaintext snapshot to disk
  2. Barrier encrypts chunks to at-rest storage (local)
  3. Leader reads snapshot, calls reencryptReceivedSnapshot to decrypt (plaintext)
  4. Sends plaintext chunks via QUIC (mTLS encrypted in transit)

Follower:
  1. Receives plaintext chunks over QUIC (mTLS decrypted by QUIC)
  2. Writes to local disk storage
  3. reencryptReceivedSnapshot encrypts at-rest with follower's own barrier
  4. Stores epoch from re-encryption in LogDB snapshot metadata
```

For a **per-shard barrier setup**, the per-shard receiver routing is:

```
Follower:
  1. Receives plaintext chunks over QUIC for target shard
  2. Writes to local disk storage
  3. Calls reencryptReceivedSnapshot(dataPath, host.barrierForShard(shardID))
  4. Re-encrypts under the RECEIVER's shard-specific barrier
  5. Stores epoch from re-encryption in LogDB snapshot metadata
```

Each node has its **own independent barrier (or set of per-shard barriers)**. A leader's at-rest encrypted snapshot is decrypted for transport, sent over mTLS-protected QUIC, and the follower re-encrypts with its own key material. Nodes cannot decrypt each other's at-rest data.

## Epoch Lifecycle and Key Retention

### Why Retain Old DEKs?

After a key rotation, old WAL segments and snapshots remain encrypted with their original epoch's DEK. The barrier must retain (or be able to re-derive) those DEKs to read old data.

**Retention is cheap:**
- DEKs are 32 bytes each
- They're derived deterministically — even if purged, they can be re-derived from the root key
- The `deks` map holds only currently-needed DEKs in memory

### When Can DEKs Be Purged?

A DEK for epoch N can be purged when **nothing on disk or in-flight** needs it:

```
minEpoch = min(
    minWALEpoch,           // lowest epoch in any live WAL segment
    minSnapshotEpoch,      // lowest epoch in any live snapshot file
    minInFlightEpoch,      // lowest epoch of any in-progress snapshot receive
)

barrier.PurgeEpochsBefore(minEpoch)  // removes DEKs for epochs < minEpoch
```

### PurgeEpochsBefore

**Source:** `pkg/seal/barrier.go`

```go
func (b *Barrier) PurgeEpochsBefore(minEpoch uint64) (int, error) {
    b.mu.Lock()
    defer b.mu.Unlock()
    b.inFlightMu.Lock()
    defer b.inFlightMu.Unlock()

    // Select first; nothing is mutated until the reduced state is durable.
    var purgeable []uint64
    for epoch := range b.deks {
        if epoch >= minEpoch || epoch == b.epoch { continue } // keep
        if b.inFlightEpochs[epoch] > 0 { continue }           // in-flight
        purgeable = append(purgeable, epoch)
    }
    if len(purgeable) == 0 {
        return 0, nil // nothing to persist, no persister needed
    }

    persist := b.config.StatePersister
    if persist == nil {
        return 0, ErrNoStatePersister
    }
    candidate := cloneSealedRootKey(b.sealed)
    candidate.KnownEpochs = sortedEpochsWithout(b.knownEpochs, setOf(purgeable))
    if perr := persist(cloneSealedRootKey(candidate)); perr != nil {
        return 0, &StatePersistError{Op: "purge", Epoch: b.epoch, Err: perr}
    }

    for _, epoch := range purgeable {
        b.deks[epoch].Clear()  // drops the enclave reference (see below)
        delete(b.deks, epoch)
        delete(b.ciphers, epoch)
        delete(b.knownEpochs, epoch)
    }
    b.sealed = candidate
    return len(purgeable), nil
}
```

**Safety invariants:**
- Current epoch is **never** purged
- In-flight epochs (refcount > 0) are **never** purged
- The reduced `KnownEpochs` is **persisted before any key is dropped**

**Persist-then-drop.** A purge shrinks `KnownEpochs`, which is persisted state: were the reduced set not written durably, a restart would resurrect the purged epochs and re-derive the DEKs the purge floor retired. `PurgeEpochsBefore` therefore persists through `StatePersister` before dropping anything, and fails closed with `ErrNoStatePersister` when there is something to purge and no persister is configured. A purge that would drop nothing needs no persister and returns `(0, nil)`. A persister error aborts the purge — every DEK stays usable — and returns `(0, *StatePersistError{Op: "purge"})`. Crash safety follows from the order: dying after the persist leaves a blob that simply omits the epochs the purge had already decided to retire.

**A purge revokes capability; it is not crypto-shredding.** `ProtectedKey.Clear()` releases the enclave reference so the DEK can no longer be *opened* — it does not zero the enclave's ciphertext (memguard exposes no API to do so). Deleting the entry from `b.ciphers` likewise only drops a reference to a `cipher.AEAD` that still holds a copy of the DEK in its key schedule: `x/crypto`'s ChaCha20-Poly1305 — the default on any host without AES-NI — keeps a verbatim, un-`mlock`'d `key [32]byte`, and AES-GCM keeps the expanded round keys, from which the key is recoverable. Neither the standard library nor `x/crypto` offers a key-wipe API. So after a purge returns, the barrier can no longer decrypt the epoch, but **DEK confidentiality against memory disclosure (a core dump, a live process read) is GC-bounded, not enclave-bounded** — the same caveat `seal()` documents for the ciphers it drops. A deployment that needs the stronger property must bound it outside the process.

### In-Flight Epoch Protection

During snapshot receives, the incoming snapshot's epoch is registered to prevent premature purge:

```go
func (b *Barrier) RegisterInFlightEpoch(epoch uint64)    // increment refcount
func (b *Barrier) DeregisterInFlightEpoch(epoch uint64)  // decrement refcount
```

The snapshot receiver calls `RegisterInFlightEpoch` before starting the receive and `DeregisterInFlightEpoch` after the snapshot file is atomically renamed into place.

### Compaction and Epoch Interaction

WAL compaction deletes old segments, which eventually allows old DEKs to be purged:

```
1. Snapshot created at index=1000
2. RemoveEntriesTo(990) marks entries as compacted
3. GarbageCollect() deletes segment files with no live entries
4. Scan remaining segments → find minWALEpoch
5. Scan snapshot directory → find minSnapshotEpoch
6. PurgeEpochsBefore(min(minWALEpoch, minSnapshotEpoch))
7. Old DEKs dropped: enclave references released and ciphers removed (GC-bounded, see PurgeEpochsBefore)
```

**No re-encryption during compaction.** Old segments encrypted with epoch N are deleted, not re-encrypted with the current epoch. This is a deliberate design choice:
- Zero-downtime rotation (no need to halt and re-encrypt)
- No I/O amplification from re-encryption
- Compaction naturally removes old data over time

## Sealing Strategies

**Source:** `pkg/seal/strategy.go`

### Interface

This is the interface go-xkms (hardware/cloud backends) and go-qrdb (Shamir)
implement, so it is reproduced here in full and verbatim:

```go
type SealingStrategy interface {
    // ID returns a unique identifier for this strategy.
    ID() StrategyID

    // Available reports whether this strategy can be used on the current system
    // (e.g. a TPM2 strategy returns false when no TPM device is accessible).
    Available() bool

    // HardwareBacked reports whether key protection uses an HSM or external KMS.
    HardwareBacked() bool

    // SealRootKey wraps the root key for persistence.
    SealRootKey(ctx context.Context, rootKey []byte, creds Credentials) (*SealedRootKey, error)

    // UnsealRootKey unwraps it. The returned slice is owned by the caller and
    // must be zeroed after use.
    UnsealRootKey(ctx context.Context, sealed *SealedRootKey, creds Credentials) ([]byte, error)

    // BarrierEncryptor returns a SymmetricEncrypter for hardware-backed
    // strategies whose keys must never leave the hardware boundary. Software
    // strategies return (nil, nil), which tells the barrier to use epoch-based
    // HKDF DEKs. Implement AADSymmetricEncrypter as well when — and only when —
    // the backend authenticates caller-supplied AAD end to end.
    BarrierEncryptor(ctx context.Context, rootKey []byte) (SymmetricEncrypter, error)

    // Close releases strategy-held resources (PKCS#11 sessions, TPM2 handles,
    // KMS client connections). Must be idempotent.
    Close() error
}

type Credentials struct {
    Secret []byte // NOT a string: []byte so the caller can wipe it after use
}
```

Every method takes `Credentials` by value and a `context.Context` where it can
block on an external device, which is what distinguishes this from a plain
`Seal`/`Unseal` pair.

```go
type SealedRootKey struct {
    Strategy    StrategyID   // NOT a string; see below
    Version     int          // sealed-blob format version
    Epoch       uint64       // key rotation epoch at time of sealing
    Salt        []byte       // 32-byte HKDF salt
    Ciphertext  []byte       // encrypted root key
    Metadata    []byte       // strategy-specific (key ID, KMS ARN, etc.)
    KnownEpochs []uint64     // all epoch numbers for DEK re-derivation
    KDFVersion  int          // KDF used when sealing (survives a FIPS-mode change)
    HKDFVersion HKDFVersion  // DEK info layout; 0 = pre-versioning, derives as v1
    Algorithm   SymmetricAlgorithm // data-path AEAD the epoch DEKs were derived for
    // ... plus brute-force backoff state and hardware/Shamir fields; see
    //     pkg/seal/strategy.go for the authoritative list
}
```

`Strategy` is a `StrategyID`, not a `string`. **Layer 1 defines the `StrategyID`
type but only two values:** `StrategyPassphrase` (`"passphrase"`) and
`StrategyNone` (`"none"`). `"tpm2"`, `"pkcs11"`, `"awskms"`, `"gcpkms"`,
`"azurekv"` and `"vault"` belong to go-xkms; `"shamir"` belongs to go-qrdb,
which owns the `StrategyID` namespace and the barrier registry. There is no
implicit preference order and no auto-ranking: the strategy is chosen
explicitly at barrier or tenant setup.

The `SealingStrategy` ONLY protects the root key. It does not participate in data encryption. Any backend that can wrap/unwrap 32 bytes can serve as a sealing strategy.

### KnownEpochs Persistence Across Restart

The `KnownEpochs` field on `SealedRootKey` is the mechanism that preserves epoch history across seal/unseal cycles and node restarts. Without it, `Unseal` would only derive the DEK for the current epoch, and data encrypted with prior epochs would be unreadable.

**Persistence flow:**

1. `Initialize()` sets `sealed.KnownEpochs = []uint64{1}` (the initial epoch). The embedder persists `SealedRootKeyData()` itself — `Initialize` does not invoke `StatePersister`.
2. Each `Rotate()` rebuilds `sealed.KnownEpochs` (ascending, so persisted blobs are reproducible) and pushes the new state through `StatePersister` *before* activating the new epoch; each purging `PurgeEpochsBefore()` pushes the reduced set through it *before* dropping a key.
3. On restart, the caller loads the persisted `SealedRootKey` and calls `SetSealedRootKey()`.
4. `SetSealedRootKey()` validates the sealed data (epoch > 0, salt length, KnownEpochs contains current epoch, `HKDFVersion` not newer than this build), restores the in-memory `knownEpochs` map.
5. `Unseal()` iterates `b.knownEpochs` and re-derives a DEK + cipher for every historical epoch.

This ensures that after a restart with multiple prior rotations, every historical DEK is available for decrypting old WAL segments and snapshots.

### Built-In: Software Strategy

**Source:** `pkg/seal/strategy_software.go`

Passphrase-based protection using a versioned KDF + AEAD encryption (algorithm-agile):

```go
type SoftwareStrategy struct {
    passphrase []byte
    kdfVersion KDFVersion  // KDFArgon2id (1) or KDFPbkdf2SHA256 (2)
    kdfParams  KDFParams
}
```

**KDF selection** is automatic based on the build mode:

| Build Mode | KDF | Parameters |
|---|---|---|
| Standard | Argon2id | time=4, memory=128MB, threads=4, keyLen=32, saltLen=32 |
| FIPS (`GOFIPS140`) | PBKDF2-SHA256 | iterations=600000, keyLen=32, saltLen=32 |

**Seal format:** `[KDFSalt:32 | Nonce:12 | EncryptedRootKey | Tag:16]`

The KDF version is stored in `SealedRootKey.Metadata[0]`, enabling cross-version unseal. When the stored version matches the strategy's configured version, the strategy's params are used. When they differ (e.g., FIPS strategy unsealing Argon2id-sealed data), defaults for the stored version are used.

**Argon2id defaults exceed OWASP 2024 minimums** (time=3, memory=64MB) because the sealed root key is a high-value offline attack target. The ~200ms unseal cost is negligible (happens once at startup).

**PBKDF2 defaults** use 600,000 iterations per NIST SP 800-132 recommendations for FIPS compliance.

Constructors:
- `NewSoftwareStrategy(passphrase)` — auto-selects KDF based on `FIPSEnabled()`
- `NewSoftwareStrategyFIPS(passphrase)` — explicit PBKDF2-SHA256
- `NewSoftwareStrategyWithParams(passphrase, time, memory, threads)` — custom Argon2id tuning

### Hardware-Backed Strategies

> **Layer boundary.** go-quicraft (L1) ships **only** the `None` and `Software`(passphrase)
> strategies and names only their `StrategyID`s (`none`, `passphrase`) plus the `StrategyID`
> *type*. The hardware/cloud strategies below are **illustrative** — their concrete
> implementations and `StrategyID`s (`tpm2`/`pkcs11`/`awskms`/`gcpkms`/`azurekv`/`vault`) live in
> **go-xkms**, and `shamir` in **go-qrdb**, injected via the `SealingStrategy` interface. There is
> no built-in strategy catalog or preference-order default in L1; selection is always explicit.

The `SealingStrategy` interface supports arbitrary backends without touching the core:

```go
// Example: PKCS#11 / HSM
type PKCS11Strategy struct { /* session, keyID */ }
func (s *PKCS11Strategy) Seal(rootKey []byte) (*SealedRootKey, error)   { /* HSM wrap */ }
func (s *PKCS11Strategy) Unseal(sealed *SealedRootKey) ([]byte, error) { /* HSM unwrap */ }
func (s *PKCS11Strategy) Close() error                                  { /* close session */ }

// Example: TPM 2.0
type TPM2Strategy struct { /* device handle */ }

// Example: AWS KMS / GCP KMS / Azure Key Vault
type CloudKMSStrategy struct { /* client, key ARN */ }
```

Custom `SealingStrategy` implementations can be provided by external modules (e.g., PKCS#11, TPM 2.0, cloud KMS). The core QuicRaft module has zero dependency on any external key management system.

## Shamir's Secret Sharing — primitive + interfaces only

**Source:** `pkg/crypto/shamir/` (GF(256) math), `pkg/seal/shamir.go` (interfaces)

The consensus engine is **Layer 1**: it owns only the low-level Shamir primitive and
the seam interfaces, never the custody machinery.

- `pkg/crypto/shamir/` — the pure GF(256) `Split`/`Combine` primitive (K-of-N threshold).
- `pkg/seal/shamir.go` — the `ShareSplitter` / `ShareAccumulator` interfaces plus the
  `ShamirConfig` / `ShamirInitResult` / `QuorumProgress` value types that a host application
  injects an implementation for.

**`Combine` integrity check is opt-out via a zero digest.** Shares produced by
`Split` carry a SHA-256 digest of the original secret, and `Combine` verifies the
reconstructed secret against it, returning an `IntegrityError` on mismatch. As a
deliberate special case, when the first share's `Digest` is the zero value
(`[32]byte{}`) the SHA-256 check is **skipped**. This supports the accumulator
path, where the seal adapter's `ShareAccumulator` constructs shares without
`Split` and therefore has no access to the original digest. When a zero digest
disables verification, the **caller MUST validate the reconstructed secret
out-of-band** — e.g. by authenticating it against an independent commitment or
by using it in an authenticated decrypt that fails on a wrong key. (The
accumulator/custody machinery that drives this path lives in go-qrdb, not here.)

The **Shamir sealing strategy** (splitting the root key), **share custody**, the
**M-of-N quorum/unseal orchestration**, and the **Security-Officer CSR challenge-response
ceremony** are NOT part of the consensus engine. They are a database-server / custody
concern and live in **go-qrdb** (`pkg/seal` strategies + `pkg/ceremony`). The consensus
engine never persists Shamir shares. See the coordination repo's
`docs/05-seal-barrier-architecture.md` / `docs/09-identity-barrier-unification-plan.md`.

## Enclave Integration

**Source:** `pkg/enclave/`

Secure enclave integration for hardware-backed key protection on supported platforms (e.g., Apple Secure Enclave, Intel SGX).

## Host Integration

**Source:** `pkg/host.go`

The Host can be configured with barriers for at-rest encryption in two modes:

### Single Per-Node Barrier

For deployments where all Raft shards on a host share a single encryption key:

```go
func WithBarrier(b Barrier) HostOption
```

This is the default and backward-compatible mode. When set, `WithBarrier` installs a single barrier that encrypts every shard's snapshots and WAL records.

### Per-Shard Barriers

For multi-tenant or per-shard key isolation, each shard can use its own barrier:

```go
func WithShardBarriers(fn func(shardID uint64) Barrier) HostOption
```

The resolver function maps each `shardID` to a distinct `Barrier`. Returning `nil` for a shard falls back to the host-wide barrier configured via `WithBarrier`. This option is purely additive and backward-compatible:

- When `WithShardBarriers` is NOT used, the host behaves exactly as before
- When `WithShardBarriers` IS used:
  - Snapshot encrypt/decrypt, snapshot transport (epoch protection), and DEK purge all dispatch per-shard
  - Purging one tenant's barrier can never remove epochs still live under another tenant's barrier
  - The caller owns per-shard key rotation; the host-wide key-rotation loop only rotates the `WithBarrier` barrier
  - When combined with a multiplexing LogDB (`logdb.PerBarrierEpochAwareLogDB`), each shard can carry its own deduplicated WAL

All barriers must be initialized and unsealed before use.

### Usage

```go
// No encryption (default):
host, _ := quicraft.NewHost(cfg)

// Single host-wide barrier, software-sealed:
strategy, _ := seal.NewSoftwareStrategy([]byte("passphrase"))
bcfg := seal.DefaultBarrierConfig(deploymentID)
bcfg.StatePersister = saveSealedBlob // REQUIRED for Rotate / PurgeEpochsBefore
barrier, err := seal.NewBarrier(bcfg) // returns an error for an invalid config;
                                      // seal.MustNewBarrier panics instead, for
                                      // compile-time-constant configs only
barrier.Initialize(ctx, strategy, seal.Credentials{})
saveSealedBlob(barrier.SealedRootKeyData()) // bootstrap blob: persisted by the embedder
host, _ := quicraft.NewHost(cfg, quicraft.WithBarrier(barrier))

// Per-shard barriers (multi-tenant):
barrierMap := make(map[uint64]Barrier)
barrierMap[1] = tenantABarrier
barrierMap[2] = tenantBBarrier
host, _ := quicraft.NewHost(cfg, quicraft.WithShardBarriers(func(shardID uint64) Barrier {
    if b, ok := barrierMap[shardID]; ok {
        return b
    }
    return nil // fall back to WithBarrier, if set
}))
```

### Lifecycle

1. `NewHost()` receives barrier via `WithBarrier` option
2. Host checks initialization state (first boot check)
3. If not initialized: `barrier.Initialize(ctx, strategy, creds)` generates root key; the embedder persists `SealedRootKeyData()`
4. Otherwise: embedder loads the persisted blob and calls `barrier.SetSealedRootKey()`
5. `barrier.Unseal(ctx, strategy, creds)` decrypts root key and derives DEKs for every known epoch
6. Barrier passed to WAL and snapshot writers
7. On `Host.Close()`: pipeline drained, `barrier.Seal()` zeros all keys

### Shutdown Sequence

```
Host.Close():
  1. Stop accepting new proposals (drain queues)
  2. Wait for in-flight commits and applies
  3. Close LogDB (flush pending WAL writes)
  4. barrier.Seal() — zero all key material
  5. strategy.Close() — release PKCS#11/TPM2/KMS resources
```

## Brute-Force Protection

**Source:** `pkg/seal/barrier.go`

After 5 consecutive failed `Unseal()` attempts:

| Attempt | Backoff |
|---------|---------|
| 1-5 | None |
| 6 | 1s |
| 7 | 2s |
| 8 | 4s |
| 9 | 8s |
| ... | doubles |
| 15+ | 60s (cap) |

Counter resets on successful unseal. Primary defense is Argon2id's computational cost (~200ms per attempt).

## Memory Protection

**Source:** `pkg/seal/barrier.go`

The barrier uses [memguard](https://github.com/awnuber/memguard) for hardware-assisted memory protection of all key material.

### memguard Protections

| Protection | Mechanism |
|---|---|
| **Core dump prevention** | `MADV_DONTDUMP` on guarded pages — key material excluded from core dumps |
| **Encrypted at-rest memory** | `memguard.Enclave` encrypts root key in process memory; decrypted only into guarded buffers for use |
| **Guard pages** | Canary pages before/after each `LockedBuffer` detect buffer overflows/underflows |
| **Memory locking** | `mlock` prevents swapping key material to disk |
| **Signal handler cleanup** | memguard installs handlers to destroy buffers on SIGTERM/SIGKILL |
| **ptrace protection** | Process-level `PR_SET_DUMPABLE(0)` prevents debugger attachment on Linux |
| **Canary values** | Integrity canaries detect in-memory corruption of guarded buffers |

### Key Material Allocation

- **Root key** (`enclave.ProtectedKey`, wrapping a `memguard.Enclave`): Encrypted at-rest in process memory. Opened into a temporary guarded buffer only when needed (e.g. DEK derivation), then immediately released.
- **DEKs** (`enclave.ProtectedKey` each): Every epoch's DEK is individually protected. `Seal()` and `PurgeEpochsBefore()` drop them via `ProtectedKey.Clear()`, which releases the reference — it does not zero the enclave's own ciphertext, which persists until garbage collection.
- **Live AEADs** (`cipher.AEAD` per epoch): each holds its own copy of the DEK in its key schedule and cannot be wiped (no stdlib/`x/crypto` API). Dropping the barrier's reference on seal or purge makes it GC-eligible, and that is the only bound: a core dump taken before collection contains every live and recently purged epoch DEK.
- **Passphrase** (`SoftwareStrategy`): Stored in a `LockedBuffer`, destroyed on `Close()`.

### Zeroing

Explicit zeroing via `zeroBytes()` remains as a defense-in-depth measure for any transient `[]byte` copies:

```go
//go:noinline
func zeroBytes(b []byte) {
    for i := range b {
        b[i] = 0
    }
    runtime.KeepAlive(&b) // prevent dead-store elimination
}
```

## Error Types

**Source:** `pkg/seal/errors.go`

### Sentinel Errors

| Error | Meaning |
|-------|---------|
| `ErrBarrierSealed` | Encrypt/Decrypt called while sealed |
| `ErrBarrierNotInit` | Barrier never initialized |
| `ErrDecryptionFailed` | Generic for ALL decrypt failure modes |
| `ErrBarrierAlreadyInit` | Initialize called on already-initialized barrier |
| `ErrBarrierUnsealed` | Unseal/Initialize called when already unsealed |
| `ErrInvalidRootKeySize` | Root key != 32 bytes |
| `ErrInvalidPassphrase` | Empty passphrase to SoftwareStrategy |
| `ErrCiphertextTooShort` | Ciphertext < minimum length |
| `ErrUnsealBackoff` | Brute-force backoff active |
| `ErrPurgeCurrentEpoch` | Attempted to purge active epoch |
| `ErrEpochInFlight` | Attempted to purge epoch with in-flight references |
| `ErrNoStatePersister` | `Rotate` (or a purging `PurgeEpochsBefore`) with no `BarrierConfig.StatePersister` |
| `ErrUnsupportedHKDFVersion` | Sealed blob names a DEK-derivation info layout this build does not implement |
| `ErrInvocationLimitExceeded` | AEAD invocation limit reached for the epoch; rotation required |

### Typed Errors (with Unwrap)

| Error | Fields |
|-------|--------|
| `KeyDerivationError` | Epoch, Err |
| `StatePersistError` | Op ("rotate"/"purge"), Epoch, Err |
| `Error` | Strategy, Err |
| `UnsealError` | Strategy, Err |
| `CipherError` | Err |

## Certificate Revocation (Transport Layer)

**Source:** `pkg/config/transportconfig.go`, `pkg/internal/transport/revocation/`

- RevocationConfig on TransportConfig
- CRL file hot-reload (SHA-256 hash comparison)
- OCSP responder support with caching
- Modes: "crl", "ocsp", "both", "any"
- Unknown status fails CLOSED by default; `AllowSoftFail: true` opts out
- OnCertRevoked callback

## Security Considerations

### Metadata Leakage

The outer WAL framing (`CRC32`, `Length`, `Type`) and `Epoch` field are unencrypted. An observer with file read access can see:
- Record size distribution
- Record type (entry batch vs. state vs. snapshot)
- Key rotation epoch

This is acceptable for v1 — an attacker with local file read access is already a high-privilege threat.

### Error Oracle Protection

`Decrypt()` returns `ErrDecryptionFailed` for all failure modes (unknown epoch, auth failure, corruption). This prevents side-channel attacks via error type discrimination.

### Compression Ordering

When both compression and encryption are enabled: **compress-then-encrypt** (Snappy → AES-GCM). Encrypted data cannot be compressed. Compression oracle attacks (CRIME/BREACH) are not practical for Raft because proposals originate from trusted application code.

### Integrity Without Encryption

Without a barrier, WAL records are protected only by CRC32 — detects accidental corruption but not intentional tampering. Users needing integrity guarantees should enable the barrier (AES-GCM provides both confidentiality and integrity).

## Security Logging

All security events logged via `log/slog`:

```go
slog.Info("barrier initialized", "epoch", epoch)
slog.Info("barrier unsealed", "epoch", epoch)
slog.Info("barrier sealed")
slog.Error("barrier unseal failed", "err", err)
slog.Info("key rotated", "from_epoch", oldEpoch, "to_epoch", newEpoch, "algorithm", alg)
slog.Error("key rotation aborted: sealed state not persisted; previous epoch remains active", ...)
slog.Error("epoch purge aborted: reduced sealed state not persisted; DEKs retained", ...)
slog.Warn("decryption failed", "err", err)  // intentionally omits epoch
slog.Info("old DEK purged", "epoch", epoch)
```

No custom callback system — users configure their `slog` handler at startup.

## Complete Data Flow

### Write Path (Encryption)

```
Client Proposal
  → Host.Propose() → creates Entry
  → Engine buffers in proposal queue
  → Step worker processes proposal → raft appends to log
  → LogDB.SaveState(updates) via waldb
  → waldb.segment.Write(data)
  → barrier.Encrypt(plaintext) → [Epoch:8 | Nonce:12 | Ciphertext | Tag:16]
  → Written to segment with [CRC32 | Length | Type | encrypted payload]
  → segment.Sync() → fsync to disk
```

### Read Path (Decryption on Recovery)

```
Node restart
  → Host.Open() → LogDB.Open()
  → waldb.Open() → scan all segments in all shards
  → For each segment:
      → Parse records (CRC32, Type, Epoch from headers)
      → barrier.Decrypt(ciphertext) or barrier.DecryptForEpoch(ct, epoch)
      → Looks up cipher from b.ciphers[epoch]
      → AES-GCM.Open() → plaintext entries
  → Entries restored to in-memory maps
```

### Snapshot Save Path

```
Snapshot trigger (appliedIndex > lastSnapshotIndex + interval)
  → rsm.SaveSnapshot() → user's StateMachine.SaveSnapshot(writer)
  → Application writes plaintext state
  → 64KB chunks read from snapshot file
  → For each chunk:
      → barrier.EncryptWithEpoch() → (ciphertext, epoch)
      → Nonce = fresh crypto/rand (random 96-bit)
      → AAD = [Epoch:8]   (epoch-only by default; optional
              context-AAD binds [SnapshotIndex | ChunkSeq])
  → Encrypted chunks written to snapshot file (at-rest)
```

### Snapshot Receive Path

```
Leader sends snapshot via QUIC (mTLS encrypted in transit)
  → Follower's transport.snapshotReceiver.receiveChunks()
  → barrier.RegisterInFlightEpoch(epoch)
  → Chunks received (rate-limited, memory-budgeted)
  → Written to local storage
  → barrier.Encrypt() with follower's own DEK (at-rest)
  → StateMachine.RecoverFromSnapshot()
  → barrier.DeregisterInFlightEpoch(epoch)
```
