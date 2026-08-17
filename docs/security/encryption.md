# At-Rest Encryption

At-rest encryption in QuicRaft is built on a **per-node Barrier** that manages AEAD encryption with epoch-based key rotation. All encryption is opt-in. `DefaultBarrierConfig` picks the algorithm from the local CPU — AES-256-GCM when AES-NI is present, ChaCha20-Poly1305 otherwise — but that default is only appropriate for data that never outlives the host that wrote it. Pin `BarrierConfig.Algorithm` explicitly; see [Algorithm is a property of the data](#algorithm-is-a-property-of-the-data-not-the-host).

**Key packages:**
- `pkg/seal/` — the whole barrier: epoch DEKs, the AEAD data path, sealing strategies, the AEAD safety trackers, the sealed-state format
- `pkg/crypto/` — a backward-compatibility shim only. It contains six type aliases and two `var` re-exports pointing at `pkg/seal` (`pkg/crypto/crypto.go`). No cipher code lives here.
- `pkg/crypto/shamir/` — the pure GF(256) `Split`/`Combine` primitive. Custody is not here; see [Shamir](#shamirs-secret-sharing--primitive--interfaces-only).
- `pkg/enclave/` — memguard-backed in-process memory protection for key material

**Source:** `pkg/seal/barrier.go`, `pkg/seal/barrier_config.go`, `pkg/seal/barrier_binding.go`, `pkg/seal/strategy.go`, `pkg/seal/algorithm.go`, `pkg/seal/errors.go`

**Related:** [Key custody and the extension points](key-custody.md) — where the sealed blob lives, what protects the root key, and how an embedder overrides both.

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
| **Barrier** | Derives DEKs via HKDF-SHA256, encrypts/decrypts data with AEAD, manages epoch rotation | Algorithm, deployment ID, AEAD safety trackers, `StatePersister`, and the enforcement flags (`RequireContextAAD`, `RequireFIPS`, `MinEpoch`) via `BarrierConfig` |
| **SealingStrategy** | Protects the root key at rest (wrapping/unwrapping) | Yes—any backend (software, HSM, TPM2, cloud KMS) |

**The embedder owns durable storage.** The barrier holds sealed state in memory and hands it to a caller-supplied `StatePersister` when that state changes; it never writes a file. Custody of the sealed blob belongs to the host application (go-qrdb), per the Layer-1 boundary.

**Per-node encryption, NOT clustered DEK.** Each node independently manages its own barrier and root key. QUIC provides in-transit encryption via mTLS; the barrier encrypts data at-rest. Nodes cannot decrypt each other's at-rest data.

## Barrier Interface

`pkg.Barrier` (`pkg/host.go`, `type Barrier`) is the interface the Host accepts, declared there to break the import cycle. `*seal.Barrier` satisfies it:

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
    Rotate() (uint64, error)
    PurgeEpochsBefore(minEpoch uint64) (int, error)
    RegisterInFlightEpoch(epoch uint64)
    DeregisterInFlightEpoch(epoch uint64)
    CurrentEpoch() uint64
    ShouldRotateKey() bool
}
```

`EncryptWithEpochPinnedContext` is deliberately **not** in the interface. It is an optional capability (`atomicEpochPinner` in `pkg/host.go`) that the at-rest encrypt loops use when the barrier provides it, sealing a frame and pinning its epoch under one lock. A `Barrier` without it still works, with a narrow window in which a `Rotate` plus `PurgeEpochsBefore` can drop the DEK of a frame that is already sealed into the artifact.

## Supported Algorithms

| Algorithm | Key Size | Hardware Acceleration | Default When |
|-----------|----------|----------------------|---------------|
| **AES-256-GCM** | 32 bytes | x86-64 (AES-NI), ARM64 (crypto ext) | **Yes, when hardware detected** |
| **ChaCha20-Poly1305** | 32 bytes | Software (no hardware acceleration) | **Yes, when no hardware AES** |
| **XChaCha20-Poly1305** | 32 bytes | Software (large nonce variant) | Manual override only |

`seal.SelectOptimalAlgorithm(isHardwareBacked bool)` (`pkg/seal/algorithm.go`) is what `DefaultBarrierConfig` calls. Under FIPS mode it returns AES-256-GCM unconditionally.

AES-128-GCM (`AlgAES128GCM`, `0x01`) and AES-192-GCM (`AlgAES192GCM`, `0x02`) are also valid `BarrierConfig.Algorithm` values; they are never auto-selected.

### Algorithm is a property of the data, not the host

`Decrypt` selects the AEAD by **epoch**, not from the ciphertext header's `algID` byte, and under HKDFV2 the algorithm is an HKDF domain separator. Both mean a deployment uses one algorithm for the life of its data. Nothing in the WAL or a snapshot names that algorithm in a form a reader can act on.

So the auto-selected default is a trap for any data directory that outlives the host that wrote it: restore it onto a machine without AES-NI, or add a node whose CPU differs, and every read comes back `ErrDecryptionFailed` with nothing to point at.

Two mechanisms narrow that:

- `Initialize` records the choice in `SealedRootKey.Algorithm` (`Barrier.Initialize`).
- `SetSealedRootKey` rejects a blob whose `Algorithm` disagrees with `BarrierConfig.Algorithm`, returning `*AlgorithmMismatchError` naming both (`Barrier.SetSealedRootKey`). The failure names the algorithm that would work instead of being indistinguishable from tampering.

A zero `Algorithm` means "unrecorded" — a blob written before the field existed. It loads without complaint under whatever the barrier is configured with, which is the pre-existing behaviour and the pre-existing risk. Pin `BarrierConfig.Algorithm` rather than rely on that.

## Barrier State Machine

**Source:** `pkg/seal/barrier.go`

```go
type Barrier struct {
    mu       sync.RWMutex
    unsealed atomic.Bool  // fast-path check

    state       barrierState
    rootKey     *enclave.ProtectedKey  // 32 bytes, encrypted at-rest in memory
    salt        []byte                 // 32 bytes, used for HKDF
    hkdfVersion HKDFVersion            // DEK-derivation info layout in force

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

    hwEncryptor SymmetricEncrypter // non-nil = delegated mode
    binding     ContextBinding     // resolved context-AAD capability
    plaintextMode bool             // StrategyNone

    loadGeneration uint64      // this barrier's claim on the sealed blob
    superseded     atomic.Bool // another loader claimed it; fail closed
    invocationsAuthoritative bool // this process created the epoch
}
```

The field list is trimmed for readability; `type Barrier` in `pkg/seal/barrier.go` is authoritative.

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
| `Initialize(ctx, strategy, creds)` | Uninitialized | Generate root key + salt, derive epoch-1 DEK, stamp `HKDFVersion`, `Algorithm`, `LoadGeneration=1` and `StateMAC` into the sealed blob | Unsealed |
| `SetSealedRootKey(sealed)` | Any | Validate and adopt a persisted blob (epoch floor, salt length, `KnownEpochs` bounds, HKDF version, algorithm match, load-generation fence) | Sealed, if it was Uninitialized |
| `Unseal(ctx, strategy, creds)` | Sealed | Unwrap root key, verify `StateMAC`, claim the blob (`LoadGeneration+1`), re-derive DEKs for all known epochs, then re-establish a sound invocation budget | Unsealed |
| `Seal()` | Unsealed | Drop root key + all DEK references | Sealed |

`Unseal` does more than the name suggests, and two of the extra steps have visible consequences: it writes to the sealed blob through `StatePersister` before the barrier will encrypt (the [load-generation fence](#the-load-generation-fence)), and it usually **rotates to a fresh epoch** (see [Invocation budget across restarts](#invocation-budget-across-restarts)). Neither happens in delegated or no-encryption mode, both of which return earlier.

`Barrier.Seal()` is exported but production code should go through `Host.Seal()` / `Host.Close()`, which quiesce the pipeline first so the engine does not take an `ErrBarrierSealed` storm (`Barrier.Seal` in `pkg/seal/barrier.go`). Sealing also clears the delegated encryptor and resets `binding` to `ContextBindingUnresolved`: the context-AAD capability is a property of the installed encryptor, so it is re-derived by the next `Unseal` and can legitimately differ.

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
    // Re-authenticate: Epoch and KnownEpochs are both covered by StateMAC,
    // and a blob whose MAC covered the OLD values would be rejected by the
    // next Unseal (correctly — it is indistinguishable from an edited one).
    if macErr := b.restampStateMACLocked(rootBuf.Bytes(), candidate); macErr != nil {
        wipeBytes(dek)
        return 0, macErr
    }
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

Rotation is rejected with `ErrBarrierSealed` unless the barrier is unsealed, with `ErrBarrierSuperseded` when another loader has claimed the sealed blob, and with `ErrHardwareMode` in delegated mode. The order of those checks is `sealed → superseded → hardware → nil sealed data → no persister` (`Barrier.rotateLocked`).

On restart, `SetSealedRootKey` restores the persisted known epochs and `Unseal()` re-derives DEKs for all of them.

`Unseal` can rotate too: `rotateLocked` is factored out of `Rotate` precisely so `resolveInvocationBudget` can call it while already holding the write lock.

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

**Required for three things**, not two:

1. `Rotate`.
2. Any `PurgeEpochsBefore` that would actually drop an epoch.
3. **Every `Unseal` after a restart**, which claims the blob (`LoadGeneration+1`) and then usually rotates.

`Initialize` does **not** call the persister — the embedder persists `SealedRootKeyData()` directly at bootstrap.

**Consequence of leaving it nil.** This is worse than "rotation is disabled", which is how it used to read.

- `Rotate` fails closed with `ErrNoStatePersister` on every call.
- The load-generation fence is inert. A claim that cannot be written down is not a claim, so a second process loading the same blob is undetectable. `claimLoadGenerationLocked` logs this at WARN (`Barrier.claimLoadGenerationLocked`).
- **The first `Unseal` after a restart leaves the barrier read-only.** With the default in-memory counter, the invocations already spent under the loaded epoch are not merely unknown but unknowable, and the barrier will not encrypt against a budget nobody can prove. It would rotate to a fresh epoch, but it cannot without a persister, so `resolveInvocationBudget` seeds the counter to the **full** limit and every `Encrypt` returns `ErrInvocationLimitExceeded`. `Decrypt` keeps working: the node can serve its data, it just cannot add to it.

The default `InvocationLimit` is `DefaultInvocationLimit` = 2^32 (NIST SP 800-38D for random 96-bit nonces).

### Invocation budget across restarts

**Source:** `Barrier.resolveInvocationBudget` in `pkg/seal/barrier.go`

With random 96-bit nonces, 2^32 invocations per DEK is a hard nonce-collision bound (collision probability below 2^-32), not advice. A GCM nonce collision yields keystream XOR and recovery of the GHASH authentication key, which is arbitrary forgery of at-rest records.

The default counter is in-memory and does not survive a restart. An earlier design seeded it to the rotation threshold (95%) after an unseal, leaving 5% — over 2×10^8 encryptions — of headroom. That headroom was re-granted in full on *every* restart against the *same* DEK, so roughly 20 restarts of a deployment that never rotates pushed the DEK past 2^32. That behaviour is gone.

`Unseal` now takes one of two sound paths, decided by `resolveInvocationBudget`:

| Condition | Action |
|---|---|
| Counting disabled (`InvocationLimit == -1`) | Nothing. The embedder opted out of the bound. |
| `BarrierConfig.InvocationCounter` injected | Nothing. The injected counter owns persistence and seeding. |
| This process created the epoch (`Initialize`, or a `Rotate` since) | Nothing. A seal/unseal cycle inside one process must not be disturbed. |
| `StatePersister` set | **Rotate to a fresh epoch**, inside `Unseal` and under the same write lock. |
| Otherwise | Seed the counter to the full limit. Every `Encrypt` fails with `ErrInvocationLimitExceeded`; `Decrypt` is unaffected. |

The rotation is performed inside `Unseal` rather than left to an embedder's rotation loop because a loop that reacts a tick later leaves a window in which writes land on the exhausted DEK, and an embedder without such a loop leaves it open forever.

**What the fresh epoch assumes.** The rotated-to epoch is a pure function of the loaded blob (`epoch+1`), and every `deriveDEK` input comes from that blob, so a second process loading the same blob derives the *same* new DEK and also starts from zero. "Count zero" is a claim about this process only. That is what the load-generation fence exists to make visible.

**Side effect worth planning for.** The forced rotation grows `KnownEpochs` by one per restart, and `SetSealedRootKey` refuses a blob naming more than `MaxKnownEpochs` (4096, the `MaxKnownEpochs` constant). Nothing reconciles the two: the bound is a DoS guard against a forged blob, and it also caps legitimate growth. An embedder that never calls `PurgeEpochsBefore` — or one in a crash loop, which reaches 4096 in minutes rather than years — ends up with a node that cannot start at all, needing bespoke tooling to recover (unwrap the root key, trim `KnownEpochs`, re-stamp the MAC). The barrier logs at ERROR once the set passes half the bound.

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
- **Actual contexts.** Both consumers are threaded today, and both use a magic-tagged, fixed-width, **big-endian** layout with no delimiters, so no two distinct positions can produce the same bytes. Snapshot frames pass a 39-byte context, WAL records a 30-byte one; see [Snapshot AAD](#aad-additional-authenticated-data) and [WAL positional binding](#positional-binding-for-wal-records) for the exact byte layouts. This binds each frame to its position and artifact, so same-epoch reorder, splice, and truncation fail authentication.
- **The magic is a scheme tag, not a version byte.** It is part of the authenticated context rather than a byte on disk, so the scheme identifies itself: a frame sealed under a future layout can never authenticate against this one, and there is no unauthenticated version marker for an attacker to rewrite. There is no epoch-only fallback — a reader that fails to authenticate a frame must **never** retry without the context, because that retry would be the vulnerability.
- **Truncation is not covered.** Nothing in the AAD names the number of frames or the total length, so every surviving frame of a shortened artifact still authenticates and a reader that stops at EOF sees a complete, shorter artifact. Detecting a removed tail is the caller's job: fold the total frame count into every frame's context, or give the final frame a distinguishable context. **Neither consumer does this today.** The snapshot receive path checks a declared chunk count for the *network* transfer only, and the WAL relies on its own durability-sequence recovery rules. At-rest tail truncation of either artifact is not detected by the AEAD.
- **Delegated (hardware) mode.** The plain `SymmetricEncrypter` seam takes no AAD. A delegated encryptor that can authenticate AAD end to end declares it by also implementing `AADSymmetricEncrypter`; the barrier then routes the context through `EncryptWithAAD`/`DecryptWithAAD` using the SAME `aadForEpoch(epoch, contextAAD)` bytes the software path builds, so hardware deployments keep their positional binding. An encryptor that does NOT implement it returns `ErrHardwareMode` for a non-empty context (fail-closed) rather than silently dropping the binding; a nil/empty context is delegated unchanged in both cases.
- **The declaration is measured, not trusted.** A Go method set is a claim, and the implementations live in other repositories. At `Initialize` and `Unseal` — never on the data path — `verifyAADBinding` (`verifyAADBinding` in `pkg/seal/barrier_binding.go`) runs one encrypt and two decrypts against the delegated encryptor with random 32-byte values: seal a probe under `aadA`, require it to open under `aadA` and return the same bytes, require it to **fail** under an independently drawn `aadB`. An encryptor that accepts the `aad` argument and drops it passes the first two and fails the third. A failing probe is logged at ERROR and the barrier downgrades the encryptor to `ContextBindingUnsupported`, so `RequireContextAAD` then refuses the deployment rather than blessing it.
- **Capability reporting.** `Barrier.SupportsContextAAD()` is what callers gate on. It is `ContextBinding().Binds()`, and `ContextBinding` has five values:

  | Value | Meaning | `Binds()` |
  |---|---|---|
  | `ContextBindingUnresolved` | Barrier not unsealed; no encryptor installed, so the question has no answer yet | false |
  | `ContextBindingBarrier` | Software path; the barrier folds the context into its own AAD | true |
  | `ContextBindingDelegated` | Delegated encryptor that implements `AADSymmetricEncrypter` and passed the probe | true |
  | `ContextBindingUnsupported` | Delegated encryptor with no AAD capability. Data is encrypted; position is not bound | false |
  | `ContextBindingPlaintext` | `StrategyNone`. No AEAD, no tag, nothing a position could bind to | false |

  `SupportsContextAAD()` is safe to **ask** at any time and unsafe to **remember**: false means either "cannot bind" or "cannot say yet", and the two demand opposite handling. A consumer that caches the answer must gate on `ContextBinding().Resolved()` (or `Barrier.ContextBindingResolved()`, which exists for consumers that assert the barrier structurally and cannot import the `ContextBinding` type) and refuse to record an unresolved one. The capability is valid only while the barrier stays unsealed and can legitimately change across a seal/unseal cycle. `IsHardwareMode()` is true for both `Unsupported` and `Plaintext` and cannot tell them apart; `IsPlaintextMode()` is what separates them.
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
- **Nonce-reuse tracker.** When enabled, the first `DefaultMaxTrackedNonces` (4Mi) nonces of an epoch are recorded and a repeat of any of them is rejected outright. That is **0.0977% of the 2^32 invocation budget**, not the whole of it — the tracker is a bounded-memory backstop against a defective RNG, not a proof of uniqueness across an epoch. Past the ceiling it *saturates*: it stops admitting new nonces, so memory stays flat, and keeps rejecting repeats within the retained prefix. See below.

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
`CheckAndRecordNonce` calls.

**Coverage is a bounded prefix, not the epoch.** Memory does **not** grow
without limit: the tracker admits at most `DefaultMaxTrackedNonces` (4Mi)
nonces per epoch and then saturates. A saturated tracker still rejects a repeat
of any nonce it retained, and `Saturated()` and `Count()` report that state
honestly — it does not disable itself or report zero. Detection therefore
covers the first 0.0977% of an epoch's 2^32 permitted invocations. Collision
safety across the remainder rests on the invocation cap plus random 96-bit
nonces, which is the NIST SP 800-38D argument; the tracker is a backstop
against a defective RNG, not the primary guarantee.

**Single-writer ownership of the sealed blob is an embedder obligation.** Every
DEK input is a function of the blob and the forced unseal rotation is
`epoch+1`, so two barrier instances loaded from the *same* blob derive the
*same* DEK, each with a zeroed invocation counter and an empty tracker. Neither
observes the other's nonces, and the per-key invocation budget is counted
twice. The [load-generation fence](#the-load-generation-fence) makes that state
detectable at the next load, but it is not a lock. `MinEpoch` does not guard it
either (no rollback is involved), and `StateMAC` cannot (both copies are
authentic). Exactly one process must own a sealed blob at a time, and only an
exclusive data-directory lock or a compare-and-swap persister makes that a
guarantee.

### Methods

**Encrypt:**
```go
func (b *Barrier) Encrypt(dst, plaintext []byte) ([]byte, error)
```
Output: `[Epoch:8 | algID:1 | Nonce:12 | EncryptedData | Tag:16]`

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

The on-disk chunk header is `recordHeaderSize = 11` bytes:

```
Plaintext chunk:  [CRC32:4 | Length:2 | Type:1 | DurSeq:4 | Data:N]
Encrypted chunk:  [CRC32:4 | Length:2 | Type:1 | DurSeq:4 | Epoch:8 | AlgID:1 | Nonce:12 | Ciphertext:N | Tag:16]
```

- `CRC32` covers `[Type][DurSeq][Data]` — that is, everything after the length field, which for an encrypted chunk means the epoch, algorithm ID, nonce, ciphertext and tag
- `Length` is **2 bytes** and holds the payload length excluding the header, so `maxRecordDataLen` is 65535; a logical record longer than that is split across First/Middle/Last chunks
- `Type` is unencrypted (entry batch, state, snapshot)
- `DurSeq` is the chunk's durability sequence. It sits **inside** the checksummed region deliberately: recovery uses it to decide whether truncating is safe, so a flipped bit in it must invalidate the chunk rather than silently move the chunk into another generation. It is placed after the type byte so the covered region stays one contiguous slice and the CRC is computed in a single pass
- `Epoch` is unencrypted (routing — tells the decryptor which DEK)

CRC32-C protects against **accidental** corruption only — bit flips, partial writes, torn pages. It is not a cryptographic integrity check: an adversary with disk access can trivially recompute a valid CRC32 after modifying record contents. Tamper protection comes from the barrier's AES-256-GCM AEAD; the CRC remains as a fast first-pass detector so garbage is not fed into the decryption path. That split is deliberate — CRC32-C runs at ~30 GB/s hardware-accelerated where HMAC-SHA256 runs at ~1-3 GB/s.

See [WAL Persistence](../storage/wal.md) for the full segment and block layout.

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

### Positional binding for WAL records

Random nonces and epoch-only AAD do not, on their own, bind a WAL record to its position: two records sealed under the same DEK and epoch authenticate identically no matter where they sit, so an at-rest attacker with write access could reorder, splice or truncate same-epoch records without the authentication tag noticing.

Every encrypted WAL record therefore additionally binds its **position** — which WAL shard, which segment, and which record ordinal within that segment — into the AAD, so a record only authenticates at the exact place it was written. The context is fixed-width and **big-endian** with no delimiters, **30 bytes** total:

```
context = "QRWAL1"(6) || WALShardIdx:8 || SegmentID:8 || RecordSeq:8      = 30 bytes
AAD     = [Epoch:8 little-endian] || context                             = 38 bytes
```

`WALShardIdx` is required, not decorative: each WAL shard is its own directory with an independent `%06d.log` ID space, and all of them share one barrier, so `(SegmentID, RecordSeq)` alone is not unique across shards.

`RecordSeq` is the ordinal of the **logical** record within its segment, 0-based — the number of logical records already committed to that segment, which is exactly the index at which `segment.ReadAll` returns the record during recovery. Chunking a record across blocks does not affect it: `ReadAll` assembles First/Middle/Last chunks into one entry before appending.

The context bytes are built in one place (`walRecordContext`), used by both the encrypt and the decrypt side, so the two cannot drift. This is **not** opt-in: there is no on-disk version marker and no epoch-only fallback, and WALs written before this scheme are not readable.

## Snapshot Encryption

Snapshots are encrypted as a stream of 64KB chunks. Each chunk is a standard barrier ciphertext frame produced by `EncryptWithEpochPinnedContext` when the barrier implements the atomic pinner, and by a pin-before / encrypt / pin-after sequence when it does not:

```
Chunk: [Epoch:8 | AlgID:1 | Nonce:N | Ciphertext:N | Tag:16]
```

### Nonce Construction

Each chunk's nonce is a **fresh random nonce** from the barrier (12 bytes for AES-GCM), exactly as for any other `Encrypt` call. There is no `[AttemptSalt | ChunkSeq]` deterministic construction; the snapshot pipeline does not synthesize nonces. Collision safety is bounded by the barrier's per-DEK invocation cap and reuse tracker — see [Nonce Generation](#nonce-generation).

### AAD (Additional Authenticated Data)

Epoch-only AAD does not bind a chunk to its position, so an at-rest attacker could reorder chunks, substitute chunks from another snapshot at the same epoch, or truncate the stream without failing per-chunk authentication.

Every snapshot frame therefore binds a **positional context** built by `engine.NewSnapshotAAD`. It is the magic followed by four **big-endian** uint64 fields — every field fixed width, so no delimiter is needed and no two distinct identities can produce the same bytes. `SnapshotFrameAADLen` is **39 bytes**:

```
 off  size  field
   0     7  magic "QRSNAP1"
   7     8  ShardID
  15     8  ReplicaID
  23     8  SnapshotIndex
  31     8  ChunkSeq (0-based ordinal within snapshot.dat)

context = 39 bytes
AAD     = [Epoch:8 little-endian] || context   = 47 bytes
```

This makes:
- **Chunk reordering** — a wrong `ChunkSeq` fails authentication.
- **Cross-snapshot substitution** — a wrong `SnapshotIndex` fails authentication.
- **Cross-replica substitution** — a wrong `ShardID` or `ReplicaID` fails authentication.

**Truncation is not covered by the AAD, and the two places it could happen are guarded differently.**

- *In transit*, the snapshot stream header declares a `ChunkCount` and the receiver refuses a transfer where `len(chunks) != hdr.ChunkCount`, or where the count is zero, or where the stream was marked truncated (`snapshotRecv`, `pkg/internal/transport/snapshot_recv.go`). A separate `maxAdmissibleChunkCount` bound rejects an inflated declared count before any memory is committed.
- *At rest*, there is **no equivalent**. The encrypted `snapshot.dat` is a bare sequence of length-prefixed frames with no frame count and no terminal marker, and `ChunkSeq` names a frame's position but not how many follow it. An attacker with write access to the file can drop trailing frames, and every surviving frame still authenticates; a reader that stops at EOF sees a complete, shorter snapshot. Detecting that would need a count folded into every frame's context or a distinguishable final frame, and neither is implemented.

`NewSnapshotAAD` first asks the barrier whether it can actually authenticate a caller-supplied context (`SupportsContextAAD`). A barrier that cannot must not be handed one: the write fails closed rather than silently dropping the binding. `ContextBinding()` and `IsPlaintextMode()` separate the two reasons a barrier may be unable to bind — no-encryption mode, where there is no AEAD and nothing a position could be bound to, versus a delegated encryptor that is protecting the data but cannot authenticate where it belongs. Only the second is a security limitation.

### Snapshot Encryption: Atomic Write Pattern

**Source:** `pkg/internal/engine/snapshot_pool.go`

`encryptSnapshotFile(path, shardID, replicaID, index, barrier) (minEpoch uint64, pinned []uint64, err error)` replaces a plaintext snapshot data file with its encrypted form using an atomic write-to-temp-then-rename pattern. A crash mid-encryption leaves the original plaintext file intact.

```
1. Stream the plaintext file in 64KB chunks (one chunk in memory at a time)
2. Create temporary file in the same directory
3. For each chunk, with ChunkSeq as its 0-based ordinal:
   a. barrier.EncryptWithEpochPinnedContext(nil, chunk, NewSnapshotAAD(...))
      → [Epoch:8 | algID:1 | Nonce:12 | Ciphertext | Tag:16], epoch pinned in-flight
   b. Write frame: [ChunkLen:4 (little-endian) | EncryptedChunk]
4. fsync the temporary file
5. Atomic rename: temp file → original path
6. fsync the directory, so the rename itself is durable
```

Step 6 matters: without it a crash can roll the directory entry back to the deleted plaintext temp name even though the encrypted bytes were fsynced.

The returned `pinned` epochs are held in-flight until the snapshot's epoch is durably recorded, so a concurrent `Rotate` plus `PurgeEpochsBefore` cannot drop the DEK of a frame already written into the artifact.

The on-disk format is a sequence of length-prefixed frames:

```
[ChunkLen:4 | EncryptedChunk₁][ChunkLen:4 | EncryptedChunk₂]...
```

Each `EncryptedChunk` carries its own epoch and nonce in the standard barrier ciphertext format, so the file is self-describing for decryption.

### Snapshot Decryption: Epoch-Aware Recovery

**Source:** `pkg/internal/engine/snapshot_pool.go`

`decryptSnapshotToSibling(path, shardID, replicaID, index, epoch, barrier) (tmpPath string, err error)` writes the plaintext to a **new sibling temp file** and returns its path. It does not modify or replace the source: the canonical encrypted `snapshot.dat` is left intact, and the caller removes the temp file once recovery is done.

```
1. Open the encrypted file (one frame in memory at a time)
2. Create a sibling temp file in the same directory
3. For each frame, with ChunkSeq as its 0-based ordinal:
   a. Read 4-byte little-endian frame length
   b. Read encrypted chunk of that length
   c. barrier.DecryptWithContext(nil, frame, NewSnapshotAAD(...)) → plaintext
   d. Write plaintext to temp file
4. fsync the temporary file
5. Return the temp path
```

**In-place decryption was deliberately rejected.** Renaming plaintext over `snapshot.dat` is not crash- or retry-safe: a recovery failure or a crash after the rename leaves plaintext on disk while LogDB still records `Epoch > 0`, so every subsequent restart re-attempts decryption on plaintext and fails permanently, losing an otherwise-valid snapshot.

The `epoch` parameter is **advisory**. Each frame is decrypted with the epoch in its own ciphertext header via `DecryptWithContext`, not with this value; it survives only for log context. The caller uses the recorded epoch solely to decide whether decryption is needed at all (`Epoch > 0`).

`shardID`, `replicaID` and `index` must be the identity the frames were sealed under, because they reproduce the positional context AAD. A mismatch fails authentication, and there is no weaker retry — an epoch-only fallback would readmit the frame splicing this binding exists to prevent.

The state machine therefore always sees plaintext during `RecoverFromSnapshot()`, whether or not encryption is enabled.

### Snapshot Transport Between Nodes

Snapshots are **NOT encrypted at the application layer during transport**. The flow for a single-barrier setup is:

```
Leader:
  1. State machine writes plaintext snapshot to disk
  2. Barrier encrypts chunks to at-rest storage (local)
  3. Leader decrypts to a sibling temp file for streaming (decryptSnapshotToSibling)
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

    if b.state != barrierUnsealed { return 0, ErrBarrierSealed }
    if b.superseded.Load()        { return 0, ErrBarrierSuperseded }
    if b.hwEncryptor != nil       { return 0, ErrHardwareMode }
    if minEpoch > b.epoch         { return 0, ErrPurgeCurrentEpoch }

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
    // KnownEpochs is covered by StateMAC, so a purge must re-stamp it or the
    // reduced blob fails its own authentication on the next Unseal.
    if macErr := b.restampStateMACFromEnclaveLocked(candidate); macErr != nil {
        return 0, macErr
    }
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
    Salt        []byte       // 32-byte HKDF salt (not secret)
    Ciphertext  []byte       // encrypted root key
    Metadata    []byte       // strategy-specific (KDF version byte, key ID, KMS ARN…)
    KnownEpochs []uint64     // all epoch numbers for DEK re-derivation
    KDFVersion  int          // KDF used when sealing (survives a FIPS-mode change)
    HKDFVersion HKDFVersion  // DEK info layout; 0 = pre-versioning, derives as v1
    Algorithm   SymmetricAlgorithm // data-path AEAD the epoch DEKs were derived for

    LoadGeneration uint64    // how many times this blob has been claimed
    StateMAC       []byte    // HMAC-SHA256 authenticating the metadata above

    FailedAttempts     int64 // brute-force backoff, persisted across restarts
    LastFailedUnixNano int64
    // ... plus Nonce and the hardware/Shamir fields; see
    //     type SealedRootKey in pkg/seal/strategy.go for the full list
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

## Sealed-State Integrity

The wrapping AEAD covers only `Ciphertext`, and its AAD is the constant strategy name. Every other field on `SealedRootKey` sits in plaintext beside it, bound to nothing. Three mechanisms cover the gap, and each closes a different hole.

### StateMAC: authenticating the metadata

**Source:** `sealedStateMAC` / `sealedStateMACAt` in `pkg/seal/barrier.go`

`StateMAC` is HMAC-SHA256 over a canonical, length-prefixed encoding of the fields that decide how the barrier uses its key, keyed by HKDF-SHA256(rootKey, info=`"quicraft/barrier/state-mac/v1"`). Every variable-length field carries a fixed 8-byte big-endian length prefix, so no field's contents can imitate another's boundary.

**Covered:** `Strategy`, `Version`, `DeploymentID`, `Epoch`, `LoadGeneration`, `Salt`, `KnownEpochs`, `HKDFVersion`, `Algorithm`, `Ciphertext`, `Metadata`, `Nonce`, `HardwarePayload`.

`Ciphertext` is included even though it authenticates itself, so genuine metadata from one blob cannot be paired with a genuine ciphertext from another. `Metadata` is included because it carries the KDF version byte, which selects the unwrapping KDF.

**Not covered, deliberately:** `FailedAttempts` and `LastFailedUnixNano`, which `persistBackoffState` writes after a *failed* unseal — the point at which no root key is available to re-MAC with. Also `CreatedAt`, `ShamirThreshold` and `ShamirTotal`, which are higher-layer custody bookkeeping that go-qrdb owns and may legitimately rewrite.

The field that matters most is `Epoch`. Rewriting it downward resurrects an already-spent DEK alongside a freshly zeroed invocation counter and an empty nonce tracker, which is two independent random-nonce sequences under one key.

`Initialize`, `Rotate` and `PurgeEpochsBefore` all stamp it — every operation that changes a covered field. `Unseal` verifies it once the root key is recovered, **before deriving a single DEK**. A verification failure is deliberately *not* counted as a failed unseal attempt: the credential was correct and the state is not, and driving the brute-force backoff from a file an attacker can write would hand them a lockout primitive.

**Version 2.** The covered-field encoding is versioned by a leading byte, currently `2`. v2 added `LoadGeneration` to the covered set, because the barrier now refuses to encrypt on the strength of that number and leaving it unauthenticated would be a durable, passphrase-free way to stop a node from writing.

A blob stamped under v1 fails the v2 comparison while being entirely authentic. `verifySealedStateMAC` recognises that case — only when `LoadGeneration == 0`, which a pre-fence blob always is — by recomputing under v1, accepting once at WARN, and letting the normal persist path restamp at v2. Accepting v1 is not a downgrade: forging either version needs the same HKDF-derived MAC key, which comes from the root key the strategy just recovered.

**A blob with no MAC at all** is refused with `ErrSealedStateUnauthenticated`. Accepting an absent tag would make the whole mechanism optional at the attacker's discretion: strip the field, keep the forgery. `BarrierConfig.AllowUnauthenticatedSealedState` is the explicit, per-deployment escape hatch for migrating a pre-MAC blob — start once with it set, re-persist `SealedRootKeyData()`, then clear it. It does **not** apply to a blob whose tag is present but wrong.

`StrategyNone` keeps no root key, so it has no MAC and none is expected of it.

### MinEpoch: refusing replay

A MAC cannot detect **replay**. An attacker with write access to the persisted state can restore an older, genuinely-MACed copy of the same blob, resurrecting a retired epoch's DEK with a fresh counter and an empty tracker. Only state the attacker cannot roll back in the same step can catch that, and the barrier holds none — the embedder does.

`BarrierConfig.MinEpoch` is that floor. `SetSealedRootKey` rejects a blob whose `Epoch` is below it with `ErrSealedEpochRollback`. The effective floor is `max(MinEpoch, b.epoch)` when the barrier already holds state: reloading a state older than the one in hand is a rollback whether or not a floor was configured.

Set it to the highest epoch any live WAL segment or retained snapshot still references — the engine already computes exactly this as its global safe-purge floor. Zero disables the check.

`SetSealedRootKey` also enforces, in order (`Barrier.SetSealedRootKey`): non-nil blob, `Epoch > 0`, the rollback floor, 32-byte `Salt`, `len(KnownEpochs) <= MaxKnownEpochs`, no epoch `0` and none above `Epoch` in `KnownEpochs`, `Epoch` present in `KnownEpochs`, `HKDFVersion` not newer than this build, `Algorithm` agreement, and finally the load-generation fence. Epoch `0` is rejected because accepting it is not inert: `Unseal` would derive and install a full epoch-0 DEK, the forced rotation would re-persist it durably, no purge floor could ever retire it, and `b.ciphers[0]` is exactly what a zero-filled ciphertext header decodes to.

### The load-generation fence

**Source:** `Barrier.claimLoadGenerationLocked` in `pkg/seal/barrier.go`; `SealedRootKey.LoadGeneration` in `pkg/seal/strategy.go`

`LoadGeneration` counts how many times a blob has been claimed. `Initialize` stamps 1; every subsequent `Unseal` that can persist writes back `LoadGeneration+1` **before the barrier will encrypt**, so each load leaves a claim in the one place two processes share.

A barrier later handed a blob whose generation exceeds its own claim knows another loader has taken the state. It marks itself superseded **permanently** and returns `*SupersededError` (which is `ErrBarrierSuperseded`). `Encrypt`, `Rotate` and `PurgeEpochsBefore` all refuse from then on; `Decrypt` keeps working. The loaded state is deliberately not adopted — quietly picking up the winner's epoch would make this instance a second writer of the winner's DEK, which is the failure being prevented. A superseded barrier also cannot launder the supersession by sealing and unsealing again.

The ordering is the whole point: the claim must be durable before this barrier can encrypt, or a process that dies between encrypting and persisting leaves a generation on disk that the next loader increments without ever learning a DEK was used under it.

**What it does not do**, stated plainly:

- **It is not continuous.** The generation is compared at load (`SetSealedRootKey`) and enforced at write (`Rotate`, `PurgeEpochsBefore`) or use (the `Encrypt` family, at the single choke point `encryptLocked`). The barrier never re-reads the blob on its own, because that would put a disk read on the encrypt hot path. A barrier superseded while running keeps encrypting until the embedder hands it the newer state.
- **It is not a lock.** Two loaders that read the same generation before either writes both claim the same number and neither can tell. Detection lands on whichever loader reads after the other has written — the common case of a second process starting later, but not a guarantee.
- **It is inert in delegated and no-encryption mode**, both of which return from `Unseal` before the claim, for the same reason they bypass the invocation counter and nonce tracker: the delegated encryptor owns its own key.
- **It is inert without a `StatePersister`.** Logged at WARN.

A generation that goes *backwards* is a rollback of the blob, not a competing claim. It is logged at ERROR and execution continues, because refusing there would let anyone who can restore an old backup stop a node from writing. That case belongs to `MinEpoch`.

### KnownEpochs Persistence Across Restart

The `KnownEpochs` field on `SealedRootKey` is the mechanism that preserves epoch history across seal/unseal cycles and node restarts. Without it, `Unseal` would only derive the DEK for the current epoch, and data encrypted with prior epochs would be unreadable.

**Persistence flow:**

1. `Initialize()` sets `sealed.KnownEpochs = []uint64{1}` (the initial epoch). The embedder persists `SealedRootKeyData()` itself — `Initialize` does not invoke `StatePersister`.
2. Each `Rotate()` rebuilds `sealed.KnownEpochs` (ascending, so persisted blobs are reproducible) and pushes the new state through `StatePersister` *before* activating the new epoch; each purging `PurgeEpochsBefore()` pushes the reduced set through it *before* dropping a key.
3. On restart, the caller loads the persisted `SealedRootKey` and calls `SetSealedRootKey()`.
4. `SetSealedRootKey()` validates the sealed data (epoch > 0, salt length, KnownEpochs contains current epoch, `HKDFVersion` not newer than this build), restores the in-memory `knownEpochs` map.
5. `Unseal()` iterates `b.knownEpochs` and re-derives a DEK + cipher for every historical epoch.

This ensures that after a restart with multiple prior rotations, every historical DEK is available for decrypting old WAL segments and snapshots.

## Strategies: Built-In and Injected

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

**Argon2id defaults exceed OWASP 2024 minimums** (time=3, memory=64MB) because the sealed root key is a high-value offline attack target. Measured on a 2025 desktop CPU (Core Ultra 9 285K), one derivation at the defaults takes about 93 ms; slower hardware will be several times that. It happens once at startup.

`DeriveKey` also enforces a work-factor floor and returns `ErrKDFWeakParams` below it: Argon2id `time >= 2`, `memory >= 32 MiB`, `threads >= 1`, and `KeyLen >= 16` with a salt of at least 16 bytes for any KDF.

**PBKDF2 defaults** use 600,000 iterations per NIST SP 800-132 recommendations for FIPS compliance.

Constructors:
- `NewSoftwareStrategy(passphrase []byte) (*SoftwareStrategy, error)` — auto-selects the KDF from `FIPSEnabled()`
- `NewSoftwareStrategyFIPS(passphrase []byte, iterations int) (*SoftwareStrategy, error)` — explicit PBKDF2-SHA256
- `NewSoftwareStrategyWithParams(passphrase []byte, time, memory uint32, threads uint8) (*SoftwareStrategy, error)` — custom Argon2id tuning. Refused with `ErrKDFNotFIPSApproved` when FIPS mode is active, since it names Argon2id explicitly.
- `NewDeferredSoftwareStrategy() *SoftwareStrategy` — no passphrase at construction; it is taken from `Credentials.Secret` at seal/unseal time. For flows where the strategy is built before the operator has supplied anything.

**FIPS mode** (`FIPSEnabled()`, `pkg/seal/fips.go`) is detected from `crypto/fips140.Enabled()` with the `GOFIPS140` environment variable as a fallback (the literal value `off` counts as disabled). It constrains two things: the KDF (`DefaultKDFVersion` selects PBKDF2-SHA256 and `DeriveKey` refuses Argon2id) and the data-path cipher (`SelectOptimalAlgorithm` returns AES-256-GCM unconditionally, and `BarrierConfig.Validate` rejects ChaCha20/XChaCha20 with `ErrAlgorithmNotFIPSApproved`).

Because that detection is environmental, a `false` return means either "FIPS was not requested" or "FIPS was requested and this binary has no module" — and the second **fails open** onto Argon2id and possibly ChaCha20-Poly1305. `BarrierConfig.RequireFIPS` turns the ambiguity into a refusal at `NewBarrier` (`ErrFIPSUnavailable`). Set it if you need FIPS.

### Hardware-Backed Strategies

> **Layer boundary.** go-quicraft (L1) ships **only** the `None` and `Software`(passphrase)
> strategies and names only their `StrategyID`s (`none`, `passphrase`) plus the `StrategyID`
> *type*. The hardware/cloud strategies below are **illustrative** — their concrete
> implementations and `StrategyID`s (`tpm2`/`pkcs11`/`awskms`/`gcpkms`/`azurekv`/`vault`) live in
> **go-xkms**, and `shamir` in **go-qrdb**, injected via the `SealingStrategy` interface. There is
> no built-in strategy catalog or preference-order default in L1; selection is always explicit.

The `SealingStrategy` interface supports arbitrary backends without touching the core. The core QuicRaft module has zero dependency on any external key management system.

**A hardware strategy has two very different modes**, and which one it is in is decided entirely by what its `BarrierEncryptor` returns:

| `BarrierEncryptor` returns | What the hardware protects | Barrier's own data path |
|---|---|---|
| `(nil, nil)` | The **root key only**. The barrier still derives epoch DEKs from it via HKDF and encrypts with its own AEAD. | Fully active: epoch header, nonce tracker, invocation counter, context AAD, `Rotate`, `PurgeEpochsBefore`. |
| a non-nil `SymmetricEncrypter` | The **entire data path**. The root key need never enter process memory. | Entirely bypassed. `Rotate` and `PurgeEpochsBefore` return `ErrHardwareMode`. Context AAD survives only if the encryptor implements `AADSymmetricEncrypter` and passes the probe. |

As of this writing, every production caller of go-xkms's `NewTPM2Strategy`, `NewPKCS11Strategy` and `NewCloudKMSStrategy` passes a nil encryptor (`go-xkms/pkg/server/seal_strategies.go`, `go-xkms/pkg/seal/autodetect.go`), which is the first row. So hardware deployments today wrap the root key in the TPM or HSM and keep the barrier's full AEAD stack, including positional context AAD. The delegated path exists and is exercised by tests; it is not the default shape of a hardware deployment.

Which of the two you want is a real decision, not a detail. See [Key custody](key-custody.md#delegated-hardware-mode) for the tradeoff.

## Shamir's Secret Sharing — primitive + interfaces only

**Source:** `pkg/crypto/shamir/` (GF(256) math), `pkg/seal/shamir.go` (interfaces)

The consensus engine is **Layer 1**: it owns only the low-level Shamir primitive and
the seam interfaces, never the custody machinery.

- `pkg/crypto/shamir/` — the pure GF(256) `Split`/`Combine` primitive (K-of-N threshold).
- `pkg/seal/shamir.go` — the `ShareSplitter` / `ShareAccumulator` interfaces plus the
  `ShamirConfig` / `ShamirInitResult` / `QuorumProgress` value types that a host application
  injects an implementation for.

**`Combine` performs NO integrity check.** This is the single most important
property for an integrator to get right, and it is easy to assume otherwise.

Shares carry **no digest of the secret**. There is no `Digest` field on
`Share`, `Combine` does no hashing of any kind, and it is a one-line
delegation to `CombineUnverified` — both names exist only to let callers
express intent. With fewer-than-threshold genuine shares, or a corrupted or
mismatched share, reconstruction yields **indistinguishable garbage, not an
error**.

Embedding a digest would be an offline brute-force oracle that defeats
Shamir's information-theoretic secrecy for low-entropy secrets, which is why
an earlier design that stored `sha256(secret)` in each share was removed.
`Threshold` and `Total` on a `Share` are likewise **unauthenticated
caller-supplied metadata**.

`IntegrityError` still exists in the package but is marked deprecated and is
**never returned**; it is retained only for API/SDK compatibility. Do not
write code that waits for it.

**The caller MUST authenticate the reconstructed secret out-of-band** — for
example by using it as the key in an authenticated decrypt that fails on a
wrong key, or by checking it against an independent commitment. An integrator
who skips this silently accepts a garbage reconstructed root key. (The
accumulator/custody machinery that drives this path lives in go-qrdb, not
here.)

The **Shamir sealing strategy** (splitting the root key), **share custody**, the
**M-of-N quorum/unseal orchestration**, and the **Security-Officer CSR challenge-response
ceremony** are NOT part of the consensus engine. They are a database-server / custody
concern and live in **go-qrdb** (`pkg/seal` strategies + `pkg/ceremony`). The consensus
engine never persists Shamir shares. go-qrdb declares `StrategyShamir` and owns the
`StrategyID` namespace and the barrier registry (`go-qrdb/pkg/seal/strategy_shamir.go`,
`strategy_registry.go`); the ceremony lives in `go-qrdb/pkg/ceremony`. See
[Key custody](key-custody.md#what-go-quicraft-does-not-ship) for the boundary in one place.

## Enclave Integration

**Source:** `pkg/enclave/`

Despite the name, `pkg/enclave` is **not** hardware enclave integration. There
is no Apple Secure Enclave support and no Intel SGX support anywhere in this
repository. The package is a thin wrapper over
[memguard](https://github.com/awnumar/memguard) providing memory-protected
containers for key material in the process's own address space:

- `ProtectedKey` wraps a `memguard.Enclave`, keeping a key encrypted at rest
  in process memory and decrypting it only into a guarded buffer for use.
- `ProtectedBuffer` provides `mlock`'d, guard-page-protected buffers for
  temporary access.
- `WipeBytes` zeroes a plain byte slice.

For hardware-backed key protection (TPM 2.0, PKCS#11, cloud KMS), the barrier
delegates to an external encryptor — that is go-xkms's domain, not this
package's. See [Delegated (hardware) mode](#optional-context-aad-positional-binding).

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
passphrase := []byte("passphrase")
strategy, _ := seal.NewSoftwareStrategy(passphrase)
bcfg := seal.DefaultBarrierConfig(deploymentID)
bcfg.Algorithm = seal.AlgAES256GCM // pin it; the default reads the local CPU
bcfg.StatePersister = saveSealedBlob // REQUIRED for Rotate, for a purging
                                     // PurgeEpochsBefore, and for every Unseal
                                     // after a restart
barrier, err := seal.NewBarrier(bcfg) // returns an error for an invalid config;
                                      // seal.MustNewBarrier panics instead, for
                                      // compile-time-constant configs only
barrier.Initialize(ctx, strategy, seal.Credentials{Secret: passphrase})
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

After 5 consecutive failed `Unseal()` attempts (`bruteForceThreshold`), a doubling backoff gates the next attempt:

| Attempt | Backoff |
|---------|---------|
| 1-5 | None |
| 6 | 1s |
| 7 | 2s |
| 8 | 4s |
| 9 | 8s |
| 10 | 16s |
| 11 | 32s |
| 12+ | 60s (`bruteForceMaxDelay`) |

Failures at or above 20 (`bruteForceErrorThreshold`) are logged at ERROR rather than WARN.

The counter resets on a successful unseal. It is **persisted** in `SealedRootKey.FailedAttempts` and `LastFailedUnixNano`, so backoff survives a process restart; those two fields are the ones `StateMAC` deliberately does not cover, because they are written on a failed unseal where no root key is available to re-MAC with.

Backoff is measured against the wall clock, so a backward clock step could otherwise lock a node out permanently. `Unseal` treats a negative elapsed time as satisfying the delay and re-anchors `lastFailedAt` to now.

Three things deliberately do **not** count as a failed attempt: a `StateMAC` verification failure (an attacker with write access to the blob would otherwise hold a lockout primitive), a superseded barrier, and — in no-encryption mode — the empty root key that would otherwise trip the 32-byte size gate on every restart.

The primary defence is the KDF's cost, not the backoff. At the Argon2id defaults that is roughly 93 ms per attempt on fast 2025 desktop hardware.

## Memory Protection

**Source:** `pkg/seal/barrier.go`

The barrier uses [memguard](https://github.com/awnumar/memguard), via
`pkg/enclave`, for in-process memory protection of all key material.

### memguard Protections

| Protection | Mechanism |
|---|---|
| **Core dump prevention** | `MADV_DONTDUMP` on guarded pages — key material excluded from core dumps |
| **Encrypted at-rest memory** | `memguard.Enclave` encrypts root key in process memory; decrypted only into guarded buffers for use |
| **Guard pages** | Canary pages before/after each `LockedBuffer` detect buffer overflows/underflows |
| **Memory locking** | `mlock` prevents swapping key material to disk |
| **ptrace protection** | Process-level `PR_SET_DUMPABLE(0)` prevents debugger attachment on Linux |

### No signal handler is installed

This library installs **no signal handler**. An earlier `InitMemguard` called
`memguard.CatchSignal(SIGTERM, SIGINT)`; it had no callers and was removed,
because wiring it up would have been actively harmful in an embedded
consensus engine:

- `memguard.CatchSignal` calls `signal.Reset()` with **no arguments**, which
  detaches *every* signal handler in the process — including the embedding
  application's own, and including handlers this library knows nothing about.
- Its handler then calls `core.Exit(1)`. SIGTERM would stop being a graceful
  shutdown and become an immediate exit: no engine quiesce, no `Host.Close`,
  no barrier seal, no WAL flush.
- It would not have bought much. memguard already wipes `LockedBuffer`s on
  `Destroy`, and the enclave ciphertext it cannot wipe is useless without the
  session coffer key, which dies with the process either way.

Note also that **SIGKILL is uncatchable** — no handler, memguard's or anyone
else's, has ever been able to run on it. Any claim that signal-handler
cleanup covers SIGKILL was false even while the handler existed.

An embedder that wants signal-triggered wiping must install it in its own
`main`, alongside — not instead of — its graceful shutdown path.
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
| `ErrNonceReuse` | The nonce tracker has already recorded this nonce for the epoch |
| `ErrPlaintextTooLarge` | Frame exceeds the algorithm's maximum message size. Checked before allocating or sealing, because both GCM and ChaCha20-Poly1305 answer an over-long plaintext with a **panic** rather than an error |
| `ErrBarrierSuperseded` | Another loader claimed this sealed blob. Encrypt/Rotate/Purge refuse permanently; Decrypt is unaffected |
| `ErrSealedStateUnauthenticated` | Sealed blob has no `StateMAC`, or its MAC does not verify |
| `ErrSealedEpochRollback` | Blob's `Epoch` is below `BarrierConfig.MinEpoch` (or below the epoch already held) |
| `ErrTooManyKnownEpochs` | `KnownEpochs` exceeds `MaxKnownEpochs` (4096) |
| `ErrKnownEpochAboveCurrent` | `KnownEpochs` names an epoch above the blob's `Epoch` |
| `ErrInvalidSealedEpoch` / `ErrInvalidSealedSalt` / `ErrInvalidSealedEpochs` | Structural validation failures in `SetSealedRootKey` |
| `ErrNilSealedData` | Operation needs sealed state and there is none |
| `ErrHardwareMode` | `Rotate`, `PurgeEpochsBefore`, or a non-empty context on an encryptor without AAD capability |
| `ErrNoBarrierEncryptor` | A strategy that must supply an encryptor returned nil |
| `ErrContextAADUnsupported` | `RequireContextAAD` is set and the barrier cannot bind context |
| `ErrAADProbeRoundTrip` / `ErrAADProbeNotBound` | A delegated encryptor declaring `AADSymmetricEncrypter` failed the behavioural probe |
| `ErrUnsupportedAlgorithm` / `ErrAlgorithmNotFIPSApproved` / `ErrFIPSUnavailable` | `BarrierConfig.Validate` failures |
| `ErrKDFNotFIPSApproved` / `ErrKDFUnsupportedVersion` / `ErrKDFWeakParams` / `ErrKDFInvalidSalt` / `ErrKDFInvalidPassphrase` | KDF selection and work-factor failures |
| `ErrNoRootKeyAvailable` | `RootKey()` in no-encryption mode |
| `ErrInvocationBudgetUnknown` | **Logged, never returned.** The cause recorded when a restart cannot re-establish a provable budget |

### Typed Errors (with Unwrap)

| Error | Fields |
|-------|--------|
| `KeyDerivationError` | Epoch, Err |
| `StatePersistError` | Op ("rotate"/"purge"/"claim"), Epoch, Err |
| `SupersededError` | Observed, Claimed |
| `AlgorithmMismatchError` | Sealed, Configured |
| `AADBindingProbeError` | Stage, Err |
| `TrackerError` | (tracker operation failures) |
| `Error` | Strategy, Err |
| `UnsealError` | Strategy, Err |
| `CipherError` | Err |

## Transport Layer (in transit)

**Source:** `pkg/internal/transport/tls.go`, `pkg/internal/transport/quic.go`, `pkg/config/transportconfig.go`, `pkg/internal/transport/revocation/`

At-rest encryption is independent of transport encryption; this section is here so the two are not confused. Snapshots in particular travel as **plaintext over the QUIC session** and are re-encrypted at rest by the receiver under its own barrier.

### mTLS is not optional

There is exactly one TLS-config constructor, `buildMTLSTLS` (`pkg/internal/transport/tls.go`), and the QUIC transport refuses to start without an mTLS config (`ErrMTLSConfigRequired`, `pkg/internal/transport/quic.go`). Fixed, not configurable:

| Setting | Value |
|---|---|
| `MinVersion` | TLS 1.3, both directions |
| `ClientAuth` | `RequireAndVerifyClientCert` against `ClientCAs` |
| Server verification | `RootCAs` plus per-dial `ServerName`, so hostname verification against the cert SANs is active |
| ALPN | `NextProtos: ["quicraft"]` |
| Curve | `X25519` only |

`InsecureSkipVerify` appears nowhere in non-test `pkg/` code, and there is no TCP listener or dialer. `TransportConfig.TransportDisabled` (set by the `WithoutTransport()` host option) skips the mTLS validation, but it also prevents the QUIC transport being constructed at all — that is "no transport", not "QUIC without TLS".

Cipher suites are not restricted, because Go does not expose TLS 1.3 suite selection.

### Peer identity

A node identity is `"spki:" + hex(sha256(leaf.RawSubjectPublicKeyInfo))`, derived from the mTLS-verified leaf and never from the message body (`NodeIdentityFromCertificate` in `pkg/internal/transport/recv.go`). It is compared against the committed membership binding in `committedSenderBinding` (`pkg/messagehandler.go`). This is deliberately an SPKI pin rather than a SPIFFE URI SAN, because four downstream products issue these certificates (see the comment above `peerNodeIdentity` in `recv.go`).

Where no identity is committed for a sender, the fallback is a SAN check against the committed Raft address (`inboundAuthorizedFor` in `conn.go`). `TransportConfig.RequireMemberIdentity` (default `false`) drops such messages instead. A peer whose leaf also covers the local node's host is refused at connection setup (`refuseInboundPromiscuousCert` in `conn.go`).

### Certificate revocation

`TransportConfig.RevocationConfig`:

- CRL file hot-reload, detected by SHA-256 comparison of the file contents (`Checker.ReloadCRLs` in `pkg/internal/transport/revocation/checker.go`), polled every `CheckInterval` (default 60s).
- OCSP via `OCSPResponderURL`, with a response cache bounded at 4096 entries and a TTL of `OCSPCacheSeconds` (default 300).
- `Mode`: `"crl"`, `"ocsp"`, `"both"`, `"any"`. Default `"any"`.
- Unknown status fails **closed** by default, and that default is intrinsic rather than conferred by one caller: the internal `revocation.Config` carries `AllowSoftFail` with the same name and sense as the public one, so its zero value enforces and `Host` copies the operator's setting straight across. A checker built by any other path enforces too. Setting `AllowSoftFail: true` opts out.
- Under enforcement, a mode with no usable source returns `ErrNoRevocationSources` (or an OCSP-unavailable error) from `IsRevoked` rather than "not revoked", in every mode including `"crl"`. `Config.Validate` rejects such a configuration at startup; the runtime check is the backstop for a checker constructed without it.
- `OnCertRevoked` callback, invoked from `buildMTLSTLS`.

The check is installed as `VerifyConnection`, not `VerifyPeerCertificate`, so resumed sessions are checked too.

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

`StrategyNone` is the same situation with an extra sharp edge: the identity encrypter preserves the 8-byte epoch prefix so downstream readers stay well-formed, but nothing is encrypted and snapshot artifacts carry no integrity check at all. `RequireContextAAD` refuses a `StrategyNone` barrier outright rather than treating it as "nothing to bind".

### What is not guaranteed

Collected here so an integrator does not have to infer them from the sections above:

- **Purge is capability revocation, not crypto-shredding.** DEK confidentiality against memory disclosure after a purge is GC-bounded.
- **Positional AAD does not stop tail truncation, and no consumer compensates.** An at-rest attacker can drop trailing WAL records or snapshot frames and the survivors still authenticate. Closing this needs a frame count or a distinguishable terminal frame folded into the context; neither exists today.
- **Nonce tracking covers a prefix of each epoch**, not the whole of it. Past 4Mi nonces the tracker saturates.
- **The load-generation fence is not a lock.** Exclusive ownership of the sealed blob remains the embedder's responsibility.
- **`StateMAC` cannot detect replay** of an older authentic blob. That is `MinEpoch`'s job, and it must be configured.
- **Delegated (hardware) mode bypasses every barrier-side AEAD safeguard.** What you get is whatever the injected encryptor provides.
- **The barrier writes nothing to disk.** Durability, exclusivity, permissions and backup of the sealed blob belong to the embedder. See [Key custody](key-custody.md).

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

Several are worth alerting on rather than merely collecting:

| Level | Event | Why it matters |
|---|---|---|
| ERROR | `sealed root key was claimed by another loader` | Two processes on one data directory. The barrier has stopped encrypting. |
| ERROR | `AEAD invocation budget … is unrecoverable … encryption is refused` | No `StatePersister`; the node is read-only until the epoch rotates. |
| ERROR | `the known-epoch set is approaching the load-time bound` | `PurgeEpochsBefore` is not being called, or the node is crash-looping. The blob will eventually become unloadable. |
| ERROR | `delegated encryptor declares seal.AADSymmetricEncrypter but does not authenticate the AAD` | The probe caught a false capability claim. |
| WARN | `barrier sealing is delegated to an external encryptor` | Emitted once per setup, naming exactly what is bypassed. |
| WARN | `barrier performs NO encryption` | `StrategyNone`. |
| WARN | `no StatePersister, so the load-generation fence is inert` | |
| WARN | `sealed root key carries no metadata MAC` | `AllowUnauthenticatedSealedState` is still set. |

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
  → barrier.Encrypt(plaintext) → [Epoch:8 | algID:1 | Nonce:12 | Ciphertext | Tag:16]
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
      → barrier.EncryptWithEpochPinnedContext() → (ciphertext, epoch)
      → Nonce = fresh crypto/rand (random 96-bit)
      → AAD = [Epoch:8 LE] || NewSnapshotAAD(...)  = 47 bytes
              (magic || ShardID || ReplicaID || SnapshotIndex || ChunkSeq)
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
