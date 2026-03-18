# At-Rest Encryption

At-rest encryption in QuicRaft is built on a **per-node Barrier** that manages AEAD encryption with epoch-based key rotation. All encryption is opt-in. The algorithm is automatically selected based on hardware acceleration availability: AES-256-GCM when AES-NI is detected, ChaCha20-Poly1305 as a portable fallback.

**Key packages:**
- `pkg/crypto/` — AES-GCM and ChaCha20-Poly1305 AEAD with automatic hardware detection
- `pkg/seal/` — Barrier/seal/unseal pattern for at-rest key protection
- `pkg/crypto/shamir/` — Shamir's Secret Sharing for key management
- `pkg/enclave/` — Secure enclave integration

**Source:** `pkg/seal/barrier.go`, `pkg/seal/strategy.go`, `pkg/seal/errors.go`

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
| **Barrier** | Derives DEKs via HKDF-SHA256, encrypts/decrypts data with AEAD, manages epoch rotation, auto-detects optimal algorithm | Algorithm selection via `BarrierConfig` |
| **SealingStrategy** | Protects the root key at rest (wrapping/unwrapping) | Yes—any backend (software, HSM, TPM2, cloud KMS) |

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
    rootKey  *memguard.Enclave      // 32 bytes, encrypted at-rest in memory
    salt     []byte                 // 32 bytes, used for HKDF

    epoch      uint64
    currentDEK *memguard.LockedBuffer // derived via HKDF
    cipher     cipher.AEAD             // current epoch's AEAD cipher
    deks       map[uint64]*memguard.LockedBuffer // epoch → DEK for reading old data
    ciphers    map[uint64]cipher.AEAD

    knownEpochs    map[uint64]struct{}  // all epochs ever used
    sealed         *SealedRootKey       // persisted sealed root key
    failedAttempts int64                // brute-force protection
    lastFailedAt   time.Time
    inFlightMu     sync.Mutex           // separate lock for in-flight tracking
    inFlightEpochs map[uint64]int64     // epoch → refcount
}
```

The root key is stored in a `memguard.Enclave` (encrypted at-rest in process memory). DEKs are stored in `memguard.LockedBuffer` instances (mlock'd, guard pages, canary values). On `Seal()`, all key material is zeroed.

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

Each epoch produces a cryptographically independent DEK using HKDF-SHA256:

```go
func deriveDEK(rootKey, salt []byte, deploymentID, epoch uint64) ([]byte, error) {
    var info [36]byte
    copy(info[:20], "quicraft/barrier/v1/")              // domain separation
    binary.BigEndian.PutUint64(info[20:], deploymentID)  // deployment isolation
    binary.BigEndian.PutUint64(info[28:], epoch)         // epoch
    hkdfReader := hkdf.New(sha256.New, rootKey, salt, info[:])
    dek := make([]byte, 32) // AES-256
    _, err := io.ReadFull(hkdfReader, dek)
    return dek, err
}
```

**Key properties:**

- **Deterministic:** Same root key + salt + deploymentID + epoch always produces the same DEK. DEKs can be re-derived on demand after `Unseal()`.
- **Independent:** Each epoch's info string differs, so DEKs are cryptographically independent.
- **Deployment isolation:** The `deploymentID` ensures different deployments produce entirely different DEKs.
- **Salt purpose:** 32-byte random salt provides additional node-level separation.

## Epoch-Based Key Rotation

Epoch rotation changes encryption keys without re-encrypting existing data and without downtime.

### How It Works

1. **Single root key** — generated once at `Initialize()`, never changes
2. **Epoch counter** — starts at 1, incremented on each `Rotate()` call
3. **DEK per epoch** — derived deterministically: `DEK(epoch) = HKDF(rootKey, salt, "quicraft/barrier/v1/" + deploymentID + epoch)`
4. **New writes use current epoch** — `Encrypt()` always uses the current DEK
5. **Old reads use historical DEKs** — `Decrypt()` reads the epoch from the ciphertext header, looks up the corresponding DEK
6. **No re-encryption** — old data stays encrypted with its original epoch's DEK until compaction deletes it

### Rotate()

```go
func (b *Barrier) Rotate() (uint64, error) {
    b.mu.Lock()
    defer b.mu.Unlock()

    newEpoch := b.epoch + 1
    dek, aead, err := deriveEpochCipher(b.rootKey, b.salt, newEpoch)
    if err != nil {
        return 0, err
    }

    b.epoch = newEpoch
    b.currentDEK = dek
    b.cipher = aead
    b.deks[newEpoch] = dek
    b.ciphers[newEpoch] = aead
    b.knownEpochs[newEpoch] = struct{}{}
    return newEpoch, nil
}
```

The write lock blocks concurrent `Encrypt`/`Decrypt` calls during rotation. Since rotation is fast (~1-2ms for HKDF + map update), this pause is negligible.

`Rotate()` updates `b.sealed.Epoch` and rebuilds `b.sealed.KnownEpochs` from the full `b.knownEpochs` map. On restart, `SetSealedRootKey` restores the known epochs and `Unseal()` re-derives DEKs for all of them.

### Ciphertext Format

Every encrypted output carries its epoch:

```
[Epoch:8 | Nonce:12 | EncryptedData | Tag:16]
```

The 8-byte epoch prefix is **unencrypted** — it tells the decryptor which DEK to use.

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

The barrier generates nonces for AES-GCM using `crypto/rand.Reader`. Each `Encrypt` call reads 12 bytes of cryptographic randomness for the nonce.

If `crypto/rand.Read` fails (entropy exhaustion), encryption returns an error rather than falling back to a weaker source. This fail-closed behavior prevents encrypting with a predictable nonce.

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

The WAL layer provides its own nonce construction separate from the barrier's random nonces:

```
[SegmentID:4 | WriteEpoch:4 | RecordSeq:4]
```

| Field | Size | Purpose | Overflow bound |
|-------|------|---------|---------------|
| `SegmentID` | 4 bytes | WAL segment file number (monotonic) | ~4B segments = ~256 EB at 64MB/seg |
| `WriteEpoch` | 4 bytes | Per-segment open counter | ~4B crash-recovery cycles per segment |
| `RecordSeq` | 4 bytes | Record sequence within (SegmentID, WriteEpoch) pair | ~4B records per segment-epoch |

### Crash-Recovery Nonce Safety

The critical invariant: **a (DEK, nonce) pair is never reused.**

Crash recovery threatens this because a recovered segment retains its `SegmentID` and in-memory `RecordSeq` resets. Without `WriteEpoch`, post-recovery writes could produce the same nonce as pre-crash writes, breaking AES-GCM.

Recovery sequence:
1. Open segment file, read `WriteEpoch` from header
2. Scan forward to find last valid record → determine recovered `RecordSeq`
3. Increment `WriteEpoch` in header, fsync header
4. Resume appending from `RecordSeq + 1` with new `WriteEpoch`

**WriteEpoch crash atomicity:** The segment header stores `WriteEpoch` at two independent offsets (0 and 512), each with its own CRC32. These reside in different 512-byte disk sectors. Under single-sector-torn-write assumptions, at most one copy can be corrupted. Recovery uses `max(primary, backup)`.

### Key Rotation Safety

When `Rotate()` creates a new epoch, the new DEK has never been used with any nonce. Even if `RecordSeq` values repeat across epochs, the (DEK, nonce) pair is unique.

## Snapshot Encryption

Snapshots are encrypted as a stream of 64KB chunks:

```
Chunk: [Epoch:8 | ChunkSeq:4 | Nonce:12 | Ciphertext:N | Tag:16]
```

### Nonce Construction (12 bytes)

```
[AttemptSalt:8 | ChunkSeq:4]
```

- `AttemptSalt`: 8 bytes of `crypto/rand` randomness per snapshot encryption attempt. Birthday bound: ~2^32 (~4 billion) attempts before >1% collision risk.
- `ChunkSeq`: monotonic chunk counter within attempt. 4 bytes = 4 billion chunks = ~256 TB at 64KB/chunk.

### AAD (Additional Authenticated Data)

```
[Epoch:8 | SnapshotIndex:8 | ChunkSeq:4]
```

AAD binds each chunk to its epoch and snapshot context, preventing:
- **Chunk reordering** — wrong `ChunkSeq` causes auth failure
- **Cross-snapshot substitution** — wrong `SnapshotIndex` causes auth failure
- **Truncation attacks** — receiver verifies `TotalChunkCount` agreement across all chunks

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

Snapshots are **NOT encrypted at the application layer during transport**. The flow:

```
Leader:
  1. State machine writes plaintext snapshot to disk
  2. Barrier encrypts chunks to at-rest storage (local)
  3. Leader reads snapshot, sends chunks via QUIC (mTLS encrypted in transit)

Follower:
  1. Receives plaintext chunks over QUIC (mTLS decrypted by QUIC)
  2. Writes to local storage
  3. Barrier encrypts at-rest with follower's own DEK
```

Each node has its **own independent barrier**. A leader's at-rest encrypted snapshot is decrypted for transport, sent over mTLS-protected QUIC, and the follower re-encrypts with its own key material. Nodes cannot decrypt each other's at-rest data.

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
func (b *Barrier) PurgeEpochsBefore(minEpoch uint64) error {
    b.mu.Lock()
    defer b.mu.Unlock()

    b.inFlightMu.Lock()
    defer b.inFlightMu.Unlock()

    for epoch, dek := range b.deks {
        if epoch >= minEpoch || epoch == b.epoch { continue } // keep
        if b.inFlightEpochs[epoch] > 0 { continue }          // in-flight
        zeroBytes(dek)     // explicit zeroing
        delete(b.deks, epoch)
        delete(b.ciphers, epoch)
        delete(b.knownEpochs, epoch)
    }
    return nil
}
```

**Safety invariants:**
- Current epoch is **never** purged
- In-flight epochs (refcount > 0) are **never** purged

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
7. Old DEKs zeroed and removed from memory
```

**No re-encryption during compaction.** Old segments encrypted with epoch N are deleted, not re-encrypted with the current epoch. This is a deliberate design choice:
- Zero-downtime rotation (no need to halt and re-encrypt)
- No I/O amplification from re-encryption
- Compaction naturally removes old data over time

## Sealing Strategies

**Source:** `pkg/seal/strategy.go`

### Interface

```go
type SealingStrategy interface {
    Seal(rootKey []byte) (*SealedRootKey, error)
    Unseal(sealed *SealedRootKey) ([]byte, error)
    Close() error  // release resources (PKCS#11 sessions, TPM2 handles, KMS connections)
}

type SealedRootKey struct {
    Strategy    string    // "software", "pkcs11", "tpm2", "cloudkms"
    Epoch       uint64    // key rotation epoch at time of sealing
    Salt        []byte    // 32-byte HKDF salt
    Ciphertext  []byte    // encrypted root key
    Metadata    []byte    // strategy-specific (key ID, KMS ARN, etc.)
    KnownEpochs []uint64  // all epoch numbers for DEK re-derivation
}
```

The `SealingStrategy` ONLY protects the root key. It does not participate in data encryption. Any backend that can wrap/unwrap 32 bytes can serve as a sealing strategy.

### KnownEpochs Persistence Across Restart

The `KnownEpochs` field on `SealedRootKey` is the mechanism that preserves epoch history across seal/unseal cycles and node restarts. Without it, `Unseal` would only derive the DEK for the current epoch, and data encrypted with prior epochs would be unreadable.

**Persistence flow:**

1. `Initialize()` sets `sealed.KnownEpochs = []uint64{1}` (the initial epoch).
2. Each `Rotate()` rebuilds `sealed.KnownEpochs` from the full `b.knownEpochs` map.
3. The caller persists the `SealedRootKey` to stable storage.
4. On restart, the caller loads the persisted `SealedRootKey` and calls `SetSealedRootKey()`.
5. `SetSealedRootKey()` validates the sealed data (epoch > 0, salt length, KnownEpochs contains current epoch), restores the in-memory `knownEpochs` map.
6. `Unseal()` iterates `b.knownEpochs` and re-derives a DEK + cipher for every historical epoch.

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

## Shamir's Secret Sharing

**Source:** `pkg/crypto/shamir/`

For scenarios requiring multiple key holders to reconstruct the barrier:

- Split master key into N shares, reconstruct from K shares (K-of-N threshold)
- Used for sealing/unsealing the barrier with multiple key holders
- Compatible with any `SealingStrategy`

## Enclave Integration

**Source:** `pkg/enclave/`

Secure enclave integration for hardware-backed key protection on supported platforms (e.g., Apple Secure Enclave, Intel SGX).

## Host Integration

**Source:** `pkg/host.go`

```go
// Minimal interface to avoid circular imports
type Barrier interface {
    IsSealed() bool
    Seal() error
}

func WithBarrier(b Barrier) HostOption
```

### Usage

```go
// No encryption (default):
host, _ := quicraft.NewHost(cfg)

// Software-sealed:
strategy, _ := seal.NewSoftwareStrategy([]byte("passphrase"))
barrier := seal.NewBarrier()
barrier.Initialize(strategy)
host, _ := quicraft.NewHost(cfg, quicraft.WithBarrier(barrier))

// HSM-sealed (custom strategy):
strategy := myHSMStrategy(hsmConfig) // implements seal.SealingStrategy
barrier := seal.NewBarrier()
barrier.Initialize(strategy)
host, _ := quicraft.NewHost(cfg, quicraft.WithBarrier(barrier))
```

### Lifecycle

1. `NewHost()` receives barrier via `WithBarrier` option
2. Host checks initialization state (first boot check)
3. If not initialized: `barrier.Initialize(strategy)` generates root key
4. `barrier.Unseal(strategy)` decrypts root key and derives DEKs
5. Barrier passed to WAL and snapshot writers
6. On `Host.Close()`: pipeline drained, `barrier.Seal()` zeros all keys

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

- **Root key** (`memguard.Enclave`): Encrypted at-rest in process memory. Decrypted into a temporary `LockedBuffer` only when needed (e.g., DEK derivation), then immediately re-sealed.
- **DEKs** (`memguard.LockedBuffer`): Each epoch's DEK lives in its own locked, guard-paged buffer. On `Seal()` or `PurgeEpochsBefore()`, buffers are destroyed via `LockedBuffer.Destroy()`.
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

### Typed Errors (with Unwrap)

| Error | Fields |
|-------|--------|
| `KeyDerivationError` | Epoch, Err |
| `Error` | Strategy, Err |
| `UnsealError` | Strategy, Err |
| `CipherError` | Err |

## Certificate Revocation (Transport Layer)

**Source:** `pkg/transport/`

- RevocationConfig on TransportConfig
- CRL file hot-reload (SHA-256 hash comparison)
- OCSP responder support with caching
- Modes: "crl", "ocsp", "both", "any"
- EnforceRevocation for hard-fail on unknown status
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
slog.Info("key rotated", "from_epoch", oldEpoch, "to_epoch", newEpoch)
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
      → Nonce = [AttemptSalt:8 | ChunkSeq:4]
      → AAD = [Epoch:8 | SnapshotIndex:8 | ChunkSeq:4]
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
