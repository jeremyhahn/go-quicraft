# Key Custody

The README shows the sealed root key being read back from a file:

```go
blob, err := os.ReadFile("/var/data/quicraft/sealed-root-key.json")
```

That is the smallest thing that works, not a requirement. QuicRaft's barrier writes nothing to disk and knows nothing about files. Two independent decisions are yours, and both have seams in `pkg/seal`:

| Decision | Seam | Default if you do nothing |
|---|---|---|
| Where the sealed blob is stored | `BarrierConfig.StatePersister`, plus `seal.StorageBackend` for a KV-shaped store | Nothing is stored. The barrier holds state in memory and the embedder must persist `SealedRootKeyData()` itself. |
| What protects the root key inside that blob | `seal.SealingStrategy` | Nothing built in is selected for you. QuicRaft ships a passphrase strategy and a no-encryption strategy; you pick one explicitly. |

They compose freely. A TPM-wrapped root key in a Raft-backed store, a passphrase-wrapped key in a file, an HSM-wrapped key in a KMS: all four combinations are the same code path.

This page is for someone embedding go-quicraft as a library. It covers the seams, the contracts you have to honour, and the one mistake that is worse than not using the seam at all.

**Related:** [At-rest encryption](encryption.md) for what the barrier does with the key once it has it.

## Axis 1: where the sealed blob is stored

### StatePersister

```go
// pkg/seal/barrier_config.go
type StatePersister func(*SealedRootKey) error

// pkg/seal/barrier_config.go
type BarrierConfig struct {
    // ...
    StatePersister StatePersister
}
```

The barrier calls this whenever state that must survive a restart has changed, and waits for it. Anything you can write to from a Go function can hold the blob: a file, a Raft-backed KV store, a database row, a KMS entry, an etcd key.

**Contract**, in the order it matters:

1. **Returning nil MUST mean the blob is durable** — readable back verbatim after a crash. The barrier activates the new epoch on a nil return, so a premature nil re-opens the data-loss window the hook exists to close. In practice: write to a temp file, fsync the file, rename, fsync the directory. Or use a store whose write is already synchronous.
2. **The argument is a private deep copy.** You may retain or mutate it.
3. **The persister runs with the barrier's write lock held.** It MUST NOT call back into the same `Barrier` — `Encrypt`, `Rotate`, `SealedRootKeyData`, any of them — or it deadlocks. If your storage layer is itself backed by this barrier, break the cycle before you get here.
4. **Returning an error aborts the operation** with the previous epoch intact and usable.

### When it is called

| Operation | Calls the persister | Fails without one |
|---|---|---|
| `Initialize` | No | No. You persist `SealedRootKeyData()` yourself at bootstrap. |
| `Rotate` | Yes, before activating the new epoch | Yes, `ErrNoStatePersister` |
| `PurgeEpochsBefore` | Yes, before dropping any key — but only when there is something to purge | Yes, when something would be purged |
| `Unseal` | Yes, twice: once to claim the blob (`LoadGeneration+1`), and again if it rotates | Degrades, see below |

That third row is the one people miss. `Unseal` writes.

### Leaving it nil costs more than rotation

The consequences compound:

- `Rotate` fails closed with `ErrNoStatePersister` on every call.
- The load-generation fence is inert. A claim that cannot be written down is not a claim, so a second process loading the same blob is undetectable. Logged at WARN from `Barrier.claimLoadGenerationLocked`.
- **The first `Unseal` after a restart leaves the barrier read-only.** The default invocation counter is in-memory, so after loading a persisted blob the invocations already spent under that epoch are not merely unknown but unknowable. The barrier will not encrypt against a budget nobody can prove. It would rotate to a fresh epoch whose count is genuinely zero, but that needs a persister, so instead it seeds the counter to the full limit and every `Encrypt` returns `ErrInvocationLimitExceeded`. `Decrypt` keeps working: the node can serve its data, it just cannot add to it.

Why so strict? With random 96-bit nonces, the 2^32 invocations per DEK is a hard nonce-collision bound, not advice. The old behaviour re-granted 5% headroom on every restart against the same DEK, so about twenty restarts of a never-rotating deployment pushed it past the limit. A GCM nonce collision there yields keystream XOR and recovery of the GHASH authentication key, which is arbitrary forgery of at-rest records.

**The README example has this defect.** It persists the blob with `os.WriteFile` at bootstrap and never sets `BarrierConfig.StatePersister`, so that barrier cannot rotate and its load-generation fence never arms. Wire the persister. The [example below](#a-minimal-correct-setup) shows the shape.

### StorageBackend

```go
// pkg/seal/interfaces.go
type StorageBackend interface {
    Get(ctx context.Context, key string) ([]byte, error)
    Put(ctx context.Context, key string, data []byte) error
    Delete(ctx context.Context, key string) error
    List(ctx context.Context, prefix string) ([]string, error)
    Exists(ctx context.Context, key string) (bool, error)
    Scan(ctx context.Context, prefix string, fn func(key string, value []byte) error) error
    PutIfAbsent(ctx context.Context, key string, data []byte) (bool, error)
    Close() error
}
```

A context-aware KV interface, satisfied by go-qrdb's `storage.Storage` through duck typing so neither package has to import the other. It is not something the barrier calls on its own; it is the shape a higher layer's store already has, so an embedder that has one can adapt it to a `StatePersister` in a few lines.

`PutIfAbsent` is worth noting because it is the primitive that turns the load-generation fence into something closer to a lock. It returns `stored=true` only when this call created the key; a non-nil error means the outcome is unknown and you must not assume either way. Atomicity is per consistency domain: single-process backends serialize internally, distributed backends must use their native compare-and-set. A persister built on a compare-and-swap that rejects a write whose expected prior generation no longer matches is the hard version of exclusivity the fence only approximates.

### Exclusivity is still yours

Only one live barrier may write a given sealed blob. This is a cryptographic requirement, not an operational preference: every DEK input (root key, salt, deployment ID, epoch) is a function of the blob, and `Unseal`'s forced rotation moves to `epoch+1`, so two barriers loaded from one blob derive the *same* DEK, each with a zeroed invocation counter and an empty nonce tracker.

The load-generation fence makes that state detectable. Each `Unseal` writes back `LoadGeneration+1` before the barrier will encrypt, and a barrier later handed a blob naming a higher generation marks itself superseded permanently and refuses to encrypt, rotate or purge (`ErrBarrierSuperseded`); decryption is unaffected.

It is not a lock. The generation is compared at load and enforced at write or use — the barrier never re-reads the blob on its own, because that would put a disk read on the encrypt hot path. So a barrier superseded while running keeps encrypting until you hand it the newer state, and two loaders that both read the same generation before either writes both claim the same number and neither can tell. What the fence buys is the common case: a second process starting on a data directory already in use turns from silent DEK sharing into a loud, attributable failure.

For a hard guarantee, enforce exclusivity outside: an exclusive data-directory lock, or a compare-and-swap persister as above.

## Axis 2: what protects the root key

```go
// pkg/seal/strategy.go
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

Every method takes a `context.Context` where it can block on an external device, and `Credentials` by value. `Credentials.Secret` is a `[]byte` rather than a `string` specifically so you can wipe it after use; a Go string is immutable and stays resident until GC.

`UnsealRootKey` returns a slice the caller owns and must zero. `Close` must be idempotent.

A TPM 2.0 or PKCS#11 strategy wraps the root key with a key that never leaves the device. The persisted blob is then inert on its own: an attacker who copies the file, the database row, or the whole disk gets ciphertext they cannot unwrap without the hardware. That is the property you are buying, and it holds regardless of where axis 1 put the blob.

`Available()` lets a strategy report that it cannot run here — a TPM2 strategy returns false when no TPM device is accessible — which is useful for startup diagnostics. It is not an auto-selection mechanism. There is no preference order and no ranking anywhere in `pkg/seal`; the strategy is chosen explicitly at barrier or tenant setup.

## Delegated (hardware) mode

`BarrierEncryptor` is the third decision, and it is the one with real consequences.

| `BarrierEncryptor` returns | What the hardware protects | The barrier's own data path |
|---|---|---|
| `(nil, nil)` | The **root key only**. The barrier derives epoch DEKs from it via HKDF and encrypts with its own AEAD. | Fully active. |
| a non-nil `SymmetricEncrypter` | The **entire data path**. The root key need never enter process memory. | Entirely bypassed. |

Delegation is total. When the barrier holds a non-nil encryptor, every `Encrypt` and `Decrypt` path returns early into it, and these are skipped along with the cipher:

- epoch DEK derivation and the `[epoch:8][algID:1]` ciphertext header
- the nonce-reuse tracker
- the NIST SP 800-38D invocation counter
- `Rotate` and `PurgeEpochsBefore`, which return `ErrHardwareMode`
- the load-generation fence, which has no epoch DEK to protect

None of that is an oversight. The delegated encryptor owns its key and nonce management, and the barrier cannot count invocations against a key it does not control. But it means the safety properties documented in [At-rest encryption](encryption.md) stop describing your deployment and are replaced by whatever the injected encryptor provides. Every barrier setup logs this at WARN, naming exactly what is delegated.

**The honest tradeoff.** You gain: the root key never enters process memory, so a core dump, a live process read, or a `kill -9` never exposes it. You give up: epoch rotation, epoch purging, per-key invocation bounding, nonce-reuse detection, and — unless the encryptor opts in below — positional AAD binding. You also take a round trip to the device or service on every frame, which for at-rest data on a hot path is usually the decisive cost.

**As of this writing, no shipped hardware strategy uses this mode.** Every production caller of go-xkms's `NewTPM2Strategy`, `NewPKCS11Strategy` and `NewCloudKMSStrategy` passes a nil encryptor (`go-xkms/pkg/server/seal_strategies.go`, `go-xkms/pkg/seal/autodetect.go`), so hardware deployments today wrap the root key in the TPM or HSM and keep the barrier's full AEAD stack, positional AAD included. The delegated path exists and is exercised by tests. Choosing it is a deliberate act.

## The AAD capability contract

This is the thing an integrator is most likely to get wrong, and the failure is silent.

`SymmetricEncrypter` carries no AAD:

```go
// pkg/seal/interfaces.go
type SymmetricEncrypter interface {
    Encrypt(plaintext []byte) ([]byte, error)
    Decrypt(ciphertext []byte) ([]byte, error)
}
```

`AADSymmetricEncrypter` is the **optional** capability an encryptor implements to **declare** that it authenticates caller-supplied AAD end to end:

```go
// pkg/seal/interfaces.go
type AADSymmetricEncrypter interface {
    SymmetricEncrypter
    EncryptWithAAD(plaintext, aad []byte) ([]byte, error)
    DecryptWithAAD(ciphertext, aad []byte) ([]byte, error)
}
```

Why it matters: a barrier in delegated mode bypasses its own AEAD, so the positional binding the software path folds into the AAD — a snapshot frame's shard, replica, index and ordinal; a WAL record's shard, segment and ordinal — exists only if the delegated encryptor carries it. Implementing this interface is that promise. The `aad` bytes passed to `EncryptWithAAD` must be authenticated such that `DecryptWithAAD` fails when a different `aad` is supplied at decrypt time.

**Do not implement it on an encryptor that merely accepts an `aad` argument and drops it.** The doc comment on `AADSymmetricEncrypter` in `pkg/seal/interfaces.go` is emphatic about this, and the reason is worth restating: advertising a binding that does not exist is strictly worse than not advertising one.

Walk the failure through:

1. The barrier type-asserts the encryptor and reports `SupportsContextAAD() == true`.
2. Callers stop suppressing their positional context and start passing it.
3. `BarrierConfig.RequireContextAAD` passes at startup, so the deployment's own guard rail confirms the property.
4. The context is discarded on every write.
5. The deployment believes it has splice and reorder protection it does not have, and nothing can retroactively bind ciphertext that is already at rest.

An encryptor that cannot bind AAD must implement **only** `SymmetricEncrypter`. The barrier then refuses a non-empty context with `ErrHardwareMode` — loud, at the write, never silently dropped — and `RequireContextAAD` refuses the deployment at setup.

**The declaration is measured, not trusted.** A Go method set is a claim, and these implementations live in other repositories. At `Initialize` and `Unseal` only, never on the data path, `verifyAADBinding` (`verifyAADBinding` in `pkg/seal/barrier_binding.go`) runs one encrypt and two decrypts against the encryptor with random 32-byte values: seal a probe under `aadA`, require it to open under `aadA` returning the same bytes, and require it to **fail** under an independently drawn `aadB`. An encryptor that accepts the argument and drops it passes the first two checks and fails the third. Both AAD values are random so a known probe cannot be special-cased, and `aadB` is an independent draw rather than a mutation of `aadA` so a length- or prefix-sensitive implementation cannot pass by accident.

A failing probe is logged at ERROR and the encryptor is downgraded to `ContextBindingUnsupported`, so `RequireContextAAD` then refuses the barrier rather than blessing it. The probe is a backstop, not a licence to implement the interface speculatively.

## Other injection points

All on `BarrierConfig`.

### AEADProvider

```go
type AEADProvider interface {
    NewAEAD(alg SymmetricAlgorithm, key []byte) (cipher.AEAD, error)
}
```

Replaces the `cipher.AEAD` factory. Nil uses `SoftwareAEADProvider`, which dispatches to the Go standard library. Injected code runs on the plaintext root key during derivation, which is why `rotateLocked` defers the destruction of the root-key buffer rather than destroying it inline: a panic in third-party provider code that some caller recovers would otherwise strand an mlock'd buffer holding the root key in the clear for the life of the process.

### NonceTrackerProvider

Replaces the nonce-reuse tracker. Nil uses `InMemoryNonceTracker`, which keys on the full nonce verbatim and saturates at `DefaultMaxTrackedNonces` (4Mi, roughly 160 MB) rather than growing without bound. A saturated tracker still rejects repeats of what it retained, and reports the state honestly through `Saturated()` and `Count()`. Detection therefore covers the first 0.0977% of an epoch's 2^32 permitted invocations; the rest rests on random 96-bit nonces plus the invocation cap. Inject a persistent or hardware-backed tracker if you need more.

### InvocationCounter

Replaces the NIST SP 800-38D counter. Nil uses an in-memory one, and that choice is what forces a rotation on every post-restart `Unseal`.

**An injected counter owns the persistence of per-epoch invocation counts across process restarts.** Seed it from durable state before `Unseal`. Do this and the forced rotation goes away, but you have taken on the correctness of that count: seeding it too low is exactly as unsound as not counting at all.

### MinEpoch

A floor on the epoch a persisted `SealedRootKey` may name. `SetSealedRootKey` rejects a lower one with `ErrSealedEpochRollback`.

The blob's metadata is authenticated by `StateMAC`, but authentication cannot detect **replay**: an attacker with write access can restore an older, genuinely-MACed copy, resurrecting a retired epoch's DEK together with a fresh invocation counter and an empty nonce tracker. Only state the attacker cannot roll back in the same step catches that, and the barrier holds none — you do.

Set it to the highest epoch any live WAL segment or retained snapshot still references. The engine already computes exactly this as its global safe-purge floor. Zero disables the check.

`MinEpoch` and the load-generation fence are complementary and neither substitutes for the other: the fence detects a competing claim, this floor refuses an older state.

### RequireContextAAD

Declares that this deployment requires positional binding of at-rest ciphertext. When true, `Initialize` and `Unseal` fail closed with `ErrContextAADUnsupported` if the constructed barrier cannot provide it. The check runs at setup, not at the first encrypt, so a misconfigured deployment is refused before it writes a single unbound byte.

| Mode | Result |
|---|---|
| Software (strategy supplies no `BarrierEncryptor`) | Satisfied |
| Delegated, encryptor implements `AADSymmetricEncrypter` and passes the probe | Satisfied |
| Delegated, encryptor implements only `SymmetricEncrypter` | Refused |
| `StrategyNone` | Refused |

`StrategyNone` is refused rather than treated as "nothing to bind": there is no AEAD and no tag, so requiring authenticated positional binding while selecting no encryption is a misconfiguration, not a satisfiable request.

Default `false` preserves the historical behaviour, where a barrier that cannot bind context is constructed normally and callers suppress their context.

### Also worth setting

Not custody, but adjacent and easy to omit. `RequireFIPS` turns a missing FIPS module from a silent downgrade into a refusal at `NewBarrier`. `Algorithm` should be pinned explicitly, because the default reads the local CPU and the algorithm is baked into key derivation. `AllowUnauthenticatedSealedState` should be cleared after the one-start migration it exists for. See [At-rest encryption](encryption.md) for each.

## A minimal correct setup

Compiles against go-quicraft 0.5.1-alpha. Error handling is real; the fsync sequence is the point.

```go
package main

import (
	"context"
	"encoding/json"
	"errors"
	"log"
	"os"
	"path/filepath"

	"github.com/jeremyhahn/go-quicraft/pkg/seal"
)

const blobPath = "/var/data/quicraft/sealed-root-key.json"

// persistSealedState writes the sealed root key durably: temp file, fsync the
// file, rename, fsync the directory. Returning nil promises the barrier that a
// crash on the next instruction still leaves this blob readable.
func persistSealedState(s *seal.SealedRootKey) error {
	blob, err := json.Marshal(s)
	if err != nil {
		return err
	}
	dir := filepath.Dir(blobPath)
	tmp, err := os.CreateTemp(dir, ".sealed-root-key-*.tmp")
	if err != nil {
		return err
	}
	defer os.Remove(tmp.Name())
	if _, err := tmp.Write(blob); err != nil {
		tmp.Close()
		return err
	}
	if err := tmp.Chmod(0o600); err != nil {
		tmp.Close()
		return err
	}
	if err := tmp.Sync(); err != nil {
		tmp.Close()
		return err
	}
	if err := tmp.Close(); err != nil {
		return err
	}
	if err := os.Rename(tmp.Name(), blobPath); err != nil {
		return err
	}
	d, err := os.Open(dir)
	if err != nil {
		return err
	}
	defer d.Close()
	return d.Sync()
}

func main() {
	ctx := context.Background()

	cfg := seal.DefaultBarrierConfig(1)
	cfg.Algorithm = seal.AlgAES256GCM // pin it; do not inherit the local CPU's choice
	cfg.StatePersister = persistSealedState
	cfg.RequireContextAAD = true

	barrier, err := seal.NewBarrier(cfg)
	if err != nil {
		log.Fatal(err)
	}

	passphrase := []byte(os.Getenv("QUICRAFT_PASSPHRASE"))
	strategy, err := seal.NewSoftwareStrategy(passphrase)
	if err != nil {
		log.Fatal(err)
	}
	defer strategy.Close()
	creds := seal.Credentials{Secret: passphrase}

	blob, err := os.ReadFile(blobPath)
	switch {
	case errors.Is(err, os.ErrNotExist):
		// First boot: create the root key and persist the blob yourself.
		// Initialize does not call StatePersister.
		if err := barrier.Initialize(ctx, strategy, creds); err != nil {
			log.Fatal(err)
		}
		if err := persistSealedState(barrier.SealedRootKeyData()); err != nil {
			log.Fatal(err)
		}
	case err != nil:
		log.Fatal(err)
	default:
		sealed := new(seal.SealedRootKey)
		if err := json.Unmarshal(blob, sealed); err != nil {
			log.Fatal(err)
		}
		// MinEpoch: the highest epoch any live WAL segment or retained
		// snapshot still references. Refuses a replayed older blob.
		// cfg.MinEpoch = engineMinLiveEpoch
		if err := barrier.SetSealedRootKey(sealed); err != nil {
			log.Fatal(err)
		}
		if err := barrier.Unseal(ctx, strategy, creds); err != nil {
			log.Fatal(err)
		}
	}

	if !barrier.SupportsContextAAD() {
		log.Fatal("barrier cannot authenticate positional context")
	}

	// host, err := quicraft.NewHost(hostCfg, quicraft.WithBarrier(barrier))
	_ = barrier
}
```

Two details that are easy to skip:

- `MinEpoch` is set on the config *before* `NewBarrier`, so a real deployment reads its minimum live epoch from the engine and constructs the barrier afterwards. It is commented out above only because there is no engine yet at that point in a self-contained example.
- The passphrase is a `[]byte` you can wipe. Reading it from an environment variable, as above, defeats that (the value is already an immutable string in the process). Read it from a pipe, a prompt, or an agent socket if you care about the distinction.

To move the blob somewhere other than a file, replace the body of `persistSealedState` and the `os.ReadFile` call. Nothing else changes.

## What go-quicraft does not ship

Being explicit, so you know where to look:

- **No TPM or HSM sealing strategy.** `pkg/seal` names only two `StrategyID` values, `StrategyPassphrase` (`"passphrase"`) and `StrategyNone` (`"none"`), and implements only those. The `StrategyID` *type* is defined here; the values `tpm2`, `pkcs11`, `awskms`, `gcpkms`, `azurekv` and `vault` are declared and implemented by **go-xkms** (`go-xkms/pkg/seal/strategy.go`).
- **No Shamir strategy, and no Shamir shares on disk.** `pkg/crypto/shamir` is the pure GF(256) `Split`/`Combine` primitive, and `pkg/seal/shamir.go` holds the `ShareSplitter` and `ShareAccumulator` interfaces plus their value types. The `shamir` strategy, share custody, M-of-N quorum, recovery, rekey, root tokens and the Security-Officer CSR ceremony are **go-qrdb**'s (`go-qrdb/pkg/seal` strategies and `go-qrdb/pkg/ceremony`). go-qrdb also owns the `StrategyID` namespace and the barrier registry. The consensus engine never persists a Shamir share.
- **No strategy catalog, no preference order, no auto-ranking.** Selection is explicit at barrier or tenant setup.

If you are combining these projects, `shamir.Combine` deserves one warning of its own: it performs **no integrity check**. Shares carry no digest by design, because one would be an offline brute-force oracle against a low-entropy secret. With fewer than threshold genuine shares, or a corrupted or mismatched share, reconstruction yields indistinguishable garbage rather than an error, and `Threshold` and `Total` on a `Share` are unauthenticated caller-supplied metadata. Authenticate the reconstructed secret out of band — use it as the key in an authenticated decrypt that fails on a wrong key, or check it against an independent commitment. `IntegrityError` still exists in the package but is deprecated and never returned; do not write code that waits for it.

Share values themselves now have a protected-memory path. `Share.Value` is an un-wipeable Go string and is deprecated, along with `Split`, `Combine` and `CombineUnverified`; `ProtectedShare` holds the value in an `enclave.MemoryEnclave` that `Destroy` wipes, built by `SplitProtected`, `NewProtectedShare` or `(*Share).Protect` and reconstructed by `CombineProtected`. The JSON encoding is identical to `Share`'s, so custody code can migrate a call site at a time — but every `ProtectedShare`, including one decoded from JSON, must be destroyed, and each is one `RLIMIT_MEMLOCK` allocation. See [At-rest encryption](encryption.md#share-values-in-protected-memory).
