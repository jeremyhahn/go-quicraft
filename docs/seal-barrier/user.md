# Seal and barrier: usage

> **Status: NORMATIVE** · Scope: go-quicraft `pkg/seal` · Last verified: 2026-10-02
> Conflicts with this document are defects. Fix them here first.

This page is for someone embedding go-quicraft and turning on at-rest
encryption. Encryption is opt-in: a `Host` built without a barrier writes
plaintext WAL records and snapshots.

## Before you start

You are making two independent decisions, and they compose freely.

| Decision | Seam | If you do nothing |
|---|---|---|
| Where the sealed blob is stored | `BarrierConfig.StatePersister` | nothing is stored; you must persist `SealedRootKeyData()` yourself, and the barrier stops encrypting after its first restart |
| What protects the root key inside it | `SealingStrategy` | nothing is selected for you; there is no default strategy. Pick `passphrase` or `none` explicitly, or inject a hardware strategy |

You also need:

- Go 1.27.0 or later.
- An `RLIMIT_MEMLOCK` large enough for the protected key buffers. Containers
  often ship with a small default.
- A decision about the algorithm. Read the next section before writing any
  code; it is the mistake that is expensive to undo.

## Pin the algorithm

`DefaultBarrierConfig` picks the algorithm from the local CPU: AES-256-GCM with
AES-NI (or in FIPS mode), ChaCha20-Poly1305 without it. That default is only appropriate for data
that never outlives the host that wrote it.

`Decrypt` selects the cipher by epoch, not from the ciphertext header, and
under HKDFV2 the algorithm is baked into key derivation. So a data directory
restored onto a machine without AES-NI, or a node added to a cluster whose CPUs
differ, reads back as `ErrDecryptionFailed` with nothing to point at.

```go
cfg := seal.DefaultBarrierConfig(deploymentID)
cfg.Algorithm = seal.AlgAES256GCM   // pin it
```

`Initialize` records the choice in `SealedRootKey.Algorithm`, and
`SetSealedRootKey` refuses a mismatch with an error naming both algorithms, so
if you do hit this, the failure tells you which one would work.

## Minimal correct setup

The fsync sequence in the persister is the point of this example, not
decoration. Returning `nil` from a persister promises the barrier that a crash
on the next instruction still leaves the blob readable.

```go
package main

import (
	"context"
	"encoding/json"
	"errors"
	"log"
	"os"
	"path/filepath"

	"github.com/jeremyhahn/go-enclave"
	quicraft "github.com/jeremyhahn/go-quicraft/pkg"
	"github.com/jeremyhahn/go-quicraft/pkg/seal"
)

// blobPath is wherever your deployment keeps the sealed root key.
const blobPath = "<data-dir>/sealed-root-key.json"

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
	if err := tmp.Sync(); err != nil { // the file's bytes
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
	return d.Sync() // the rename itself
}

func main() {
	ctx := context.Background()

	cfg := seal.DefaultBarrierConfig(deploymentID)
	cfg.Algorithm = seal.AlgAES256GCM
	cfg.StatePersister = persistSealedState
	cfg.RequireContextAAD = true
	// cfg.MinEpoch = engineMinLiveEpoch  // see "Set MinEpoch" below
	// cfg.RequireFIPS = true             // if you need FIPS

	barrier, err := seal.NewBarrier(cfg)
	if err != nil {
		log.Fatal(err)
	}

	passphrase := readPassphrase() // []byte you can wipe; see below
	strategy, err := seal.NewSoftwareStrategy(ctx, passphrase)
	if err != nil {
		log.Fatal(err)
	}
	defer strategy.Close()
	enclave.Wipe(passphrase) // the strategy holds its own protected copy
	creds := seal.Credentials{} // ignored by a strategy bound at construction

	blob, err := os.ReadFile(blobPath)
	switch {
	case errors.Is(err, os.ErrNotExist):
		// First boot. Initialize does NOT call StatePersister.
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

	host, err := quicraft.NewHost(hostCfg, quicraft.WithBarrier(barrier)) // hostCfg: see multi-group-host/user.md
	if err != nil {
		log.Fatal(err)
	}
	defer host.Close() // seals the barrier; the strategy is closed by the defer above
	// ...
}
```

`deploymentID`, `readPassphrase` and `hostCfg` are yours to supply.

To move the blob somewhere other than a file (a Raft-backed KV store, a database
row, a KMS entry), replace the body of `persistSealedState` and the
`os.ReadFile`. Nothing else changes.

**On the passphrase**: it is a `[]byte` rather than a `string` so you can wipe
it. The strategy copies it into protected memory and leaves your slice to you. Reading it from an environment variable defeats that, because the
value is already an immutable string in the process. Read it from a pipe, a
prompt, or an agent socket if the distinction matters to you. A strategy bound
at construction ignores `Credentials`; the deferred form reads
`Credentials.Secret` per call, which is a `*enclave.SealedEnclave` built with
`seal.NewCredentials(ctx, secret)`. That call moves the secret into protected memory
and wipes the slice it was given; you destroy the credential when you are done.

## Recover from a failed first boot

If something after `Initialize` fails (for example, distributing key shares)
and you cannot complete the bootstrap, roll the barrier back instead of building
a new one:

```go
if err := barrier.Seal(); err != nil {
	return err
}
if err := barrier.Reset(); err != nil {
	return err // ErrResetNotFirstBoot if this was not a fresh Initialize
}
// Delete any blob you already persisted, then Initialize again.
```

`Reset` only works on a first boot: a key this process created with
`Initialize`, with no blob loaded since and no rotation. It never touches what
you persisted, and the next `Initialize` creates an unrelated root key, so
anything encrypted under the discarded one is unreadable.

## Wire the barrier into a Host

```go
// One barrier for every shard on this host (the usual case):
host, _ := quicraft.NewHost(cfg, quicraft.WithBarrier(barrier))

// Per-shard barriers, for multi-tenant key isolation:
host, _ := quicraft.NewHost(cfg, quicraft.WithShardBarriers(func(shardID uint64) quicraft.Barrier {
    if b, ok := barriers[shardID]; ok {
        return b
    }
    return nil // falls back to WithBarrier, if set
}))
```

With per-shard barriers, snapshot encrypt/decrypt, snapshot transport epoch
protection and DEK purge all dispatch per shard, and purging one tenant's
barrier can never remove epochs still live under another's. **You own per-shard
rotation**: the host-wide rotation loop only rotates the `WithBarrier` barrier.
All barriers must be initialized and unsealed before use.

Shut down through `Host.Close()`, which drains the pipeline, closes the LogDB and
then seals the `WithBarrier` barrier and every barrier the `WithShardBarriers`
resolver returned, once even when shards share it (sharing is pointer identity:
return the same `*seal.Barrier` for every shard of a tenant). Every strategy is yours
to close after `Close` returns. Calling `Barrier.Seal()` directly
on a running host gives the engine an `ErrBarrierSealed` storm.

## Set `MinEpoch`

`StateMAC` authenticates the sealed blob's metadata but cannot detect replay: an
attacker with write access can restore an older, genuinely-MACed copy,
resurrecting a retired epoch's DEK with a fresh invocation counter and an empty
nonce tracker. That is two independent random-nonce sequences under one key.

`MinEpoch` is the floor that refuses it. Set it to the highest epoch any live
WAL segment or retained snapshot still references; the engine already computes
exactly this as its global safe-purge floor. Zero disables the check.

It must be set on the config before `NewBarrier`, so a real deployment reads
its minimum live epoch from the engine and constructs the barrier afterwards.

## Rotate keys

```go
newEpoch, err := barrier.Rotate(ctx)
```

Or let the host do it on a schedule with `HostConfig.KeyRotationInterval` (24h
is a reasonable production value; 0 means no scheduled rotation).
Invocation-threshold rotation runs regardless whenever a barrier is configured.

Rotation does not re-encrypt existing data. Old segments and snapshots keep
their epoch's DEK until compaction deletes them.

`Rotate` requires a `StatePersister` and fails closed with
`ErrNoStatePersister` without one.

## Purge old DEKs

```go
minEpoch := min(minWALEpoch, minSnapshotEpoch, minInFlightEpoch)
n, err := barrier.PurgeEpochsBefore(ctx, minEpoch)
```

**Do this.** It is not housekeeping. Every restart without an injected
invocation counter costs you one epoch, `KnownEpochs` grows by one each time,
and `SetSealedRootKey` refuses a blob naming more than 4096 epochs. A node in a
crash loop reaches that in minutes rather than years and then cannot start at
all, needing bespoke tooling to recover. The barrier logs at ERROR once the
set passes half the bound.

Understand what a purge buys: the barrier can no longer decrypt the epoch. It is
not crypto-shredding. Dropping the key reference makes the material
GC-eligible; the live `cipher.AEAD` holds a copy of the DEK in its key schedule
that no Go API can wipe. A core dump taken before collection contains every live
and recently purged DEK.

## Build for FIPS

```sh
GOFIPS140=latest go build -tags fips ./...
```

Set `cfg.RequireFIPS = true` as well. Without it, "FIPS was not requested" and
"FIPS was requested but this binary has no module" are indistinguishable, and
the second fails open onto Argon2id and possibly ChaCha20-Poly1305.
`RequireFIPS` turns that silent downgrade into a refusal at `NewBarrier`.

Know the boundary: **`-tags fips` makes go-quicraft FIPS at rest and on the
wire**, with one exemption you can configure away. The QUIC transport passes
`GODEBUG=fips140=only`, because `quic-go` routes packet protection through Go's
approved TLS 1.3 AES-GCM. What still refuses `only` is OCSP: its
`CertID` is SHA-1 by protocol definition, inside `golang.org/x/crypto/ocsp`.
Revocation `Mode: "crl"` never computes it.

That exemption is recorded, not assumed: which primitive, under which RFC, and
what it means for FedRAMP/PCI/HIPAA are in
[`../quic-transport/architecture.md`](../quic-transport/architecture.md#fips-140-3-exemption-record).
The excluded tree still runs under `make verify-fips-exempt` at
`GODEBUG=fips140=on`, so it is gated at a weaker mode rather than left
unmeasured.

To migrate an existing deployment into FIPS: unseal once with a non-FIPS build
and re-seal with `NewSoftwareStrategyFIPS`, which stamps PBKDF2-SHA256 into the
metadata and unseals identically in either build. A FIPS build cannot unseal a
root key wrapped under Argon2id.

## Run without encryption

`NewNoneStrategy()` gives you `StrategyNone`: the identity encrypter, which
preserves the 8-byte epoch prefix so downstream readers stay well-formed but
encrypts nothing. Snapshot artifacts then carry no integrity check at all, and
WAL records are protected only by CRC32, which detects accidental corruption, not
tampering.

`RequireContextAAD` refuses a `StrategyNone` barrier outright rather than
treating it as "nothing to bind", because requiring authenticated positional
binding while selecting no encryption is a misconfiguration, not a satisfiable
request.

## Troubleshooting

**Every `Encrypt` returns `ErrInvocationLimitExceeded` right after a restart**:
you have no `StatePersister`. The invocations already spent under the loaded
epoch are unknowable, and the barrier will not encrypt against a budget nobody
can prove, so it seeds the counter to the full limit. `Decrypt` still works: the
node serves its data but cannot add to it. Fix by wiring a persister, or by
injecting an `InvocationCounter` that persists its own count. That counter then
owns the correctness of the count, and seeding it too low is exactly as unsound
as not counting at all.

**`ErrNoStatePersister` from `Rotate`**: same cause, same fix.

**`ErrBarrierSuperseded`**: another process loaded the same sealed blob. This
barrier has stopped encrypting, permanently, and sealing and unsealing again
will not clear it. Find the second process. Exactly one process must own a
sealed blob at a time; the fence detects the violation but is not a lock, so
enforce it with an exclusive data-directory lock or a compare-and-swap
persister.

**`ErrSealedEpochRollback`**: the blob you loaded is older than `MinEpoch`, or
older than the state the barrier already holds. Either you restored a backup, or
someone rolled the file back. Do not lower `MinEpoch` to make it load.

**`ErrSealedStateUnauthenticated`**: the blob has no `StateMAC`, or its MAC does
not verify. If this is a genuine pre-MAC blob, start once with
`AllowUnauthenticatedSealedState = true`, re-persist `SealedRootKeyData()`, then
clear the flag. The flag does not apply to a MAC that is present but wrong; that
is tampering or corruption.

**`ErrResetNotFirstBoot`**: you called `Reset` on a barrier that loaded a blob or
rotated past epoch 1. `Reset` is only a first-boot rollback; there is no way to
discard a key that has been used beyond that.

**`ErrDecryptionFailed` on data that used to read**: most often an algorithm
mismatch (see "Pin the algorithm"), or a positional context that does not
reproduce the one used at encrypt time. The error is deliberately generic and
will not tell you which; that is the anti-oracle design. Check
`SealedRootKey.Algorithm` against `BarrierConfig.Algorithm` first, then the
context bytes. **Never** retry the decrypt without the context.

**`ErrTooManyKnownEpochs` and the node will not start**: `KnownEpochs` passed
4096. You were not calling `PurgeEpochsBefore`, or the node was crash-looping.
Recovery needs bespoke tooling: unwrap the root key, trim `KnownEpochs`,
re-stamp the MAC. Prevent it rather than fix it.

**`ErrContextAADUnsupported` at startup**: `RequireContextAAD` is set and the
barrier cannot bind. Ask `ContextBinding()` for which of the three reasons:
`ContextBindingUnsupported` (a delegated encryptor with no AAD capability),
`ContextBindingPlaintext` (`StrategyNone`), or `ContextBindingUnresolved` (the
barrier is not unsealed yet). `IsHardwareMode()` cannot distinguish the first
two; `IsPlaintextMode()` can.

**A log line saying a delegated encryptor declares `AADSymmetricEncrypter` but
does not authenticate the AAD**: the behavioural probe caught a false claim.
Fix the encryptor; do not work around the probe. It runs at `Initialize` and
`Unseal` only, never on the data path.

**`ErrUnsealBackoff`**: five or more consecutive failed unseals. Backoff doubles
from 1s to a 60s cap and is persisted, so it survives a restart. A `StateMAC`
failure and a superseded barrier deliberately do not count toward it.

**Unseal is slow (~90 ms)**: that is the Argon2id KDF, once at startup, and it
is the primary defence against offline attack on the sealed blob. The defaults
deliberately exceed OWASP 2024 minimums because the sealed root key is a
high-value target.

**`SupportsContextAAD()` returned false and my consumer disabled binding
forever**: do not cache that answer. False means either "cannot bind" or
"cannot say yet", and those demand opposite handling. Gate on
`ContextBinding().Resolved()` (or `ContextBindingResolved()`) and refuse to
record an unresolved one. The capability is valid only while the barrier stays
unsealed and can legitimately change across a seal/unseal cycle.

**A deadlock inside my `StatePersister`**: it runs with the barrier's write
lock held and must not call back into the same barrier. If your storage layer is
itself backed by this barrier, break the cycle before you get there.
