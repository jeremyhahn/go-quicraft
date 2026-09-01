# Changelog

All notable changes to QuicRaft will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added

- **`SoftwareStrategy.DeferredPassphrase() bool` (#43)** — reports whether a
  strategy binds its passphrase at Seal/Unseal time or at construction. The two
  modes have opposite credential semantics (a bound strategy IGNORES the
  `Credentials` handed to `UnsealRootKey`; a deferred one validates against
  them), and until now the distinction was invisible from outside the package.
  A consumer replacing a registered strategy therefore could not preserve the
  mode of the one it replaced, and installing a bound strategy where a deferred
  one stood turns a barrier that authenticates its unsealer into one that
  accepts any passphrase for the lifetime of the process — measured on the
  production shape in go-qrdb #157. Follows the `KDFVersion()` precedent for
  exposing a construction-time property; no behaviour of either mode changes.

### Documentation

- **`SealingStrategy` now states that honouring `Credentials` is
  implementation-defined (#43)**, that replacing a registered strategy can
  silently change whether the barrier authenticates its unsealer, and — on
  `UnsealRootKey` — that a nil error is not proof the supplied credentials were
  correct.

## [0.9.0-alpha] - 2026-08-24

### Changed — BREAKING: every share and root-key surface is protected memory (#37)

Owner-ruled 2026-08-23 on #37, and a breaking change by intent: pre-1.0, MINOR
is where breakage lives, and a pre-release redesign lands clean rather than
behind deprecated aliases.

- **`ShareSplitter` and `ShareAccumulator` are REMOVED.** `ProtectedShareSplitter`
  and `ProtectedShareAccumulator` — added one release ago by #35 — are now the
  only custody interfaces this package declares. Keeping the string forms as
  deprecated siblings preserved a shape whose only use was to undo the guarantee:
  an implementation could split into locked memory and still have to copy every
  share back out into an immortal heap string to satisfy `Split`, so a consumer
  could migrate, silence every `SA1019`, and leave the exposure exactly where it
  was.
- **`ShamirInitResult.Shares` is `[]*shamir.ProtectedShare`**, and the result now
  OWNS its shares: `Destroy()` wipes and releases every one, is safe on a nil
  receiver, and is safe to call twice, so a ceremony can `defer` it from the line
  the result exists. This was the last string-typed share surface in `pkg/seal`,
  and it is the moment a ceremony holds all N shares at once.
- **The `"shares"` WIRE SHAPE CHANGES with it**, which is the part to read twice:
  it goes from an array of bare share values to an array of share OBJECTS — the
  `{"index","threshold","total","value"}` encoding `shamir.Share` and
  `shamir.ProtectedShare` already produce everywhere else. An init result written
  by an older build does not decode into this type. No dual-format reader ships;
  local state is wiped and re-bootstrapped.

### Added — `ProtectedSealingStrategy`, the protected root-key pair (#37)

- **`SealRootKeyProtected` and `UnsealRootKeyProtected`**, on a new optional
  interface that embeds `SealingStrategy`. Sealing takes the root key in an
  `enclave.MemoryEnclave` and borrows it; unsealing returns one the caller owns
  and must `Destroy`.
- **Why.** The `[]byte` signatures force an implementation to RETAIN the root key
  in ordinary memory. go-qrdb's recovery strategy reconstructs it into an enclave
  and then copies it into a plain field purely to have something to return from
  `UnsealRootKey` — so the most sensitive value in the system sits in
  collectable, swappable memory for the whole barrier-unseal and is wiped only at
  `Close` (go-qrdb PR #76, Codex round 4). No care on the implementer's side can
  avoid that while the signature demands a `[]byte`, which is why the review's own
  conclusion was to fix the interface rather than assert protected custody above
  it.
- **Optional, and additive.** `SealingStrategy` is untouched, so every strategy in
  the portfolio keeps compiling; a caller type-asserts for the protected pair.
  There is no fallback, because a fallback is the unprotected copy wearing the
  protected API's name.
- **`SoftwareStrategy` implements both.** The plaintext root key never exists as
  an ordinary heap allocation on the unseal path: GCM's plaintext length is known
  in advance, so the enclave is allocated at exactly that size and `aead.Open`
  writes into the enclave's own storage. That length is a function of a blob
  nothing has authenticated yet, so it is refused unless it is exactly the
  root-key size — before anything is locked. memguard answers a lock it cannot
  take by terminating the process, which would otherwise make an oversized sealed
  blob a remote kill switch that fires before the AEAD tag rejecting it is ever
  checked. The KDF runs BEFORE the borrow, because opening the passphrase enclave
  inside one would deadlock against `pkg/enclave`'s process-wide purge lock.
- **`NoneStrategy` implements both too**, and `UnsealRootKeyProtected` returns
  `ErrNoRootKeyAvailable`: no-encryption mode has no root key, and an enclave
  cannot hold nothing — memguard answers a zero-length allocation by terminating
  the process. Its `SealRootKeyProtected` still VALIDATES the enclave it never
  reads, so a caller that destroyed or lost its root key before sealing learns so
  there rather than at an unseal it can no longer perform.
- **`ProtectedCredentials`, because both sides of the boundary have to be
  protected.** The protected methods take a passphrase held in an
  `enclave.ProtectedKey`, not `Credentials`' plain `[]byte`. For a
  deferred-passphrase strategy the passphrase is not a lesser secret than the key
  it wraps — it IS that key's protection — so a protected root-key API that
  accepted an unprotected passphrase would be protecting the wrong half. There is
  no overload taking `Credentials`: converting one to the other would reintroduce
  the plain copy the signature exists to remove. `NewProtectedCredentials` moves
  a secret into locked memory and WIPES the caller's slice; the caller owns the
  result and releases it with `Clear`.
- **The deferred protected paths no longer build a temporary inner strategy.**
  That construction existed to copy a plain passphrase into locked memory, and a
  `ProtectedCredentials` passphrase is already there. One behaviour moves with
  it: a FIPS-unapproved KDF is refused at `DeriveKey` rather than at the inner
  construction — same error, one frame later.
- **`Credentials` itself is untouched**, and whether `SealingStrategy`'s own
  methods should move to protected credentials is #41 (owner ruling): that one
  is a portfolio-wide interface change touching every strategy in go-xkms and
  go-qrdb, which the new pair is not — it had no consumers to break.
- **The `Barrier` still uses the `[]byte` pair.** It needs raw bytes for HKDF
  epoch-DEK derivation and the sealed-state HMAC and wipes them on every path.
  Moving the barrier itself onto the protected pair is tracked as #39.

### Added — protected-memory custody interfaces (#35)

- **`seal.ProtectedShareSplitter` and `seal.ProtectedShareAccumulator`**, the
  protected-memory counterparts of `ShareSplitter` and `ShareAccumulator`. The
  string forms are now marked deprecated but keep working.
- **Why they were needed.** 0.8.0-alpha deprecated the string-valued Shamir API
  because a Go string cannot be zeroed — yet `ShareSplitter` returns
  `[]string` and `ShareAccumulator.AddShare` takes one. A consumer could
  therefore migrate to `SplitProtected`/`CombineProtected`, silence every
  SA1019, and still have each share become an immortal heap string the moment
  it crossed the interface. As go-qrdb put it when reporting this, a migration
  that silences the linter while leaving the exposure intact is worse than not
  migrating, because it removes honest signposts.
- **Both ends are protected.** `AddProtectedShare` takes ownership of a share
  already in locked memory, so a transport can decode into a wipeable buffer,
  hand it over via `NewProtectedShare` (which wipes its source), and own
  nothing afterwards. `ReconstructProtected` returns the secret in an
  `enclave.MemoryEnclave` rather than an ordinary `[]byte`. Protecting only one
  end would leave the exposure intact at the other.
- **Additive, not a retype**, mirroring the precedent set by
  `Split`/`SplitProtected`: every existing implementation still compiles, and
  consumers migrate one call site at a time rather than in lockstep with a tag.

### Fixed — a process-wide memguard purge could unmap pages mid-borrow (#18)

- **`pkg/enclave` gains a package-wide `purgeMu`.** memguard's failure path is
  process-wide, not per-buffer: `core.Panic` calls `core.Purge` first, which
  snapshots every live buffer and destroys each one, ending in `memcall.Free` —
  the pages are UNMAPPED, not merely wiped — while holding none of this
  package's locks. The per-enclave borrow discipline added in 0.8.0-alpha could
  not see that. Goroutine A inside `WithBytes` on one enclave could have its
  pages unmapped by goroutine B exhausting `RLIMIT_MEMLOCK` constructing an
  *unrelated* one. Losing that race is a SIGSEGV, the same failure class as the
  original use-after-destroy defect, and it shipped in 0.7.0-alpha too.
- **Purge-capable operations take it exclusively; borrows take it shared.**
  Which operations qualify was read out of memguard v0.23.0 rather than assumed:
  `Panic` is reachable from `NewBuffer`, `Freeze`, `Melt`, `Scramble` and
  `Destroy` in `core/buffer.go`, and from `NewEnclave` and `Open` in
  `core/enclave.go`. So `Destroy` and `Freeze` are allocation-class operations,
  not reads — releasing and re-protecting pages can purge too. Lock order is
  always `purgeMu` then the enclave's own mutex, so the two cannot cycle.
- **BEHAVIOUR CHANGE for callers: a `WithBytes` callback must not touch ANY
  enclave, not merely its own.** Go's `RWMutex` refuses a new `RLock` once a
  writer is queued, so a nested borrow of a *second* enclave deadlocks against
  any purge-capable operation that queued after the outer borrow began. The
  previous documentation explicitly permitted this, and `CombineProtected` in
  `pkg/crypto/shamir` relied on it; it now materializes its decode buffer before
  borrowing. The type doc, `WithBytes`' contract, `docs/memory-enclave/api.md`
  and `docs/memory-enclave/engineering.md` are corrected in the same commit —
  the overclaim they carried ("every method is safe to call concurrently") was
  false from outside these locks in a way no caller could defend against.
- Verified by two probes that interrogate the lock directly rather than waiting
  on a crash. `-race` cannot see reads of memguard's region — `memcall.Alloc` is
  `mmap`, which the detector does not instrument — so a green `-race` run is not
  evidence for this class. Each probe carries a vacuity check and fails if it
  never observes the coordination being exercised. Mutation-verified in both
  directions, with and without `-race`.

### Added — `StorageBackend.CompareAndDelete` (#29)

- **BREAKING for implementers of `seal.StorageBackend`.** The interface gains
  `CompareAndDelete(ctx, key, expected) (deleted bool, err error)`, the release
  half of the pair `PutIfAbsent` opens. Because the interface is satisfied by
  duck typing, any store that does not grow the method stops satisfying it.
  Pre-1.0 this takes the MINOR position at the next release.
- **Why it is not optional.** A guarded create is only as strong as its
  release. With `PutIfAbsent` to acquire and a plain `Delete` to release, two
  releasers can both read holder `A`, the first deletes, `B` legitimately
  claims the now-absent key, and the second executes its unguarded delete and
  removes **`B`'s** value — `B` having acquired under the guarantee
  `PutIfAbsent` advertises. Re-reading before the delete cannot close it: the
  window is *between* the read and the delete. Reported by go-qrdb, where the
  consequence was a tenant left holding a record no by-id operation could reach
  (go-qrdb#41); consumers holding only the interface had been refusing with a
  typed error rather than degrading to read-then-delete.
- The contract records two obligations implementers get wrong: an encrypting
  backend must guard on the ciphertext it read rather than a fresh encryption
  of `expected` (with random-nonce GCM a re-encrypted guard refuses *every*
  release), and a refused delete is a normal outcome reported through
  `deleted=false`, never an error — while a failed guard *read* must refuse
  rather than decide.

## [0.8.0-alpha] - 2026-08-19

### Fixed — use-after-destroy race on protected memory (#15)

- **`MemoryEnclave` now serializes its accessors against teardown.** `Bytes`,
  `Size` and `WithBytes` register a borrow; `Destroy`, `Freeze` and
  `SealMemoryEnclave` wait for every borrow to end before touching the region.
  A reader observes either a live buffer or a destroyed one, never a buffer
  mid-destruction. The buffer reference was previously an `atomic.Pointer`,
  which was not enough: an atomic makes access to the POINTER race-free and says
  nothing about the LIFETIME of what it addresses, so `Destroy` could unmap the
  region between a reader's load and its dereference.

  **Mutators take priority over readers**: once one is queued, no new borrow is
  admitted until it finishes. That fairness rule is part of the fix, not a
  polish on it. Without it a steady stream of overlapping `WithBytes` callbacks
  holds the borrow count above zero indefinitely and a parked `Destroy` never
  wipes the secret — unbounded starvation of the wipe, in a type whose entire
  purpose is that the wipe happens. Raised by Codex review of PR #17.

  **A callback must not re-enter its own enclave.** The counter was originally
  chosen over an `RWMutex` on the belief that callbacks would need to nest, and
  that turned out to be wrong twice over: nothing in this repository nests (the
  reads inside `CombineProtected`'s and `MarshalJSON`'s callbacks are on a
  *different* enclave, with its own lock), and the only admission policy that
  prevents starvation is exactly the one that forbids nesting, since a nested
  borrow is a new borrow. The properties are mutually exclusive; a deadlock is
  loud, reproducible and fixable at the call site, where starvation is silent
  and unbounded. The counter is kept over an `RWMutex` — which would give the
  same policy — only because the mutator's park is an observable point the
  concurrency tests need.

- **`CombineProtected` reads every share through `WithValue`.** It previously
  took the bare aliasing slice and decoded from it after the borrow had ended,
  which is precisely the scenario #15 was filed about — a ceremony cancelled or
  timed out mid-reconstruction — and a caller could not repair it from outside,
  because reconstruction obtains the slices itself.

- **`ProtectedShare.MarshalJSON` builds the document inside `WithValue`.** The
  same defect one call site further on: it took `val := s.Value()`, ending the
  borrow, and then appended `val` into its output buffer — reading protected
  memory the enclave was free to unmap. Copying the value out first would have
  been worse, not better, since that mints exactly the unprotected copy of share
  material the type holds an enclave to avoid, so the encode moved inside the
  borrow instead. The exact-capacity sizing that keeps an `append` from
  reallocating and stranding an un-wiped copy is preserved. A destroyed share
  still reports the same `ValidationError` it always did.

  The race is real at the memguard layer, not merely theoretical. In v0.23.0
  `core.Buffer` embeds a mutex carrying its author's own note that it "does not
  protect 'data' field"; `Buffer.Data()` reads that field holding nothing while
  `Buffer.destroy()` writes it under the lock and then `memcall.Free`s the
  region. `-race` reports it, and because the memory is genuinely unmapped
  rather than zeroed, losing the race is a SIGSEGV rather than a stale read.

  Reported by the go-quicbaas agent from a Codex review of its PR #41.

- **`MemoryEnclave.WithBytes` and `ProtectedShare.WithValue`**, scoped accessors
  that hold the read lock across a callback. They exist because locking inside
  `Bytes`/`Value` cannot close the second half of the defect: those return a
  slice ALIASING the protected region, the lock is released on return, and the
  caller then uses a slice `Destroy` may unmap at any moment. Holding the lock
  across the callback makes the valid span exactly the callback. Use them
  wherever teardown may be concurrent with the read; `Bytes` and `Value` remain
  correct and cheaper under single-goroutine ownership, which is the shape the
  seal barrier already uses.

- **`Freeze` now takes the write lock.** It changes the region's page
  protection, which is a mutation, not a read. A fresh `NewMemoryEnclave` is
  deliberately mutable and `WithBytes` hands its callback a writable slice, so a
  `Freeze` holding only a read lock could flip the pages read-only midway
  through a callback still filling the buffer — and that callback's next write
  would fault and take the process down. Found in review of PR #16, not by the
  original report.

- **`ErrEnclaveDestroyed`** distinguishes a destroyed enclave from a
  zero-length secret, so a callback is never handed a nil slice it might read as
  an empty value.

- **Corrected `Destroy`'s doc comment**, which claimed it was "safe to call
  concurrently with Bytes/Size" while the type's own OWNERSHIP paragraph said
  the opposite. The claim is now true rather than removed, but it was a false
  assurance sitting next to the hazard, which is what made the gap easy to read
  past. `ProtectedShare.Destroy` documented the same adjacent property.

### Testing

`enclave.TestOnlyOnMutatorParked` and `TestOnlyOnReaderParked` are test-only
observation hooks (gated by `testing.Testing()`, following
`shamir.testOnlyRandReader`) that fire when a mutator, or a reader queued behind
one, is about to block. It exists because the
central guarantee — a mutator cannot COMPLETE while a reader holds a borrow —
could otherwise only be tested by waiting a while and concluding the mutator was
probably still blocked, which this repository's testing convention forbids: a
sleep is never a synchronization primitive, because a loaded runner turns "give
it 100ms" into a hang or a vacuous pass. The concurrency probes now learn that
the mutator IS parked and assert from there. Raised by Codex review of PR #17.

Both hooks pass the enclave to the callback and every caller filters on it. They
are package-global, so a mutator parking on an unrelated enclave fires a hook a
test installed for its own — a probe treating any notification as its own could
conclude its target had parked when it had not, and pass against a broken
implementation whose mutator merely ran late. The reader hook exists for the
half of writer preference the mutator hook cannot reach: that the readmitting
`Broadcast` actually happens. Without it the starvation probe released its
borrow before the late readers had queued, and an implementation that blocks
readers correctly and then never wakes them passed. Both found by the local
Codex review before merge, after four hosted rounds had missed them.

Two further gaps came out of the same review and are closed by
`TestMemoryEnclave_TwoQueuedMutatorsBothComplete` and
`TestMemoryEnclave_SealExcludesInFlightCallback`. Every other probe parks at
most ONE mutator, and one waiter cannot tell `Broadcast` from `Signal` — the
swap would have passed the whole suite and deadlocked in production. And
`SealMemoryEnclave` ends the buffer's life exactly as `Destroy` does, but only
`Destroy` and `Freeze` were exercised against an in-flight callback, so deleting
Seal's exclusion passed too. Both mutation-verified.

### Hardening found by the local review

- **`lockIdle` is now panic/Goexit-safe.** It returns holding `mu` and the
  CALLER installs the `unlockMutated` defer, so an abnormal exit between the two
  left the mutex locked and `waiting` inflated — wedging the enclave for good.
  Reachable through the park hook: a panic in it, or a `t.Fatal` from the
  mutator's own goroutine, which is a `runtime.Goexit` and unwinds defers like a
  panic. Cleanup now runs on the abnormal path only.
- **The `CONCURRENCY` contract said a reader observes "a live, writable
  buffer".** A frozen enclave is live and read-only, and the borrow orders a
  callback only against a `Freeze` it races — not one that already won. Writing
  inside `WithBytes` is sound for the producer's own allocate/fill/`Freeze`
  sequence; concurrent freezing must be ordered by the caller. Corrected in the
  type doc and `api.md`, which had the stronger claim.
- **Test-hook lifetimes do not nest across goroutines** — restore puts back
  whatever was installed at call time, so overlapping installs restore
  incorrectly and can resurrect a stale hook. Documented as a one-goroutine,
  no-parallel restriction rather than replaced with a registry.

### Documentation

The design rationale had to move with the code, because it argued for the thing
being removed. `docs/memory-enclave/architecture.md` carried a section titled
"An `atomic.Pointer` reference, not a mutex" that explicitly rejected a lock;
`engineering.md`'s invariant table stated `Bytes`/`Size` were "**NOT** safe with
`Destroy`" as though that were a design choice rather than the defect. Both now
describe the borrow discipline, and the general lesson is recorded where the
mistake was made: *an atomic on the handle is not a lifetime discipline for the
thing it points at.* Also updated: `api.md` (the scoped accessors, the error
vocabulary, and a §1 exported-surface listing that predated `MemoryEnclave` and
`Freeze` entirely), `user.md`'s troubleshooting entry, and the custody
obligations in `docs/security/encryption.md` (that file has since been folded
into `docs/shamir-primitive/engineering.md`; see #10).

### Performance

Accessor benchmarks are committed so the next argument about the barrier's hot
path is with a measured figure rather than an assumption. Measured back to back
on one machine (Core Ultra 9 285K, 5x3,000,000 iterations, best of five):

| | `Bytes` | `WithBytes` |
|---|---|---|
| unsynchronised (the pre-fix shape) | 2.40 ns/op | 1.85 ns/op |
| borrow counter (this change) | 21.6 ns/op | 22.1 ns/op |

So the borrow costs roughly **20 ns per accessor call**, uncontended, against a
`Barrier.Decrypt` 64B floor of ~123 ns/op on the same machine — about 16% on the
smallest frame the barrier handles, and proportionally less on every larger one.

That is more than a `sync.RWMutex` read lock would cost (~9.6 ns/op when the
first version of this fix was measured, though on a differently loaded machine,
so treat it as indicative rather than a like-for-like), because a borrow takes a
full mutex twice — once to register, once to release — where an `RWMutex` takes
one `RLock`. The extra ~10 ns buys the nesting property, and nesting is what
makes `CombineProtected` and `MarshalJSON` fixable by reading *through* the
accessor instead of being exempted from it. Correctness on a path that already
spends an order of magnitude more is the right trade.

`BenchmarkMemoryEnclave_BytesParallel` measures the contended case (112-138
ns/op across 24 threads on one enclave), since both borrow and release serialize
on the same mutex. Nothing in the current tree shares one enclave across
goroutines on a hot path — the barrier's shape is `Open` / use / `defer Destroy`
per frame, single-owner — but the number is committed so that if something ever
does, the cost is already on record.

### Note for whoever cuts the next tag — this is a MINOR, not a PATCH

`WithBytes`, `WithValue`, `ErrEnclaveDestroyed` and `DestroyedError` are ADDED
exported API. Under the pre-1.0 rule this repository follows — and exactly as
`0.7.0-alpha` recorded for the same reason — added API takes the MINOR position.
Nothing was removed or changed incompatibly, so no consumer needs to adapt, but
the version is `0.8.0-alpha` rather than `0.7.1-alpha`.

## [0.7.0-alpha] - 2026-08-18

MINOR, not PATCH: this release adds exported API (the `ProtectedShare` family,
`MemoryEnclave` and `Freeze`) and changes cryptographic behaviour on the
AES-GCM data path. Pre-1.0, MINOR is where added API and breakage live.

The headline for consumers is the FIPS section below. `pkg/seal` no longer
constructs AES-GCM with a caller-supplied IV, which is what every downstream
repo running `GODEBUG=fips140=only` was failing on — go-qrdb attributed 205 of
its FIPS failures, across 14 packages, to this one construction.

**Read the BREAKING section first.** This is not a routine bump for a consumer
that obtains a raw `cipher.AEAD` from this package.

### BREAKING — the AEAD returned to consumers reports `NonceSize() == 0`

Added retroactively on 2026-08-19, after go-qrdb took this release and found the
break. The original notes for this version described the change as
consumer-visible "in one place" and said stored bytes were unchanged. **The
first was an understatement and the second is true only of this package's own
frames.** The tag is immutable, so the correction lives here.

`SoftwareAEADProvider.NewAEAD` — and any `cipher.AEAD` this package hands back
for an AES-GCM algorithm — changed its calling convention, because
`cipher.NewGCMWithRandomNonce` generates the nonce itself:

| | before (`0.6.2-alpha`) | after (`0.7.0-alpha`) |
|---|---|---|
| `aead.NonceSize()` | 12 | **0** |
| `aead.Overhead()` | 16 | **28** |
| `len(aead.Seal(nil, nonce, pt5, nil))` | 21 | **33** |
| `SymmetricAlgorithm.NonceSize()` | 12 | 12 — the WIRE width, unchanged |

The two conventions are mutually exclusive: the old AEAD requires a 12-byte
nonce, and the new one panics if handed any nonce at all.

**The damage is to existing data, and it is silent.** A consumer that writes
`nonce := make([]byte, aead.NonceSize())` and persists that nonce as a field
beside the ciphertext now writes a ZERO-LENGTH nonce. New writes and new reads
stay self-consistent, so a fresh store works and a smoke test passes — but a
record written by `0.6.2-alpha` has a 12-byte nonce field and a nonce-less
ciphertext, and a length check against `aead.NonceSize()` now rejects it. Where
that record is a wrapped root key, the barrier does not unseal. That is the one
blob whose loss costs the store.

**Before taking this release, answer one question:** does the consumer obtain a
`cipher.AEAD` from this package and either size a nonce from `aead.NonceSize()`
or store the nonce separately from the ciphertext? It is a grep for
`NonceSize()` and for any struct pairing a `Nonce` field with a `Ciphertext`
field. A consumer that only calls `Barrier.Encrypt` / `Barrier.Decrypt` and
never touches the raw AEAD is unaffected. Where the answer is yes, the fix is a
dual-read migration accepting both the separate-nonce and embedded-nonce
shapes; a straight bump loses the data.

**This package's own frames are NOT affected**, which is why the break is easy
to miss. `NewGCMWithRandomNonce` emits `nonce ‖ ciphertext ‖ tag`, byte-identical
to the layout `pkg/seal` already wrote by hand, so the barrier frame and the
sealed-root-key blob are unchanged in both directions and
`pkg/seal/legacy_format_test.go` decrypts checked-in pre-migration bytes. The
break is in the exported AEAD contract, not in the wire format.

### Added — Shamir share values in protected memory

- **`ProtectedShare`, the protected-memory counterpart of `Share`
  (`pkg/crypto/shamir/protected_share.go`).** `Share.Value` is a Go string and
  a string is immortal: it cannot be zeroed, so every share stays readable in
  the heap until the collector reclaims it, and a ceremony holding all N shares
  is the widest exposure in the scheme. A `ProtectedShare` holds its value in
  an `enclave.MemoryEnclave` — mlock'd, guard-paged, canary-checked, and
  genuinely wiped by `Destroy`.

  New API, all additive:

  - `SplitProtected(secret, threshold, total)` — `Split` with protected values.
  - `CombineProtected(shares)` — reconstructs from protected shares and returns
    the secret in a `*enclave.MemoryEnclave` the caller destroys. The share
    values are read in place and never copied out of protected memory.
  - `NewProtectedShare(index, threshold, total, value)` — copies a share value
    into protected memory and WIPES the source, so custody code can carry share
    bytes in wipeable buffers from the moment they exist.
  - `(*Share).Protect()` — the migration bridge for callers that already hold
    plain shares. It bounds further exposure; it cannot unmake the string.
  - `(*ProtectedShare)` `Value`, `Destroy`, `Validate`, `String`, `LogValue`,
    `MarshalJSON`, `UnmarshalJSON`.
  - `UnmarshalShareSet(data)` and `MaxShareSetBytes` — the bounded ingestion
    path for a share set arriving from outside the process (below).

  The JSON encoding is byte-for-byte what `Share` produces, so the two types
  are interchangeable on the wire and a consumer can migrate one call site at a
  time. `MarshalJSON` is written by hand rather than delegated to
  `json.Marshal` for the reason the type exists — marshalling through a string
  field would mint exactly the immortal share string being avoided — and sizes
  its buffer exactly so no append reallocates and strands an un-wiped copy.
  `UnmarshalJSON` decodes the value as a `json.RawMessage` for the same reason,
  and destroys any value the target share already held, because `json.Unmarshal`
  reuses the elements of a non-empty slice.

  `CombineProtected` holds exactly ONE decoded share at a time, whatever the
  size of the set: it walks the shares once to validate them and fix the
  x-coordinates, then again to fold each share's contribution into the result
  through the reused buffer (`accumulateShareInto`, `interpolateSecretInto`'s
  inner sum turned inside out). Holding a decoded copy of every share would
  make the peak locked footprint of a reconstruction proportional to the
  submitted set — another ~16 MiB for 255 maximum-size shares — and locked
  memory is the one allocation this process cannot fail gracefully. Decoding
  base64 twice is cheap; a peak an untrusted set gets to choose is not.

  Both paths are protected-**native**: `SplitProtected` reads the polynomial
  coefficients straight into locked memory and encodes each share value
  directly into the locked buffer that share will own, and `CombineProtected`
  decodes each share value in place into locked memory and interpolates
  directly into the locked buffer it returns. Neither routes through
  `SplitBytes` or `CombineBytes`, which by contract hand back ordinary heap
  buffers: no secret-derived byte is written to unprotected memory on either
  path, not even to a buffer that is wiped afterwards. Only the share
  x-coordinates and the Lagrange basis derived from them stay on the ordinary
  heap — an x-coordinate is a published share index, not secret.

  The rules are still not duplicated: both paths run `parseRawShare`, the
  single copy of the wire-format rules that `combine` and `CombineBytes` use,
  so the string and protected paths cannot drift into accepting different share
  sets, and `validateSplitParams` (extracted from `SplitBytes`, no behaviour
  change) is the single copy of the scheme-parameter rules. `validateShareHeader`
  is likewise now the single copy of the index/threshold/total rules, shared by
  `Share.Validate` and `ProtectedShare.Validate` (no behaviour change).

  Share values are validated against the full standard-base64 grammar —
  length, alphabet and padding placement — before they enter protected memory,
  rather than alphabet membership alone: `"A"`, `"===="` and `"A=AA"` are made
  of legal characters and none of them decodes. The check is exactly what
  `base64.StdEncoding.Decode` accepts, so it never rejects a share the
  deprecated `Share` path would take, and it still does not decode (decoding to
  validate would materialize a second plaintext copy of the payload).

  A share value is also bounded to the encoding of a `MaxSecretBytes` payload
  **before** any locked memory is allocated for it. On the string path an
  oversized share costs a heap allocation and `parseRawShare` rejects it during
  combine; on this path the allocation is `mlock`'d memory, a finite kernel
  resource whose exhaustion memguard answers by terminating the process, and
  `UnmarshalJSON` constructs from operator-supplied JSON that a recovery
  ceremony cannot authenticate before combining it. The bound is exactly the
  one `parseRawShare` applies, moved ahead of the allocation it has to guard.

  Those bounds are per VALUE, so the two paths that allocate a whole SET price
  the set itself against `MaxShareSetBytes` — the locked memory this package
  will spend on one share set — before allocating any of it. `SplitProtected`
  prices the footprint its arguments imply (coefficients, scratch and every
  returned value) and returns a `*SplitError` if it does not fit: individually
  legal arguments multiply, and a 64 KiB secret split 255 ways needs ~16 MiB of
  polynomial coefficients before a single share exists. And a set decoded from
  untrusted JSON is bounded by `UnmarshalShareSet`: 255 individually legal shares
  still add up to ~22 MiB of locked payload, past the 8 MiB `RLIMIT_MEMLOCK` a
  Linux host commonly defaults to, and `CombineProtected`'s 255-share check
  runs on shares that have already been allocated. `UnmarshalShareSet` checks
  the element count (255, the scheme's own limit) and the document size
  (`MaxShareSetBytes`, 1 MiB) against the UNDECODED document, before the first
  locked buffer exists, and destroys the accepted prefix if a later element is
  rejected. Since shares cannot be authenticated before they are combined, a
  recovery endpoint decodes whatever it is handed, so that ceiling has to be
  the service's choice rather than the submission's. The budget is stated as
  policy rather than derived from the scheme: a full 255-share ceremony over a
  32-byte root key is ~12 KiB of JSON, and a caller that legitimately needs
  more does its own accounting and decodes share by share with
  `NewProtectedShare`. `ProtectedShare.UnmarshalJSON` documents that it decodes
  one share and is not the ingestion path for an untrusted set.

  Two obligations come with the type, both documented on it: every
  `ProtectedShare` must be destroyed — **including one produced by
  `json.Unmarshal`**, since decoding is a construction — and each share is one
  `RLIMIT_MEMLOCK` allocation, so a ceremony should destroy shares as it
  consumes them rather than at the end.

### Changed — `pkg/enclave`: `ProtectedBuffer` is now `MemoryEnclave`

- **The locked-memory type is named `MemoryEnclave`** (owner ruling,
  2026-08-18): the name describes the mechanism — a region of locked, zeroized
  memory — where "buffer", "secret" or "secure string" describe nothing in
  particular and already mean other things in this stack. One vocabulary across
  the portfolio is what keeps the protected-memory audit a grep.

  `NewMemoryEnclave`, `NewMemoryEnclaveFromBytes` and `SealMemoryEnclave` join
  it. **Nothing broke**: `ProtectedBuffer` remains as a type ALIAS and the old
  constructors as thin wrappers, all marked deprecated, so every consumer —
  including `seal.ProtectedBuffer`, which re-exports it — keeps compiling and
  the two names remain interchangeable. Removing the old names belongs to the
  coordinated bottom-up wave, not here.

- **`(*MemoryEnclave).Freeze`** makes an enclave's memory read-only, completing
  the allocate → write in place → freeze pattern. It is what lets a producer
  build its output inside locked memory (as `SplitProtected` now does) and still
  end up with the read-only buffer `NewMemoryEnclaveFromBytes` would have
  produced, without the unprotected intermediate copy.

### Changed - FIPS 140-3 compliance

- **`pkg/seal` no longer supplies its own AES-GCM IVs.** `newAESGCM` now builds
  the cipher with `cipher.NewGCMWithRandomNonce` instead of `cipher.NewGCM`, so
  the nonce is generated inside the FIPS module boundary. FIPS 140-3 (SP
  800-38D §8.2.2, CMVP IG C.H) requires exactly that, and Go refuses the old
  construction under `GODEBUG=fips140=only` — 332 rejections originated here,
  and every consumer sealing through the barrier inherited them.

  **No stored bytes move.** `NewGCMWithRandomNonce` emits
  `nonce ‖ ciphertext ‖ tag`, which is byte-for-byte the layout this package
  already wrote by hand, so the barrier frame
  (`[epoch:8][algID:1][nonce:12][ct+tag]`) and the sealed-root-key blob
  (`[KDFSalt:32|Nonce:12|EncryptedRootKey|Tag:16]`) are unchanged, in both
  directions: an old reader reads new data and a new reader reads old data.
  There is no envelope version discriminator because there are not two
  envelopes. `pkg/seal/legacy_format_test.go` proves it against checked-in
  pre-migration bytes.

  Inside the barrier, `Barrier.Encrypt` no longer draws the data-path nonce
  from `BarrierConfig`-adjacent randomness, so a failing random source can no
  longer fail an AES-GCM encryption, and the nonce-reuse tracker records the
  nonce after the `Seal` rather than before it (still fail-closed — the
  ciphertext is never returned).

  This paragraph originally claimed consumer-visible behaviour changed "in one
  place". It does not: the AEAD handed to consumers also changed its calling
  convention, and for anyone persisting a nonce field that is a silent
  data-compatibility break. See the BREAKING section at the top of this release.

- **New `fips` build tag compiles out every non-approved algorithm.** Built
  with `-tags fips`, `pkg/seal` contains no ChaCha20-Poly1305 /
  XChaCha20-Poly1305 (`algorithm_chacha.go`) and no Argon2id
  (`kdf_argon2.go`); the registries do not hold them, so the refusal is absence
  rather than a runtime check. **The exported API is unchanged by the tag**:
  `NewSoftwareStrategyWithParams` still exists and still compiles everywhere,
  and in a FIPS build returns `ErrKDFUnsupportedVersion` because the deriver it
  names is absent — a Layer 1 library must not break a consumer's build over a
  line that consumer already guards at runtime. The wire and metadata identifiers remain defined in every build so a
  FIPS binary can still name data it cannot read. The untagged build is
  unchanged and keeps every algorithm. See
  [docs/seal-barrier/engineering.md](docs/seal-barrier/engineering.md#4-configuration);
  `make build-fips` now compiles both variants.

- **New `make verify-fips`, and `build-fips` is documented as the compile check
  it has always been.** `build-fips` only proves the tree compiles under
  `GOFIPS140`; it produces no artifact (this module is a library with no `main`
  package) and never executes a cryptographic operation, so it cannot fail on a
  non-approved primitive. `verify-fips` runs `-tags fips` under
  `GOFIPS140=latest GODEBUG=fips140=only`, which can. Its scope is every package
  except `pkg/internal/transport/...`, excluded by deny-list so new packages are
  gated by default: QUIC packet protection is arbitrary-IV AES-GCM by protocol
  definition (RFC 9001 §5.3, in `quic-go`) and OCSP `CertID` is SHA-1 by
  protocol definition (RFC 6960 §4.1.1, in `x/crypto/ocsp`). **`-tags fips`
  therefore makes go-quicraft FIPS at rest, not FIPS on the wire**; the
  transport's status is an open owner-level decision (issue #5).

### Deprecated — the plain-string Shamir share path

- **`Share.Value`, `Split`, `Combine` and `CombineUnverified` are marked
  deprecated**, each naming its replacement above. Nothing was removed and no
  exported signature changed: this release is source- and behaviour-compatible
  for every existing consumer, and `Share` remains the JSON shape shares are
  persisted and distributed in. Moving the exported signatures onto the
  protected types is a breaking change across the whole consumer chain and is
  deferred to a single coordinated wave (owner ruling, 2026-08-18); this change
  is the additive half so that new and migrated call sites have somewhere to
  go.

- `SplitBytes` and `CombineBytes` are **not** deprecated: they remain the path
  for a caller that manages its own wipeable buffers. `ProtectedShare` is for
  the caller that wants the buffer managed for it.

### Fixed - consensus safety

- **A replica that joined via snapshot transfer stopped applying the moment it
  caught up.** `raftLog.restore` moves the log's committed AND processed
  markers to the snapshot index the instant the step worker steps an
  `InstallSnapshot`, but the state machine adopts that index only later, when
  the snapshot pool finishes `RecoverFromSnapshot` and seeds `lastApplied`.
  The pool runs on its own goroutine and cannot even raise the `recovering`
  gate until it has OBSERVED the install (`awaitRecoverableLog` polls for it),
  so every instant between the install and that observation was unguarded —
  and it is exactly the window the leader fills with the entries that follow
  the snapshot.

  Committed entries extracted in that window start at `snapshotIndex+1` over a
  `lastApplied` still holding its pre-snapshot value, which is the gap the
  apply worker exists to refuse:

  ```
  ERROR committed entry gap before apply shard=1 replica=4
        last_applied=0 batch_first_index=54
  ```

  `ErrApplyGap` is terminal by design, so the new replica acknowledged the
  leader, counted toward quorum and never applied another entry.

  Both extraction sites now also gate on `snapshotAdoptionPending()` — raft's
  `processed` marker standing above `lastApplied` — read from the Peer on its
  owning goroutine, one statement before the extraction it guards:
  `processReady`'s `GetUpdate` and the commit worker's piggyback path. The
  entries stay in the log and are offered again once recovery seeds the
  marker; extraction is the only thing that advances `processed`, so
  withholding them cannot wedge the log.

  No Raft semantics changed: this is the `recovering` gate already documented
  for this path, applied from the moment the log adopts the snapshot rather
  than from the moment another goroutine notices. The gap check's
  `lastApplied == 0` case — whose escape hatch the fourth review round removed
  on the argument that "processed == 0 implies the first batch starts at index
  1 on every path" — is correct and stays. Snapshot transfer is the path that
  argument missed, and the check was reporting a real discontinuity, not
  causing one.

  Reproduced as `TestIntegration_DB_SnapshotTransferToNewNode`, which failed
  under load rather than deterministically (1 failure in 20 `-race`
  iterations); the timing decides whether the pool wins the race, not whether
  the bug is present.

## [0.6.2-alpha] - 2026-08-17

Recorded retroactively on 2026-08-18. This tag was cut without bumping
`VERSION` (which stayed at `0.6.1-alpha` through the tag) and without a
CHANGELOG entry. Both are reconciled as of `0.7.0-alpha`, and the release
workflow now fails a tag whose `VERSION` or CHANGELOG section does not match
it. The tag itself is immutable and is left as published.

### Fixed - consensus safety

- **A replica could drop committed entries it had not yet applied.** Committed
  entries are now held until the state machine adopts a snapshot (#4).

### Changed - CI

- `contrib/dragonboat` made report-only rather than a blocking gate.
- Adopted the SDLC workflows (`codex-review`, `task-worker`, `claude-review`).

## [0.6.1-alpha] - 2026-08-17

### Fixed - consensus safety

- **A follower could accept an AppendEntries that opened a hole in its log.**
  `raftLog.term()` had no upper bound: above the in-memory tail it fell through
  to the LogReader unconditionally, while `lastIndex()` prefers the in-memory
  tail whenever the buffer is non-empty. The two disagree inside one window --
  a conflicting append discards the in-memory suffix immediately, but the
  LogReader only learns of the truncation a whole commit/apply/fsync cycle
  later, and until then it still serves the SUPERSEDED terms for entries the
  replica has already thrown away.

  So `matchTerm` could pass against an entry the follower no longer held, and
  the append that followed was asked to write past its own tail. Observed in
  production shape as `invariant violated: gapped merge past in-memory tail`,
  which panicked the step worker; the worker restarted and the shard never
  regained a leader, because a peer that can trigger the condition once
  triggers it on every retransmission.

  The silent variant is the more serious one. The empty-buffer branch below
  that assertion reaches the same state through the same unbounded lookup and
  takes the hole WITHOUT asserting -- the replica then acknowledges a
  `lastIndex` covering entries it holds nothing for, and the leader counts that
  phantom match toward quorum.

  `term()` now reports `ErrUnavailable` above `lastIndex()`, restoring an
  invariant the log already claimed. That routes the message into
  `handleAppend`'s existing shorter-log branch, which was written for exactly
  this case and had been unreachable, so the leader gets a `lastIndex` hint and
  backtracks. No effect on a leader, which never truncates its own log. The
  assertion was NOT relaxed: it was correct, and the code reaching it was not.

  Reachable in production; it needs a leader change followed by a Replicate
  landing in the just-discarded suffix within one commit cycle. Load only
  widens the window. Found by the comparison benchmark under sustained
  multi-node load, which no unit, integration or fuzz gate reproduces.

### Fixed

- `pkg/internal/raft`'s `testLogDB` declared `IndexRange() == (0, 0)` -- "I hold
  nothing" -- while serving terms for indices 1..4, an impossible LogReader that
  four tests were built on. Two of them would have kept passing while no longer
  exercising the error translation they exist for. The fixture was corrected
  rather than the production semantics it misdescribed.

## [0.6.0-alpha] - 2026-08-17

Breaking in both directions against `0.5.x`. On disk, the WAL payload type bytes
were renumbered, so a WAL written by this build is unreadable by `0.5.1-alpha`
and a `0.5.x` WAL is refused here. In the API, `HostConfig.MaxApplyRetries`,
`engine.CircuitBreakerError` and `proto.Bootstrap` were removed and the
revocation checker's enforcement field was renamed and inverted, so an embedder
setting any of them no longer compiles. Wipe data directories and rebuild every
node and consuming service from one commit.

### Changed — comparison benchmarks are stored per machine profile

- **Every stored benchmark result now belongs to a named machine profile, and
  the machine's specifications are captured on that machine rather than typed
  in.** `test/performance/profiles/<name>/` holds `env.json` (captured by
  `capture-env.sh` **on the target**, at run time), `results/*.json`, generated
  `charts/`, generated `<system>.md` documents, and a hand-written
  `analysis.md`. Two profiles are tracked long-term: `omega` (single-socket,
  desktop-grade) and `r640` (dual-socket, server-grade).

  The profile name is an operator argument with no default, because it cannot be
  derived: `lscpu` describes hardware but does not know which machine is the
  reference desktop. Everything else is derived on the target, so a
  specification table beside a number can no longer go stale or be edited into
  something the hardware never was. A field the capture could not obtain is
  stored empty and renders as **not captured** — never as a zero, which would be
  indistinguishable from a measurement.

- **One command measures any machine:**
  `make perf-remote PERF_HOST=user@host PERF_PROFILE=omega`. It stages the
  working tree, captures `env.json` there, runs the suite in Docker, fetches the
  results *and* the capture back into the profile, and regenerates that
  profile's documents. This replaced `run-remote.sh`'s four disjoint
  subcommands with one pipeline; the phases remain available individually, and
  the remote run stays detached so a dropped ssh costs the log tail, not the run.

- **`analysis.md` is structurally protected.** The single document write path
  refuses that filename outright, so no generator change can reach it. Generated
  documents are disposable and say so in their own text.

- **A system with no results now produces no document at all** rather than one
  of `N/A` rows, which at a glance is indistinguishable from a real result of
  zero. A stale document for a system that is no longer measured is deleted.
  Generation still refuses to run when a system it was asked to cover is
  missing; `PERF_SYSTEMS` narrows it explicitly, and the documents it writes
  name the gap on their face. `omega` currently has no `etcd.md` for this
  reason.

- **The R640 baseline was preserved verbatim.** Those numbers were never
  captured as JSON and existed only as rendered markdown; reconstructing JSON by
  parsing the tables would have put a lossily transcribed baseline into a file
  the project treats as measured data. `test/performance/README.md`, `etcd.md`,
  `dragonboat.md` and `openraft.md` were moved byte for byte into
  `profiles/r640/analysis.md`, verified with `cmp`, and their charts moved to
  `profiles/r640/charts/` so every relative image link still resolves. The
  harness fairness audit and scenario definitions moved with them and apply to
  every profile.

- **Two `.gitignore` rules were deleting the data.** The root ignored
  `test/performance/profiles/` and the local file ignored `results/`, so stored
  results and captures were invisible to git. `make clean` also did
  `rm -rf test/performance/profiles`. All three are fixed: the profiles tree is
  data, not build output.

- Removed the `perf-compare-{etcd,dragonboat,openraft}` and
  `perf-compare-all-visualize` targets, whose `--chart-dir` paths were
  repo-relative while the command ran from `test/performance/`, so they wrote
  into `test/performance/test/performance/charts/` — `clean` had a line removing
  the resulting `charts/charts` and `results/results` directories. `perf-docs`
  and the four `perf-run-<system>` targets collapsed into `perf-run`,
  `perf-generate` and `perf-all`, each taking `PERF_PROFILE` and `PERF_SYSTEMS`.

### BREAKING — WAL payload type bytes renumbered

- **A 0.4.x WAL is now refused by name instead of misparsed.**
  `payloadTypeEntries` moved `0x07` → **`0x0b`** and `payloadTypeSnapshot`
  `0x0a` → **`0x0c`**. The retired values are left unallocated and never
  reallocated, so a record still carrying one fails replay with the standard
  `ErrUnknownPayloadType` rather than being misparsed.

  0.5.0 grew the entry record header 76 → 84 bytes (`OwnerID`) and the snapshot
  record's membership session-policy tail 24 → 32 (`MaxSessionsPerOwner`)
  without allocating new payload-type bytes, against this codebase's own
  precedent that a record layout change always takes one. Measured before the
  fix: a 0.4.x entry record decoded with `err == nil`, `OwnerID` came back as
  `0x4443424100000018` — the record's own `CmdLen` followed by four command
  bytes — and the command reached the state machine truncated from 24 bytes to
  16. A 0.4.x snapshot record usually failed as truncated, which is a corruption
  diagnosis for a version mismatch; in one realistic shape it was accepted in
  silence with a fabricated `MaxSessionsPerOwner` and a `RemovedIdentities`
  deny-list read eight bytes off, populated with a replica nobody had removed.
  That list is checked against the authenticated connection identity, so
  misreading it revokes a revocation.

  `walFormatVersion` stays at **1**. The pre-release pinning policy holds, and
  detection belongs at the record level, where the layout actually changed. A
  0.4.x directory therefore opens cleanly and fails at the first legacy record.

  **The renumbering breaks compatibility in both directions.** A WAL written by
  this build is not readable by 0.5.1-alpha either: `0x0b` and `0x0c` fail there
  with `ErrUnknownPayloadType`. Wipe the data directory and re-sync.

### Changed

- **`sm.StateMachine`'s serialization promise is enforced rather than merely
  documented.** `rsm.StateMachine.Apply` now takes the adapter's `readGuard` for
  writing, so it excludes `Lookup`, `NALookup`, `NALookupBuf` and `GetHash`. The
  promise — "QuicRaft serializes all method calls, Update and Lookup are never
  called concurrently" — was inherited from dragonboat and never implemented
  here: `Apply` and `Lookup` both took `RLock`, which excludes neither from the
  other. `apply_lookup_exclusion_test.go` pins the contract; with the old
  `RLock` it reports 10,017 overlaps, with the write lock zero.

  `GetHash` now takes the guard for reading and checks `closed`. It previously
  took no guard at all, so a replica-divergence check could hash a half-applied
  batch and report a mismatch that does not exist, and could read a closed state
  machine.

  **Embedders relying on concurrent reads during catch-up will notice this as
  read latency.** `SyncRead`, `SyncReadBuf`, `FollowerRead`, `QueryLocalNode`,
  `StaleRead` and `StaleReadBuf` all park behind an in-flight `Apply`, which on
  a catching-up follower can carry up to `maxEntriesToApplySize` (64 MiB) of
  entries, and `sync.RWMutex` has no context-aware acquire, so a read's deadline
  is not observed while it is blocked on the lock. The guard lives on the
  adapter rather than per tier, so `ConcurrentStateMachine` and
  `DiskStateMachine` get the same exclusion even though their contracts promise
  concurrent reads. Keep implementing those two tiers concurrency-safe: the
  contract is what they are held to, and the adapter may stop excluding them.

### Removed

- **`config.HostConfig.MaxApplyRetries` is gone, along with
  `config.DefaultMaxApplyRetries` and `engine.CircuitBreakerError`.** The field
  was defaulted to 100, validated (`Validate` rejected 0) and carried all the
  way into `engine.Node` — and then never read by anything. No apply-retry
  policy was ever implemented for it to configure.

  **This breaks any embedder that sets the field: the code no longer
  compiles.** That is the intended outcome. A setting that silently does nothing
  is worse than one that is absent, because the name invites operators to tune
  it during an incident and to build failure handling around retries that never
  happen. Deleting it makes the absence checkable at build time. The removal is
  compile-time only — no on-disk format, wire format or runtime behaviour
  changes with it, and a config that never set the field needs no edit.

  **Shard-failure behaviour is unchanged and was never governed by the field.**
  Apply is fail-fast: the FIRST `SM.Apply()` (or entry-decompression) error
  fails the shard, `lastApplied` is deliberately not advanced, the batch's
  pending proposals are failed, and `EventListener.OnShardFailed` fires. The
  shard stays offline until it is reloaded or recovered from a snapshot. That is
  a Raft state-machine safety requirement, not a tuning choice: a deterministic
  apply failure cannot be made to succeed by retrying, and advancing
  `lastApplied` past a batch this replica skipped would diverge it from its
  peers. `ErrShardFailed`, `*engine.ApplyError`, `*engine.SMPanicError` and
  `OnShardFailed` are all untouched;
  `host_applyfailfast_test.go` pins the policy by counting how many times a
  poisoned entry is offered to the state machine (exactly one) rather than by
  reading the engine's own flags.

  `engine.CircuitBreakerError` went with it: it was the error that retry policy
  would have reported, was never constructed anywhere, and so could not reach
  any caller. `*engine.SMPanicError` remains the type that matches
  `ErrShardFailed`.

- **`proto.Bootstrap` and its codec are gone.** The type had **no production
  caller anywhere in the repository**: nothing outside `pkg/proto`'s own tests
  ever constructed or decoded it, and no downstream consumer imported it. Its
  `Size`/`MarshalTo`/`UnmarshalFrom`/`Reset` methods, `bootstrap_test.go` and
  the `writeside_types_test.go` block that covered it are removed with it.

  **The record LogDB persists is `logdb.Bootstrap`
  (`pkg/logdb/types.go:186`), and it is unaffected.** It is written through
  `SaveBootstrap`/`GetBootstrap` in each backend's own layout, it carries a
  `DataDir` field `proto.Bootstrap` never had, and no on-disk byte was ever in
  the `proto.Bootstrap` shape. Shard bootstrap, restart and `ImportSnapshot`
  behaviour are all unchanged.

  The type's doc comment and its test both used to assert it was "PERSISTED to
  LogDB during shard initialization", which was never true of *this* type and
  made an unused encoding read as load-bearing.

  The write-side map limits the removed test pinned (`ErrAddressTooLong`,
  `ErrTooManyMembershipEntries`, plus the hand-encoded read-side refusal) are
  not lost: `checkWritableMap`, `putMap` and `getMap` are shared, and
  `writeside_limits_test.go` pins all three on `Membership`, where the same
  helpers are on the durable path.

  This is a breaking change to an exported API. Pre-1.0, and no caller exists
  to break.

## [0.5.1-alpha] - 2026-08-17

The first publishable 0.5.x. Identical library content to 0.5.0-alpha, whose
release pipeline failed its own fuzz gate, plus the three fixes that failure and
the pre-tag measurement exposed.

**This version cannot read a 0.4.x data directory.** `proto.Entry`'s header went
60 → 68 bytes, the WAL entry record 76 → 84, and the sealed-state MAC v1 → v2.
There is no in-place upgrade: wipe the data directory, or export snapshots with
0.4.x and re-import. See 0.5.0-alpha below for the full list and the migration.

`v0.5.0-alpha` is not withdrawn: Go module versions are immutable and it is
already resolvable, so the tag stays where it is rather than being moved. It
simply never completed its release pipeline. Depend on 0.5.1-alpha.

### Fixed — security

- **Certificate revocation was fail-closed only by the grace of one caller.**
  `revocation.Config.EnforceRevocation` is renamed to **`AllowSoftFail`** with
  the inverted sense, matching the name and meaning of the operator-facing
  `config.RevocationConfig.AllowSoftFail`. The zero value now **enforces**:
  a `revocation.Config` nobody configured rejects a peer whose status cannot be
  determined, and soft-fail has to be asked for.

  Before this, the safe state was the one you had to remember. `Host` set
  `EnforceRevocation: !rc.AllowSoftFail` when it built the transport config, and
  that single line was the whole reason deployments failed closed; every other
  construction path — an embedding of the transport, a tool, a test standing in
  for one — silently got soft-fail, which admits a peer whose certificate may
  have been revoked. The two layers also disagreed by name, so the mapping was a
  negation that could be dropped or inverted without anything noticing.

  **Deployments configured through `Host` are unaffected**, in either posture:
  `Host` now copies `AllowSoftFail` across instead of negating it, and the
  resulting checker behaves exactly as before. **Embedders that construct
  `revocation.Config` directly move from soft-fail to fail-closed** unless they
  set `AllowSoftFail: true`. The field lives in an internal package, so this is
  a source-compatible change for anyone using the public API.

- **`ModeCRL` with no CRL file consulted nothing and reported every certificate
  as not revoked.** `checkCRLStatus` can only report staleness when `CRLPaths`
  is non-empty, so under enforcement a checker in `"crl"` mode with no path
  returned `(false, nil)` for every peer — no error, no log, no metric — while
  `"ocsp"`, `"both"` and `"any"` all failed closed in the same situation.
  `IsRevoked` now returns `ErrNoRevocationSources` there, matching the `"any"`
  branch it was already meant to parallel. `Config.Validate` rejects the
  combination at startup; this is the runtime backstop for a checker built
  without it, or whose paths were lost in a refactor. A verdict the checker
  actually holds — a programmatic `AddRevokedSerial` — still wins over the
  missing-source error.

### Fixed

- **Proposals to a failed shard are refused immediately instead of timing out.**
  Once the shard failure latch trips — an `SM.Apply` error or a state-machine
  panic — the step worker skips the node, but the propose path did not check.
  Every later proposal was accepted, registered as pending, delivered to an
  inbox nothing drains, and answered only when the sweeper reclaimed it at its
  deadline. A permanently dead shard therefore presented as a slow one, and
  `HealthCheck` was the only place that told the truth. `Host.proposeEntry` now
  returns `ErrShardFailed` as soon as the latch is set.

  `ErrShardFailed` remains deliberately absent from `Temporary`: the shard stays
  failed until it is reloaded or recovered, so a retry loop against it would
  never terminate. The identity of the *first* failing proposal is unchanged —
  it still carries the state machine's own error.

- **A fuzz harness built a membership its own decoder would refuse.**
  `FuzzMembershipRoundTrip` clamped an address to exactly `MaxAddressLength` and
  then derived identity values as `"spki:" + addr`, five bytes over — and
  `Identities` is bounded on both the read and write sides. It surfaced only now
  because this round gave `Membership.MarshalTo` the write-side bound it lacked;
  before that the encoder silently produced frames it could not parse back. The
  minimised input is committed as a corpus seed.

- **The release fuzz gate failed on its own scheduling, and could not be run
  locally.** Go's fuzzing coordinator can lose its shutdown RPC and report a bare
  `context deadline exceeded` after the full budget has elapsed, with no crash
  and no reproducer — indistinguishable from a finding at a glance. It reproduces
  on an idle host running one target at a time, so the previously assumed cause
  (cross-job contention) was wrong.

  The gate now separates the two on the signal Go itself gives: a crash always
  writes a reproducer, so `Failing input written to` fails immediately and is
  never retried, while the no-reproducer deadline case is retried and anything
  else fails as unrecognised. A real crash cannot be retried away, because the
  retry is conditioned on the absence of the artefact a crash produces.

  It also moved out of inline workflow shell into `make ci-fuzz-mutation`, so the
  gate that blocks a release can be reproduced by the developer whose release it
  blocked, and it now runs as part of `make ci-full`.

- **19 of the 21 `errfs` fault-injection tests could not inject on a flush.**
  They ran with `WithNoSync(true)`, so no fault could land there, and the sweeps
  self-calibrated onto whichever operation happened to fail instead. A broken
  injector does not announce itself: it injects nothing, and every suite built on
  it passes against a healthy system.

### Added

- **Unit tests for the fault injectors themselves**, in the ordinary build rather
  than behind the tags of the suites that consume them. `errfs`: that `injectAt`
  fires on the Nth operation exactly, that `OnlyOps` restricts which operation can
  fire, that `Times` bounds injection so a recovery path is observable, that
  `ShortWrite` disturbs the medium before failing, and that a misspelled operation
  name panics rather than matching nothing. `netfault`: the directional property
  asymmetric partitions depend on, that `Isolate` cuts both directions, that
  `Partition` cuts across the split without cutting within either side, that
  `Heal` clears everything, that cuts are node-level rather than per-shard, and
  that the copy-on-write publication is race-free against the send path.

  This is also what restored the release coverage floor. The round's production
  changes had taken the module total to 90.7%, below the 91% gate — found by
  measuring before tagging rather than by a failed release — and it is back above
  the floor by covering code that needed it rather than by lowering the threshold.

## [0.5.0-alpha] - 2026-08-17

A full-codebase review and remediation round: 6 critical, 31 high and roughly 60
medium/low findings closed across 42 commits. Every fix was mutation-verified —
the defect it closes was re-introduced and the test watched to fail — and the
whole gate runs green, including a new network-partition suite.

**A minor bump, not a patch, because on-disk and wire formats changed in place.**

### BREAKING — existing data will not load

Formats are pinned at v1 until production release, so layouts change in place
rather than gaining version negotiation. A 0.4.x data directory is not readable
by this version.

- `proto.Entry` header 60 → 68 bytes (`OwnerID` at +56)
- `proto.SessionPolicy` 24 → 32 bytes (`MaxSessionsPerOwner`)
- `proto.Snapshot`: `Compression` added; `Type`, `Imported` and `Witness`
  removed — all three were written or reserved and never read
- WAL entry record 76 → 84 bytes; WAL membership tail 66 → 74 bytes
- `EntryOverheadBytes` 76 → 84
- Sealed barrier state MAC v1 → v2 (`sealedStateMACVersion`), so that the new
  load-generation counter is covered. This one migrates in place; the rest do not

**Migration:** wipe the data directory, or export snapshots with 0.4.x and
re-import. Sealed barrier state migrates in place: `sealedStateMACVersion` moved
1 → 2 to cover the new load-generation counter, and a pre-fence blob is
recognised once and restamped automatically on the next persist.

### Fixed — safety

- **`InstallSnapshot` installed its sender as leader with no term or leadership
  check**, above the stale-snapshot rejection. Any replica could replace another
  replica's log and suppress its elections. `handleHeartbeat` already gated
  exactly this on a strictly weaker message.
- **ReadIndex confirmed linearizable reads with a heartbeat context the leader
  never issued.** One stale or forged response acknowledged every pending read,
  including reads whose confirming heartbeat had not been broadcast — with the
  leader's own self-ack, already a quorum of two.
- **An update lost between extraction and `SaveState` left the peer claiming
  durability it did not have.** Because `ReadOnly` is derived from that diff, the
  next update skipped the WAL as well: a vote answered from memory and never
  written.
- **A membership in a peer's snapshot was unbounded**, so 65 voters panicked the
  step worker and every shard it owned. Reachable despite the origin gate,
  because the receive path rebuilds the message locally.
- **An over-cap membership already persisted panicked the node on every
  restart** with no way out. It now fails that one shard; the host stays up.
- **`configChangeIndex` could regress**, permanently wedging membership changes
  on the shard. Three writers, only one monotonic.
- **`handleTimeoutNow` burned a term it could not campaign with**, unseating a
  healthy leader for nothing.
- **`ReconstructMembership` silently skipped an undecodable committed config
  change**, letting two replicas rebuild different memberships from one log.

### Fixed — durability and data loss

- **A re-delivered `InstallSnapshot` truncated the live snapshot** before
  validating anything, and the failure path deleted a directory it had not
  created. Receives now stage and publish by rename.
- **A rollback truncation was not fsynced**, so a crash could resurrect bytes
  from a batch the caller was told had failed.
- **`FileSize` was recorded with every snapshot and never verified**, so a
  truncated file recovered as if complete.
- **Snapshots recorded a membership from the future** — folded at commit time,
  indexed at apply time.
- **A `SaveState` at or below the compaction floor destroyed the live suffix**;
  both backends now reject it.
- **Read paths handed callers live aliases** of the log served to followers.
- **memdb `Compact` could drop entries, hard state and snapshot** with the whole
  conformance suite green. Losing hard state that way is a lost vote.

### Fixed — security

- **Decode amplified 9.77× on membership-dense frames**, and the receive budget
  charged wire bytes, so a 256 MB limit admitted frames occupying ~2 GB. Bounded
  at 8× and charged accordingly.
- **Two barriers loaded from one sealed blob derived the same DEK** with
  independent nonce counters. A load-generation fence makes the second loader a
  loud failure.
- **The nonce tracker disabled itself at its ceiling** — reporting zero tracked,
  and returning nil for a repeat of a nonce it still held. It now saturates.
- **`Encrypt`/`Decrypt` had no length bound**; an oversized frame panicked inside
  `crypto/cipher`.
- **One peer could pin the entire snapshot receive budget** — the cap bounded
  slots, not bytes.
- **Inbound connections from removed peers were unreclaimable**, and close
  reason phrases leaked the authorization verdict to the peer.
- **One embedder could exhaust a shard's session table.** Sessions are now capped
  per embedder-supplied `OwnerID`, enforced from replicated state.

### Added

- **Quorum-loss recovery.** `WithImportMembership` reconstitutes a shard whose
  quorum is permanently lost — previously impossible, since removing a dead
  replica requires the quorum that is gone. Deliberately unsafe and documented as
  such, with a runbook.
- **Per-follower replication progress** on `ShardInfo.Replicas` (match, state,
  paused, active, inflights) — previously reachable only by parsing a debug
  string.
- **A real network-partition suite.** Every prior "partition" test killed a
  process, so a live-but-deposed leader could not be constructed anywhere.
  Weakening the election quorum to `n/2` passed 18 of 19 linearizability tests;
  it is now caught in 1.1s.
- **`make no-mutants`**, guarding against mutation-testing edits reaching a
  commit. It exists because one did: an `if false` disabled a durability fsync
  and every suite stayed green, since an unrestored mutation makes tests pass.
- **`make remote-<target>`**, running heavyweight suites on a dedicated host.

### Fixed — tests that could not fail

The dominant defect class this round. Among them: the election-safety test passed
with one-vote-per-term removed entirely (5/5); persist-before-respond had four
tests and none could fail; both determinism sorts in the session encoder were
deletable with the suite green; membership tests asserted one replica, so a
config change failing on a follower was invisible; `make security` reported "All
security scans passed" with a scanner absent; and the coverage gate passed with
no coverage data at all.

## [0.4.2-alpha] - 2026-08-15

Release-workflow fixes only. Nothing outside `.github/workflows/` differs from
0.4.1-alpha, so the library, its formats and its behaviour are unchanged and
0.4.1-alpha remains a perfectly good version to depend on — it resolves and
builds. What it did not do is get through its own release gate.

### Fixed

- **The release gates all ran at once, and the runner could not carry them.**
  Every gate hung off `needs: validate`, so the integration suite — multi-node
  QUIC clusters under `-race`, with waits measured in seconds — was fanned out
  beside six fuzz gates, the errfs suite and the module build on one 20-core
  self-hosted runner.

  This produced three failures wearing three different disguises across two
  release attempts: a 30s rolling-restart wait, a 30s snapshot-catch-up wait,
  and `context deadline exceeded` from a fuzz gate that had run at 79,000
  exec/s and completed its full budget before dying in shutdown. None was a
  defect in the code under test. The last is the one worth naming: it is
  indistinguishable from a crash at a glance, on the gate whose entire purpose
  is to report crashes.

  The integration gate now runs alone, after the cheap gates, and the fuzz
  matrix is capped at three concurrent jobs — Go's fuzzing engine starts one
  worker per CPU, so six jobs schedule ~24 workers on 20 cores by themselves.
  `ci.yml` got the same treatment in the same release for the same reason.

## [0.4.1-alpha] - 2026-08-15

Repairs to the release and CI gates, plus one behaviour fix those gates
uncovered. The formats-and-safety notices in the 0.4.0-alpha entry still apply
in full, and there is no format change here, so consumers move from
0.4.0-alpha to this without changing their own code.

0.4.0-alpha never published. Its release run failed three gates, and the
`Publish` job correctly refused to run — the tag exists and resolves, but the
gates behind it did not pass. This version is the same tree with those three
causes fixed. It is a NEW version rather than a moved tag because go-qrdb had
already recorded 0.4.0-alpha in its `go.sum`, and repointing a tag it has
checksummed would fail its build with a security error.

### Fixed

- **The multi-module CI gate had never built a nested module.** It fanned out
  over `matrix: ${{ fromJSON(needs.discover-modules.outputs.modules) }}`, which
  the runner does not expand: it scheduled one job literally named
  `Module ${{ matrix.module }}` whose `working-directory` interpolated to the
  empty string, so it rebuilt the repository root and reported success. Five
  modules were enrolled and none were compiled. Both workflows now use a shell
  loop, collect failures instead of stopping at the first, and fail if
  discovery returns fewer modules than exist — the defect was a gate quietly
  verifying nothing, not a red build.

- **`test/performance/dragonboat` could not build on a clean checkout.** It
  resolved its comparison engine through
  `replace github.com/lni/dragonboat/v4 => ../../../../dragonboat`, a path
  outside the repository, so it compiled only where the fork happened to be
  checked out four levels up. The fork is published as
  `github.com/jeremyhahn/dragonboat/v4 v4.0.1` and is now required by version
  and imported under its own name. Everything that existed to work around an
  unpublishable dependency — the staged `.build-cache` copy, the Dockerfile's
  rewrite of the `replace`, `perfcompare`'s staging hook and `DRAGONBOAT_SRC` —
  is gone with it.

- **The seal fuzz targets spent their entire budget on a key derivation they do
  not test.** Each built a barrier per input with Argon2id at 64 MiB, holding
  the package to ~50 exec/s and then to 0 exec/s as the GC fell behind; the
  release gate's bounded mutation pass failed with `context deadline exceeded`,
  which reads like a crash and was not one. The work factor bought nothing
  because the suite runs with the weak-parameter bypass enabled. At 1 MiB the
  same ten-second budget goes from ~500 executions to ~185,000.

### Fixed (behaviour)

- **`Propose`, `ProposeWithSession` and `ReadIndex` now reject a context that
  is already done, instead of submitting the request anyway.** They validated
  closed, draining, witness, disk-full and payload size and then submitted, so
  a request the caller had already abandoned still entered the raft log.

  The visible symptom was worse than the omission. `SyncPropose` waits in a
  `select` over the result channel and `ctx.Done()`; when a shard commits
  before the cancellation is observed both cases are ready, and Go chooses
  among ready cases uniformly at random — so with an already-canceled context
  the call returned success or `ErrCanceled` by coin flip. It surfaced as a
  unit test that failed on a loaded CI runner and passed forty consecutive
  runs locally.

  This narrows what `ErrCanceled` means rather than changing it. Cancellation
  racing an in-flight proposal remains undecidable, and `ErrCanceled` still
  never proves the entry did not commit; a context that was already done
  before the call is not a race, and now neither submits nor reports success.

### Changed

- A replication wait that times out in the host and e2e integration suites now
  prints each live node's view of the leader, its term, applied index, local
  log span, queue depth and health. A single timeout line could not distinguish
  a leaderless cluster from a wedged replica from a slow runner, and the one
  such failure observed on a release runner did not reproduce in ~30 local runs
  under contention, so it could not be interpreted at all.

## [0.4.0-alpha] - 2026-08-15

**This release breaks both the wire format and the on-disk format, and the
version numbers do NOT record it.** Every wire and persisted format is now
pinned at **version 1** and stays there until the first production release —
`proto.WireVersion`, the `proto.Membership` section byte, the transport frame
header, the `waldb` snapshot-record membership section and the `WALFORMAT`
marker alike. The LAYOUTS changed underneath those numbers.

- Pre-release peers and on-disk data written by any earlier commit are
  **incompatible**. There is no rolling upgrade, no migration and no dual-read
  path.
- **Dev clusters must be wiped and re-synced.** Stop every node, replace every
  binary, delete every data directory, start fresh.
- Consuming repositories (go-qrdb, go-xkms, go-quicbaas, go-trusted-ca) are
  updated **in lockstep** with the new layout, at the same version.
- **Two binaries from different pre-release commits may be mutually unreadable
  while both stamp version 1, and that is intended.** The failure is a decode
  or recovery error, not a version mismatch that names itself — so do not rely
  on the version gate to catch a commit skew. Build everything from one commit.

What actually changed in the layouts: `proto.Membership` now carries **six
maps**, including `RemovedIdentities`; `proto.Snapshot.Checksum` was
**removed**; and write-side limits were added so the encoders refuse what their
own decoders would reject.

The version numbers increment for the first time at the first production
release. From then on a bump means what it normally means.

Read the two runtime-only breakages below before upgrading: sealed-state
`state_mac` and session-managed proposals on on-disk state machines both
compile cleanly and fail at run time.

### BREAKING

- **Every format version is PINNED AT 1 for the whole pre-release period, and
  every gate on it is EXACT equality.** `proto.WireVersion` is 1,
  `proto.Membership`'s section version byte is 1, `waldb`'s snapshot-record
  membership section version is 1, `waldb`'s `WALFORMAT` marker version is 1
  (**was 2**), and `transport.WireVersion` remains 0x01. The multi-version
  machinery is gone: no supported range, no per-version decode path, no legacy
  layout constants.

  The `WALFORMAT` change is the one an operator can trip over without touching
  a wire: a data directory written by any earlier build records version 2 and
  is now refused with `*WALFormatVersionError` naming 2 and 1. That is the
  intended outcome — the chunk framing is unchanged (11 bytes,
  `[CRC32:4][Len:2][Type:1][DurSeq:4]`), but the pinning policy renumbers the
  marker, and reading a directory whose marker this build does not recognise is
  exactly the misparse-presented-as-a-repair the marker exists to prevent.
  **Wipe the WAL directory and re-sync.**

  `transport.ValidateFrameHeader` gates the frame version byte on
  `!= WireVersion` rather than on the range `>= 1 && <= WireVersion`. The two
  accept the same single value today; the range form would have widened into a
  compatibility window at the first bump, with no test failing to announce it.
  The rejection is unchanged: `ErrUnsupportedVersion`.

  The policy, stated as a rule. A layout change during pre-release is made **IN
  PLACE, at version 1**; the consuming repos are updated in the same change to
  read and write the new layout at the same version; and dev clusters are wiped
  and re-synced rather than migrated. There is no deployed data and no deployed
  peer whose bytes anyone has promised to keep reading, so carrying a decode
  path for every development layout bought nothing and cost a version number
  per change. The numbers increment for the first time at the first production
  release, and from then on a bump means a decoder taught the old layout, a
  migration story, and a supported window that widens rather than moves.

  **The version byte itself is KEPT.** Only its value is pinned. It is the
  mechanism that makes the 1.0 bump possible — a format with no in-band
  discriminator cannot be superseded, which is exactly the state `Membership`
  was in before it grew a leading byte — and it turns a commit skew into a
  clean refusal (`ErrUnsupportedVersion` /
  `*UnsupportedMembershipVersionError` / `ErrUnsupportedMembershipVersion`)
  rather than fields read at the wrong offsets.

  **The transport version remains a SUPREMUM over the persisted ones.**
  `Membership` rides the wire inside `Snapshot` → `Message` → `MessageBatch`
  and directly inside `SnapshotChunk`, both of which gate on `BinVer` by exact
  equality, so a Membership layout change alters the transport bytes without
  altering the discriminator meant to describe them. The compile-time assertion
  `WireVersion >= membershipFormatVersion` is retained: it holds trivially at
  `1 >= 1` and is what stops the two drifting apart at the 1.0 bump. The two
  constants stay separate and neither is derived from the other — if the
  persisted version were `uint8(WireVersion)`, the first post-1.0
  transport-only bump would change the version byte every replica expects at
  log index 1 and a released cluster could not replay its own log.

- **The `proto.Membership` LAYOUT changed: six maps, including
  `RemovedIdentities`.** The layout, at version 1, is
  `[Version:1][ConfigChangeID:8][MaxWitnesses:8][RequireFullReplicaMajority:8]`
  `[SessionExpiryEntries:8][MaxTotalSessions:8][MaxSessionsPerClient:8]` then
  `Addresses`, `Observers`, `Witnesses`, `Removed`, `Identities`,
  `RemovedIdentities`. Fixed region 49 bytes; an empty section is 73. Every map
  is written unconditionally, so an empty map is four zero bytes rather than an
  absent field, and an unset session policy is 24 zero bytes rather than an
  absent one.

  `RemovedIdentities` maps each replica ID in `Removed` to the node identity
  the configuration held for it **at the moment of removal**. It exists
  because the removal deny-list has to be answerable against the
  AUTHENTICATED peer identity and not the CLAIMED `msg.From`. Deleting the
  identity along with the member left nothing in replicated state to ask that
  question of, so a removed replica evaded its own tombstone simply by naming
  a different replica ID — it still holds its private key, so it still
  authenticates. Entries are **never evicted**: no eviction policy exists
  that is not also a re-admission policy. The map adds no new growth axis —
  its key set is a subset of `Removed`'s — and `MarshalTo` refuses to write
  past `MaxMembershipEntries` so an operator gets
  `ErrTooManyMembershipEntries` on the config change rather than a membership
  that is durable and unreadable.

  `Membership.UnmarshalFrom` is **all or nothing**: it decodes into a local and
  assigns the receiver only on success, so a failed decode cannot leave a stale
  `Identities` map (which would authorize a replica the decoded configuration
  never named) or a stale `RemovedIdentities` map (which would deny a sender it
  never tombstoned).

  The `waldb` snapshot-record membership section carries the same fields in its
  own layout — policy flags in one byte where proto spends eight, and the
  session policy as a tail after `Identities` rather than in the fixed region.
  Its fixed region is 18 bytes and an empty section is 66, which is also the
  read floor replay enforces before it looks at the version byte; the two are
  equal by construction now that every field is unconditional.

- **`proto.ConfigChange` collapsed to ONE fixed layout, and a truncated config
  change is now REFUSED instead of silently downgraded.** The type is
  length-discriminated no longer: every field, including the trailing
  `NodeIdentity`, is mandatory, and `UnmarshalFrom` returns `ErrBufferTooSmall`
  for any payload that does not span the whole layout.

  This is a security fix, not only a cleanup. A payload ending where the
  identity's length prefix begins used to decode with `err == nil` and an EMPTY
  `NodeIdentity` — a 65-byte strict prefix of a valid 108-byte encoding parsed
  cleanly — and an empty identity is precisely the compatibility state that
  falls back to the weaker address-based authorization at the receive gate
  (`membership.recordIdentity` records nothing for it). A **truncated**
  `ConfigChange` was therefore byte-indistinguishable from a legitimate
  identity-less one, and truncation silently downgraded the check the identity
  exists to raise. Length is not a safe discriminator for a field whose absence
  weakens an authorization decision.

  The other affordance went with it: a payload ending after `Initialize` used to
  be reported as a pre-witness-policy layout with `ErrUnsupportedVersion`. Under
  the pinning policy there is no other layout to name, so it is simply short.

  **Nothing an in-tree writer emits changes.** `MarshalTo` always wrote the
  trailing 4-byte length prefix, so an identity-less change — `RemoveNode`,
  `UpdateWitnessPolicy`, or an add made without `RequestAddNodeWithIdentity` —
  is spelled with an explicit zero-length prefix and still round-trips. What is
  refused is a payload no encoder in this repository produces.

- **Write-side limits were added to `Membership.MarshalTo`.** It now returns
  `ErrTooManyMembershipEntries` or `ErrAddressTooLong` for anything its own
  decoder would reject. That asymmetry was reachable through a legal API call
  with a durable consequence: `Host.StartShard` copies the caller's members map
  straight into `Addresses` and the result is committed as the
  `EntryInitialMembership` at log index 1, so the next start decoded its own
  bootstrap entry, refused it, and aborted permanently.

- **`proto.Snapshot.Checksum` was REMOVED.** The layout is now
  `…[FileSize:8][Type:8][Imported:8][OnDiskIndex:8][Witness:8][Epoch:8]`.
  The field rode the wire through an earlier pre-release layout and was **never
  computed and never verified anywhere in the repository** — empty on every real snapshot,
  costing the receive path one allocation and two copies per message to keep
  an empty slice alive. It was removed rather than implemented because a
  field named `Checksum` that is never checked reads as an integrity control
  to anyone auditing the format, and an integrity control that does not exist
  is worse than an absent one. Doing it properly is not a codec change: the
  value must be computed over the FINAL artifact bytes (post-compression,
  post-encryption) and verified in three separate consumers (recovery,
  streamed receive, `ImportSnapshot`), so it belongs in its own change.

  Stated plainly, snapshot integrity now rests on TLS in transit and barrier
  AEAD at rest. A shard configured with a plaintext barrier (`seal.None`) has
  **no** at-rest integrity check on its snapshot artifacts. That was equally
  true while the field existed; removing it makes the gap visible instead of
  apparently covered.

  `proto.MaxChecksumLen` and `proto.ErrChecksumTooLong` are removed with it.

- **Sealed-state blobs now carry a `state_mac` and are REFUSED without one.**
  *(Source-compatible; breaks at RUNTIME on first unseal.)* The wrapping AEAD
  covers only `Ciphertext` — its AAD is the constant strategy name — so every
  other field of a `SealedRootKey` sat in plaintext beside it, bound to
  nothing. The epoch is the field that matters: rewriting it downward
  resurrects an already-spent DEK with a freshly zeroed invocation counter
  and an empty nonce tracker, giving two independent random-nonce sequences
  under one key.

  `SealedRootKey.StateMAC` is an HMAC-SHA256 over a canonical,
  length-prefixed encoding of `(Strategy, Version, DeploymentID, Epoch, Salt,
  KnownEpochs, HKDFVersion, Algorithm, Ciphertext, Metadata, Nonce,
  HardwarePayload)`, keyed from the root key. The ciphertext is covered even
  though it authenticates itself, so that genuine metadata from one blob
  cannot be paired with a genuine ciphertext from another. It is
  stamped by `Initialize`, `Rotate` and `PurgeEpochsBefore`, and verified by
  `Unseal`. A blob written by a pre-MAC build carries none, and `Unseal`
  fails closed with `ErrSealedStateUnauthenticated` — accepting an absent MAC
  is a downgrade an attacker triggers by deleting the field.

  **An embedder that re-wraps the root key itself must stamp the result**, and
  gets two entry points to do it. A `SealingStrategy` produces only the
  ciphertext and its own strategy fields, so a password change, a rekey or a
  Shamir re-split assembles the new blob outside the barrier — and an
  unstamped blob is refused by its own next `Unseal`, indistinguishable from
  a stripped one:

  - `(*Barrier).StampSealedStateMAC(*SealedRootKey) error` — requires the
    barrier unsealed, because the root key is the MAC key.
  - `StampSealedStateMACWithRootKey(rootKey []byte, deploymentID uint64,
    *SealedRootKey) error` — for a caller holding the plaintext root key with
    no unsealed barrier. This is the import case, which cannot satisfy the
    method's precondition: a replica seals a leader-exported root key and
    persists the blob *before* any barrier exists to unseal, and that
    persisted blob is what the subsequent `Unseal` reads.

  Both stamp LAST, after every covered field has its final value. Stamping
  before the ciphertext is final produces a blob that fails its own
  authentication.

  **One-start migration:** set `BarrierConfig.AllowUnauthenticatedSealedState
  = true`, start once (`Unseal` proceeds and logs a warning, then restamps
  the MAC), re-persist `SealedRootKeyData()`, and clear the flag.

  Two limits, stated: `FailedAttempts`/`LastFailedUnixNano` are NOT covered
  (they are written on a failed unseal, where no root key exists to re-MAC
  with), and a MAC cannot detect **replay** of an older genuinely
  authenticated blob — `BarrierConfig.MinEpoch` is the other half of that
  defence.

- **Session-managed proposals now return `ErrSessionsUnsupportedOnDisk` on
  on-disk state machines.** *(Source-compatible; breaks at RUNTIME.)* This is
  the change most likely to surprise: nothing fails to compile, and code that
  worked before now returns an error on first call. It affects
  `GetNewSession`, `RegisterSession`, `ProposeWithSession`,
  `SyncProposeWithSession` and `CloseSession` on any shard whose state
  machine is an `sm.DiskStateMachine`. **NO-OP sessions still work** — use
  `Host.GetNoOPSession` and make the operations idempotent, which is the same
  constraint dragonboat places on `IOnDiskStateMachine`.

  It was permitted before and silently diverged the cluster. The client
  session table is replicated state that lives only inside the snapshot
  stream, so it is restored by `RecoverFromSnapshot` — and an on-disk state
  machine whose `Open()` returns a durable applied index never takes that
  path on restart, because recovering would roll the user's database back to
  the snapshot index. The replica came up with the correct data and an EMPTY
  session table: a session-managed proposal was then applied by the peers
  that still held the session and answered `ResultSessionExpired` by the
  restarted replica, which never applied the write at all, while the client
  saw success from the leader's own apply.

  The rejection is deliberately a local, API-boundary decision. It reads only
  the state machine's own type — fixed for the life of the shard by the
  `sm.CreateFunc` passed to `StartShard` — and never replicated state, so it
  cannot be evaluated differently on two replicas.

- **Under FIPS mode, an Argon2id-wrapped blob cannot be unsealed.**
  `DeriveKey` returns `ErrKDFNotFIPSApproved` for `KDFArgon2id` while FIPS is
  active, and unsealing re-derives the same wrapping key. Migrate by
  unsealing once with FIPS **off** and re-sealing with
  `NewSoftwareStrategyFIPS` (PBKDF2-SHA256), which stamps
  `KDFPbkdf2SHA256` into `SealedRootKey.Metadata` and unseals identically in
  either mode.

- **`seal.BarrierConfig.NonceTracking` is now `*bool`.** As a plain `bool`
  the zero value meant "off", so every hand-built `BarrierConfig` silently
  constructed a barrier with NO nonce tracker at all. As a pointer the zero
  value means "unset, use the default", and **nil defaults to true**. Read
  the effective value with `GetNonceTracking()`; disable deliberately with
  `BoolPtr(false)`.

- **`StopShard` and `Host.Close` return errors they previously swallowed.**
  `Close` in particular now reports a drain that timed out: returning nil
  after abandoning in-flight work told the embedding application its shutdown
  was clean when proposals it had accepted were dropped.

- **`batch.EncodeBatch` returns `([]byte, error)`** (was `[]byte`).

- **`raft.Peer.SetProcessedIndex` returns `(uint64, error)`.** Internal
  (`pkg/internal/raft`); listed for completeness, not part of the public API.

### Added

- `proto.MaxMembershipEntries` is now **exported** (10000), joining
  `MaxMessagesPerBatch` and `MaxEntriesPerMessage`. These are properties of
  the wire format, not of the decoder: `Host.StartShard` takes the operator's
  members map straight into `Membership.Addresses` and commits it at log
  index 1, so a caller that exceeds the cap would write a bootstrap entry no
  binary can read back. Producers MUST bound themselves by these values.
- Write-side limit enforcement across `pkg/proto`. `MarshalTo` now mirrors
  the read side's bounds on `Membership`, `ConfigChange`, `Entry`,
  `MessageBatch` and `Snapshot`, so the package can no longer emit a payload
  it refuses to parse back. For the persisted types that asymmetry was a
  durability bug, not a style complaint.
- `SECURITY.md` (vulnerability reporting policy) and `CONTRIBUTING.md`.

### Changed

- `Membership.UnmarshalFrom` is now **all or nothing**: decoding runs into a
  local value and the receiver is assigned only on success. Previously a v3
  decode that failed early kept a STALE `Identities` map — which would
  authorize a replica the decoded configuration never named — and a v4 decode
  that failed in the maps left a NEW session policy on an OLD configuration.
  Every call site abandoned the value on error, so neither was reachable; the
  invariant is now a property of the decoder rather than of its callers.
- `enclave.InitMemguard` was **deleted**. It had no callers, and calling it
  would have been actively harmful: `memguard.CatchSignal` invokes
  `signal.Reset()` with no arguments, detaching EVERY signal handler in the
  process including the embedder's own, and its handler calls `core.Exit(1)`
  — turning SIGTERM from a graceful shutdown into an immediate exit with no
  engine quiesce, no `Host.Close`, no barrier seal and no WAL flush. No
  signal handler is installed by this library. An embedder that wants
  signal-triggered wiping must install it in its own `main`, alongside — not
  instead of — its graceful shutdown path.

### Documentation

This release corrects documentation that had drifted far enough from the code
to produce broken integrations. The corrections are listed because several
were actively misleading, not merely stale.

- **Nine binary-layout specs were regenerated from the encoders.**
  `MessageBatch` omitted `SourceEpoch`, so every documented offset past byte
  16 was wrong (the fixed portion is 24, not 16) — in `wire-format.md`,
  `transport/quic.md` and `architecture/overview.md`. `Membership` was stale
  by multiple revisions. The WAL snapshot record was documented at 60 bytes
  without `Compression` (it is 62, with `[Epoch:8][Compression:2]
  [FilePathLen:4]`). `snapshot.meta` was documented as 60 bytes / 6 fields;
  it is 72, and 60 is not even a legacy size (40/48/56 are). `ConfigChange`
  ended at `Initialize`, a payload the decoder EXPLICITLY REJECTS as legacy.
  `SnapshotChunk` omitted `Epoch`, so a reader built to that table reads the
  chunk length prefix out of the epoch field. `WireVersion` was documented as
  1 with a `<=` gate.
- **The entry type tables omitted `EntryMetadata` (3) and
  `EntryInitialMembership` (4)**, and the config-change table omitted
  `UpdateWitnessPolicy` (4). The first is the sharpest: a reader built to
  that table rejects the bootstrap entry at log index 1 and cannot replay any
  shard's log.
- **Context-AAD composition was wrong for both consumers.** Snapshot frames
  bind a 39-byte big-endian context (`"QRSNAP1"(7) || shardID:8 ||
  replicaID:8 || index:8 || seq:8`); WAL records bind a 30-byte one
  (`"QRWAL1"(6) || shardIdx:8 || segID:8 || recordSeq:8`). Anyone writing a
  re-encryption tool against the old spec failed authentication on every
  frame. The docs also described both as opt-in; both are threaded.
- **The WAL chunk header was documented in the pre-v2 format.** It is
  `recordHeaderSize = 11` — `[CRC32:4][Length:2][Type:1][DurSeq:4]` — with
  the CRC covering `[Type][DurSeq][Data]` and `maxRecordDataLen = 65535`.
  (`storage/wal.md` had this right; `security/encryption.md` did not.)
- **`sm.Hasher` and `sm.NALookup` were documented with THREE-value
  signatures** in `architecture/overview.md`. They are two-value:
  `GetHash() (uint64, error)` and `BytesLookup(key []byte) ([]byte, error)`.
  The three-value shape is an internal `pkg/internal/rsm` wrapper. Because
  detection is by type assertion, an implementer of the documented signature
  did not satisfy the interface, got NO compile error, and had their
  hash-verification or zero-alloc path silently never invoked.
- **`storage/wal.md` omitted 12 fields from the `logdb` type listings**,
  including the four it elsewhere calls load-bearing — `Entry.Key`,
  `ClientID`, `SeriesID`, `RespondedTo`. An alternative `logdb.LogDB` backend
  written from that list silently disables session deduplication and
  downgrades sender authorization. Also missing: six `Membership` fields
  (seven with `RemovedIdentities`), `Snapshot.Compression`, and
  `Bootstrap.DataDir`/`DataDirRecorded`.
- **`security/encryption.md` claimed Shamir `Combine` performs a SHA-256
  integrity check. It performs none.** There is no `Digest` field, `Combine`
  is a one-line delegation to `CombineUnverified`, and `IntegrityError` is
  marked never-returned and retained only for API compatibility. A digest
  would be an offline brute-force oracle against low-entropy secrets. The
  caller MUST authenticate the reconstructed secret out of band; an
  integrator trusting the old text silently accepted a garbage root key.
- **`pkg/enclave` was described as Apple Secure Enclave / Intel SGX
  integration.** It is a memguard wrapper; neither is supported anywhere in
  this repository.
- **A `WriteVersion` rolling-upgrade procedure was documented for an API that
  does not exist** — `grep -rn WriteVersion` returns zero code hits — and the
  real gate is exact equality, which makes the mixed-version state that
  procedure creates impossible by construction. An operator following it took
  an outage. Deleted, with a pointer to `operations/compatibility.md`.
- **Shutdown and startup ordering were documented backwards** in
  `operations/deployment.md` and `operations/lifecycle.md`. The engine stops
  FIRST (its step workers are the only producers of snapshot sends, so
  stopping them is what eliminates the illegal `WaitGroup.Add`-concurrent-
  with-`Wait` race) and the transport stops LAST, after those sends drain.
  Startup mirrors it: the transport starts last, after all wiring, or the
  receive goroutines race `NewHost` writing the fields they read.
- **Removed API that does not exist**: `Host.LeaseRead` (only
  `config.Config.LeaseRead` exists; lease read is internal and transparent to
  ReadIndex/SyncRead), `config.MembershipChangedInfo`,
  `config.ProposalDroppedInfo`, `config.ReadIndexDroppedInfo` (all three
  callbacks take `config.EventShardInfo`), `HostConfig.EnableMetrics`, and
  `RequestDeleteNode`/`RequestAddNonVoting` (the methods are
  `RequestRemoveNode` and `RequestAddObserver`; only the *sync* remove is
  named `SyncRequestDeleteNode`).
- **Corrected call signatures in examples**: four `deployment.md` membership
  examples omitted the required trailing `configChangeIndex`;
  `barrier.Initialize`/`Unseal` omitted `(ctx, …, creds)`; `RequestSnapshot`
  omitted the required `SnapshotOption`. `Host.Drain` was documented as
  polling every 50ms — it is event-driven on a channel close.
- **"Lock-free" was false in six places.** The `EntryQueue` behind
  `proposalQ`/`inboxQ`/`readIndexQ` is mutex-guarded; the win is amortizing
  one acquisition over a batched drain, not eliminating locks.
  `performance/architecture.md` contradicted itself two lines apart. The
  genuinely lock-free claims (`Host.nodes` sync.Map, the rate limiter's CAS
  loop, the atomic leader/membership snapshots) are unchanged.
- **The step worker does NOT skip a shard while `commitPending` is set.**
  `Tick` runs unconditionally, because a leader must keep advancing its
  heartbeat timer while WAL fsync and apply are in progress. The
  `Tick`/`Commit` race is prevented by the per-node `raftMu` mutex, which no
  document previously mentioned.
- **README**: added the missing `go get` line; corrected the Go badge
  (1.26.5 → 1.26.6) and version badge (0.1.0-alpha → 0.3.1-alpha); removed
  the stale "64 buckets" ReadyMap description (it is one `sync.Mutex` over
  one map plus a two-map rotation); and replaced "all above 91% coverage"
  with the measured per-package figures — `logdb` 85.2%, `waldb` 88.3%,
  `transport` 89.2%, `engine` 90.4%, aggregate 91.5%. Only the aggregate
  cleared the old claim, and the packages that miss it are the durability
  layer.
- **Public-repo hygiene**: private lab identifiers removed from
  `docs/performance/benchmarking.md`, `test/performance/run-remote.sh` and
  the `Makefile`. `PERF_HOST` and `DRAGONBOAT_SRC` are now required
  environment settings with no baked-in defaults, `REMOTE_DIR` defaults to a
  path relative to the remote login's home directory, and the release remote
  is `RELEASE_REMOTE` (default `origin`).
- `operations/compatibility.md` gained a `0.3.1-alpha` entry and the
  supremum rule for the transport version.

### Known Issues

- `pkg/config/config.go:292` — the godoc for `MaxApplyEntrySize` says
  "Default: 64 MB". The default is `MaxReplicationPayloadBytes` = 16711672
  (~15.94 MiB), and `Validate` REJECTS anything larger, so 64 MB is not even
  a legal value. Both `deployment.md` and `api/reference.md` state it
  correctly; the code comment is the only place carrying the stale figure.
- `pkg/config/config.go:361` — the godoc for `SessionExpiryEntries` says "Set
  to 0 to disable automatic expiry". `SetDefaults` rewrites 0 to
  `DefaultSessionExpiryEntries` (100000). The disable sentinel is
  `SessionExpiryDisabled` (`math.MaxUint64`), and that constant's own godoc
  says so — the two comments in the same file contradict each other.

## [0.3.1-alpha] - 2026-08-14

### Fixed

- WAL `SaveState` permission-based failure tests (read-only shard
  directories) now skip when running as root. CI job containers run as
  root, which bypasses permission bits, so the writes the tests expect to
  fail were succeeding — failing the Test job and the release gate.

### Changed

- CI: SARIF uploads and `trivy-action` are GitHub-only (Gitea has no
  code-scanning API, and the action's setup cannot authenticate to
  github.com with a Gitea token); Gitea runs gate with the trivy CLI
  installed from the release tarball.
- Makefile: `release-alpha` / `deps-latest` targets and `GOWORK=off`
  (workspace-proof builds), per dependency-resolution.md.

## [0.3.0-alpha] - 2026-08-12

### Security

- **A remote peer could force a term REGRESSION, breaking Election Safety.**
  `Term` is a plain uint64 in the wire encoding with nothing bounding it, and
  `raft.step` adopted a higher term verbatim. A peer naming a term at the top of
  the range drove the receiving replica to it; `becomeCandidate`'s `r.term++`
  then wrapped to **term 0** on the next campaign. A replica whose term has gone
  backwards can vote a second time in a term it has already voted in, which is
  an Election Safety violation reachable from a single message. Even without the
  wrap, a replica driven to the ceiling can never campaign again, so one message
  permanently removed a voter.

  Closed at two layers, because they fail differently: `step` now refuses a wire
  message whose term falls in a reserved band below the ceiling
  (`ErrTermCeilingExceeded`), which stops a replica being parked there; and the
  increment in `becomeCandidate` is guarded, so the wrap is impossible even if
  some future path reaches a ceiling term without passing through `step`. Terms
  advance by one per election, so no cluster reaches the reserved band by
  running — refusing it costs nothing real.

  Found by the raft fuzz harness (`FuzzRaftStepSequence`) while verifying an
  unrelated change; it predates this release line.

This release is a security and durability hardening pass over `0.2.0-alpha`. It
is **not compatible with `0.2.0-alpha` data directories**: the WAL record
framing, the snapshot-record membership section, `snapshot.meta`, and the
at-rest AEAD binding all changed. The engine fails closed with a specific,
named error on each rather than misparsing, but there is **no migration** —
wipe the data directories, or re-sync each node from its peers.

Source compatibility for embedders is better than the format break suggests:
`go-qrdb`, `go-xkms` and `go-quicbaas` all build against this release without
source changes. The interface additions recorded under BREAKING affect only
embedders that IMPLEMENT those interfaces, which none of them do today.

### BREAKING

- **`msg.From` is now bound to a cryptographic NODE IDENTITY recorded in committed membership, not to an address.** `msg.From` — the replica ID Raft counts toward election and commit quorum — travels in-band and is not authenticated by mTLS. The previous gate required the peer's verified certificate to cover the address committed membership assigns that replica. That closed registry poisoning and left three holes: a **promiscuous certificate** (a wildcard SAN, or one certificate carrying every node's SAN) covers every member's address, so its holder could assert any member's `From` and have forged `ReplicateResp` and `RequestVoteResp` counted toward quorum; **two replica IDs at one address** gave the single host behind it two quorum identities; and **address is not identity**, which is why a member that recovered at a new address had its quorum traffic dropped until a config change committed — and during quorum loss no config change can commit.

  Committed membership now records `Identities`, a replica ID to node identity map. The identity is `sha256(leaf.RawSubjectPublicKeyInfo)` rendered `spki:<hex>` — a hash of the node's PUBLIC KEY, so only the holder of the matching private key can present it, and it survives certificate renewal on the same keypair. Where an identity is committed it is the ONLY check performed and the address becomes routing metadata. A SAN URI (`quicraft://<deployment>/<replicaID>`) would bind the replica ID more directly and was rejected because it requires a certificate-issuance change in four downstream products; SPKI needs none, at the cost that the replica-ID-to-identity mapping must be supplied out of band.

  Wire and persisted format changes: `proto.Membership` gained a **v4 layout** — the v3 fixed region extended with the replicated session policy (`SessionExpiryEntries`, `MaxTotalSessions`, `MaxSessionsPerClient`, base 25 → 49 bytes) and an `Identities` map appended after `Removed`; **v3 still decodes**, so a node can replay a log whose index-1 entry an older binary wrote, and it decodes with no identities and no session policy. `proto.ConfigChange` gained a trailing `NodeIdentity` field, discriminated by the buffer's end rather than a version byte (a `ConfigChange` is exactly one `Entry.Cmd`, so its end is readable). The `waldb` snapshot-record membership section is likewise at **version 4**, accepting the range 3-4 on replay; it carries the session policy as a tail after `Identities` rather than in its fixed region, because its decoder parses straight-line and a grown fixed region would give v3 and v4 different map offsets. The session policy and the identity map landed in the same layout revision deliberately: shipping one as v4 and the other as v5 would cost downstream a second on-disk format break for a version nothing outside this tree has written.

  API: `Host.NodeIdentity()` reports this node's own identity; `Host.RequestAddNodeWithIdentity` / `RequestAddObserverWithIdentity` / `RequestAddWitnessWithIdentity` / `SyncRequestAddNodeWithIdentity` commit one; `Config.MemberIdentities` seeds them at bootstrap; the public `Membership` gained `Identities`. The identity is an ARGUMENT and is never captured from the joiner's connection — a leader that captured it would have only the certificate's address coverage to decide which connection speaks for the replica, so a promiscuous-certificate holder would register ITSELF as the target replica permanently, which is worse than no identity at all. `transport.MessageHandler` now takes a `PeerAuth` (identity plus certificate coverage) in place of a `senderAuthorized` closure.

  **Existing clusters keep working**: a replica with no committed identity falls back to the address check, and `TransportConfig.RequireMemberIdentity` (default **false**) refuses that fallback for operators who have finished migrating. Migration is per-replica, needs no downtime, and is documented in `docs/operations/compatibility.md`. Before enabling the strict mode, confirm through `Host.GetShardMembership` that every member carries an identity: a member added by an earlier build has none until an operator re-adds it with `RequestAddNodeWithIdentity`, and enabling the strict mode first drops that member's quorum traffic.

  **Identities survive a restart**, which is what makes the strict mode usable at all. They reach membership through the log, so a replica whose log has been compacted past the `EntryInitialMembership` at index 1 and the config-change entries has only the `logdb` snapshot-metadata record to relearn them from. That record now carries them end to end: `logdb.Membership.Identities`, the `memdb` deep copy, the `waldb` snapshot-record membership section at **version 4**, `host.go`'s two converters and `engine`'s save-path converter. Without any one of those a restarted node rebuilds an identity-free configuration — silently downgrading every member to the address check under the default posture, and dropping all quorum-bearing traffic under the strict mode. The `waldb` section accepts the **range 3-4** on replay rather than an exact match, so a record written before identities existed still replays and decodes with an empty identity map; its pre-version-byte length guard stays at the v3 minimum so those sections are not rejected as truncated before the version check runs.

  This is pinned by the shared LogDB conformance suite, which runs `memdb` and `waldb` through ONE set of assertions and reopens the database rather than reading back through the live handle — `waldb` answers `GetSnapshot` and `ReadState` from an in-memory map that `SaveSnapshot` fills from the caller's value, so a save/read pair inside one process never reaches the record encoding and would pass against a backend that serialized nothing.

- **The three session limits are now REPLICATED state, and `Config.SessionExpiryEntries` defaults to 100000 instead of 0.** `SessionExpiryEntries`, `MaxTotalSessions` and `MaxSessionsPerClient` were per-node `config.Config` values that the deterministic apply path read: `rsm.StateMachine.Apply` uses the expiry threshold as the interval of its checkpoint walk, `session.Manager.Register` refuses past `MaxTotalSessions`, and the response cache is bounded by `MaxSessionsPerClient`. Two replicas holding different values expire different sessions at different log indexes, or disagree on whether a session exists at all — after which one answers a session-managed proposal from its response cache while the other reports `ResultSessionExpired`. That is a **state-machine divergence with every Raft invariant intact**: identical log, identical commit index, identical applied index, no error raised anywhere and nothing in the protocol able to detect it. Nothing replicated the values and nothing checked them against peers.

  They now live in `proto.Membership` (see the v4 layout above), seeded at bootstrap from the founder's `Config` exactly as `MaxWitnesses` is seeded from `config.WitnessConfig`, and travel through the log, snapshots, the WAL and LogDB like the rest of committed membership. The apply path reads the COMMITTED values; local configuration is used only while a shard has none — a shard bootstrapped by an earlier build (its membership decodes as v3), or a `join=true` replica that has not yet replicated the shard's configuration. `MaxTotalSessions` is the "policy recorded" discriminator, because `config.Validate` rejects a zero value, which is what lets an expiry threshold of 0 keep its own meaning of *expiry explicitly disabled cluster-wide*.

  **A node whose local configuration disagrees with what its shard committed applies the shard's values and logs the disagreement at WARN**, naming both. It does not refuse to start: the committed value can only be changed through consensus, so a node that refused would be unable to participate in changing the thing that keeps it from starting. The warning matters because two of the three bound this process's memory — a replicated limit above the local one is a resource decision taken elsewhere, and must be discoverable from the node's own logs.

  **The default became safe to change only because of the above.** With expiry disabled the per-shard session table never shrinks, is serialized into every snapshot, and a client that dies without `CloseSession` leaks an entry that counts against `MaxTotalSessions` forever — but flipping the default while the value was node-local would have made a rolling upgrade diverge silently, which is why `0.3.0-alpha` shipped the analysis instead of the change. `SetDefaults` now fills 100000: 1000x the floor `Validate` enforces, and the value `docs/operations/deployment.md` already recommended. Because zero can no longer mean "disabled", **`config.SessionExpiryDisabled` is added** for deployments that want no expiry; it is `math.MaxUint64` and needs no special case anywhere, being an inactivity threshold no log reaches.

  **There is no config change for these yet.** Unlike the witness policy (`Host.RequestUpdateWitnessPolicy`), the committed session policy is fixed for the life of the shard, so `MaxTotalSessions` cannot be raised on a running shard. That is a deliberate scope decision and a known gap: a runtime change also requires replacing the apply path's read of the published membership with adoption from the apply stream itself, because commit-time adoption plus a lagging apply path would let two replicas switch policies at different points in the log — the same class of bug, equally silent. `rsm.StateMachine.sessionPolicyFn` records that requirement where the change would be made.

  API: the public `Membership` gained `SessionExpiryEntries`, `MaxTotalSessions` and `MaxSessionsPerClient`, so an operator can read what a shard actually committed rather than what any one node is configured with. `rsm.NewStateMachine` takes a `proto.SessionPolicy` and a policy accessor in place of three adjacent `uint64` parameters — transposing the two limits used to compile cleanly while capping a shard at `MaxSessionsPerClient` clients.

- **Every WAL record now carries a durability generation; the chunk header grew from 7 to 11 bytes and `walFormatVersion` is 2. Existing WAL directories cannot be read by this version.** WAL recovery could not distinguish two physically different situations and resolved the ambiguity by refusing to start. A batch larger than one block issues one `WriteAt` per block before a single `fdatasync`, so a power loss can persist an arbitrary SUBSET of those pages; losing a leading or interior page (rather than a whole block, which reads as a zero hole) left CRC garbage with valid records after it — indistinguishable from interior rot, where truncating destroys entries the node already acknowledged to its leader. The safe reading was to fail closed, so an **ordinary power loss could leave a node that would not boot**, with no in-package remedy and, for a single-node deployment, no exit at all.

  The chunk header is now `[CRC32:4][Len:2][Type:1][DurSeq:4]`, with `DurSeq` inside the checksummed region. `DurSeq` is the number of `Sync` calls the segment has COMPLETED — a durability generation, not a record ordinal. A record ordinal cannot discriminate (rot destroys records too, so "records are missing above the damage" is true in both cases and only the magnitude differs — a heuristic). The generation gives a proof: the writer only emits generation `g+1` after `Sync #(g+1)` has RETURNED, so the presence on disk of any record above `g` proves everything at or below `g` was acknowledged. Contrapositive: records carrying the segment's highest generation have no completed fsync behind them, so discarding them destroys nothing acknowledged. Recovery therefore **truncates and self-heals** when everything above the damage is one generation, and refuses with `ErrMidSegmentCorruption` when it spans two, or with `ErrRecordSequenceRegression` when a survivor sits BELOW the clean prefix (bytes this writer cannot have produced). The torn-multi-block refuse-to-start condition is now unreachable.

  Cost, measured A/B interleaved on an idle bench node: **+1.1 ns/op** on `encodeRecord` for a 48-byte payload and **+4 bytes** on disk per record, against a ~2.5 ms `fdatasync` — four orders of magnitude below the operation it rides on. Allocations unchanged; write amplification stays at 1.0x.

  **Segments with no format marker are now fatal** (`ErrWALFormatMissing`), reversing the previous "adopt an unmarked populated directory" rule: a pre-marker segment parsed by this version fails every CRC, yields an empty clean prefix, and the torn-tail repair would truncate the whole log to zero — a silent misparse presented as a recovery. A v1 marker gets `*WALFormatVersionError` naming both versions, deliberately not `ErrWALFormatMalformed`, because the file is intelligible and the remedy is an upgrade rather than a corruption hunt. The `payloadType` ladder is unchanged: it versions what is INSIDE a chunk, while the marker versions the framing around it, and a record whose framing cannot be parsed never reaches a payload-type check. **There is no migration: wipe existing data directories, or re-sync the node from its peers.**

- **An inbound peer whose certificate also covers THIS node's host is refused at the transport**, before it can occupy a connection slot or open a stream. A certificate that covers this member covers every member, so its holder can satisfy the address binding for any replica. Peers that share this node's machine are exempt — the same host under any spelling, or any loopback address — because a certificate is matched on host alone and no certificate policy can separate them. **A cluster running one wildcard or shared-SAN certificate across DISTINCT hosts will stop connecting.** This is the enforcing counterpart of the existing `StartShard` / config-change exclusivity checks, which inspect the LOCAL node's own certificate and are therefore misconfiguration detectors a malicious joiner simply declines to run. The check runs once per CONNECTION, not per message: probing member addresses per message thrashes the 4-slot per-connection decision cache and trips its 16-distinct-miss fail-closed cap, killing every connection on any shard with five or more members

- **Duplicate member addresses are compared canonically.** They were compared as raw strings, so `"node-b:5001"` and `"NODE-B:5001"` were two distinct members to config-change validation and ONE host to `x509.VerifyHostname` — which is what the receive-side gate ultimately matches on. Two committed replica IDs the certificate cannot tell apart is the same quorum forgery the duplicate check exists to prevent. Comparison is now on a canonical form (lower-cased host, trailing FQDN dot removed, IP literals normalised, port preserved) shared by the transport and by raft through a new `pkg/internal/netaddr` package — raft cannot import the transport, which is why the normaliser previously existed on one side only. It canonicalises SPELLING ONLY and never resolves DNS: config-change validation is replicated logic that must give the same answer on every replica and on every replay. `StartShard` and `validateAddNode`/`validateAddWitness`/`validateAddNonVoting` all reject spelling-variant duplicates, and a configuration that already holds one fails closed on the receive path

- **At-rest ciphertext is now bound to its position; existing encrypted snapshots and WALs cannot be read by this version.** The barrier previously authenticated only the epoch, so every frame sealed under one DEK+epoch authenticated identically no matter where it sat. An attacker with write access to the ciphertext — but no key — could reorder frames, splice a frame from one artifact into another at the same epoch, or truncate a trailing run, and every individual frame still verified. Each snapshot frame and each WAL record now carries its identity in the AEAD's additional authenticated data: `"QRSNAP1" | ShardID | ReplicaID | SnapshotIndex | ChunkSeq` for snapshot frames, `"QRWAL1" | WALShardIdx | SegmentID | RecordSeq` for WAL records. Moving, duplicating, or substituting one now fails authentication.

  The binding is **unconditional and there is no marker on disk**, which is deliberate: a marker is only useful if a reader may fall back to the weaker epoch-only binding, and that fallback would itself be the vulnerability — an attacker would simply strip the marker. Every candidate location for one is unauthenticated anyway (the WAL's CRC32 is a checksum, not a MAC, and `snapshot.meta` carries no MAC at all). The scheme is chosen by code, never read off disk, and a reader never retries with a weaker AAD. Both on-disk framings are otherwise byte-identical to before.

  **There is no migration: wipe existing data directories, or re-sync the node from its peers**, consistent with the `0.2.0-alpha` break. `waldb.Reencrypt` re-serializes every record and so produces correctly-bound output for a barrier migration. Barriers in hardware mode (`IsHardwareMode`) keep epoch-only binding as an explicit, logged capability limitation — the underlying encryptor accepts no caller AAD — decided once at construction from the deployment, never per artifact and never in response to a decrypt failure.

  API changes that follow from this: `Barrier` gained `EncryptWithEpochContext`, `DecryptWithContext` and `IsHardwareMode` (all already present on `*seal.Barrier`); `ReencryptSnapshotFile` gained `shardID, replicaID, index` parameters — a `SnapshotReencryptFunc` implementation already receives all three. A `Barrier` implementation that does not support caller-supplied AAD is refused when the WAL opens rather than silently writing weaker ciphertext.

- **`snapshot.meta` is now self-describing and grew to 72 bytes.** Every record written by this version carries a `QRSNAPMD` magic, a format version and a body length ahead of the existing fields (`Index | Term | ShardID | ReplicaID | CreatedAt | Epoch | Compression`), so a reader can tell a field that is ABSENT — written by a different version — from bytes that are CORRUPT. Body fields are append-only from here: appending one bumps the version and leaves every existing offset alone.

  Compatibility is one-way and requires no migration step. **Records written by earlier versions are still read**, positionally, at their original offsets: the 40-byte layout (the snapshot receive and import paths, which recorded no `Epoch`), and the 48- and 56-byte layouts (the snapshot save path, with `Epoch`, and with `Epoch` + `Compression`). Nothing rewrites an existing record; a legacy directory keeps its record until the snapshot rolls off. **An older binary cannot read a record written by this version** — it will read one as corrupt, which is fail-safe there (it suppresses DEK purging) but means a downgrade must be treated as a wipe-and-restore, consistent with the `0.2.0-alpha` on-disk break

- `TransportConfig.MaxDecompressedSize` is pinned to `MaxFrameSize` and rejected if set to anything else. Configuring it lower bought no bomb protection — a peer that wants the node to buffer 16 MB can simply send an uncompressed 16 MB frame — while creating an asymmetric wire contract in which a receiver silently stopped accepting replication that every sender considered valid. `SetDefaults` still maps 0 to the pinned value, so deployments that never set it are unaffected

- **`NewPrometheusMetricsCollector` no longer labels most metrics with `shard_id` by default.** The collector targets a library whose documented design point is 10,000+ shards on one host, and per-shard labelling cost far more than the count of shard-labelled metric names suggested: a histogram child is one series per bucket plus `_sum` and `_count`, so each active shard cost a measured **113 active Prometheus series**. At 10k shards that is ~1.13M series from a single host, enough to destabilise a shared Prometheus — a failure that lands on every other tenant of it, with no way for an operator to opt out.

  The new `config.MetricsConfig.PerShardMetrics` selects the mode, and it defaults to **off**: proposal/commit/apply/ReadIndex latency, proposal batch size, snapshot counts and snapshot byte histograms now aggregate to one host-level series each, taking a shard to a measured **6 series**. Pass `NewPrometheusMetricsCollectorWithConfig(config.MetricsConfig{PerShardMetrics: true})` to restore the previous behaviour. The mode is fixed when the collector is built, because a Prometheus vector's label set is decided at construction; it is deliberately not a `HostConfig` field, which could not take effect.

  **Alerting is unaffected.** Every signal an operator pages on keeps its `shard_id` in both modes: `elections_total` (leader change), `proposals_dropped_total`, `messages_dropped_total`, `snapshots_rejected_total`, and both `quicraft_security_*` counters. What aggregates is volume-and-latency telemetry, whose per-shard breakdown is a debugging aid rather than an alerting input.

  Aggregated series keep the **same metric name** and drop only the `shard_id` label, so a rule written as `sum without(shard_id) (...)` is correct in both modes; dashboards that group *by* `shard_id` collapse to a single host-wide series rather than reporting wrong numbers. The change is made visible in three places: the `# HELP` text of an aggregated series says so, the new `quicraft_metrics_per_shard_enabled` gauge exports the mode (1/0) so a dashboard can branch on it without reading the config, and `docs/operations/deployment.md` gained a "Metrics cardinality" section with the measured budget.

- **`seal.NewBarrier` returns `(*Barrier, error)` instead of panicking on an invalid configuration.** An exported constructor in a public package that panics takes down the EMBEDDER's process, and `pkg/seal` is embedded in go-qrdb, go-xkms and go-quicbaas. The previous doc comment told runtime callers to call `BarrierConfig.Validate` first — a convention, not a guarantee, and the invalid configs it guards against (an unknown algorithm, a non-FIPS-approved cipher under FIPS mode) are exactly what a config read from a file, an API request, or per-tenant settings produces. An operator's typo must be an error the embedder can reject, not a crash.

  `seal.MustNewBarrier` is the panicking form, following the `regexp.MustCompile` precedent, for configs built from compile-time constants where an invalid value is a programmer error. `NewEpochBarrier` remains an alias of `NewBarrier` and so changes signature with it; `MustNewEpochBarrier` completes the alias set. `crypto.NewBarrier` re-exports the new signature and `crypto.MustNewBarrier` is added.

  `BarrierConfig.Validate` stays exported and is unchanged, but is now an admission-time convenience rather than a safety requirement: `NewBarrier` applies the same rules and returns the same error identity, so a caller that forgets it no longer risks a panic.

  **Downstream call sites needing coordination** (not edited here): `go-qrdb/pkg/seal/barrier.go:244` (`epochBarrier := quicraftseal.NewBarrier(epochConfig)` — inside `NewStorageBarrier`, which already returns an error, so the new error threads straight through) and `go-qrdb/pkg/seal/strategy.go:122` (`var NewEpochBarrier = quicraftseal.NewBarrier` — a re-export whose signature changes with it, making this a breaking change in go-qrdb's own API surface). go-xkms and go-quicbaas do **not** call this constructor: their `seal.NewBarrier` is their own package's four-argument function, and neither imports `go-quicraft/pkg/seal` at all — only `pkg/crypto/shamir`.

### Fixed

- **A restarted replica could permanently fail to resume replication.** With the leader alive, healthy and committing, a replica that restarted underneath a saturated pipeline was never sent another entry and stayed thousands of entries behind indefinitely — the shard kept running on the remaining members, one replica short, until that replica was restarted again. The leader had 256 append messages outstanding to the peer's previous incarnation, so no acknowledgement for them could ever arrive. The in-flight window only drains from ABOVE (records at or below an acknowledged index are freed), while every acknowledgement the returning replica can still send reports a position BELOW the window, so it freed nothing. A full window is what makes a peer "paused", and the paused peer was skipped on every send path: the leader could not send it the one entry that would let it advance, so it could never acknowledge anything that would free a slot. A heartbeat response now releases one slot, letting exactly one message through — acknowledged normally if the window was merely busy, rejected if the peer restarted, and the rejection drives the existing backtracking that repairs the peer's position. This restores etcd/raft's `MsgHeartbeatResp` behaviour: the primitive it needs (`inflights.freeFirstOne`) existed here and had been deleted as unused, which is what left the stall unrecoverable. The condition deliberately does not try to detect the restart from leader state alone — a peer that is merely busy is indistinguishable from a wedged one by match/next/window shape, so any such test fires on healthy pipelined replication and resets it continuously
- **A connection to a removed replica was never closed.** quic-go's keep-alive period sits below the idle timeout, so the connection was PINGed indefinitely and never idle-closed, while the transport's cleanup only reaps connections whose context is already dead. A replica removed from the configuration but still running therefore pinned a connection, its pooled streams and its epoch records for the life of the process. A committed removal now releases the connection — but only when no other shard still resolves to that address (one connection is address-keyed and multiplexed across shards) and no snapshot transfer to it is in flight. This is deliberately event-driven rather than a sweep: registry entries are legitimately absent under a live, healthy connection during shard teardown-and-restart, speculative pre-registration rollback, and a joining replica's first contact, so a sweep would close healthy connections in exactly those windows
- **A leader-change notification fired for campaign churn.** Broadening the change check from the leader ID to the `(leader, term)` pair — so a same-leader re-election at a higher term is not swallowed — also made a replica that cannot win publish every term it campaigned through while the leader was still unknown. That overwrote the host's cached leader with "none" and could complete pending proposals with a `NotLeaderError` for a leadership loss that never happened
- **A restarted peer could black-hole every vote response for ~30 seconds.** Survivors held outbound QUIC connections to the dead incarnation; `stream.Write` succeeded locally while nothing arrived until `MaxIdleTimeout`. With two nodes down in a five-node shard every survivor's grant is mandatory, so one silently broken direction left the cluster leaderless with quorum available. Outbound connections are now attributed to a peer incarnation and evicted when they cannot be
- **A leader could re-dial one follower thousands of times per second.** `sendAppend` bounded a `Replicate` message in bytes but never in entry COUNT, so a follower catching up on small entries drew a message every peer must reject — and that rejection was indistinguishable from a dead connection, so the sender tore down, re-dialed, and retried the identical message forever. Entry count is now bounded, oversized batches are split, and a rejected frame evicts only that stream
- **PreVote messages were withheld behind WAL persistence.** They persist nothing, so the ordering rule never applied to them; under sustained write load `commitPending` is set almost continuously and pre-vote traffic never left the node, so elections could not complete
- **A replica recovering from a snapshot was absent from consensus entirely** — no votes, no heartbeat replies — while its tick kept campaigning. Two concurrent recoveries could dissolve quorum with every node up. The gate now covers state-machine access only
- **A replica removed from the configuration could still elect itself**, counting its own vote toward a quorum it was not part of — two leaders in one term
- A same-term vote was granted while a live leader was known, and the grant then silently blackholed higher-term vote requests for a full election timeout
- `lastTerm` fabricated term 0 when the log was unreadable, so a replica holding a committed entry no one else had would vote for a candidate missing it
- A recovered commit index ahead of the last recovered entry was trusted absolutely, letting a follower acknowledge entries it did not have
- A follower whose election timer expired could suppress its own election to enter quiesce, then run at 30x the election timeout while leaderless
- Unload and reload raced through a random `select`, which could leave a shard permanently unable to apply while it still voted and won elections
- Apply-gap detection was disabled at `lastApplied == 0`, so a dropped first batch silently diverged the state machine
- A data race on the snapshot recovery markers could abandon a recovering replica's snapshot install, leaving it admitted to the configuration and starving

- `docs/api/reference.md` and the metrics tables in `docs/operations/deployment.md` were stale: they named `readindex_latency_seconds` (the metric is `read_index_latency_seconds`), showed `messages_sent_total`/`messages_received_total` as carrying `shard_id`/`to`/`msg_type` when those labels belong to the opt-in `*_detailed_total` variants, and omitted `piggyback_commits_total`, `messages_dropped_total`, `snapshots_rejected_total` and both security counters entirely.

### Changed

- **A pending request made with a deadline-less context now expires.** A proposal, read, snapshot or config change registered under `context.Background()` recorded the zero time, which the sweeper skips by design, so the entry was never reclaimed: its `RequestState` never returned to the pool and `pendingOps` never fell back to zero, which made every later `Drain()` burn the full `ShutdownTimeout`. Such an entry is now clamped at registration, so `RequestState.Result()` returns `ErrTimeout` after 5 minutes for a proposal, read or config change instead of blocking indefinitely — a backstop for a request that can never complete (leader crash, partition, a lost apply), not a request SLA. Snapshots carry their own 1-hour backstop, matching the snapshot transfer ceiling, because serializing a multi-gigabyte state machine legitimately outruns the commit-path bound. A caller-supplied context deadline always wins
- `ShardInfo.Health` reports `recovering` and `snapshotting` instead of `healthy` while those gates are up; `Host.DiagShardGates` exposes them directly. A replica invisible to consensus previously reported healthy
- `BarrierConfig.Validate` is exported so a runtime-built config can be rejected instead of panicking

### Added

- `config.MetricsConfig` and `quicraft.NewPrometheusMetricsCollectorWithConfig`, controlling metric label cardinality (see BREAKING above).
- `quicraft_metrics_per_shard_enabled` gauge, reporting whether per-shard metric labels are enabled.

- **`quicraft.Temporary(err) bool` and `quicraft.NotLeader(err) (replicaID uint64, ok bool)` classify the package's ~60 sentinels.** The package mixed transient conditions (`ErrTimeout`, `ErrSystemBusy`, `ErrShardNotReady`, `ErrNotLeader`) and permanent ones (`ErrShardNotFound`, `ErrInvalidOperation`, `ErrRejected`, `ErrClosed`) with nothing distinguishing them, so a caller could not ask "should I retry?" without string-matching or enumerating sentinels — and four downstream products would each have re-derived the classification and disagreed.

  Every sentinel is classified deliberately, not by pattern. `Temporary` is true only for conditions that clear on their own without operator or caller intervention: `ErrBusy`/`ErrSystemBusy`, `ErrTimeout`, `ErrAborted`, `ErrDropped`, `ErrShardNotStarted`, `ErrShardNotReady`, `ErrNotLeader`, `ErrReadIndexNotReady`, `ErrSnapshotInProgress`. **Everything else is permanent, including errors this package never produced** — retrying a permanent failure forever converts one bad request into sustained load and hides the real fault. Several plausible-looking candidates are excluded on purpose and the reason is recorded next to each in `pkg/errors.go`: `ErrStaleConfigChange` (the identical request fails forever; the caller must re-read membership and build a new one), `ErrDraining` (a one-way transition into shutdown), `ErrCanceled` (the caller's own decision, whereas a server-side deadline surfaces as `ErrTimeout`), `ErrSessionLimitExceeded`, `ErrBarrierSealed`, `ErrShardBarrierUnresolved`, `ErrDiskFull`, `ErrLogDBWrite` and `ErrShardFailed`.

  `NotLeader` surfaces `NotLeaderError.LeaderID` so callers redirect without type-asserting; `ok` is true for any not-leader error and `replicaID` is 0 when the leader is unknown. Both use `errors.Is`/`errors.As`, so wrapped errors and the typed errors' `Is` methods (`*BusyError`, `*NotLeaderError`) classify correctly. A table test asserts every exported sentinel, and a companion test parses `errors.go` and fails when it declares an exported `Err*` var the table does not classify — so a sentinel added later cannot become silently permanent without someone deciding that it should be.

### Documentation

- **The deferred `SessionExpiryEntries` decision was resolved, not carried forward.** This section previously recorded that the default had to stay 0 because the threshold was node-local configuration the apply path treated as replicated, and proposed replicating it as the alternative. That alternative shipped: see the session-policy entry under `0.3.0-alpha` BREAKING. `TestSessionExpiry_DisagreeingThresholdsDiverge`, which demonstrated the divergence, was replaced by `TestSessionExpiry_ReplicatedThresholdGovernsBothReplicas`, which asserts that two replicas with different LOCAL configuration now apply identically — and, in a second subtest, that they both follow the shard's policy when it enables expiry rather than merely agreeing to do nothing.

### Security

- **A delegated (hardware-backed) encryptor can now bind positional context, and a deployment can require it.** When a sealing strategy supplies its own encryptor the barrier hands off every operation and returns early, so that path had no positional binding — and, because it bypasses the barrier's own encryption entirely, no epoch AAD, no nonce-reuse tracking and no NIST SP 800-38D invocation counting either. The seam carried no AAD to bind with. A delegated encryptor may now implement the optional `seal.AADSymmetricEncrypter` to declare it authenticates caller-supplied AAD end to end, in which case context binding works exactly as it does in software mode (both paths build the AAD through the same helper, so they cannot drift). An encryptor that does not implement it still refuses a non-empty context rather than silently dropping it. Note this is a claim about end-to-end authentication: a backend that accepts an AAD parameter and discards it must not implement the interface, since advertising a binding that does not exist is worse than not having one.

  `BarrierConfig.RequireContextAAD` lets a deployment demand positional binding; it is checked when the barrier is initialized or unsealed, so a configuration that cannot provide it fails at startup rather than at the first write. It is refused for `StrategyNone`, where authenticated positional binding cannot exist because nothing is encrypted. Default is false, preserving existing behavior.

  `IsHardwareMode` previously conflated two unrelated situations, because `StrategyNone` also installs an (identity) encryptor: "nothing is encrypted, so there is nothing to bind" and "real encryption that cannot bind". Its meaning is unchanged, but `ContextBinding()`, `SupportsContextAAD()` and `IsPlaintextMode()` now distinguish them, and the snapshot and WAL layers decide suppression on the precise capability instead of the blunt flag. The delegated-mode bypass of the nonce tracker and invocation counter is now documented and logged at setup rather than left implicit

- **A LogDB that persists encrypted records but cannot name their epoch no longer has its keys destroyed.** The host-wide DEK purge floor treated any LogDB that does not implement `EpochAwareLogDB` as imposing no floor at all and purged on the snapshot floor alone. That is correct for an in-memory store, but `logdb.LogDB` is a public extension point: a durable store that encrypts under the host barrier without implementing the optional epoch interface had the DEKs its own records still needed purged irreversibly. Purging now proceeds only on a positive `AtRestNonDurable` declaration — a store that declares `AtRestBarrierEncrypted`, declares `AtRestUnprotected`, or declares nothing at all suppresses purging and logs why. Silence costs an unbounded keyring, which is recoverable; the previous behavior cost the keys, which is not. The per-shard-barrier floor already failed safe here and is unchanged
- `WithShardBarriers` without `WithBarrier` or `WithLogDB` is refused: the default WAL takes one barrier, so per-shard encryption alone produced encrypted snapshots over a plaintext log while an operator's spot-check showed encryption working
- **A failed sweep of a snapshot decrypt scratch file is now reported.** Both decrypt paths (recovery and snapshot send) write a full plaintext copy of the state machine to a sibling scratch file and remove it when done; the sweeper reclaims what a crash orphaned. A failed removal — a read-only mount, a permissions or ownership change — was logged at DEBUG and returned to no caller, so the plaintext copy stayed next to its ciphertext for the life of the snapshot directory with nothing to signal it. Failures (and a directory the sweep cannot even list) now log at WARN and increment a process-wide counter, `engine.StaleDecryptSweepFailures()`; any increase warrants an alert
- **One snapshot directory with an indeterminate epoch no longer hides the others.** The DEK purge-floor scan returned on the first directory it could not evaluate, so its log named one directory while the next scan named the next. The scan now visits every directory and reports them all in a single record, with the reason for each (`legacy_record_without_epoch` vs `unreadable`). The VERDICT is unchanged and deliberately so: an epoch that cannot be established may be any epoch, so such a directory suppresses purging for the barrier that could have sealed it — in per-shard-barrier mode that is one tenant's barrier, and other tenants keep purging normally

## [0.2.0-alpha] - 2026-08-06

### BREAKING

- **On-disk and wire formats changed; there is no migration from `0.1.0-alpha`.** Witness policy became replicated state: `MaxWitnesses` and `RequireFullReplicaMajority` now live in `proto.Membership` and travel through the log, snapshots, the WAL, and LogDB. `Membership` grew from an 8-byte base to 24, `ConfigChange` from a 36-byte fixed section to 52, and the WAL membership section by 9 bytes. `config.WitnessConfig` is now only a bootstrap seed; the policy is changed at runtime through `Host.RequestUpdateWitnessPolicy` (and its `Sync` variant). Wipe existing data directories before upgrading

### Security

- **Sealed barriers no longer downgrade to plaintext.** A barrier that is configured but sealed means encryption is temporarily unavailable, not that the data is plaintext. Three paths conflated the two and durably wrote plaintext: `saveSnapshot` (reachable via `EmergencySeal`, so the control inverted during incident response), `ImportSnapshot` (recorded `Epoch: 0` unconditionally), and `ReencryptSnapshotFile`. All now fail closed with `ErrSnapshotBarrierSealed`. In `ReencryptSnapshotFile` a sealed *source* was worse than a downgrade: it copied ciphertext and recorded it as plaintext
- Imported snapshots are re-encrypted under the importing host's own barrier, so they are encrypted at rest like every other snapshot for the shard
- Certificate revocation configured fail-closed with no CRL or OCSP source was a silent total no-op, disabling the only mechanism for evicting a compromised certificate
- `Term: 0` wire messages bypassed every term check, letting a non-leader advance a follower's commit index
- A client could kill a shard cluster-wide by pairing `ProposeWithSession` with `CloseSession`: both batched into one `Apply`, which then errored, failing the shard deterministically at the same index on every replica
- `BarrierConfig.Validate` is now exported so embedders building a config at runtime can reject a bad algorithm instead of taking the process down on `NewBarrier`'s panic

### Fixed

- **Two leaders could be elected in one term.** Vote grants were counted by ranging the vote map while quorum came from the live voter set, so a `RemoveNode` committed mid-election shrank quorum while a removed node's stale vote survived. Votes are now tallied over configuration voters, as etcd and dragonboat do
- **An empty snapshot could be streamed to a follower.** The receive path never recorded `FileSize`, so dedup re-delivery published 0 alongside a non-empty path, and the receiving replica recovered its state machine from zero bytes
- **A WAL shard could be silently discarded and then truncated.** Any non-`ENOENT` `Stat` error was treated as `ENOENT`, so `EACCES`/`EIO`/a stale NFS handle skipped recovery and reopened the surviving segment with `O_TRUNC` — a double vote plus permanent loss of acknowledged writes
- **Snapshot recovery could roll the state machine backwards**, regressing `lastApplied` into a permanent `ErrApplyGap` outage
- A stale pending snapshot could supersede a newer one, rewinding the `LogReader` behind the apply frontier or failing the shard, and letting a rejected snapshot roll membership backwards
- A compacted index returned a term instead of `ErrCompacted`, suppressing the leader's `InstallSnapshot` fallback so a lagging follower could never catch up
- Raft could accept entry batches the WAL could never persist: one `Update` was one WAL record, and a record must fit one segment. Batches are now split across records
- A log-replay joiner kept membership `{self}` forever and could elect itself a single-node leader that acknowledged writes the cluster discarded
- `replayConfigChangesTo` applied uncommitted configuration changes
- The WAL fsynced only its root's immediate parent, losing acknowledged writes when the root sat more than one level below the durable point
- A non-monotonic persisted `compactedTo` turned `ErrCompacted` into a permanent `ErrLogGapDetected`, so raft never fell back to `InstallSnapshot`

### Changed

- Reads no longer block on a writer's fsync. The WAL shard mutex is split: segment file state keeps the existing lock, while the in-memory maps readers consult move to a separate `RWMutex` taken only for the brief commit. Previously every read for every raft group on a WAL shard waited out the fsync
- Toolchain pinned to Go 1.26.5 across `go.mod`, Docker, and CI
- `contrib/dragonboat` gained the `replace` directive it always needed; it previously resolved its parent only through a `go.work` outside the repo and could not be built or tested in CI

### Added

- `BarrierConfig.Validate` is exported, so a caller assembling a barrier config at runtime (from a file, an API request, or per-tenant settings) can reject a bad algorithm — or a non-FIPS-approved cipher under FIPS mode — instead of taking the process down on `NewBarrier`'s panic
- `BenchmarkThroughputQUIC`: the first end-to-end transport throughput benchmark. All prior transport benchmarks measured serialization only, so no test could observe a change to the QUIC receive window
- Per-shard barriers via `WithShardBarriers(fn func(shardID uint64) Barrier)` option, enabling multi-tenant or per-shard key isolation while maintaining backward compatibility with single host-wide barrier setup
- Logical snapshot re-encryption on receive: `reencryptReceivedSnapshot` decrypts sender's plaintext snapshots and re-encrypts under receiver's own barrier before install, eliminating shared cross-node keys for at-rest encryption
- WAL re-encryption primitive: `waldb.Reencrypt(srcDir, srcBarrier, dstDir, dstBarrier)` performs zero-downtime logical migration of WAL through public LogDB surface, enabling none→keyed encryption migrations and key rotation
- Per-shard barrier routing in snapshot transport via `host.barrierForShard(shardID)` for multi-tenant isolation

### Changed

- Host snapshot pool DEK purge now dispatches per-shard when `WithShardBarriers` is configured, preventing one tenant's barrier purge from removing epochs live under another tenant's barrier

## [0.1.0-alpha] - 2026-03-10

### Added

- Multi-group Raft consensus with per-shard configuration and sharded ReadyMap (64 buckets) for O(1) dispatch
- Full Raft specification compliance: leader election, log replication, membership changes, snapshots, PreVote, CheckQuorum, leadership transfer
- Linearizable reads via ReadIndex and LeaseRead
- QUIC-only transport built on quic-go with mTLS and multiplexed streams
- QUIC 0-RTT reconnection support for reduced handshake latency
- Custom fixed-layout binary wire format with zero-copy semantics (no protobuf dependency)
- Algorithm-agile at-rest encryption (AES-128/192/256-GCM, ChaCha20-Poly1305, XChaCha20-Poly1305) with automatic hardware detection, barrier/seal/unseal pattern, and epoch-based key rotation
- HKDF-SHA256 derived data encryption keys with brute-force protection
- 16-shard write-ahead log (WAL) for parallel persistence with fdatasync durability and CRC32 checksums
- WAL compaction and crash recovery with log replay
- Engine worker pipeline: step, commit, and apply workers with snapshot pool
- Client session management for at-most-once proposal delivery and cross-replica deduplication
- Three state machine interfaces: StateMachine, ConcurrentStateMachine, and DiskStateMachine
- Optional NALookup interface for zero-allocation reads
- Optional Hasher interface for cross-replica consistency checks
- RSM adapter wrapping state machine interfaces with two-phase snapshot support
- Cluster discovery via static peer lists, UDP multicast with HMAC-SHA256 authentication, and DNS SRV records
- Automated bootstrap orchestration with quorum validation
- Global and per-shard rate limiting for proposal backpressure
- Lock-free hot paths using padded atomics to prevent false sharing
- 16-shard QUIC stream pool with dedicated heartbeat stream (stream 0)
- Pooled byte buffers on send and receive paths via sync.Pool
- Connection coalescing via singleflight.Group
- Prometheus metrics for proposals, commits, elections, snapshots, transport, and WAL operations
- Graceful ordered shutdown: step, commit, apply, snapshot with drain guarantee
- In-memory KV state machine for testing and examples
- Porcupine-based linearizability test suite
- 500+ unit tests with race detector coverage
