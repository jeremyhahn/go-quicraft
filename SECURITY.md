# Security Policy

QuicRaft is a Raft consensus library. A defect in it can cost an embedding
application its data, its availability, or the confidentiality of what it has
written to disk. Vulnerability reports are handled privately until a fix is
available.

## Supported versions

| Version | Supported |
|---------|-----------|
| The newest `0.x` minor release | Yes. Fixes ship as the next `0.x` release. |
| Every earlier minor | No |

QuicRaft has not reached 1.0. There is no long-term support branch and no
backporting: a security fix is released as the next `0.x` version. Before 1.0,
on-disk formats can change in place, so upgrading may mean wiping a replica's
data directory and re-syncing it from peers. See [CHANGELOG.md](CHANGELOG.md)
and [docs/compatibility/](docs/compatibility/architecture.md).

## Reporting a vulnerability

**Do not open a public issue for a security vulnerability.**

Report privately through GitHub's private vulnerability reporting: open the
repository's **Security** tab and choose **Report a vulnerability**. This
creates a private advisory visible only to you and the maintainers.

The security contact is the maintainer, Jeremy Hahn. If you cannot use the
private reporting channel, contact him directly and say only that you have a
security report; do not include details until a private channel is
established.

Please include, as far as you can determine it:

- The affected version or commit.
- The component (transport, WAL, seal/barrier, raft protocol, state machine
  adapter).
- What an attacker gains, and what position they must already hold to attempt
  it: an unauthenticated network peer, a cluster member, a removed member,
  an operator with disk access, or a local process.
- A reproduction: a test, a sequence of API calls, or a crafted payload.
- Any mitigation you have found.

## What to expect

- **Acknowledgement within 3 business days.** If you have not heard back,
  assume the report did not arrive and try the alternate channel.
- **An initial assessment within 10 business days**, stating whether the
  report is accepted, the severity assigned, and the intended fix timeline.
- **Coordinated disclosure.** A fix is prepared privately, released, and then
  published as a GitHub Security Advisory with credit to the reporter unless
  you ask to remain anonymous.
- We ask for **90 days** before public disclosure, or until a fix ships,
  whichever comes first. If a fix will take longer we will say so and explain
  why.

There is no bug bounty program.

## Scope

**In scope.** Anything reachable by an attacker who does not already hold the
privileges the defect grants:

- Raft safety violations reachable from a crafted or replayed message --
  election safety, log matching, leader completeness, state machine safety.
- Anything that lets an unauthorized peer's `msg.From` be counted toward
  election or commit quorum, or that lets a **removed** replica rejoin
  consensus.
- Cryptographic defects in `pkg/seal`, `pkg/crypto/shamir` or the WAL and
  snapshot at-rest paths: nonce reuse, missing or forgeable AEAD binding, key
  material reachable after a purge, downgrade to an unauthenticated path.
- Anything that lets an attacker with write access to the persisted
  `SealedRootKey` change how the barrier uses its key: forging or stripping
  `StateMAC`, rewriting `Epoch` or `KnownEpochs` downward, or getting a
  rolled-back blob accepted past `MinEpoch`.
- Memory-safety or panic conditions reachable from network input, including
  unbounded allocation from a crafted length prefix.
- Transport authentication and authorization defects: certificate validation,
  revocation handling, cross-deployment message acceptance.
- Data loss or corruption from a crash at any point in the WAL or snapshot
  write path.

**Out of scope.** These are documented behaviours, not vulnerabilities. Please
read the linked material before reporting:

- **A host declared `WithoutEncryption()`, or one whose barrier is plaintext
  (`seal.NoneStrategy`), provides no at-rest confidentiality or integrity.** Snapshot
  artifacts in particular have no integrity check under it. Plaintext is never
  reached by omission: `NewHost` refuses a host that neither configures a
  barrier nor makes that declaration. See [docs/seal-barrier/architecture.md](docs/seal-barrier/architecture.md).
- **DEK confidentiality after a purge is GC-bounded, not enclave-bounded.**
  `PurgeEpochsBefore` revokes the capability to decrypt; it is not
  crypto-shredding, and neither the standard library nor `x/crypto` offers a
  key-wipe API for a constructed AEAD.
- **At-rest rollback of the WAL is not detected.** Someone who can write a
  replica's data directory can remove the WAL's trailing records together
  with the sync mark after them, and the result is byte-for-byte what a crash
  inside the writeback window leaves, so recovery truncates as it would for
  the crash. No in-file format can separate the two; that needs a trusted
  monotonic anchor outside the disk (a hardware counter, or peers). Raft's
  safety assumes stable storage, so treat write access to a data directory as
  compromise of that replica. Snapshots are different: every frame's context
  carries a terminal flag, so a truncated or extended snapshot fails
  authentication, and one truncated to nothing is refused. See
  [docs/seal-barrier/architecture.md](docs/seal-barrier/architecture.md); a
  report restating the WAL property adds nothing, but a path that exploits it
  WITHOUT write access to the data directory is welcome.
- **Frames can be spliced between two snapshot artifacts of the same
  identity.** A frame's context binds shard, replica, snapshot index, ordinal
  and terminal flag, but nothing unique to one artifact, so two artifacts
  sealed under the same key at the same identity (a rebuild re-save, or a local
  save and a receive at one index) can exchange frames. Doing so needs write
  access to the data directory and a retained copy of the other artifact, which
  is the same threat class as the WAL rollback above, and accepted with it.
- **CRC32-C in the WAL detects accidental corruption only.** It is not a
  tamper control; an adversary with disk access can recompute it. Tamper
  protection comes from the barrier's AEAD.
- **Shamir reconstruction performs no integrity check.** `shamir.CombineProtected`,
  `CombineBytes` and the deprecated `Combine` all return whatever the shares
  produce. Shares carry no digest by design, because one would be an offline
  brute-force oracle. The caller must authenticate the reconstructed secret out
  of band.
- **Two live barriers loading the same sealed blob is an embedder error, not a
  barrier defect.** Every DEK input is a function of the blob, so both derive
  the same key with a zeroed invocation counter and an empty nonce tracker.
  The load-generation fence (`SealedRootKey.LoadGeneration`) makes the second
  loader *detectable* (it fails closed with `ErrBarrierSuperseded`), but it
  is checked at load and enforced at write, never polled, so it is not a lock.
  Exclusive ownership of the blob is the embedder's to guarantee. See
  [docs/seal-barrier/user.md](docs/seal-barrier/user.md).
- **Delegated (hardware) mode bypasses the barrier's own AEAD safeguards.**
  When a `SealingStrategy` returns a non-nil `BarrierEncryptor`, epoch DEK
  derivation, the ciphertext header, the nonce-reuse tracker and the invocation
  counter are all skipped by design, because the external encryptor owns its
  key and nonce management. `Rotate` and `PurgeEpochsBefore` return
  `ErrHardwareMode`. A weakness in the injected encryptor is that encryptor's
  defect; report it to whichever project supplies it.
- **No signal handler is installed**, and SIGKILL is uncatchable in any case.
  Key material surviving a `kill -9` is expected.
- Anything requiring an attacker who already has code execution in the host
  process, or write access to the configuration the host reads at startup.
- Missing hardening in a *deliberately* unauthenticated mode the operator
  selected explicitly (`AllowZeroDeploymentID`,
  `AllowUnauthenticatedSealedState`, `WithoutTransport`).
- Vulnerabilities in dependencies, unless QuicRaft's use of them is what makes
  the defect reachable. Report those upstream; tell us if we need to bump.

## Security-relevant configuration

Several defaults are permissive for development and must be changed for
production. A deployment that leaves them is misconfigured rather than
vulnerable, but the distinction is not useful to an operator who has been
breached, so they are listed here:

- Set a non-zero `HostConfig.DeploymentID`. `HostConfig.AllowZeroDeploymentID`
  disables only the validation rule that rejects a zero value, and with it the
  cross-cluster isolation the ID provides.
- Configure `TransportConfig.MTLSConfig`. QuicRaft never falls back to TCP and
  never runs QUIC without TLS: the only TLS config it builds requires TLS 1.3,
  `RequireAndVerifyClientCert` and the `quicraft` ALPN, with a fixed list of
  key-exchange groups (X25519, P-256, P-384; P-256 and P-384 only in a
  `-tags fips` build). It refuses to start without an mTLS config
  (`ErrMTLSConfigRequired`). None of that is adjustable. What you control is
  which CAs go in the pool and which certificates you issue against them.
- Give every member its node identity (`Config.MemberIdentities` at bootstrap
  and as the operator anchor, `Host.RequestAddNodeWithIdentity` thereafter).
  An identity is the SHA-256 of the peer leaf's SubjectPublicKeyInfo, taken
  from the verified TLS handshake, and it is the ONLY thing a member is
  authorized by: a host with a transport refuses to admit a member without
  one, and a sender no configuration names is dropped. There is no address
  fallback and no switch.
- Issue one keypair per host. Two hosts sharing a leaf share an identity, and
  one identity cannot be committed for two replicas
  (`ErrDuplicateMemberIdentity`). Avoid wildcard or shared-SAN certificates for
  the same reason (`refuseInboundPromiscuousCert` catches the case where the
  peer's leaf also covers the local node's host).
- Configure a barrier. `seal.NoneStrategy` is not encryption.
- Set `BarrierConfig.StatePersister`. Without it the barrier cannot rotate
  (`ErrNoStatePersister`), the load-generation fence is inert, and the first
  unseal after a restart refuses to encrypt at all, because the loaded epoch's
  spent invocation budget is unknowable. This is the single configuration
  omission with the widest blast radius; see
  [docs/seal-barrier/user.md](docs/seal-barrier/user.md).
- Pin `BarrierConfig.Algorithm` to an explicit value. The default is derived
  from the local CPU, and the algorithm is both an HKDF domain separator and
  the cipher `Decrypt` selects by epoch, so data that outlives one host needs a
  cluster-wide choice.
- Set `BarrierConfig.RequireContextAAD` if you require positional AEAD
  binding; it fails at startup rather than at the first write.
- Set `BarrierConfig.MinEpoch` from the highest epoch any live WAL segment or
  retained snapshot still references. The sealed blob's metadata is
  authenticated, but a MAC cannot detect replay of an older authentic copy.
- Set `BarrierConfig.RequireFIPS` if you require the FIPS 140-3 module.
  `FIPSEnabled()` is environment-derived, so without this flag a binary built
  without the module fails open onto Argon2id and possibly ChaCha20-Poly1305.
- Leave `NonceTracking` unset (a nil `*bool` defaults to enabled) unless you
  have a specific reason. Tracking covers a bounded prefix of each epoch, not
  the whole of it.
- Leave `AllowUnauthenticatedSealedState` false. It exists only to load, once,
  a sealed blob written before the barrier stamped `StateMAC`; Unseal restamps
  it, after which the flag should be cleared.
