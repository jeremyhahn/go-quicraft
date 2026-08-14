# Security Policy

QuicRaft is a Raft consensus library. A defect in it can cost an embedding
application its data, its availability, or the confidentiality of everything it
has written to disk. Vulnerability reports are taken seriously and handled
privately until a fix is available.

## Supported Versions

| Version | Supported |
|---------|-----------|
| `0.3.x-alpha` | Yes -- fixes land on the latest `0.3.x-alpha` |
| `0.2.x-alpha` and earlier | No |

QuicRaft is **alpha software**. There is no long-term support branch and no
backporting: a security fix is released as the next `0.x` version, and
upgrading may require a format migration. See
[CHANGELOG.md](CHANGELOG.md) and
[docs/operations/compatibility.md](docs/operations/compatibility.md).

## Reporting a Vulnerability

**Do not open a public issue for a security vulnerability.**

Report privately through GitHub's private vulnerability reporting: open the
repository's **Security** tab and choose **Report a vulnerability**. This
creates a private advisory visible only to you and the maintainers.

If you cannot use that channel, contact the maintainer listed in the
repository metadata and say only that you have a security report; do not
include details until a private channel is established.

Please include, as far as you can determine it:

- The affected version or commit.
- The component (transport, WAL, seal/barrier, raft protocol, state machine
  adapter).
- What an attacker gains, and what position they must already hold to attempt
  it -- an unauthenticated network peer, a cluster member, a removed member,
  an operator with disk access, or a local process.
- A reproduction: a test, a sequence of API calls, or a crafted payload.
- Any mitigation you have found.

## What to Expect

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
- Memory-safety or panic conditions reachable from network input, including
  unbounded allocation from a crafted length prefix.
- Transport authentication and authorization defects: certificate validation,
  revocation handling, cross-deployment message acceptance.
- Data loss or corruption from a crash at any point in the WAL or snapshot
  write path.

**Out of scope.** These are documented behaviours, not vulnerabilities. Please
read the linked material before reporting:

- **A plaintext barrier (`seal.None`) provides no at-rest confidentiality or
  integrity.** Snapshot artifacts in particular have no integrity check under
  it. See [docs/security/encryption.md](docs/security/encryption.md).
- **DEK confidentiality after a purge is GC-bounded, not enclave-bounded.**
  `PurgeEpochsBefore` revokes the capability to decrypt; it is not
  crypto-shredding, and neither the standard library nor `x/crypto` offers a
  key-wipe API for a constructed AEAD.
- **CRC32-C in the WAL detects accidental corruption only.** It is not a
  tamper control; an adversary with disk access can recompute it. Tamper
  protection comes from the barrier's AEAD.
- **`shamir.Combine` performs no integrity check.** Shares carry no digest by
  design -- one would be an offline brute-force oracle. The caller must
  authenticate the reconstructed secret out of band.
- **No signal handler is installed**, and SIGKILL is uncatchable in any case.
  Key material surviving a `kill -9` is expected.
- Anything requiring an attacker who already has code execution in the host
  process, or write access to the configuration the host reads at startup.
- Missing hardening in a *deliberately* unauthenticated mode the operator
  selected explicitly (`AllowZeroDeploymentID`,
  `AllowUnauthenticatedSealedState`, `WithoutTransport`).
- Vulnerabilities in dependencies, unless QuicRaft's use of them is what makes
  the defect reachable. Report those upstream; tell us if we need to bump.

## Security-Relevant Configuration

Several defaults are permissive for development and must be changed for
production. A deployment that leaves them is misconfigured rather than
vulnerable, but the distinction is not useful to an operator who has been
breached, so they are listed here:

- Set a non-zero `DeploymentID`. `AllowZeroDeploymentID` disables the
  cross-cluster isolation check.
- Configure mTLS on `TransportConfig`. QuicRaft never falls back to TCP and
  never runs QUIC without TLS, but certificate *verification* strictness is
  yours to set.
- Commit node identities (`Config.MemberIdentities` at bootstrap,
  `Host.RequestAddNodeWithIdentity` thereafter) and then enable
  `TransportConfig.RequireMemberIdentity`. Without committed identities the
  removal deny-list can only be evaluated against the claimed `msg.From`,
  which a removed replica evades by naming a different replica ID.
- Avoid wildcard or shared-SAN certificates. A certificate covering every
  member's address lets its holder assert any member's identity on the
  address-only path.
- Configure a barrier. `seal.None` is not encryption.
- Set `BarrierConfig.RequireContextAAD` if you require positional AEAD
  binding; it fails at startup rather than at the first write.
- Leave `NonceTracking` unset (it defaults to enabled) unless you have a
  specific reason.
- Clear `AllowUnauthenticatedSealedState` after the one-start migration.
