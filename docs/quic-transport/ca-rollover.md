# CA rollover runbook

This runbook replaces the certificate authority that signs a cluster's node
certificates without restarting a node and without losing quorum. It uses
`Host.ReloadMTLS` (contrib/dragonboat: `NodeHost.ReloadMutualTLS`), which
swaps a node's certificate, key and CA pool in place.

The same call also renews a leaf before it expires, without a rollover. That
is step 2 on its own, using the current CA.

## What a reload does

- **New handshakes only.** Every handshake that starts after `ReloadMTLS`
  returns uses the new material, inbound and outbound. An established
  connection keeps the session it was authenticated with until it closes and
  reconnects. A reload never tears down a live connection. A session ticket
  issued before a reload does not resume after it, so every reconnect after a
  reload is a full handshake verified against the new pool.
- **All or nothing.** The new material is validated before anything changes.
  A refused reload returns `*MTLSReloadError` and leaves the old material in
  force.
- **The key stays.** A node's identity is the SHA-256 of its leaf's public
  key (`Host.NodeIdentity`). Peers commit that identity in
  `MemberIdentities`, so a reload must keep the same key pair. A new leaf
  issued for a new key is refused with `NodeIdentityChangedError`. Changing a
  node's key is a membership change, not a reload.
- **Exclusivity.** The new leaf must not cover a peer's host, which is the
  same rule `StartShard` enforces (`SharedPeerCertificateError`). A reload
  and a peer admission (`StartShard`, a membership add) are serialized: each
  checks against the state the other publishes, so one of them is refused
  when they conflict, never both admitted.
- **Revocation.** With revocation checking enabled, the CA bundle must hold
  the issuers of the peer leaves. The checker's issuer set is swapped
  together with the pool. A CA the new bundle drops is still kept by the
  checker for the connections it admitted, and only for them: their CRL
  entries and OCSP lookups keep resolving against it, so after step 3 such a
  session stays up while its leaf is good and is closed by the next 30-second
  sweep once its leaf is revoked. The dropped CA admits no new handshake. The
  checker releases it after the last such connection closes.

## Procedure

Let `OLD` be the CA in use and `NEW` its replacement. Each step is applied to
**every** node before the next step starts on **any** node. Node order within
a step does not matter. Confirm after each node that `ReloadMTLS` returned
nil, and after each step that the shard still commits.

1. **Trust both.** Reload every node with its current leaf and key and the
   CA bundle `OLD + NEW`.
2. **Rotate the leaves.** Have `NEW` issue a leaf for each node's **existing**
   key, with the same SANs. Reload every node with that leaf, its key, and
   still `OLD + NEW`.
3. **Drop the old CA.** Reload every node with its `NEW` leaf, its key, and
   the bundle `NEW` alone.

Then, to make every connection authenticate against `NEW`, let established
connections cycle as they normally do. A connection made before step 3 keeps
working until it reconnects.

`test/integration/host/ca_rollover_test.go` runs this procedure on a live
3-node shard under continuous proposals.

## What fails if a step is skipped or misordered

| Mistake | Result |
|---|---|
| Step 2 on a node before step 1 reached every peer | That node's new leaf is signed by `NEW`, which some peers do not yet trust. Its new connections to those peers are refused in both directions. If that splits enough nodes, the shard loses quorum once old connections drop. |
| Step 3 on a node before step 2 reached every peer | That node no longer trusts `OLD`, but some peers still present `OLD` leaves. Its new connections to them are refused. |
| Step 3 skipped | The rollover is incomplete. Anything signed by `OLD`, including a compromised or retired `OLD`, is still trusted. |
| Leaf re-issued for a new key | The reload is refused (`NodeIdentityChangedError`), and nothing changes. |
| Leaf covering another member's host | The reload is refused (`SharedPeerCertificateError`), and nothing changes. |
| Revocation enabled and the bundle has no CA | The reload is refused (`ErrCACertificateParse`), and nothing changes. |
| Restart a node with files from an earlier step | The node starts with that step's material. That is safe only if the current step is compatible with it, by the same rules as above. Update the files on disk with each reload. |

A refused reload is always safe to retry with corrected material. A
**successful** reload with the wrong bundle cannot be detected locally. It
shows up as refused handshakes in the peer logs, and is fixed by reloading the
previous step's material.
