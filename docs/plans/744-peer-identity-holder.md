# #744 Peer.IdentityHolder raft-level contract

`TestPeerIdentityHolder` (pkg/internal/raft/peer_identity_holder_test.go) pins:

- **Committed bindings.** A peer built from a committed membership whose
  Identities bind 1→`spki:one`, 2→`spki:two` reports those holders.
- **Unknown / empty.** An unknown identity and `""` are `(0, false)`.
- **Empty identity member.** Replica 5 is a member with an empty identity;
  `identityHoldersOf` skips it and `membershipSnapshot.identityHolder` refuses
  `""`, so no `""` holder exists.
- **Committed add.** An AddNode for replica 3 with `spki:three`, applied through
  `applyMembershipEntry` and published, makes it `(3, true)`.
- **Committed removal.** `handleRemoveNode` moves the identity from
  `identities` to `removedIdentities`: `spki:two` becomes `(0, false)` and
  `IsRemovedIdentity("spki:two")` is true.
- **Duplicate identity.** The refusal is validate-only
  (`validateMemberIdentity` → `ErrDuplicateMemberIdentity`); the apply path
  never refuses a committed entry (`handleAddNode` calls `recordIdentity`
  unconditionally). So the test asserts the validate refusal and that
  `spki:one` stays `(1, true)`, not an apply-path refusal as the brief assumed.
- **Concurrency.** Readers of `IdentityHolder`/`IsRemovedIdentity` race-clean
  while an add and a removal apply.
