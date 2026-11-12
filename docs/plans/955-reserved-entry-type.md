# Plan: #955 reserved entry types are refused at propose time

## Objective

Pin `ErrReservedEntryType` (pkg/internal/raft/errors.go): the leader's
`handleLeaderPropose` (pkg/internal/raft/raft.go) refuses a Propose batch
carrying an `EntryInitialMembership` or `EntryMetadata` entry, before any
state mutation, whether the Propose is local or follower-forwarded.

## Slices

1. This plan.
2. pkg/internal/raft/reserved_entry_type_test.go:
   - a single `EntryInitialMembership` entry and a single `EntryMetadata`
     entry are each refused with an error `errors.Is` matches to
     `ErrReservedEntryType`;
   - a three-entry batch with the reserved entry last is refused whole:
     `log.lastIndex()`, the pending outbound messages and
     `log.confChangeIndex` are unchanged;
   - `EntryNormal`, `EntryConfigChange` (valid encoded AddNode) and
     `EntryEncodedEntry` are accepted, and so is an `EntryNormal` with
     compression bits above `EntryTypeMask` set;
   - the same refusal for a forwarded Propose (`Origin: OriginRemote`,
     `From` a follower).
3. Per divergence between code and docs: a failing test commit, then a fix
   commit in pkg/internal/raft/raft.go (or a doc fix where the code is right).

## Out of scope

Any change to wire or on-disk formats; other propose-time gates
(config-change validation, log index bound, leadership transfer).

Refs #955
