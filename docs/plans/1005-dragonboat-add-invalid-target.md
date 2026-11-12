# Plan: #1005 dragonboat add calls refuse a malformed target

Test gap under the v1.0.0 bar (#744).

## Objective

Pin that the contrib/dragonboat membership-add calls validate the target
address before they touch the shard:

- the six async calls (`RequestAddReplica`, `RequestAddNonVoting`,
  `RequestAddWitness` and their `WithIdentity` forms) return a nil
  `*RequestState` and an error matching `ErrInvalidAddress`;
- the three identity-less sync calls (`SyncRequestAddReplica`,
  `SyncRequestAddNonVoting`, `SyncRequestAddWitness`) return
  `ErrInvalidAddress`;
- `SyncGetShardMembership` is unchanged after every refused call;
- `StartConcurrentReplica` and `StartOnDiskReplica` with a malformed initial
  member return `ErrInvalidTarget` and leave no shard running.

Targets: no port (`localhost`), empty host (`:63002`), non-numeric port
(`localhost:abc`).

## File map

- docs/plans/1005-dragonboat-add-invalid-target.md (this file)
- contrib/dragonboat/nodehost_invalid_target_test.go (new)
- contrib/dragonboat/nodehost.go only for a bug found (test commit, then fix
  commit)

## Acceptance

`cd contrib/dragonboat && go test -race -count=1 -run InvalidAddTarget .`,
`go vet .` and `make lint` pass; the diff touches only File map files.
