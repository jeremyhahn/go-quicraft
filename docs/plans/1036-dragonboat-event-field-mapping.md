# Plan: #1036 dragonboat event field mapping

Refs #1036 (v1.0.0 bar test gap, tracked under #744).

## Objective

Prove that `ConvertEventListeners` (contrib/dragonboat/raftio/events.go)
passes every field of each quicraft event to the dragonboat listener:

- (a) every snapshot callback delivers a `SnapshotInfo` equal to one with
  distinct ShardID, ReplicaID, From and Index;
- (b) `MembershipChanged` delivers the ShardID and ReplicaID it was given;
- (c) `ConnectionEstablished` and `ConnectionFailed` deliver the address
  with `SnapshotConnection` false;
- (d) with both listeners non-nil, `OnLeaderUpdated` and `OnNodeReady` are
  set and each reaches only its own listener.

## File map

- docs/plans/1036-dragonboat-event-field-mapping.md (this file)
- contrib/dragonboat/raftio/events_mapping_test.go (new)
- contrib/dragonboat/raftio/events.go (only for a bug found)

## Approach

New recording fakes in the test file log every call (method name and
payload) to one shared log, so a test can assert both the payload and that
no other listener method fired. Test names contain `FieldMapping`; helpers
take `context.Context` first.

## Acceptance

- `cd contrib/dragonboat && go test -race -count=1 -run FieldMapping ./raftio/`
- `cd contrib/dragonboat && go vet ./raftio/`
- `make lint` clean
- `git diff --name-only 89cd7783..HEAD` lists only File map files
