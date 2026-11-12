# Plan: #1022 Peer.DrainReadRejects

Test gap on the v1.0.0 bar (tracked under #744).

## Subject

- `pkg/internal/raft/peer.go` `Peer.DrainReadRejects`: returns and clears
  `raft.readRejects`, nil when empty.
- `pkg/internal/raft/raft.go` `raft.handleReadIndexResp`: a response not from
  the leader returns `ErrReadIndexRespNotFromLeader`; a reject appends
  `msg.Hint` to `readRejects`, or returns `ErrReadIndexOverloaded` once the
  list holds `maxReadStates` keys.

## Test

New `pkg/internal/raft/peer_read_rejects_test.go` (package raft). A follower
Peer of a three-replica shard whose leader is replica 2 has ReadIndexResp
rejects stepped through `Peer.Handle`:

- (a) no rejects: `DrainReadRejects` returns nil;
- (b) Hints 7, 8, 9 from the leader drain as `[7 8 9]`; a second drain is nil;
- (c) a reject from replica 3 (not the leader) returns
  `ErrReadIndexRespNotFromLeader` and is absent from the next drain;
- (d) after `maxReadStates` rejects the next returns `ErrReadIndexOverloaded`,
  the list keeps `maxReadStates` keys with the last accepted key at the end;
  after a drain one more reject is accepted.

Every test name contains `DrainReadRejects`. Helpers take
`context.Context` first.

## Bugs

A divergence between code and doc is a failing test commit then a fix commit
in peer.go or raft.go.

## Acceptance

`go test -race -count=1 -run DrainReadRejects ./pkg/internal/raft/`,
`go vet ./pkg/internal/raft/`, `make lint` clean.
