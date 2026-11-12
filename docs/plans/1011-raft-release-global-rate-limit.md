# Plan: #1011 Peer.ReleaseGlobalRateLimit test gap

`Peer.ReleaseGlobalRateLimit` (pkg/internal/raft/peer.go) zeroes the shard's
contribution to the host-wide in-memory rate limiter. Its doc says the call
is idempotent and leaves other shards' shares alone. Only the inMemory level
is tested today; nothing tests the Peer method.

New file pkg/internal/raft/peer_release_globalrl_test.go (package raft):

- (a) two peers from `NewPeer` share one `server.NewRateLimiter`; entries
  merged into each peer's in-memory log put both shares in the limiter;
  releasing one leaves `Get()` equal to the other's share.
- (b) a second release on the same peer changes nothing; releasing the other
  brings `Get()` to zero.
- (c) 20 create/fill/release cycles on one limiter end at zero and
  `RateLimited()` is false.

If the code differs from its doc: a failing test commit, then a fix commit
in peer.go. A wrong doc comment is corrected in peer.go instead.

Refs #1011
