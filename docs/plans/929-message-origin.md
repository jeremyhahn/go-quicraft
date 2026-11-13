# #929: Message origin tests

## Objective

Pin the documented contract of `proto.MessageOrigin` (pkg/proto/message.go):
the zero value is `OriginRemote`, only in-process injectors produce
`OriginLocal`, `UnmarshalFrom` and `Reset` always leave `OriginRemote`, and the
origin never reaches the wire.

## Slices

1. `docs: plan for #929` — this file.
2. `test: ...` — pkg/proto/message_origin_test.go:
   - (a) zero value is `OriginRemote`; `IsLocal` true only for `OriginLocal`;
     `String` is `local` / `remote`, and an out-of-range origin is stable
     (`remote`, not local);
   - (b) `Message.UnmarshalFrom` into an `OriginLocal` message leaves
     `OriginRemote`;
   - (c) `Message.Reset` restores `OriginRemote`;
   - (d) two messages differing only in `Origin` marshal to identical bytes;
   - (e) a decoded `MessageBatch` holds only `OriginRemote` requests, also
     when decoded into a reused batch whose requests (in length or spare
     capacity) were `OriginLocal`.
3. Per divergence from the docs: a failing `test:` commit, then a `fix:`
   commit in message.go or messagebatch.go.

## Out of scope

Wire or on-disk format changes; the raft-side use of the origin
(raft.step exemptions, DeliverLocal); any file outside pkg/proto and this plan.
