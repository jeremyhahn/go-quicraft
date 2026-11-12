# Plan: #1041 transport OnSnapshotRejected listener dispatch

Test gap: `QUICTransport.SetListenerDispatcher` (pkg/internal/transport/quic.go)
installs the dispatcher on the snapshot receiver; `snapshotReceiver.notifyRejected`
(pkg/internal/transport/snapshot_recv.go) returns without a listener or an
`OnSnapshotRejected`, hands delivery to the dispatcher, and logs
"listener notification dropped" with event and shard_id when it refuses.

New file pkg/internal/transport/rejected_listener_dispatch_test.go asserts on a
`NewQUICTransport`:

- (a) an accepting dispatcher is called exactly once per rejected transfer and
  the listener runs only when the queued function runs, with the transfer's
  shard, replica and index;
- (b) a refusing dispatcher: the listener never runs and the log carries
  "listener notification dropped", `event=OnSnapshotRejected` and the shard id;
- (c) no listener, or an EventListener whose `OnSnapshotRejected` is nil: the
  dispatcher is never called.

A divergence from the docs is a failing test commit, then a fix commit.
