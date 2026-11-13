# 744: Engine.DeliverLocalTo and Engine.TickInterval tests

**DeliverLocalTo.** `Engine.DeliverLocalTo(node, msg)` is how the snapshot
receive path injects an install into the node whose verdict it awaits, not
whatever node now holds the shard. It returns false for a nil node or a
stopped node (`Node.IsStopped`), and otherwise returns `Node.DeliverLocal`,
which stamps `proto.OriginLocal` and queues the message on that node's inbox.
`TestEngine_DeliverLocalTo` pins this on an engine that is never started, so
only the test drains an inbox. It covers: a nil node is refused; a loaded node
receives the message stamped OriginLocal (even when it was sent as
OriginRemote); and after UnloadNode(10), LoadNode of a replacement for shard
10, and old.Stop(), a delivery to the old node returns false and leaves both
inboxes empty, while a delivery to the replacement lands only in the
replacement. Deleting the `IsStopped` check in engine.go makes the
stopped-node subtest fail, so the test is not vacuous.

**TickInterval.** `Engine.TickInterval()` is one RTT: RTTMillisecond
milliseconds, with 0 treated as 1 ms (set in NewEngine). Host hands it to each
peer. `TestEngine_TickInterval` checks that RTTMillisecond 200 gives 200 ms
and 0 gives 1 ms.
