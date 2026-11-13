# Plan: #1072 waldb metrics sink calls

Test gap: no test opens a waldb DB with a `LogDBMetrics` sink and counts calls.

New file `pkg/logdb/waldb/metrics_test.go` (package waldb), a mutex-guarded
counting sink, counts taken as deltas after `Open` returns:

- (a) synced DB: one `SaveState` of one Update (entries 1..3, 100-byte Cmd)
  gives exactly 1 `ObserveWriteLatency`, >= 1 `ObserveFsyncLatency`,
  `IncWriteBytes` summing to >= 300.
- (b) `SaveState(nil)` and `SaveState([]logdb.Update{})` change no counter.
- (c) `WithNoSync(true)`: 1 write latency, bytes > 0, 0 fsync latency.
- (d) `Compact(1, 1)` adds exactly 1 `ObserveCompactionLatency`.
- (e) after `Close`, `SaveState` returns `logdb.ErrClosed`, no counter changes.
- (f) no `WithMetrics`: the same `SaveState` and `Compact` return nil.

A divergence from the `LogDBMetrics` documentation gets a failing test commit,
then a fix commit in `pkg/logdb/waldb/db.go`.
