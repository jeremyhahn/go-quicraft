# Health check: healthy-startup flake under -race

## Symptom

`TestHealthCheck_HealthyStartupNeverNotReady` failed on a -race CI shard
(run 44581832844, shard 0/4): "round 0: healthy startup reported not-ready:
not_ready_shards = [5]", about 0.16s after `StartShard`.

## Cause: test defect

The test ran a single-member shard with `RTTMillisecond = 1` and
`ElectionRTT = 50`, so the leaderless window (`Host.leaderlessGrace`,
`3 * ElectionRTT * RTTMillisecond`) was 150ms of wall-clock time.

The election runs on logical ticks. The step worker drives them from a
`time.Ticker` (`pkg/internal/engine/step.go`, the `case <-ticker.C` arm),
and a Ticker drops ticks whenever its reader overruns the interval. Under
-race on a loaded runner, `tick()` plus `processReady()` regularly takes
longer than 1ms, so the randomized 50-100 tick first election took longer
than 150ms of wall-clock time.

The documented contract (`docs/observability/api.md`, `HealthStatus`;
`docs/observability/user.md`) defines not-ready in wall-clock terms: a
started, unpaused shard with no known leader for longer than
`3 * ElectionRTT * RTTMillisecond` since it started. At 0.16s the shard
really had been leaderless for longer than 150ms, so not-ready was the
contract's answer. The product is right. The test asserted more than the
contract promises: that a 1ms-tick election always finishes inside the
window on any machine.

`leaderlessGrace` is not widened. A wider window would hide a slow
election that the contract tells operators about. With production tick
intervals (tens of milliseconds), the per-tick cost is small against the
interval, and the window covers the startup path.

## Reproduction

Two -race test binaries were built: one from the unchanged tree and one
with the fix. Each ran `-test.count=200` of the test with `GOMAXPROCS=1`,
both at once so that each loaded the other.

- Without extra load, the unchanged test passed all 200 runs. The fixed
  binary passed `-test.run 'TestHealthCheck_'` 200 times as well.
- Eight `GOMAXPROCS=1` instances of each binary were then run on the
  4-CPU lane, with 12 `yes > /dev/null` CPU hogs alongside them.
  - Every instance of the unchanged test failed, with the CI message at the
    same line: "round 0: healthy startup reported not-ready:
    not_ready_shards = [5]", about 0.2s after "shard started".
  - The fixed test, run on its own, passed all 8 x 40 runs under the
    same load.

## Same cause in two `pkg/host_test.go` tests (fixed in the second slice)

Under the same CPU-hog load, `TestHealthCheck_AllHealthy`
(`pkg/host_test.go`, `Status = "not-ready", want "ok"`) and
`TestHealthCheck_DiskFull` (`pkg/host_test.go`, same message) also failed,
and the verifier saw `TestHealthCheck_DiskFull` fail under `GOMAXPROCS=1`.
Both start a single-member shard with the default `ElectionRTT` at
`RTTMillisecond = 1`, which gives a 30ms window. They then called
`HealthCheck` once and expected `ok`, but under load more than 30ms can pass
between `StartShard` and that call.

Fixed: both tests now call `waitForLeader` after `StartShard` and before
`HealthCheck`, so each asserts what it is about (all healthy, disk full)
and not election timing. The product code and the window are unchanged.

## Fix (test only)

- The test uses a 10ms tick (`RTTMillisecond = 10`, `ElectionRTT = 10`:
  100ms nominal, 300ms window). The step worker's per-tick cost is small
  against that interval.
- It fails on a not-ready report only when the report arrives inside the
  window, measured from just before `StartShard`. That time is no later
  than the moment the window opened, so the elapsed time overstates the
  shard's age, and a report that the test calls early really is early.
- Once the leader is known it requires `Status == "ok"`.
