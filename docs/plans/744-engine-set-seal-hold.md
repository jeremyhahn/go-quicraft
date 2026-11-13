# 744: Engine.SetSealHold reaches every commit worker

`TestEngineSetSealHold` (pkg/internal/engine/seal_hold_engine_test.go) builds an
Engine with `NumWorkers` 4 and does not start it.

**Worker count.** `len(e.commitWorkers) == 4`, so the per-worker loops below
cover four workers and a hold that reached only some of them is visible.

**Hold installed.** After `SetSealHold` with an `IsSealed` that matches one
sentinel error and an `UnsealedC` that records the shard it is asked about,
every worker's `isSealedSaveErr(sentinel)` is true, `isSealedSaveErr(other)`
is false, and `sealHold.UnsealedC(7)` reaches the recorder with shard 7 and
returns its channel.

**Zero hold restores fail-the-shard.** After `SetSealHold(SealHold{})`, every
worker's `isSealedSaveErr(sentinel)` is false and `sealHold.UnsealedC` is nil,
so every SaveState failure fails the shard again.

**Not vacuous.** Making `SetSealHold` install on `e.commitWorkers[0]` only
(never committed) fails the test at worker 1.
