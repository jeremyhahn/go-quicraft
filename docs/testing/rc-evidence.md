# Pre-RC evidence

> **Status: ARCHIVE** · Scope: the pre-RC pass of v1.0.0 bar row V1-M (#464),
> run on `main` · Recorded: 2026-10-21

The pre-RC pass collects, in one place, the evidence a release candidate is
cut on: the drafts this code implements are still at their pinned revision, the
fuzz corpora are committed and pass, the hot path has not regressed against
the last release, and the leadership and election tests pass. Each section
names the commit it ran on, the command, and the result.

All local runs were on one shared 80-core Linux worker running other jobs at
the same time; the benchmark section says what that does to the numbers.

## Draft pins

Commit: `cd4cb4f`. Method: fetch `https://datatracker.ietf.org/doc/<name>/`
and read the current revision (the repository has no `scripts/draftrev.sh`).

| Draft | Pinned | Latest on the datatracker | Result |
|---|---|---|---|
| `draft-irtf-cfrg-xchacha` | `-03` | `-03` | current |
| `draft-ietf-tls-ecdhe-mlkem` | `-05` | `-05`, the final revision, published as RFC 10024 (Proposed Standard, August 2026) | no technical change |

`draft-ietf-tls-ecdhe-mlkem` is no longer a draft: its datatracker page
redirects to RFC 10024, and `-05` is the last revision in the archive. Compared
with the pinned `-05` (`docs/references/draft-ietf-tls-ecdhe-mlkem-05.txt`), the
RFC changes nothing this code implements:

- the code points are unchanged: X25519MLKEM768 `0x11EC`, SecP256r1MLKEM768
  `0x11EB`, SecP384r1MLKEM1024 `0x11ED`;
- the share layouts and sizes are unchanged: the X25519MLKEM768 client share
  is ML-KEM-768 encapsulation key then X25519 (1216 bytes), and the server share
  is ML-KEM ciphertext then X25519 (1120 bytes);
- the shared secret is unchanged: ML-KEM secret then X25519 secret, 64 bytes;
- the normative checks are unchanged (FIPS 203 §7.2 encapsulation key check,
  ciphertext length check, X25519 all-zero check, each failing with
  `illegal_parameter`). The only differences are editorial: "ECDH" becomes
  "ECDHE", the TLS 1.3 reference moves from RFC 8446 to RFC 9846 (so
  "Section 4.2.8.2" becomes "Section 4.3.8.2"), and some commas.

The pin is not moved here. Re-pinning the reference from the draft to RFC 10024
(`docs/references/`, its README index and the `ROADMAP.md` standards-watch
line) is a separate documentation change.

## Fuzz corpora

Commit: `cd4cb4f`. Every `Fuzz*` target in `pkg/internal/raft`,
`pkg/logdb/waldb` and `pkg/proto` was mutation-fuzzed for 5 minutes, all 24 at
once with two workers each:

```sh
go test -tags=quicraft_fuzz -count=1 -run='^$' -fuzz='^<Target>$' -fuzztime=5m -parallel=2 ./<pkg>
```

Result: **24 of 24 `ok`, no crasher.** The inputs the fuzzer kept as
interesting (`$(go env GOCACHE)/fuzz/<module>/<pkg>/<Target>/`) were copied
into `<pkg>/testdata/fuzz/<Target>/`, at most 200 per target, and committed. A
target with no new interesting input beyond its `f.Add` seeds has nothing to
commit. "Committed" is the file count in the directory after the copy,
including entries that were already there.

| Package | Target | Execs | New interesting | Committed |
|---|---|---:|---:|---:|
| `pkg/internal/raft` | `FuzzRaftLogRecovery` | 554,388 | 47 | 47 |
| `pkg/internal/raft` | `FuzzRaftStepSequence` | 448,021 | 122 | 122 |
| `pkg/logdb/waldb` | `FuzzAddrMapRoundTrip` | 626,183 | 1 | 1 |
| `pkg/logdb/waldb` | `FuzzBootstrapRecordRoundTrip` | 249,674 | 3 | 3 |
| `pkg/logdb/waldb` | `FuzzEntryRecordRoundTrip` | 214,866 | 4 | 4 |
| `pkg/logdb/waldb` | `FuzzRecordDecodeCrash` | 557,910 | 0 | 0 |
| `pkg/logdb/waldb` | `FuzzRecordRoundTrip` | 302,269 | 5 | 5 |
| `pkg/logdb/waldb` | `FuzzSegmentReplayCrash` | 82,116 | 25 | 25 |
| `pkg/logdb/waldb` | `FuzzStateRecordRoundTrip` | 610,444 | 0 | 0 |
| `pkg/logdb/waldb` | `FuzzUnmarshalAddrMapCrash` | 1,116,822 | 8 | 8 |
| `pkg/logdb/waldb` | `FuzzUnmarshalRemovedMapCrash` | 478,712 | 9 | 9 |
| `pkg/proto` | `FuzzConfigChangeRoundTrip` | 1,058,364 | 0 | 3 |
| `pkg/proto` | `FuzzEntryRoundTrip` | 1,028,002 | 0 | 1 |
| `pkg/proto` | `FuzzEntryUnmarshalCrash` | 1,765,730 | 4 | 7 |
| `pkg/proto` | `FuzzMembershipRoundTrip` | 733,078 | 1 | 6 |
| `pkg/proto` | `FuzzMessageBatchRoundTrip` | 1,052,717 | 0 | 3 |
| `pkg/proto` | `FuzzMessageBatchUnmarshalCrash` | 1,228,842 | 6 | 12 |
| `pkg/proto` | `FuzzMessageRoundTrip` | 916,058 | 0 | 1 |
| `pkg/proto` | `FuzzMessageUnmarshalCrash` | 1,857,630 | 4 | 9 |
| `pkg/proto` | `FuzzSnapshotChunkRoundTrip` | 925,480 | 1 | 1 |
| `pkg/proto` | `FuzzSnapshotChunkUnmarshalCrash` | 1,899,456 | 17 | 17 |
| `pkg/proto` | `FuzzSnapshotRoundTrip` | 876,153 | 0 | 4 |
| `pkg/proto` | `FuzzSnapshotUnmarshalCrash` | 1,850,248 | 14 | 21 |
| `pkg/proto` | `FuzzStateRoundTrip` | 1,001,218 | 0 | 1 |

The seed pass over the committed corpora, on `146779d`, the commit that adds
them (the `ci-fuzz-seeds` command, scoped to the three packages):

```sh
go test -race -count=1 -tags=quicraft_fuzz -run=Fuzz ./pkg/internal/raft ./pkg/logdb/waldb ./pkg/proto
```

```
ok  	github.com/jeremyhahn/go-quicraft/pkg/internal/raft	1.214s
ok  	github.com/jeremyhahn/go-quicraft/pkg/logdb/waldb	1.178s
ok  	github.com/jeremyhahn/go-quicraft/pkg/proto	1.120s
```

## Hot-path benchmarks

Current: `146779d` (main `cd4cb4f` plus the fuzz corpora above, with no code
change). Baseline: `75559bd`, the `v0.23.0-alpha` release commit (the tag
itself was not in the clone the pass ran from). Method: `make bench-compare`
([Benchmark regression testing](benchmarking.md)), which builds the baseline
in a worktree and interleaves the rounds, current then baseline. benchstat was
not installed, so the tables are the script's median and delta fallback: no
confidence intervals and no significance test. The host was shared and busy
(load average 104 on 80 CPUs at the start of the first run, 64 at the start
of the second); the Go runtime ran with `GOMAXPROCS=6` (cgroup limit).

The benchmarks are the append path (`BenchmarkRaftLogAppend`,
`BenchmarkInMemoryMerge`), the replicate and commit path
(`BenchmarkTryCommit`, `BenchmarkTryCommit_2Witnesses`,
`BenchmarkPeerGetUpdate`) and the apply path (`BenchmarkInMemoryMergeApply`,
`BenchmarkApplyWithReaders`).

```sh
make bench-compare BASELINE=75559bd BENCH_PKGS="./pkg/internal/raft/ ./pkg/internal/rsm/" \
  BENCH_FILTER='BenchmarkRaftLogAppend$|BenchmarkInMemoryMerge$|BenchmarkInMemoryMergeApply$|BenchmarkTryCommit$|BenchmarkTryCommit_2Witnesses$|BenchmarkPeerGetUpdate$|BenchmarkApplyWithReaders$' \
  BENCH_ROUNDS=5 BENCH_ITERS=200000x
```

| Benchmark | v0.23.0-alpha ns/op | main ns/op | Delta | n |
|---|---:|---:|---:|---:|
| `ApplyWithReaders/concurrent/readers=0` | 95.9 | 89.5 | −6.68% | 5 |
| `ApplyWithReaders/concurrent/readers=4` | 89.0 | 112.3 | +26.25% | 5 |
| `ApplyWithReaders/ondisk/readers=0` | 87.3 | 93.5 | +6.98% | 5 |
| `ApplyWithReaders/ondisk/readers=4` | 113.7 | 108.8 | −4.31% | 5 |
| `ApplyWithReaders/regular/readers=0` | 101.1 | 95.2 | −5.81% | 5 |
| `ApplyWithReaders/regular/readers=4` | 44712.0 | 43246.0 | −3.28% | 5 |
| `InMemoryMerge` | 3966.0 | 3944.0 | −0.55% | 5 |
| `InMemoryMergeApply` | 749.5 | 679.3 | −9.37% | 5 |
| `PeerGetUpdate` | 1770.0 | 1870.0 | +5.65% | 5 |
| `RaftLogAppend` | 3781.0 | 3859.0 | +2.06% | 5 |
| `TryCommit` | 96.6 | 113.4 | +17.40% | 5 |
| `TryCommit_2Witnesses` | 148.3 | 190.0 | +28.12% | 5 |

The three large deltas overlap in their raw samples. `TryCommit` ranged
92–136 ns at the baseline and 73–125 ns at main, and `TryCommit_2Witnesses`
119–194 ns at the baseline and 159–230 ns at main. The only commits between the
two that touch the Raft core are #398 (heartbeat during a WAL fsync stall) and
#456 (the rolling-upgrade wire window). `TryCommit` was therefore re-run with
ten times the iterations and twice the rounds:

```sh
make bench-compare BASELINE=75559bd BENCH_PKGS="./pkg/internal/raft/" \
  BENCH_FILTER='BenchmarkTryCommit$|BenchmarkTryCommit_2Witnesses$' \
  BENCH_ROUNDS=10 BENCH_ITERS=2000000x
```

| Benchmark | v0.23.0-alpha ns/op | main ns/op | Delta | n |
|---|---:|---:|---:|---:|
| `TryCommit` | 83.3 | 86.7 | +4.03% | 10 |
| `TryCommit_2Witnesses` | 145.4 | 149.6 | +2.85% | 10 |

The first run's gap does not reproduce, so it is not counted as a regression.
`ApplyWithReaders/concurrent/readers=4` (samples 69–108 ns at the baseline,
102–123 ns at main) was not re-run, and a release gate should re-measure it
with benchstat on a quiet host.

The WAL append benchmark, `BenchmarkDBAppendEntries` in `pkg/logdb/waldb`,
was added after `v0.23.0-alpha`, so it has no baseline and `bench-compare`
stops with "no baseline benchmark results produced". It was run on main only,
for the record. It is fsync-bound, and the spread is I/O variance:

```sh
go test -run='^$' -bench='BenchmarkDBAppendEntries$' -benchtime=500x -count=5 -benchmem ./pkg/logdb/waldb
```

| Benchmark | ns/op (5 runs) | Median | B/op | allocs/op |
|---|---|---:|---:|---:|
| `DBAppendEntries` | 65606, 33103, 25094, 34974, 31999 | 33103 | 929 | 4 |

## Leadership and election tests

Commit: `146779d` (main `cd4cb4f` plus the fuzz corpora). Every test whose
name matches `Leader` or `Election`, three times each, in the unit packages
and then in the integration packages:

```sh
go test -run 'Leader|Election' -count=3 -timeout=60m ./pkg/...
go test -tags integration -run 'Leader|Election' -count=3 -timeout=90m ./test/...
```

Result: both exit 0, no `FAIL`. The packages with matching tests:

```
ok  	github.com/jeremyhahn/go-quicraft/pkg	12.941s
ok  	github.com/jeremyhahn/go-quicraft/pkg/config	0.007s
ok  	github.com/jeremyhahn/go-quicraft/pkg/internal/engine	3.874s
ok  	github.com/jeremyhahn/go-quicraft/pkg/internal/raft	0.983s
ok  	github.com/jeremyhahn/go-quicraft/pkg/internal/transport	4.539s
ok  	github.com/jeremyhahn/go-quicraft/pkg/proto	0.006s

ok  	github.com/jeremyhahn/go-quicraft/test/buildgates	0.506s
ok  	github.com/jeremyhahn/go-quicraft/test/electionbudget	0.004s
ok  	github.com/jeremyhahn/go-quicraft/test/integration/e2e	123.634s
ok  	github.com/jeremyhahn/go-quicraft/test/integration/host	30.472s
ok  	github.com/jeremyhahn/go-quicraft/test/integration/linearizability	158.772s
ok  	github.com/jeremyhahn/go-quicraft/test/integration/raft	44.057s
ok  	github.com/jeremyhahn/go-quicraft/test/membership	3.654s
ok  	github.com/jeremyhahn/go-quicraft/test/proposer	1.216s
```

The remaining packages reported `ok ... [no tests to run]`: 20 under
`./pkg/...`, and `test/errfs`, `test/lockedmem`, `test/netfault`,
`test/netport` and `test/releasability` under `./test/...`.

## Batch run

The full batch on `cd4cb4f`: fuzz for 20 minutes per target, the full race
suite, and a 480-minute soak.

Run: 60277

result: pending
