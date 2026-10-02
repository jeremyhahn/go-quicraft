# Benchmark regression testing

> **Status: REFERENCE** · Scope: `make bench-compare` / `make bench-baseline`,
> `scripts/bench-compare.sh`, and the `make perf-*` comparison harness ·
> Last verified: 2026-10-02

Changes to the hot path (append, replicate, apply) are benchmarked before they
merge. This page describes the harness that makes that check meaningful, and why
the obvious way of benchmarking gives wrong answers.

## Why `make bench-<pkg>` cannot detect a regression

The per-package targets run each benchmark once, on whatever machine you are
on. Running the same binary twice with no source change on a busy dev
container gave:

```
BenchmarkInMemoryMerge-24    1137 ns/op
BenchmarkInMemoryMerge-24    5507 ns/op
```

Any regression smaller than that 5x spread is invisible, and the noise invents
regressions that do not exist. A naive block-versus-block comparison of two
revisions once reported a 2x regression on `BenchmarkInMemoryMerge` and
`BenchmarkRaftLogAppend`. Re-run with the interleaved method below, the same
revisions differed by -0.2% and -0.4%: the blocks had simply run under
different machine conditions.

## The method

The script enforces four rules.

1. **One toolchain builds both sides.** Baseline and current test binaries are
   compiled on the invoking machine with the same Go toolchain and
   `GOWORK=off`, so they differ only by source revision.
2. **You choose where they run.** With `PERF_HOST` unset the binaries run
   locally. With `PERF_HOST=user@host` they are copied to that host and run over
   ssh. The method is identical; only precision changes, because anything
   competing for CPU widens the intervals. The run's metadata records which mode
   was used and the CPU frequency governor it found.
3. **Rounds interleave.** Rounds alternate current, baseline, current,
   baseline. Machine conditions drift slowly, and a contiguous layout
   attributes that drift to whichever side ran second. Interleaving splits it evenly.
4. **Iteration count is fixed, never a duration.** `BENCH_ITERS` must end in
   `x` (for example `100000x`); the script refuses `1s`. With a duration, a
   round on a busy core runs fewer iterations, so the two sides do different
   work.

## Running a comparison

```sh
# Working tree against origin/main, on this machine.
make bench-compare

# The same comparison, executed on a dedicated host.
make bench-compare PERF_HOST=user@bench-host.example

# A specific revision, one package, more rounds.
make bench-compare BASELINE=v0.15.0-alpha BENCH_PKGS=./pkg/internal/raft/ BENCH_ROUNDS=15

# Only the benchmarks a change touches. '$|' is safe: the recipe passes
# BENCH_FILTER through unexpanded.
make bench-compare BENCH_FILTER='BenchmarkRaftLogAppend$|BenchmarkTryCommit$'
```

`PERF_HOST` accepts anything ssh does. ssh runs with `BatchMode=yes`, so
key-based auth must already work. The target needs no Go installation and no
checkout: the binaries are self-contained, run from a temporary directory, and
are deleted afterwards. It must have the same GOOS/GOARCH as the build machine.

### Variables

| Variable | Default | Meaning |
|---|---|---|
| `BASELINE` | `origin/main` | git ref to compare against |
| `PERF_HOST` | empty | where the binaries run; empty means this machine |
| `BENCH_PKGS` | `./pkg/internal/raft/ ./pkg/internal/engine/ ./pkg/proto/` | packages to benchmark; the default is the hot path |
| `BENCH_FILTER` | `.` | `-test.bench` regex |
| `BENCH_ROUNDS` | `8` | interleaved rounds per side; benchstat needs at least 6 for a confidence interval |
| `BENCH_ITERS` | `100000x` | fixed iteration count |
| `BENCH_TIMEOUT` | `30m` | per-invocation test binary timeout |
| `BASELINE_REF` | empty | `bench-baseline` only: pin the baseline to a committed revision |
| `BASELINE_LABEL` | `<run-host>-<short-sha>` | `bench-baseline` only: filename stem under `test/performance/baselines/` |

Read from the environment by the script: `BENCH_OUT_DIR` (default
`build/bench-compare/<timestamp>/`), `BENCH_KEEP_WORKTREE=1` (keep the baseline
worktree to skip a rebuild), `BENCH_WORKTREE_ROOT` (default
`build/bench-worktrees/`) and `BASELINE_DIR` (default
`test/performance/baselines`). Make exports command-line variables, so
`make bench-compare BENCH_KEEP_WORKTREE=1` reaches the script.

`PERF_HOST` is the one variable shared with the `perf-*` harness below. That
harness has its own `BENCHTIME`, `PERF_TIMEOUT`, `PERF_SYSTEMS` and
`PERF_PROFILE`; the shared prefix is not a shared setting.

### Tuning `BENCH_ITERS`

One count applies to every benchmark in the run, so run each scale class as its
own invocation. That is why stored baselines are split into `-hotpath` and
`-codec` files.

- **Microsecond-scale** (`pkg/internal/raft`, `pkg/internal/engine`):
  `200000x` is about 1s per benchmark per round.
- **Nanosecond-scale** (`pkg/proto` codec, 3 to 60 ns/op): at `200000x` timer
  resolution dominates, with intervals as wide as ±57% on an idle host.
  `5000000x` brought most to ±4–9%. Benchmarks under about 10 ns/op stay noisy
  at any count; judge them on `B/op` and `allocs/op`.
- **fsync-bound** (`pkg/logdb/waldb`): use something like `BENCH_ITERS=500x`
  and expect I/O variance to dominate.

## Interpreting the output

```
                 │   baseline   │              current               │
                 │    sec/op    │    sec/op     vs base              │
InMemoryMerge-20   4.872µ ± 14%   4.993µ ±  9%       ~ (p=0.279 n=8)
RaftLogAppend-20   2.649µ ± 28%   2.596µ ± 22%       ~ (p=0.574 n=8)
geomean            3.592µ         3.600µ        +0.22%
```

1. **`vs base`.** `~` means no statistically significant difference at
   α=0.05: the pass condition. A percentage with `p < 0.05` is a finding.
2. **`±`.** The sample spread. At ±20% or more, a `~` is weak evidence; raise
   `BENCH_ROUNDS` or `BENCH_ITERS`, or use a quieter host, and re-run.
3. **`n=`.** Under 6 samples there is no confidence interval; under 4, no
   p-value. Do not gate on those.
4. **`B/op` and `allocs/op`.** Near-deterministic and more sensitive than
   timing. An allocation appearing on a zero-alloc hot path is a regression even
   when `sec/op` shows `~`.

A regression is real when the p-value is significant, the intervals do not
overlap, and the result reproduces on a second independent run.

Without benchstat installed (`make install-benchstat`), the script falls back to
a median and percentage-delta table and says so. The fallback has no confidence
intervals, no significance test and no allocation comparison, and cannot tell a
17% noise swing from a real change. Install benchstat before gating a release.

## Stored baselines

`make bench-baseline` records the current numbers as a committed file under
`test/performance/baselines/`:

```sh
make bench-baseline \
    PERF_HOST=user@bench-host.example \
    BASELINE_REF=HEAD \
    BENCH_ROUNDS=10 \
    BENCH_ITERS=200000x \
    BENCH_PKGS='./pkg/internal/raft/ ./pkg/internal/engine/' \
    BASELINE_LABEL=bench-01-$(git rev-parse --short HEAD)-hotpath
```

Pin `BASELINE_REF`: a baseline built from a dirty tree is not reproducible, and
the script warns. Files are named `<run-host>-<short-sha>[-<scale-class>].txt`;
the host is in the name because absolute ns/op is a property of the machine.
Each file starts with a provenance header (generated time, run host, CPU model
and count, governor, load average, Go version, pinned ref, packages, filter,
rounds, iterations, command) followed by raw samples in Go benchmark format.
benchstat ignores the `#` lines, so a stored file diffs directly:

```sh
benchstat test/performance/baselines/<run-host>-<sha>-hotpath.txt \
          build/bench-compare/<run>/current.txt
```

Only compare baselines recorded on the same host. The difference between two
machines dwarfs any regression you are looking for.

## Choosing a `PERF_HOST`

Any ssh-reachable Linux box works. In order of impact:

- **Idle.** Nothing else competing for CPU during the run.
- **CPU governor `performance`.** Otherwise cores change clock mid-run and every
  interval widens; changing it needs root. The script reports what it found.
- **Consistent.** Use the same host for every baseline and comparison in a
  series.

The committed baselines were measured on a 20-thread Core i9-9900X with the
`powersave` governor, which is why their intervals sit around ±9–28% rather than
the ±2–3% a clock-pinned machine gives.

## Running in CI

The harness runs unattended: no prompts, no TTY, and with `PERF_HOST` unset no
ssh. Output and the baseline worktree live under `build/` and are removed on
every exit path. No workflow in `.github/workflows/` runs `bench-compare` today.
A job that does needs:

- `fetch-depth: 0` on checkout. The baseline is checked out into a git
  worktree, which a shallow clone cannot satisfy; the script detects that case
  and says so.
- The baseline branch fetched if it is not the one checked out
  (`git fetch --no-tags origin main:refs/remotes/origin/main`), or a SHA.
- A runtime budget: cost is rounds × 2 sides × packages × `BENCH_ITERS`. Narrow
  with `BENCH_FILTER` or `BENCH_PKGS` and keep `BENCH_ROUNDS` at 10 or more.
- No merge gate on `sec/op` from a shared runner, where intervals are wide.
  `B/op` and `allocs/op` are safe to gate on anywhere. To gate on timing, use a
  dedicated runner or a `PERF_HOST` you control, with the ssh key loaded first.

## Failure modes

The script exits non-zero instead of emitting plausible-looking numbers on:

- an unresolvable `BASELINE` (checked first; a shallow clone is reported as
  such);
- an unreachable `PERF_HOST`. It never falls back to the local machine, because
  that would attach the wrong provenance to the result;
- `BENCH_ITERS` given as a duration;
- a package that fails to build at the baseline revision;
- a `BENCH_FILTER` that matches no benchmarks. The check is on combined output,
  so a filter that matches in one package and not another still passes; confirm
  you got samples from every package you expected.

Cleanup always runs: the baseline worktree (unless `BENCH_KEEP_WORKTREE=1`),
local binaries, and the remote temporary directory.

## The cross-system harness (`make perf-*`)

`bench-compare` asks whether a change regressed QuicRaft against itself. The
harness under `test/performance/` compares QuicRaft with etcd-io/raft,
Dragonboat and OpenRaft, and stores results per machine profile.

```bash
make perf-remote PERF_HOST=user@host PERF_PROFILE=<profile>   # everything, on another machine

make perf-all      PERF_PROFILE=<profile>                     # run, store, generate here
make perf-run      PERF_PROFILE=<profile> PERF_SYSTEMS=etcd   # one engine
make perf-generate PERF_PROFILE=<profile>                     # documents from stored JSON
make perf-env      PERF_PROFILE=<profile>                     # capture this machine's specs

make perf-compare-all                                         # terminal report only, no profile
make perf-compare SYSTEMS="quicraft etcd"
```

`PERF_PROFILE` has no default, because a throughput number without its machine
cannot be interpreted. `make perf-remote` stages the tree on the target, runs
`capture-env.sh` there to record `env.json`, runs the suite in Docker, fetches
results into `test/performance/profiles/<profile>/`, and regenerates that
profile's documents. The run is detached; `make perf-remote-status` reattaches.

Inside a profile, `<system>.md` and `charts/` are generated, while
`analysis.md` is hand-written and never touched by a tool. Generation refuses
to run if a system it was asked to cover has no results; narrow with
`PERF_SYSTEMS` when the gap is intended. The profiles on record are listed in
[test/performance/README.md](../../test/performance/README.md).

## See also

- [Multi-group host engineering](../multi-group-host/engineering.md): the hot paths and the performance contract
- [Testing overview](overview.md): benchmark targets
- [Testing infrastructure](infrastructure.md): fuzzing, linearizability, fault injection
