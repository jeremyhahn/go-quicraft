# Benchmark Regression Testing

CLAUDE.md requires that any change to the hot path (append, replicate, apply) be
benchmarked. This document describes the harness that makes that requirement
enforceable, and -- just as importantly -- explains why the obvious way of doing
it produces answers that are worse than useless.

The entry points are `make bench-compare` and `make bench-baseline`, both backed
by `scripts/bench-compare.sh`.

## The problem: `make bench-raft` cannot detect a regression

The per-package targets (`make bench-raft`, `make bench-proto`, ...) run each
benchmark exactly once on whatever machine you are sitting at. On a shared
developer box that is not a measurement, it is a coin flip.

Measured on the dev container, running the *same binary* twice with no source
change in between:

```
BenchmarkInMemoryMerge-24    1137 ns/op
BenchmarkInMemoryMerge-24    5507 ns/op
```

A 5x spread between identical runs. Any real regression smaller than 5x is
invisible underneath it, and -- worse -- the noise routinely manufactures
regressions that do not exist.

### The false-alarm incident

While validating this harness, a comparison was run the naive way: build the
baseline, run all its benchmarks as one block, then build the current tree and
run all of its benchmarks as a second block. The result reported a **2x
regression on `BenchmarkInMemoryMerge` and `BenchmarkRaftLogAppend`**.

There was no regression. The two blocks had simply run under different machine
conditions -- background compilation, container churn, thermal state. Re-running
the identical revisions with the interleaved method described below produced:

| Benchmark            | Baseline | Current  | Delta |
|----------------------|----------|----------|-------|
| `InMemoryMerge`      | 2828 ns  | 2822 ns  | -0.2% |
| `RaftLogAppend`      | 1469 ns  | 1463 ns  | -0.4% |

Run-to-run variance fell from ~5x to ~5%. Nothing about the code changed between
the two experiments; only the measurement method did.

The same effect is reproducible on demand. Comparing `HEAD~1` against `HEAD` for
two proto benchmarks *that neither commit touches*, on a busy machine:

```
benchmark                                      baseline ns/op  current ns/op      delta        n
BenchmarkEntryMarshalTo                                   8.7            7.2    -17.44%        5
BenchmarkMessageMarshalTo                                26.2           29.2    +11.16%        5
```

Both numbers are pure noise. A double-digit delta from an unreplicated run
with no confidence interval is not a signal, whatever machine produced it.

## The method

Four rules, all enforced by the script.

### 1. Build both sides with one toolchain

Both sides are compiled by the machine you invoke `make` on, with the same Go
toolchain and `GOWORK=off` (so neither picks up a shared `go.work` above the
repo). The two test binaries therefore differ only by source revision.

### 2. Pick where they run

Go test binaries are self-contained, so the machine that compiles them need not
be the machine that runs them. `PERF_HOST` chooses:

| `PERF_HOST` | Behaviour |
|-------------|-----------|
| unset (default) | The binaries run on the machine you invoked `make` on. |
| `user@host`     | The binaries are `scp`'d to that host and run there over ssh. |

**Both modes are supported and use exactly the same measurement method** --
same interleaving, same fixed iteration count, same benchstat comparison. Only
the execution location differs, and the run's metadata header records which was
used. Local is the right choice for a quick check during development and for
CI; there is nothing to configure.

What changes between the modes is *precision*, not validity. Anything competing
for CPU -- compilers, containers, editors, other CI jobs -- widens the
confidence intervals benchstat reports. A quiet machine dedicated to the run
narrows them. On this project's shared dev container, load average during a
routine session sits at 25--31 on 24 threads, and `BenchmarkInMemoryMerge` has
been seen to swing 5x between identical back-to-back runs; a small regression is
simply not resolvable under that. Read the `±` column and decide whether the
interval is tight enough to support the conclusion you want to draw. If it is
not, raise `BENCH_ROUNDS`/`BENCH_ITERS`, or point `PERF_HOST` at a quieter
machine.

The same applies to the CPU frequency governor of whichever host runs the
benchmarks. A governor other than `performance` lets cores ramp their clock up
and down mid-run, which shows up directly as wider intervals. The script reports
the governor it found and notes when it is not `performance`; changing it
requires root on that host.

### 3. Interleave the two sides

Rounds alternate `current, baseline, current, baseline, ...` rather than running
each side as one contiguous block. Machine conditions drift slowly -- thermals,
background jobs, cache state -- and a contiguous layout attributes all of that
drift to whichever side happened to run second. Interleaving splits the drift
evenly between the two sides, where it cancels.

This single change is what turned the 2x false alarm into a 0.2% null result.

### 4. Fix the iteration count, never the duration

The script requires `BENCH_ITERS` to end in `x` (e.g. `200000x`) and refuses a
duration such as `1s`:

```
ERROR: BENCH_ITERS must be a fixed iteration count ending in 'x' (got '1s').
       A duration lets the two sides run different iteration counts, which makes
       the samples incomparable. Use e.g. BENCH_ITERS=100000x.
```

With `-benchtime=1s`, Go keeps adding iterations until the wall clock elapses.
A round that happens to run on a busy core does *fewer* iterations, so the two
sides are not doing the same work, and per-iteration overheads (setup, GC
pressure, cache warmth) land differently on each side. A fixed count removes
that whole class of confound.

## Running a comparison

```sh
# Compare the working tree against origin/main, on this machine.
make bench-compare

# Same comparison, executed on a dedicated host instead (any ssh destination).
make bench-compare PERF_HOST=user@bench-host

# Against a specific revision, one package, more rounds.
make bench-compare \
    BASELINE=v0.9.0 \
    BENCH_PKGS=./pkg/internal/raft/ \
    BENCH_ROUNDS=15

# Narrow to the benchmarks a change actually touches. Anchored alternations
# containing '$|' are safe: the recipe passes BENCH_FILTER through unexpanded,
# so make does not eat the '$'.
make bench-compare \
    BENCH_FILTER='BenchmarkRaftLogAppend$|BenchmarkTryCommit$'
```

`PERF_HOST` accepts anything ssh does (`host`, `user@host`, or a `Host` alias
from `~/.ssh/config`). ssh runs with `BatchMode=yes`, so key-based auth to that
destination must already work -- the script never prompts. Nothing about the
target host needs preparing beyond that: no Go installation, no checkout, no
shared filesystem. The binaries are self-contained, land in a temp directory
under `/tmp` on the target, and are deleted when the run ends.

### Variables

| Variable        | Default                                                  | Meaning |
|-----------------|----------------------------------------------------------|---------|
| `BASELINE`      | `origin/main`                                             | Git ref to compare against. Anything `git rev-parse` accepts. |
| `PERF_HOST`     | *(empty)*                                                 | Where the benchmark binaries run. Empty = this machine. `user@host` = run them there over ssh. |
| `BENCH_PKGS`    | `./pkg/internal/raft/ ./pkg/internal/engine/ ./pkg/proto/` | Packages to benchmark. The default is the hot path: append, apply, replicate-codec. |
| `BENCH_FILTER`  | `.`                                                       | `-test.bench` regex. |
| `BENCH_ROUNDS`  | `8`                                                       | Interleaved rounds per side. benchstat needs >= 6 for a confidence interval. |
| `BENCH_ITERS`   | `100000x`                                                 | Fixed iteration count. Must end in `x`. |
| `BENCH_TIMEOUT` | `30m`                                                     | Per-invocation test binary timeout. |
| `BASELINE_REF`  | *(empty)*                                                 | `bench-baseline` only: pin the recorded baseline to a committed revision. |
| `BASELINE_LABEL`| `<host>-<short-sha>`                                      | `bench-baseline` only: filename stem under `test/performance/baselines/`. |

Additional script-level knobs: `BENCH_OUT_DIR` (artifact directory, defaults to
a timestamped dir under `build/bench-compare/`), `BENCH_KEEP_WORKTREE=1` (keep
the baseline worktree between runs to skip a rebuild), `BENCH_WORKTREE_ROOT`
(where that worktree lives, default `build/bench-worktrees/`), and
`BASELINE_DIR` (where `bench-baseline` writes, default
`test/performance/baselines`). These are read from the environment rather than
declared in the Makefile, but make exports command-line variables, so
`make bench-compare BENCH_KEEP_WORKTREE=1` reaches the script.

`PERF_HOST` names the *execution location* and is the one variable shared with
the other perf harness in this repo (`test/performance/run-remote.sh`, driven by
`make perf-remote`, which requires it and has no local mode). The variables above
configure the *measurement* and are read only by `bench-compare`. `run-remote.sh`
has its own unrelated `BENCHTIME`, `PERF_TIMEOUT`, `PERF_SYSTEMS`,
`PERF_PROFILE` and `BENCH_TMPDIR`; the shared prefix is not a shared setting.

## The comparison harness (`make perf-*`)

`bench-compare` answers "did this change regress QuicRaft against itself".
The comparison harness under `test/performance/` answers "how does QuicRaft
compare to etcd-io/raft, Dragonboat and OpenRaft", and it is a different tool
with a different unit of storage: the **machine profile**.

```bash
# Everything, against any machine, in one command.
make perf-remote PERF_HOST=user@host PERF_PROFILE=omega

# On this machine.
make perf-all      PERF_PROFILE=omega                   # run, store, generate
make perf-run      PERF_PROFILE=omega PERF_SYSTEMS=etcd # one engine
make perf-generate PERF_PROFILE=omega                   # documents from stored JSON
make perf-env      PERF_PROFILE=omega                   # capture this machine's specs

# Ad-hoc: terminal report only, stores nothing, needs no profile.
make perf-compare-all
make perf-compare SYSTEMS="quicraft etcd"
```

`PERF_PROFILE` is required by everything that reads or writes stored results and
has no default, because a throughput number without its machine cannot be
interpreted. Two profiles are tracked long-term: `omega` (single-socket,
desktop-grade) and `r640` (dual-socket, server-grade).

`make perf-remote` stages the working tree onto the target, runs
`capture-env.sh` **there** to record the machine into `env.json`, runs the suite
in Docker, fetches the results and the capture back into
`test/performance/profiles/<name>/`, and regenerates that profile's documents.
The remote run is detached; `make perf-remote-status` or
`test/performance/run-remote.sh wait` reattaches to one in flight.

Inside a profile, `<system>.md` and `charts/` are generated and disposable,
while `analysis.md` is hand-written and is never written by any tool. A system
with no stored results produces no document at all rather than a document of
`N/A` rows, and generation refuses to run if any system it was asked to cover is
missing — narrow it explicitly with `PERF_SYSTEMS` when the gap is intended. See
[test/performance/README.md](../../test/performance/README.md).

### Tuning `BENCH_ITERS`

One iteration count applies to every benchmark in the run, so it has to suit the
slowest and the fastest benchmark in `BENCH_PKGS` simultaneously. It usually
cannot.

- **µs-scale benchmarks** (`pkg/internal/raft`, `pkg/internal/engine`):
  `200000x` gives ~1 s per benchmark per round. Higher values mostly cost time.
- **ns-scale benchmarks** (`pkg/proto` codec: 3--60 ns/op): `200000x` is only a
  few milliseconds of work, so timer resolution and scheduler jitter dominate.
  On an idle dedicated host that still produced intervals as wide as ±57%.
  Raising it to `5000000x`
  pulled most of them to ±4--9%:

  | Benchmark                     | @ 200000x | @ 5000000x |
  |-------------------------------|-----------|------------|
  | `MessageMarshalTo`            | ±20%      | ±4%        |
  | `MessageBatchUnmarshalFrom`   | ±14%      | ±4%        |
  | `EntrySlicePool_GetPut`       | ±54%      | ±6%        |
  | `EntryMarshalTo`              | ±4%       | ±9%        |
  | `StateMarshalTo` (2.97 ns/op) | ±57%      | ±39%       |

  Note the last row: benchmarks below ~10 ns/op stay noisy at any iteration
  count, because a single timer tick is a large fraction of the measurement.
  Judge those on `B/op` and `allocs/op`, and treat their timing as
  indicative only.
- **fsync-bound benchmarks** (`pkg/logdb/waldb`): a fixed count in the hundreds
  of thousands would run for hours. Benchmark these separately with something
  like `BENCH_ITERS=500x`, and expect I/O variance to dominate regardless.

The practical consequence: **run each scale class as its own invocation**. That
is why the stored baselines are split into `-hotpath` and `-codec` files.

## Running in CI

The harness is designed to run unattended. There are no interactive prompts, no
TTY assumptions, and with `PERF_HOST` unset no ssh at all -- so no agent, key or
network access is required. Output directories and the baseline worktree are
created on demand under `build/` (gitignored) and removed on every exit path,
including failures; nothing outside the checkout is touched, so a runner with an
ephemeral or unshared `/tmp` is fine. The exit status is 0 for a successful run
(local or remote) and non-zero for a genuine failure: a build error, an
unresolvable `BASELINE`, an unreachable `PERF_HOST`, a `BENCH_ITERS` that is a
duration rather than a fixed count, or a `BENCH_FILTER` matching no benchmarks.

There is currently **no benchmark job in `.github/workflows/`** -- `ci.yml` and
`release.yml` do not invoke `bench-compare`. Add one deliberately if you want
it; this is the shape it takes:

```yaml
  bench:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
        with:
          # REQUIRED. bench-compare checks out BASELINE into a git worktree,
          # which a shallow (default depth-1) clone cannot satisfy. The script
          # detects that case and tells you so, but the fix is here.
          fetch-depth: 0
      - uses: actions/setup-go@v5
        with: { go-version-file: go.mod }
      - run: make install-benchstat
      - name: Benchmark vs base branch
        run: |
          make bench-compare \
            BASELINE=origin/${{ github.base_ref || 'main' }} \
            BENCH_FILTER='BenchmarkRaftLogAppend$|BenchmarkTryCommit$' \
            BENCH_ROUNDS=10
      - uses: actions/upload-artifact@v4
        if: always()
        with:
          name: bench-compare
          path: build/bench-compare/
```

Notes for whoever wires this up:

- **`fetch-depth: 0`.** Without it `origin/main` either does not resolve or has
  no usable tree. The script fails with an explicit "shallow clone" message
  rather than a raw git error, but it still fails.
- **Single-branch clones.** `actions/checkout` fetches only the ref being built.
  If `BASELINE` is a branch other than the one checked out, fetch it first
  (`git fetch --no-tags origin main:refs/remotes/origin/main`) or pass a SHA.
- **Budget the runtime.** Cost is `rounds x 2 sides x packages x BENCH_ITERS`.
  The defaults (8 rounds, 3 packages, `100000x`) take a while; narrow with
  `BENCH_FILTER`/`BENCH_PKGS` and keep `BENCH_ROUNDS` at 10 or more so benchstat
  still emits a confidence interval.
- **Do not gate a merge on the `sec/op` verdict from a shared CI runner.** The
  intervals will be wide, so a `~` is weak evidence and an occasional
  significant-looking delta is noise. `B/op` and `allocs/op` are
  near-deterministic and *are* safe to gate on; they will catch an allocation
  appearing on a zero-alloc hot path regardless of how noisy the runner is.
- To gate on timing, give the job a dedicated runner and either set `PERF_HOST`
  to a machine you control (the CI runner then needs the ssh key loaded before
  `make` is invoked -- `BatchMode=yes` means no prompting and no fallback) or
  run the job directly on that machine as a self-hosted runner with
  `PERF_HOST` unset.

## Interpreting the output

With benchstat installed, the comparison looks like this:

```
                 │   baseline   │              current               │
                 │    sec/op    │    sec/op     vs base              │
InMemoryMerge-20   4.872µ ± 14%   4.993µ ±  9%       ~ (p=0.279 n=8)
RaftLogAppend-20   2.649µ ± 28%   2.596µ ± 22%       ~ (p=0.574 n=8)
geomean            3.592µ         3.600µ        +0.22%
```

Read it in this order:

1. **The `vs base` column.** `~` means benchstat found no statistically
   significant difference at α=0.05. That is the pass condition. A percentage
   there (e.g. `+12.4%`) with `p < 0.05` is a finding.
2. **The `±` figures.** These are the sample spread. If they are wide (say
   ±20% or more) the run had too few rounds or too much machine noise, and a
   `~` verdict is weak evidence rather than a clean pass. Raise `BENCH_ROUNDS`
   or `BENCH_ITERS` and re-run before concluding anything.
3. **`n=`.** Fewer than 6 samples means no confidence interval; fewer than 4
   means no p-value at all. Do not gate on those.
4. **`B/op` and `allocs/op`.** These are near-deterministic and far more
   sensitive than timing. An allocation appearing on a zero-alloc hot path is a
   regression even when `sec/op` shows `~`. Check these tables every time.

A regression is real when the p-value is significant, the confidence intervals
do not overlap, and the result reproduces on a second independent run.

## Stored baselines

`make bench-baseline` records the current numbers as a committed file under
`test/performance/baselines/`, so future work has something to diff against.

```sh
# Pin to a committed revision - a baseline built from a dirty tree is not
# reproducible, and the script warns if you try.
make bench-baseline \
    PERF_HOST=user@bench-host \
    BASELINE_REF=HEAD \
    BENCH_ROUNDS=10 \
    BENCH_ITERS=200000x \
    BENCH_PKGS='./pkg/internal/raft/ ./pkg/internal/engine/' \
    BASELINE_LABEL=bench-host-$(git rev-parse --short HEAD)-hotpath
```

### Naming convention

`BASELINE_LABEL` defaults to `<run-host>-<short-sha>` and becomes the filename
stem under `test/performance/baselines/`. Add a scale-class suffix by hand when
a revision has more than one baseline, giving:

```
<run-host>-<short-sha>[-<scale-class>].txt
```

The host name is part of the filename on purpose: absolute ns/op is a property
of the machine, so the host is what makes the file interpretable. The existing
files follow it -- `omega-64bc288-hotpath.txt` and `omega-64bc288-codec.txt` are
the µs-scale and ns-scale halves of one revision measured on a host named
`omega`. Baselines recorded on your own machine will carry your machine's name,
which is correct and expected.

Each file carries the provenance needed to make it meaningful -- a baseline
without provenance invites comparison against numbers from unknown hardware,
which is worse than having no baseline at all:

```
# quicraft benchmark run
# generated:     2026-08-04T19:32:33Z
# mode:          baseline
# run-host:      omega  (PERF_HOST=user@bench-host.example)
# cpu-model:     Intel(R) Core(TM) i9-9900X CPU @ 3.50GHz
# cpu-count:     20
# governor:      powersave
# loadavg:       0.02 0.12 0.13  (at start of run)
# build-host:    <build-host>
# go-version:    go version go1.26.5 linux/amd64
# current-ref:   64bc2882844a4109c698b99ef022ef9219a391ef  [clean (pinned to HEAD (64bc288))]
# packages:      ./pkg/internal/raft/ ./pkg/internal/engine/
# bench-filter:  .
# rounds:        10 (interleaved)
# iterations:    200000x (fixed)
# command:       BASELINE_REF='HEAD' PERF_HOST='...' ... make bench-baseline
```

Below the header are the raw samples in standard Go benchmark format. The `#`
comment lines are ignored by benchstat, so a stored baseline can be diffed
directly:

```sh
benchstat test/performance/baselines/omega-<sha>-hotpath.txt \
          build/bench-compare/<run>/current.txt
```

**Only compare baselines recorded on the same host.** Absolute ns/op is a
property of the CPU: a baseline from a 20-thread i9-9900X says nothing about a
run on a 24-thread Core Ultra 9 285K, because the difference between the two
machines dwarfs any regression you are looking for. A stored baseline is a
reference point for *that* host, and every file carries the provenance header
needed to check that before you diff against it.

## Choosing a machine for `PERF_HOST`

Any ssh-reachable Linux box works. Nothing needs installing on it -- the test
binaries are self-contained and the target's Go toolchain, if any, is never
invoked. Requirements are only: key-based ssh that works non-interactively
(`BatchMode=yes`), a writable `/tmp`, and the same GOOS/GOARCH as the machine
doing the compiling.

What makes a good one, in rough order of impact:

- **Idle.** Nothing else competing for CPU while the run is in progress. This is
  the whole point.
- **CPU governor set to `performance`.** Otherwise cores ramp their clock up and
  down mid-benchmark and every interval widens. Setting it requires root on that
  host. The script reports the governor it found so the result is at least
  interpretable when it is not `performance`.
- **Consistent.** Use the same host for every baseline and comparison in a
  series; switching hosts invalidates the comparison entirely (see above).

As a worked example, this project has used a dedicated node referred to in the
stored baselines as `omega` -- an Intel Core i9-9900X @ 3.50 GHz, 20 threads,
62 GB, idle at ~0.00 load. You point the harness at your own machine with
`PERF_HOST=user@host`; there is no default and no built-in host. It is an
example of a `PERF_HOST`, not a requirement: the harness has no knowledge of it, and every stored baseline it
produced is labelled with its hostname precisely so nobody mistakes those
numbers for universal ones. Its governor is `powersave` and nobody has root to
change it, which is why the intervals in the examples above sit at ±9--28%
rather than the ±2--3% a clock-pinned machine gives.

## benchstat

The comparison statistics come from `benchstat`:

```sh
make install-benchstat        # go install golang.org/x/perf/cmd/benchstat@latest
```

If benchstat is missing the script does not fail. It falls back to a median and
percentage-delta table and says so:

```
# benchstat unavailable - median/delta fallback (no statistics)
benchmark                                      baseline ns/op  current ns/op      delta        n
BenchmarkEntryMarshalTo                                   8.7            7.2    -17.44%        5
```

The fallback is **substantially weaker**: no confidence intervals, no
significance test, no allocation comparison. It cannot distinguish the example
above -- pure noise between two revisions that do not touch the code -- from a
genuine 17% improvement. Install benchstat before gating a release.

## Failure modes

The script fails loudly rather than emitting numbers that look plausible but
are not. It exits non-zero on:

- an unresolvable `BASELINE` ref (checked before anything expensive runs). A
  shallow clone is reported as such, with the fix, rather than as a raw git
  "unknown revision" error;
- an unreachable `PERF_HOST` -- it will **not** silently fall back to the local
  machine. That is not because local runs are illegitimate (they are the
  default) but because the caller asked for numbers from a specific machine, and
  substituting a different one would attach the wrong provenance to the result;
- `BENCH_ITERS` given as a duration instead of a fixed count;
- a baseline package that fails to build at the baseline revision (e.g. the
  package was added after it);
- a `BENCH_FILTER` that matches no benchmarks anywhere in `BENCH_PKGS`, which is
  otherwise indistinguishable from "no regression". Note the gap: the check is
  on the combined output, so a filter that matches in one package and not
  another still passes. If you narrow `BENCH_FILTER`, confirm the run produced
  samples from every package you expected.

Cleanup runs on every exit path: the baseline git worktree is removed (unless
`BENCH_KEEP_WORKTREE=1`), local test binaries are deleted, and the remote temp
directory is removed over ssh.

## See also

- [Performance Architecture](../multi-group-host/engineering.md) -- what the hot paths are and why
  they are allocation-free
- [Testing Overview](../testing/overview.md) -- benchmark categories and Makefile
  targets
- [Test Infrastructure](../testing/infrastructure.md) -- fuzzing, linearizability,
  I/O error injection
