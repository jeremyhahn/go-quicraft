# Performance benchmarks

> **Status: REFERENCE** · Scope: the comparative benchmark suite under
> `test/performance/` and its machine profiles · Last verified: 2026-10-02

Comparative benchmarks measuring QuicRaft against etcd-io/raft, Dragonboat and
OpenRaft across four deployment configurations: single-node in-memory,
single-node WAL, 3-node cluster with mTLS, and 5-node cluster with mTLS.

Every number lives under a **machine profile**, because a throughput figure is a
property of the machine as much as of the code. There is no profile-less place
to put a result, and no command will invent a profile for you.

## The profiles

| Profile | Machine | State |
|---|---|---|
| [`profiles/ci-node-2`](profiles/ci-node-2/) | Dual-socket server-grade box (2x Xeon Gold 6258R, 112 threads, 372 GiB) | Measured 2026-10-02, all four systems | <!-- lab-string-ok: benchmark provenance: profile names record which machine measured the numbers -->
| [`profiles/omega`](profiles/omega/) | Single-socket desktop-grade box | Measured 2026-08-17, etcd not measured | <!-- lab-string-ok: benchmark provenance: profile names record which machine measured the numbers -->
| [`profiles/r640`](profiles/r640/) | Dual-socket server-grade box | Baseline of 2026-03-18, preserved as rendered tables |

Each profile's hardware is recorded in its own `env.json`, captured on that
machine at run time. Do not compare a number from one profile against a number
from another, and do not compute a ratio across two: different machines,
generally also different builds of the engine.

## Running against a machine

```bash
make perf-remote PERF_HOST=user@host PERF_PROFILE=<profile>
```

One command: stage the working tree onto the target, capture the target's
specifications into `env.json`, run the suite there in Docker, fetch the results
and the capture back into `profiles/<profile>/`, and regenerate that profile's
documents locally. The remote run is detached, so a dropped ssh session costs
you the log tail, not the run.

```bash
# Knobs
make perf-remote PERF_HOST=user@host PERF_PROFILE=<profile> \
     BENCHTIME=30s PERF_TIMEOUT=7200s PERF_SYSTEMS=quicraft,dragonboat

# Reattach to a run already in flight
make perf-remote-status PERF_HOST=user@host PERF_PROFILE=<profile>
test/performance/run-remote.sh wait     # stream until it exits

# Individual phases
test/performance/run-remote.sh {stage|env|run|wait|status|fetch|generate}
```

`PERF_PROFILE` is required and has no default: the name is your choice, and
nothing can derive it. Everything else about the machine is captured by
`capture-env.sh` on the target at run time, so the specification table beside
a number cannot go stale or be hand-edited. A field the capture could not
obtain is recorded empty and renders as **not captured**, never as a zero.

## Running on this machine

```bash
make perf-all      PERF_PROFILE=<name>                  # run everything, then generate
make perf-run      PERF_PROFILE=<name>                  # run only, store JSON
make perf-run      PERF_PROFILE=<name> PERF_SYSTEMS=etcd  # one engine
make perf-generate PERF_PROFILE=<name>                  # documents + charts from stored JSON
make perf-env      PERF_PROFILE=<name>                  # capture this machine's specs
```

Ad-hoc runs that print a terminal report and store nothing need no profile:

```bash
make perf-quicraft
make perf-etcd
make perf-dragonboat
make perf-openraft
make perf-compare-all
make perf-compare SYSTEMS="quicraft etcd"
```

## Generated versus hand-written

Inside a profile:

| File | Owner | Lifecycle |
|---|---|---|
| `env.json` | `capture-env.sh`, on the target | Overwritten by each run; never hand-edited |
| `results/*.json` | The benchmark run | One file per system; the data of record |
| `logs/*.log` | The benchmark run | One container console per system, verbatim; overwritten by each run, never committed (`*.log` is ignored) |
| `charts/*.svg` | The generator | Deleted and recreated freely |
| `etcd.md`, `dragonboat.md`, `openraft.md` | The generator | **Disposable.** Deleted and recreated freely; edits are lost |
| `analysis.md` | **You** | **The generator never writes it, ever** |

The last row is enforced: the single write path in `perfprofile` refuses the
filename `analysis.md`, and a test asserts the file is byte-identical after a
run that rewrote its siblings. So:

- Put anything you want to keep in `analysis.md`. Anything you write in a
  generated document is gone on the next run.
- A system with no results gets **no document**, not a document of `N/A` rows.
  A file full of `N/A` is indistinguishable at a glance from a real result of
  zero. `omega` currently has no `etcd.md` for exactly this reason, and both <!-- lab-string-ok: benchmark provenance: profile names record which machine measured the numbers -->
  documents it does have say so on their face.

Generation refuses to run at all if any system it was asked to cover has no
results, so a partial comparison is never published as a whole one. Narrow it
explicitly when a gap is intended:

```bash
make perf-generate PERF_PROFILE=<profile> PERF_SYSTEMS=quicraft,dragonboat,openraft
```

## Where the analysis lives

- [`profiles/r640/analysis.md`](profiles/r640/analysis.md): the R640 baseline.
  It holds the 2026-03-18 measurements themselves, because those numbers were
  never captured as JSON. It also holds two sections that describe the
  harnesses rather than one machine, and so apply to every profile: the
  [fairness audit](profiles/r640/analysis.md#fairness-and-comparability) and
  the [scenario definitions](profiles/r640/analysis.md#scenario-definitions).
  Read the fairness audit before drawing a conclusion from any row; the
  election rows in particular are not like-for-like across columns.
- No other profile has an `analysis.md` yet. Nothing generates one, and nothing
  overwrites it once it exists.

## Layout

```
test/performance/
  README.md              <- this file, hand-maintained
  capture-env.sh         <- runs ON the target; emits env.json
  run-remote.sh          <- the one remote pipeline (make perf-remote drives it)
  profiles/
    <profile>/
      env.json           <- captured on that machine
      results/*.json     <- one per system
      logs/*.log         <- one container console per system, verbatim
      charts/*.svg       <- GENERATED
      dragonboat.md      <- GENERATED
      openraft.md        <- GENERATED
      analysis.md        <- HAND-WRITTEN
    r640/
      charts/*.svg       <- preserved from the 2026-03-18 run
      analysis.md        <- HAND-WRITTEN, includes the preserved baseline
  perfprofile/           <- profile layout, env capture parsing, doc generation
  perfresult/            <- shared result types, comparison engine, charts
  benchutil/             <- shared warmup retry budget, and the port
                            reservation, which delegates to test/netport
  cmd/perfcompare/       <- orchestrator CLI (Docker-based execution)
  quicraft/              <- QuicRaft benchmarks (standalone module)
  etcd/                  <- etcd-io/raft benchmarks (standalone module)
  dragonboat/            <- Dragonboat benchmarks (standalone module)
  openraft/              <- OpenRaft (Rust) benchmarks (standalone crate)
  baselines/             <- committed bench-compare baselines
```

`profiles/` is data, not build output. `make clean` does not touch it.

### Diagnosing a failed system

A failing container is explained in two places, and you need both.

The run log carries the summary the orchestrator extracts: the relayed
`b.Errorf` messages and, always, the `--- FAIL:` lines naming the benchmarks
that failed. It ends with the path of the full console.

That console is `logs/<system>.log` inside the profile: the container's stdout
and stderr exactly as it produced them, `=== RUN` lines and all. A remote run
writes it into the profile **on the bench host**, and `fetch` brings back
results and `env.json` only, so it stays there. `./run-remote.sh status` lists
what is on the target and prints the command to read one.

## System overview

| System | Version | Language | Type | WAL | Memory | Reads | Multi-Group | TLS |
|--------|---------|----------|------|-----|--------|-------|-------------|-----|
| **QuicRaft** | this tree | Go | Engine | waldb (16 shards) | memdb | SyncRead, StaleRead | Yes | QUIC mTLS (always on) |
| **etcd-io/raft** | v3.7.0 | Go | Library | User-provided | MemoryStorage | User-implemented | No | TCP mutual TLS (3N/5N only) |
| **Dragonboat** | fork v4.0.1 | Go | Engine | Tan (log-structured LogDB) | N/A | SyncRead, StaleRead | Yes | TCP MutualTLS |
| **OpenRaft** | 0.9.21 | Rust | Library | Disk WAL, write-only (benchmark) | BTreeMap | User-implemented | No | TCP mutual TLS, 3N/5N only |

The Dragonboat row is the fork `github.com/jeremyhahn/dragonboat/v4`, not
upstream `lni/dragonboat`; it exists so the benchmark can compile against
Pebble v1.1.5. The harness sets `Expert.LogDBFactory` to `tan.Factory`, so
Dragonboat runs Tan, its log-structured LogDB, rather than the default
`sharded-pebble` key-value store. `TestNodeHostConfigUsesTan` pins this.

All benchmarks run inside Docker containers with `--net=host` (QuicRaft also
uses `--privileged` for UDP buffer tuning). Systems are benchmarked sequentially
to avoid interference.

### Module credentials for the image builds

The quicraft image runs `go mod download` inside its build. If a dependency
resolves from a private module host in your environment, the build reads these
variables from the environment that invoked it:

```
GOPRIVATE=github.com/jeremyhahn/*
GIT_CONFIG_COUNT=1
GIT_CONFIG_KEY_0=url.https://<user>:<read-only token>@forge.example/jeremyhahn/.insteadOf
GIT_CONFIG_VALUE_0=https://github.com/jeremyhahn/
```

On a workstation, put that block in `.devcontainer/.env` and every `perf-*`
target reads it. That file is ignored by git, excluded from every Docker build
context by `.dockerignore`, and excluded from the tree `run-remote.sh` stages
onto a remote host; `test/buildgates` checks all three. `perfcompare` passes
the variables to `docker build` as a BuildKit secret mounted only for the
`RUN go mod download` step and never written into a layer, and `run-remote.sh`
sends them to the remote host over ssh's stdin, so the token never appears on a
command line, in an image or in a run log. The build reports which way it
went:

```
  module credentials: mounted as a build secret
  module credentials: none; github.com/jeremyhahn/* resolves from the public proxy only
```

The secret mount needs BuildKit, i.e. the `docker-buildx` plugin on the
building machine; the legacy builder rejects the Dockerfile with `the --mount
option requires BuildKit`.

## This is not the regression gate

These tables are a single run per system on one machine. The gate that catches
regressions is `make bench-compare`, an interleaved baseline-vs-current
comparison with benchstat; see
[docs/testing/benchmarking.md](../../docs/testing/benchmarking.md).
