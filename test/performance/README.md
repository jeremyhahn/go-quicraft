# Performance Benchmarks

Comparative benchmarks measuring QuicRaft against etcd-io/raft, Dragonboat and
OpenRaft across four deployment configurations: single-node in-memory,
single-node WAL, 3-node cluster with mTLS, and 5-node cluster with mTLS.

Every number lives under a **machine profile**, because a throughput figure is a
property of the machine as much as of the code. There is no profile-less place
to put a result, and no command will invent a profile for you.

## The profiles

| Profile | Machine | State |
|---|---|---|
| [`profiles/ci-node-2`](profiles/ci-node-2/) | Dual-socket server-grade box (2x Xeon Gold 6258R, 112 threads, 372 GiB) | Measured 2026-09-14; all four systems |
| [`profiles/omega`](profiles/omega/) | Single-socket desktop-grade box | Measured 2026-08-17; etcd not yet measured |
| [`profiles/r640`](profiles/r640/) | Dual-socket server-grade box | Baseline of 2026-03-18; hardware offline pending repair |

Each profile's hardware is recorded in its own `env.json`, captured on that
machine at run time. Do not compare a number from one profile against a number
from another, and do not compute a ratio across two: different machines,
generally also different builds of the engine.

## Running against a machine

```bash
make perf-remote PERF_HOST=user@host PERF_PROFILE=omega
```

One command: stage the working tree onto the target, capture the target's
specifications into `env.json`, run the suite there in Docker, fetch the results
*and* the capture back into `profiles/omega/`, and regenerate that profile's
documents locally. The remote run is detached, so a dropped ssh session costs
you the log tail rather than the run.

```bash
# Knobs
make perf-remote PERF_HOST=user@host PERF_PROFILE=omega \
     BENCHTIME=30s PERF_TIMEOUT=7200s PERF_SYSTEMS=quicraft,dragonboat

# Reattach to a run already in flight
make perf-remote-status PERF_HOST=user@host PERF_PROFILE=omega
test/performance/run-remote.sh wait     # stream until it exits

# Individual phases
test/performance/run-remote.sh {stage|env|run|wait|status|fetch|generate}
```

`PERF_PROFILE` is required and has no default. The name is a decision, not a
measurement: `lscpu` describes hardware, but it cannot tell you which of your
machines is the desktop-class reference box. Everything else about the machine
*is* derived, by `capture-env.sh`, on the target, at run time — so the
specification table printed beside a number can never go stale and can never be
hand-edited into something the hardware never was. A field the capture could not
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

That last row is enforced rather than agreed: the single write path in
`perfprofile` refuses the filename `analysis.md` outright, and a test asserts
the file is byte-identical after a run that rewrote its siblings.

Two consequences worth stating plainly:

- Put anything you want to survive in `analysis.md`. Anything you write in a
  generated document is gone on the next run.
- A system with no results gets **no document**, not a document of `N/A` rows.
  A file full of `N/A` is indistinguishable at a glance from a real result of
  zero. `omega` currently has no `etcd.md` for exactly this reason, and both
  documents it does have say so on their face.

Generation refuses to run at all if any system it was asked to cover has no
results, so a partial comparison is never published as a whole one. Narrow it
explicitly when a gap is intended:

```bash
make perf-generate PERF_PROFILE=omega PERF_SYSTEMS=quicraft,dragonboat,openraft
```

## Where the analysis lives

- [`profiles/r640/analysis.md`](profiles/r640/analysis.md) — the R640 baseline. It holds the
  2026-03-18 measurements themselves, because those numbers were never captured
  as JSON and exist only as rendered tables. It also holds two things that
  describe the *harnesses* rather than one machine, and so apply to every
  profile: the **fairness audit** (§ *Fairness and comparability*) and the
  **scenario definitions** (§ *Scenario Definitions*). Read the fairness audit
  before drawing a conclusion from any row — the election rows in particular are
  not like-for-like across columns.
- `profiles/omega/analysis.md` — not written yet. Create it when there is
  something to say about the omega dataset; nothing generates it, and nothing
  will overwrite it once it exists.

## Layout

```
test/performance/
  README.md              <- this file, hand-maintained
  capture-env.sh         <- runs ON the target; emits env.json
  run-remote.sh          <- the one remote pipeline (make perf-remote drives it)
  profiles/
    omega/
      env.json           <- captured on omega
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
`b.Errorf` messages and — always, however many messages precede them — the
`--- FAIL:` lines naming the benchmarks that failed. It ends with the path of
the full console.

That console is `logs/<system>.log` inside the profile: the container's stdout
and stderr exactly as it produced them, `=== RUN` lines and all. A remote run
writes it into the profile **on the bench host**, and `fetch` brings back
results and `env.json` only, so it stays there. `./run-remote.sh status` lists
what is on the target and prints the command to read one.

## System overview

| System | Version | Language | Type | WAL | Memory | Reads | Multi-Group | TLS |
|--------|---------|----------|------|-----|--------|-------|-------------|-----|
| **QuicRaft** | this tree | Go | Engine | waldb (sharded) | memdb | SyncRead, StaleRead | Yes (10K+ shards) | QUIC mTLS (always on) |
| **etcd-io/raft** | v3.7.0 | Go | Library | User-provided | MemoryStorage | User-implemented | No | TCP mutual TLS (3N/5N only) |
| **Dragonboat** | jeremyhahn fork v4.0.1 | Go | Engine | Tan (custom log-file DB) | N/A | SyncRead, StaleRead | Yes | TCP MutualTLS |
| **OpenRaft** | 0.9.21 | Rust | Library | Disk WAL, write-only (benchmark) | BTreeMap | User-implemented | No | TCP mutual TLS, 3N/5N only |

The Dragonboat row is the fork `github.com/jeremyhahn/dragonboat/v4`, not
upstream `lni/dragonboat`; it exists so the benchmark can compile against
PebbleDB v1.1.5.

All benchmarks run inside Docker containers with `--net=host` (QuicRaft also
uses `--privileged` for UDP buffer tuning). Systems are benchmarked sequentially
to avoid interference.

### Module credentials for the image builds

The quicraft image runs `go mod download` inside its build, and go-enclave is
an internal Gitea module: with no credentials the container reaches for
github.com, where it is not, and the build dies at `could not read Username`
(#184). The build takes the four variables of
`../handbook/software/conventions/dependency-resolution.md` §6 from the
environment of whatever invoked it:

```
GOPRIVATE=github.com/jeremyhahn/*
GIT_CONFIG_COUNT=1
GIT_CONFIG_KEY_0=url.http://x:<read-only token>@git.example.internal/jeremyhahn/.insteadOf
GIT_CONFIG_VALUE_0=https://github.com/jeremyhahn/
```

A Gitea Actions runner injects them; on a workstation put that block, verbatim,
in `.devcontainer/.env` and every `perf-*` target reads it. That file is
ignored by git, excluded from every Docker build context (`.dockerignore` —
six Dockerfiles `COPY . .`), and excluded from the tree `run-remote.sh` stages
onto the bench host; `test/buildgates` pins all three.
`perfcompare` hands them to `docker build` as a BuildKit secret mounted for the
single `RUN go mod download` and never exported into a layer; `run-remote.sh`
carries them to the bench host over ssh's stdin. The token is therefore never
on a command line, in an image, or in a run log on either machine. Both ends
report which way the build went, once:

```
  module credentials: mounted as a build secret
  module credentials: none; github.com/jeremyhahn/* resolves from the public proxy only
```

The second line is correct outside the Gitea network and a build failure
inside it. The secret mount needs BuildKit, i.e. the `docker-buildx` plugin on
the machine that builds (`apt install docker-buildx` on Ubuntu); the legacy
builder rejects the Dockerfile outright with `the --mount option requires
BuildKit`.

## This is not the regression gate

These tables are a single run per system on one machine. `make bench-compare`
— interleaved baseline-vs-current with benchstat, see
[docs/performance/benchmarking.md](../../docs/performance/benchmarking.md) — is
the gate that catches regressions.
