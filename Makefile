# go-quicraft Build System
# Multi-group Raft library

.DEFAULT_GOAL := build

# Project metadata
MODULE        := github.com/jeremyhahn/go-quicraft
BINARY        := go-quicraft
BUILD_DIR     := build
DOCKER_DIR    := .devcontainer
COVER_DIR     := $(BUILD_DIR)/coverage

# Version injection.
#
# Two separate defences here, because renaming alone was not enough.
#
# 1. BUILD_VERSION, not VERSION. `VERSION` is the release-alpha ARGUMENT
#    (`make release-alpha VERSION=vX.Y.Z-alpha`). A variable read in an
#    immediate `:=` assignment is EXPANDED AT PARSE TIME, before any target is
#    selected and before any recipe guard exists, so while this read was
#    spelled $(VERSION), `make <anything> VERSION='$(shell ...)'` executed that
#    function -- on `build`, on `release-alpha`, and on a target that does not
#    exist at all.
#
# 2. `override`, because a command-line variable BEATS an ordinary makefile
#    assignment. Renaming alone just moved the hole to the new name:
#    `make build BUILD_VERSION='$(shell ...)'` reproduced it exactly, and so did
#    GIT_COMMIT and BUILD_DATE, which LDFLAGS expands on the same line.
#    `override` makes the assignment below win, so a command-line value for any
#    of the three is discarded and never reaches an expansion.
#
# THE LINE THIS DRAWS, so the next reader can place a new variable correctly.
# These three are DERIVED -- from a file, from git, from the clock. Nothing a
# caller supplies for them could be meaningful, so they are `override`. A
# documented knob (GO, BUILD_DIR) stays settable and is NOT covered here: naming
# the program make should run is choosing a command on purpose, whereas
# `VERSION=v1.2.3-alpha` looks like pure data and is the value someone copies
# from an issue or a release note without reading it as code. The rule is that
# no variable expanded at parse time may carry a value a caller supplies.
override BUILD_VERSION := $(shell cat VERSION 2>/dev/null || echo "0.0.1-alpha")
override GIT_COMMIT    := $(shell git rev-parse --short HEAD 2>/dev/null || echo "unknown")
override BUILD_DATE    := $(shell date -u +"%Y-%m-%dT%H:%M:%SZ")
LDFLAGS       := -X main.version=$(BUILD_VERSION) -X main.gitCommit=$(GIT_COMMIT) -X main.buildDate=$(BUILD_DATE)

# Go tooling
export GOTOOLCHAIN := auto
GO            := go
GOTEST        := $(GO) test -race -count=1 -v
GOBENCH       := $(GO) test -run=^$$ -bench=. -benchmem
GOVET         := $(GO) vet
GOFMT         := gofmt
# The analyser's name on PATH. Which BINARY `make lint` actually runs is
# GOLANGCI_BIN below, which prefers the one `make install-lint` builds.
GOLINT        := golangci-lint
# Must match the version quality.yml installs. A different MAJOR version is not
# a cosmetic difference: v2 moved flags (`--timeout`) and changed which linters
# are enabled by default, so a developer and CI would gate on different rules.
GOLANGCI_VERSION := v2.13.2
# Where `make install-lint` puts the analyser, and the first place `make lint`
# looks. Repo-local so the binary this repo lints with is built by this repo's
# Go and cannot be replaced by another project's `go install` into GOPATH/bin.
TOOLS_BIN     := $(CURDIR)/.tools/bin
# The analyser `make lint` runs, resolved at use time: the repo-local build
# first, then PATH (which is where CI's baked runner-image tool lives). Kept
# recursive (`=`) rather than `:=` so an uninstalled tool is not a parse-time
# failure for every other target. See the guard at check-lint-toolchain.
GOLANGCI_BIN   = $(shell if [ -x '$(TOOLS_BIN)/golangci-lint' ]; then \
			echo '$(TOOLS_BIN)/golangci-lint'; \
		else command -v $(GOLINT) 2>/dev/null; fi)
# The module's own language floor. DERIVED from go.mod, hence `override` per
# the rule above: nothing a caller supplies for it could be meaningful, and it
# is the value the analyser's build toolchain is checked against.
override GO_DIRECTIVE := $(shell awk '$$1=="go"{print $$2; exit}' go.mod)
GOSEC         := gosec
GOVULNCHECK   := govulncheck

# gosec exclusions. Keep this list identical to the -exclude flag in
# .github/workflows/ci.yml, and keep the reasons here so the next person does
# not have to re-derive them:
#   G103: unsafe calls — the proto codec is a fixed-layout zero-copy encoding
#   G104: unhandled errors — false positive on hash.Write, which returns none
#   G115: integer overflow on conversion — the flagged conversions sit behind an
#         explicit bounds check on the same value (frame and entry sizes are
#         validated against MaxFrameSize / MaxReplicationPayloadBytes first).
#         This is NOT a blanket acceptance of overflow: a new narrowing
#         conversion without a preceding bound is a real finding this exclusion
#         hides, so review narrowing conversions by hand
#   G301: directory permissions — 0755 is the intended posture for data dirs
#   G304: file path via variable — snapshot/WAL paths are built internally from
#         (shard, replica, index), never from a request
#   G306: file write permissions — 0644 for data files; secret material is
#         written 0600 explicitly
#   G404: weak random — false positive. The only math/rand use is election
#         jitter in pkg/internal/raft/raft.go, whose PRNG newRaft seeds from
#         crypto/rand. Jitter desynchronizes campaigns; it is not a security
#         primitive
GOSEC_EXCLUDE := G103,G104,G115,G301,G304,G306,G404

# Trivy
TRIVY         := trivy

# Coverage
#
# COVER_MIN is the floor every coverage target enforces; the run-coverage canned
# recipe further down is the single place the flags live.
#
# Two variables used to sit here — COVER_FLAGS (`-coverprofile`) and
# COVER_EXCLUDE (`-coverpkg=./...`, a name that says the opposite of its value)
# — and NEITHER was referenced by any target, so both are gone. -coverpkg was
# measured before dropping it rather than wired in on the strength of the
# variable's name: see the note on run-coverage for why passing it would loosen
# this gate rather than tighten it.
COVER_MIN     := 91

# Docker
DOCKER_IMAGE  := go-quicraft-dev
DOCKER_TAG    := latest
COMPOSE_FILE  := $(DOCKER_DIR)/docker-compose.yml
INT_COMPOSE   := $(DOCKER_DIR)/docker-compose.integration.yml

# Package paths
PKG_PROTO       := ./pkg/proto/...
PKG_CONFIG      := ./pkg/config/...
PKG_QUEUE       := ./pkg/internal/queue/...
PKG_INVARIANT   := ./pkg/internal/invariant/...
PKG_LOGDB       := ./pkg/logdb/...
PKG_RAFT        := ./pkg/internal/raft/...
PKG_WALDB       := ./pkg/logdb/waldb/...
PKG_SEAL        := ./pkg/seal/...
PKG_RSM         := ./pkg/internal/rsm/...
PKG_SESSION     := ./pkg/internal/session/...
PKG_SNAPSHOT    := ./pkg/internal/snapshot/...
PKG_ENGINE      := ./pkg/internal/engine/...
PKG_REGISTRY    := ./pkg/internal/registry/...
PKG_TRANSPORT   := ./pkg/internal/transport/...
PKG_QUICRAFT    := ./pkg
PKG_ERRORS      := ./pkg
PKG_SM          := ./pkg/sm/...
PKG_SERVER      := ./pkg/internal/server/...
PKG_WRITEMODE   := ./pkg/writemode/...
PKG_BATCH       := ./pkg/batch/...
PKG_CRYPTO      := ./pkg/crypto/...

# Integration test paths
INT_TEST_DIR    := ./test/integration

# Performance comparison paths
PERF_DIR        := ./test/performance
PERF_DRAGONBOAT := $(PERF_DIR)/dragonboat
PERF_ETCD       := $(PERF_DIR)/etcd

# ============================================================================
# Primary targets
# ============================================================================

.PHONY: all
all: build test lint

.PHONY: build
build:
	$(GO) build ./...

.PHONY: clean
clean:
	$(GO) clean ./...
	rm -rf $(COVER_DIR) $(BUILD_DIR)/quicraft *.log *.test *.out *.prof .build-cache
	rm -rf test/performance/openraft/target
	@# test/performance/profiles/ is DATA, not build output: stored result JSON,
	@# captured machine specifications, and the hand-written analysis.md of a
	@# machine that is currently offline and cannot be re-measured. `clean` used
	@# to delete the whole tree. It does not go near it now.
	rm -f test/performance/perfcompare test/performance/perfcompare.remote
	rm -f test/performance/cmd/perfcompare/perfcompare
	rm -f test/performance/quicraft/*.prof test/performance/quicraft/*.test
	rm -f contrib/dragonboat/*.test

# ============================================================================
# Unit tests
# ============================================================================

.PHONY: test
test:
	$(GOTEST) ./...

.PHONY: test-proto
test-proto:
	$(GOTEST) $(PKG_PROTO)

.PHONY: test-config
test-config:
	$(GOTEST) $(PKG_CONFIG)

.PHONY: test-queue
test-queue:
	$(GOTEST) $(PKG_QUEUE)

.PHONY: test-invariant
test-invariant:
	$(GOTEST) $(PKG_INVARIANT)

.PHONY: test-logdb
test-logdb:
	$(GOTEST) $(PKG_LOGDB)

.PHONY: test-raft
test-raft:
	$(GOTEST) $(PKG_RAFT)

.PHONY: test-waldb
test-waldb:
	$(GOTEST) $(PKG_WALDB)

.PHONY: test-seal
test-seal:
	$(GOTEST) $(PKG_SEAL)

.PHONY: test-rsm
test-rsm:
	$(GOTEST) $(PKG_RSM)

.PHONY: test-session
test-session:
	$(GOTEST) $(PKG_SESSION)

.PHONY: test-snapshot
test-snapshot:
	$(GOTEST) $(PKG_SNAPSHOT)

.PHONY: test-engine
test-engine:
	$(GOTEST) $(PKG_ENGINE)

.PHONY: test-registry
test-registry:
	$(GOTEST) $(PKG_REGISTRY)

.PHONY: test-transport
test-transport:
	$(GOTEST) $(PKG_TRANSPORT)

.PHONY: test-quicraft
test-quicraft:
	$(GOTEST) $(PKG_QUICRAFT)

.PHONY: test-errors
test-errors:
	$(GOTEST) $(PKG_ERRORS)

.PHONY: test-sm
test-sm:
	$(GOTEST) $(PKG_SM)

.PHONY: test-server
test-server:
	$(GOTEST) $(PKG_SERVER)

.PHONY: test-writemode
test-writemode:
	$(GOTEST) $(PKG_WRITEMODE)

.PHONY: test-batch
test-batch:
	$(GOTEST) $(PKG_BATCH)

.PHONY: test-crypto
test-crypto:
	$(GOTEST) $(PKG_CRYPTO)

# ============================================================================
# Integration tests (always run in devcontainer)
# ============================================================================

# Public targets: build devcontainer image and run tests inside it.
# Build is a separate quiet step so test output is not drowned by Docker layer logs.
# These are the targets users invoke on the host.

.PHONY: integration-test
integration-test:
	@echo "==> Building integration container..."
	@docker compose -f $(COMPOSE_FILE) build --quiet integration
	@echo "==> Running integration tests..."
	docker compose -f $(COMPOSE_FILE) run --rm integration make _integration-test

.PHONY: integration-test-raft
integration-test-raft:
	@echo "==> Building integration container..."
	@docker compose -f $(COMPOSE_FILE) build --quiet integration
	@echo "==> Running raft integration tests..."
	docker compose -f $(COMPOSE_FILE) run --rm integration make _integration-test-raft

.PHONY: integration-test-host
integration-test-host:
	@echo "==> Building integration container..."
	@docker compose -f $(COMPOSE_FILE) build --quiet integration
	@echo "==> Running host integration tests..."
	docker compose -f $(COMPOSE_FILE) run --rm integration make _integration-test-host

.PHONY: integration-test-e2e
integration-test-e2e:
	docker compose -f $(INT_COMPOSE) up --build --abort-on-container-exit --exit-code-from test-runner
	docker compose -f $(INT_COMPOSE) down -v

# Internal targets: executed inside the devcontainer. Not intended for host use.

# -p 1 serializes package execution: each integration package spins up
# multi-node QUIC clusters with -race, and running all four concurrently
# in the container starves CPU enough that snapshot catch-up tests
# (30s waits) flake. Every package is stable standalone.
.PHONY: _integration-test
_integration-test:
	# 900s, not 300s: the linearizability package alone runs ~305s now that it
	# carries the rolling-restart catch-up probe, and -p 1 means every package
	# shares one deadline. A 300s cap failed the suite with a timeout panic and
	# zero test failures, which reads as a product bug rather than a budget.
	$(GOTEST) -tags=integration -timeout=900s -p 1 $(INT_TEST_DIR)/...

.PHONY: _integration-test-raft
_integration-test-raft:
	$(GOTEST) -tags=integration $(INT_TEST_DIR)/raft/...

.PHONY: _integration-test-host
_integration-test-host:
	$(GOTEST) -tags=integration $(INT_TEST_DIR)/host/...

.PHONY: _integration-test-e2e
_integration-test-e2e:
	$(GOTEST) -tags=integration $(INT_TEST_DIR)/e2e/...

.PHONY: integration-test-linearizability
integration-test-linearizability:
	@echo "==> Building integration container..."
	@docker compose -f $(COMPOSE_FILE) build --quiet integration
	@echo "==> Running linearizability tests..."
	docker compose -f $(COMPOSE_FILE) run --rm integration make _integration-test-linearizability

.PHONY: _integration-test-linearizability
_integration-test-linearizability:
	$(GOTEST) -tags=integration -timeout=600s $(INT_TEST_DIR)/linearizability/...

# CI-parity target: byte-identical to the `integration` job in
# .github/workflows/ci.yml and the `integration` gate in release.yml. It runs
# on the HOST, not in the devcontainer, because that is what a CI runner does —
# `integration-test` above is the devcontainer path and is the one to use when
# you want the container's tuned UDP buffers and pinned toolchain.
#
# The differences from _integration-test are deliberate and must not be
# "harmonised" away: no -v (CI logs are already large), and -timeout=1800s
# rather than 900s because a shared CI runner is slower than a dev box and -p 1
# makes every package share the one deadline.
.PHONY: ci-integration
ci-integration:
	$(GO) test -race -count=1 -tags=integration -timeout=1800s -p 1 $(INT_TEST_DIR)/...

# Real network partitions between LIVE replicas.
#
# A separate lane rather than part of ci-integration because it needs a second
# build tag: quicraft_netfault compiles the send-path drop hook into the
# transport, which a normal build folds away as a false constant. The suite is
# self-guarding — every test asserts netfault.BlockedCount() is non-zero — so
# running it with the tag missing fails loudly instead of silently exercising a
# healthy cluster, which is exactly the defect it was written to correct.
#
# -p 1 for the same reason ci-integration uses it: the injector state is
# process-global and multi-node QUIC clusters under -race starve a shared
# runner when run concurrently.
.PHONY: ci-netfault
ci-netfault:
	$(GO) test -race -count=1 -tags='integration quicraft_netfault' -timeout=1800s -p 1 \
		$(INT_TEST_DIR)/e2e/...

# ============================================================================
# Benchmarks
# ============================================================================

.PHONY: bench-proto
bench-proto:
	$(GOBENCH) $(PKG_PROTO)

.PHONY: bench-raft
bench-raft:
	$(GOBENCH) $(PKG_RAFT)

.PHONY: bench-waldb
bench-waldb:
	$(GOBENCH) $(PKG_WALDB)

.PHONY: bench-engine
bench-engine:
	$(GOBENCH) $(PKG_ENGINE)

.PHONY: bench-registry
bench-registry:
	$(GOBENCH) $(PKG_REGISTRY)

.PHONY: bench-seal
bench-seal:
	$(GOBENCH) $(PKG_SEAL)

.PHONY: bench-transport
bench-transport:
	$(GOBENCH) $(PKG_TRANSPORT)

.PHONY: bench-quicraft
bench-quicraft:
	$(GOBENCH) $(PKG_QUICRAFT)

.PHONY: bench-server
bench-server:
	$(GOBENCH) $(PKG_SERVER)

.PHONY: bench-writemode
bench-writemode:
	$(GOBENCH) $(PKG_WRITEMODE)

.PHONY: bench-batch
bench-batch:
	$(GOBENCH) $(PKG_BATCH)

.PHONY: bench-crypto
bench-crypto:
	$(GOBENCH) $(PKG_CRYPTO)

# ----------------------------------------------------------------------------
# Regression-grade benchmarking (release gate)
#
# The bench-<pkg> targets above are smoke tests: a single unreplicated run on
# whatever machine you happen to be on. They CANNOT detect a hot-path
# regression - run-to-run spread on a loaded dev box reaches 5x.
#
# bench-compare is the gate. It builds baseline and current test binaries from
# the same toolchain, interleaves the two sides across many rounds at a fixed
# iteration count, and compares the distributions with benchstat.
#
# The binaries run on this machine by default, or on PERF_HOST when set. The
# method is identical either way; only the execution location changes. Both
# modes are supported, including from CI.
#
# See docs/performance/benchmarking.md.
# ----------------------------------------------------------------------------

BENCH_SCRIPT   := scripts/bench-compare.sh

# Git ref to compare the working tree against.
BASELINE       ?= origin/main
# Where the benchmark binaries execute. Unset = this machine. Set to an ssh
# destination (user@host) to run them on a quiet, dedicated machine instead,
# which yields tighter confidence intervals.
PERF_HOST      ?=
# Hot path per CLAUDE.md: append (raft log), replicate (proto codec), apply (engine).
BENCH_PKGS     ?= ./pkg/internal/raft/ ./pkg/internal/engine/ ./pkg/proto/
BENCH_FILTER   ?= .
BENCH_ROUNDS   ?= 8
BENCH_ITERS    ?= 100000x
BENCH_TIMEOUT  ?= 30m

# $(value ...) passes these through UNEXPANDED. A benchmark filter such as
# 'BenchmarkA$|BenchmarkB$' contains '$|', which make would otherwise expand as
# the automatic variable for order-only prerequisites (empty), silently
# corrupting the regex into 'BenchmarkABenchmarkB$'.
BENCH_ENV = BASELINE='$(value BASELINE)' PERF_HOST='$(value PERF_HOST)' \
            BENCH_PKGS='$(value BENCH_PKGS)' BENCH_FILTER='$(value BENCH_FILTER)' \
            BENCH_ROUNDS='$(value BENCH_ROUNDS)' BENCH_ITERS='$(value BENCH_ITERS)' \
            BENCH_TIMEOUT='$(value BENCH_TIMEOUT)'

.PHONY: bench-compare
bench-compare:
	@$(BENCH_ENV) $(BENCH_SCRIPT) compare

# BASELINE_REF pins the stored baseline to a committed revision (recommended:
# a baseline recorded from a dirty tree is not reproducible).
.PHONY: bench-baseline
bench-baseline:
	@$(BENCH_ENV) BASELINE_REF='$(BASELINE_REF)' BASELINE_LABEL='$(BASELINE_LABEL)' \
		$(BENCH_SCRIPT) baseline

.PHONY: install-benchstat
install-benchstat:
	@echo "==> Installing benchstat..."
	$(GO) install golang.org/x/perf/cmd/benchstat@latest

# ============================================================================
# Fuzz Testing (Phase 1)
# ============================================================================

FUZZ_TIME     ?= 60s
FUZZ_TAGS     := -tags=quicraft_fuzz
GOFUZZ        := $(GO) test $(FUZZ_TAGS) -count=1

.PHONY: fuzz-proto
fuzz-proto:
	$(GOFUZZ) -fuzz=Fuzz -fuzztime=$(FUZZ_TIME) $(PKG_PROTO)

.PHONY: fuzz-transport
fuzz-transport:
	$(GOFUZZ) -fuzz=Fuzz -fuzztime=$(FUZZ_TIME) $(PKG_TRANSPORT)

.PHONY: fuzz-waldb
fuzz-waldb:
	$(GOFUZZ) -fuzz=Fuzz -fuzztime=$(FUZZ_TIME) $(PKG_WALDB)

.PHONY: fuzz-seal
fuzz-seal:
	$(GOFUZZ) -fuzz=Fuzz -fuzztime=$(FUZZ_TIME) $(PKG_SEAL)

.PHONY: fuzz-session
fuzz-session:
	$(GOFUZZ) -fuzz=Fuzz -fuzztime=$(FUZZ_TIME) $(PKG_SESSION)

# The raft package has two targets and `go test -fuzz` runs exactly one per
# invocation, so they are listed explicitly rather than matched by -fuzz=Fuzz.
.PHONY: fuzz-raft
fuzz-raft:
	$(GOFUZZ) -run=Fuzz -fuzz=FuzzRaftStepSequence -fuzztime=$(FUZZ_TIME) $(PKG_RAFT)
	$(GOFUZZ) -run=Fuzz -fuzz=FuzzRaftLogRecovery -fuzztime=$(FUZZ_TIME) $(PKG_RAFT)

.PHONY: fuzz-all
fuzz-all: fuzz-proto fuzz-transport fuzz-waldb fuzz-seal fuzz-session fuzz-raft

# CI-parity target: byte-identical to the `fuzz-seeds` job in
# .github/workflows/ci.yml.
#
# -run=Fuzz WITHOUT -fuzz is not fuzzing. It executes each target once per
# f.Add seed and once per file in its testdata/fuzz/<Target>/ corpus, then
# exits — a deterministic regression suite over every input a previous fuzzing
# run found interesting. That is what belongs in a PR gate; the unbounded
# search lives in .github/workflows/fuzz.yml on a nightly schedule, and in the
# fuzz-<pkg> targets above for local use.
#
# ./... rather than the six fuzzing packages: a new fuzz target in a package
# not on that list would otherwise never run.
.PHONY: ci-fuzz-seeds
ci-fuzz-seeds:
	$(GO) test -race -count=1 $(FUZZ_TAGS) -run=Fuzz ./...

# ============================================================================
# Bounded mutation fuzzing — the release gate
#
# This is a REGRESSION gate, not a fuzzing campaign: a short mutation budget per
# target, enough to catch a codec change that the committed corpus does not
# already encode. The real campaign is fuzz.yml's nightly schedule.
#
# It lives here rather than inline in release.yml so CI runs the target a
# developer runs (devops.md §4). It previously existed only as shell inside the
# workflow, which meant the release gate could not be reproduced locally at all.
#
# # Why the retry, and why it is not a weakened gate
#
# Go's fuzzing coordinator can lose its shutdown RPC to the scheduler and report
# a bare "context deadline exceeded" AFTER the full -fuzztime has elapsed. It
# reads exactly like a crash and is not one. -parallel bounds the workers, which
# reduced it, but it still occurs — reproduced on an IDLE host running one target
# at a time, so it is not cross-job contention.
#
# A gate that fails randomly is not a stricter gate, it is a gate people re-run
# until it passes. So the two outcomes are separated on the signal Go itself
# gives: a crash ALWAYS writes a reproducer and says "Failing input written to".
#
#   - that string present            -> real finding, fail immediately, never retry
#   - absent, deadline message only  -> shutdown artefact, retry
#   - anything else                  -> unknown failure, fail
#
# A real crash therefore cannot be retried away, because the retry is gated on
# the ABSENCE of the reproducer Go writes for one.
# ============================================================================

FUZZ_MUT_PKGS ?= ./pkg/proto ./pkg/internal/transport ./pkg/internal/session \
                 ./pkg/internal/raft ./pkg/logdb/waldb ./pkg/seal
# The release gate is a SMOKE gate: the nightly's depth divided by
# FUZZ_MUT_DIVISOR. It exists to catch a target that is broken outright on a
# tag, not to search — the nightly does the searching.
FUZZ_MUT_DIVISOR ?= 6

.PHONY: ci-fuzz-mutation
ci-fuzz-mutation:
	@set -u; overall=0; \
	budget() { \
		case "$$1" in \
		  ./pkg/proto)              base=4000000 ;; \
		  ./pkg/internal/transport) base=1700000 ;; \
		  ./pkg/internal/session)   base=3400000 ;; \
		  ./pkg/internal/raft)      base=680000 ;; \
		  ./pkg/logdb/waldb)        base=2000000 ;; \
		  ./pkg/seal)               base=2500 ;; \
		  *) echo "FAIL: no execution budget is calibrated for $$1 -- add one before adding the package to FUZZ_MUT_PKGS" >&2; return 1 ;; \
		esac; \
		case "$$2" in \
		  FuzzMessageFrameDecode) base=160000 ;; \
		  FuzzFrameHeaderUnmarshalCrash) base=4500 ;; \
		  FuzzRecordRoundTrip|FuzzRecordDecodeCrash|FuzzSegmentReplayCrash|\
		  FuzzUnmarshalAddrMapCrash|FuzzUnmarshalRemovedMapCrash) base=20000 ;; \
		esac; \
		n=$$((base / $(FUZZ_MUT_DIVISOR))); \
		if [ "$$n" -lt 1 ]; then n=1; fi; \
		echo "$$n"; \
	}; \
	for pkg in $(FUZZ_MUT_PKGS); do \
		targets=$$($(GO) test $(FUZZ_TAGS) -list='^Fuzz' "$$pkg" | grep '^Fuzz' || true); \
		if [ -z "$$targets" ]; then \
			echo "FAIL: no fuzz targets found in $$pkg"; overall=1; continue; \
		fi; \
		for t in $$targets; do \
			n=$$(budget "$$pkg" "$$t") || { overall=1; break; }; \
			if ! $(GO) test $(FUZZ_TAGS) -count=1 -run='^$$$$' -parallel=2 \
				-fuzz="^$$t\$$$$" -fuzztime=$${n}x "$$pkg"; then \
				echo "FAIL: $$pkg $$t"; overall=1; \
			fi; \
		done; \
		echo "ok $$pkg"; \
	done; \
	if [ $$overall -ne 0 ]; then exit 1; fi; \
	echo "All bounded mutation fuzz targets passed"

# ============================================================================
# ErrorFS I/O Error Injection (Phase 3)
# ============================================================================

ERRFS_TAGS    := -tags=quicraft_errfs
ERRFS_PKGS    := ./test/errfs/...
GOERRFS       := $(GO) test $(ERRFS_TAGS) -race -count=1 -v

# Verbose developer variant. -v is what you want when an injected-error test
# fails, because the failure is a sequence of I/O outcomes and the test names
# are the only way to tell which injection point produced it.
#
# This target previously had no caller: no other target depended on it and no
# workflow ran it, so the 21 tests behind quicraft_errfs were only ever run by
# hand. ci-errfs below is now the wired-in path (ci-full -> ci-errfs), and this
# stays as the target you reach for once something is red.
.PHONY: errfs-waldb
errfs-waldb:
	$(GOERRFS) $(ERRFS_PKGS)

# CI-parity target: byte-identical to the `errfs` job in
# .github/workflows/ci.yml and the `errfs` gate in release.yml. Same suite as
# errfs-waldb without -v. -race is kept in both: these tests drive the WAL
# through injected I/O failures from concurrent goroutines, which is exactly
# where a torn recovery path shows up as a race rather than a wrong answer.
.PHONY: ci-errfs
ci-errfs:
	$(GO) test -race -count=1 $(ERRFS_TAGS) $(ERRFS_PKGS)

# ============================================================================
# Performance Comparison Benchmarks
# ============================================================================

PERF_CMD := $(PERF_DIR)/cmd/perfcompare
# The dragonboat comparison engine is the published fork
# github.com/jeremyhahn/dragonboat/v4, pinned in
# test/performance/dragonboat/go.mod. It needs no local checkout and no
# DRAGONBOAT_SRC override.
#
# Everything these targets store lives under a MACHINE PROFILE,
# test/performance/profiles/<name>/. Two are tracked long-term: omega, a
# single-socket desktop-grade box, and r640, a dual-socket server-grade box. A
# throughput number is a property of the machine as much as of the code, so
# there is no profile-less place to put one.
#
# PERF_PROFILE is required by every target that reads or writes stored results,
# and has no default. The name cannot be inferred from the hardware — lscpu
# cannot tell you which of your machines is the reference desktop — so the
# operator supplies it, and capture-env.sh derives everything else on the target.
SYSTEMS ?= all
BENCHTIME ?= 30s
PERF_TIMEOUT ?= 3600s
PERF_EXTRA ?=
PERF_PROFILE ?=
# Systems to run, and the systems the generated documents must cover.
# Generation refuses to run if anything named here has no results, so a profile
# that is genuinely missing an engine is narrowed explicitly:
#   make perf-generate PERF_PROFILE=omega PERF_SYSTEMS=quicraft,dragonboat,openraft
PERF_SYSTEMS ?= all
# ssh destination for the remote targets, e.g. PERF_HOST=jhahn@198.51.100.90.
PERF_HOST ?=

PROFILE_DIR = profiles/$(PERF_PROFILE)

# Internal: common perfcompare flags.
PERF_FLAGS = --benchtime=$(BENCHTIME) --timeout=$(PERF_TIMEOUT) $(PERF_EXTRA)

# Internal: module credentials for the image builds. The quicraft benchmark
# image runs `go mod download` inside a container, and go-enclave lives on
# Gitea, so the build needs the four variables of
# ../handbook/software/conventions/dependency-resolution.md section 6. They
# come from the job environment when a runner injected them; otherwise from
# the gitignored .devcontainer/.env, which holds that section's block
# verbatim. perfcompare turns them into the BuildKit secret the Dockerfile
# mounts, and run-remote.sh carries them to the bench host over stdin, so the
# token is never on a command line or in an image. Absent both, the build
# resolves github.com/jeremyhahn/* from the public proxy only and says so.
MODULE_AUTH_ENV = if [ -z "$${GIT_CONFIG_KEY_0:-}" ] && [ -f $(CURDIR)/.devcontainer/.env ]; then \
	set -a; . $(CURDIR)/.devcontainer/.env; set +a; fi;
# Internal: every local perfcompare invocation, credentials staged.
PERFCOMPARE = cd $(PERF_DIR) && $(MODULE_AUTH_ENV) ./perfcompare

# Internal: refuse to touch a nameless profile. Writing results into a directory
# whose machine nobody recorded is how a number ends up uninterpretable.
REQUIRE_PROFILE = @if [ -z "$(PERF_PROFILE)" ]; then \
	echo "PERF_PROFILE is required (e.g. PERF_PROFILE=omega). Profiles: $$(ls $(PERF_DIR)/profiles 2>/dev/null | tr '\n' ' ')"; \
	exit 1; fi

.PHONY: perf-build
perf-build:
	cd $(PERF_DIR) && $(GO) build -o perfcompare ./cmd/perfcompare/

# ----------------------------------------------------------------------------
# Ad-hoc runs. These print a terminal report and store nothing, so they need no
# profile: nothing they produce can later be mistaken for a machine's record.
# ----------------------------------------------------------------------------
.PHONY: perf-quicraft
perf-quicraft: perf-build
	$(PERFCOMPARE) $(PERF_FLAGS) quicraft

.PHONY: perf-etcd
perf-etcd: perf-build
	$(PERFCOMPARE) $(PERF_FLAGS) etcd

.PHONY: perf-dragonboat
perf-dragonboat: perf-build
	$(PERFCOMPARE) $(PERF_FLAGS) dragonboat

.PHONY: perf-openraft
perf-openraft: perf-build
	$(PERFCOMPARE) $(PERF_FLAGS) openraft

.PHONY: perf-compare
perf-compare: perf-build
	$(PERFCOMPARE) $(PERF_FLAGS) $(SYSTEMS)

.PHONY: perf-compare-all
perf-compare-all: perf-build
	$(PERFCOMPARE) $(PERF_FLAGS) all

# ----------------------------------------------------------------------------
# Profile pipeline: run into a profile, then generate that profile's documents.
#
#   make perf-all PERF_PROFILE=omega
#   make perf-run PERF_PROFILE=omega PERF_SYSTEMS=etcd     # one engine
#   make perf-generate PERF_PROFILE=omega
# ----------------------------------------------------------------------------
.PHONY: perf-run
perf-run: perf-build
	$(REQUIRE_PROFILE)
	$(PERFCOMPARE) $(PERF_FLAGS) --profile=$(PROFILE_DIR) $(PERF_SYSTEMS)

# Regenerates <system>.md and every chart in the profile from its stored JSON.
# analysis.md is hand-written and is never touched.
.PHONY: perf-generate
perf-generate: perf-build
	$(REQUIRE_PROFILE)
	cd $(PERF_DIR) && ./perfcompare generate --profile=$(PROFILE_DIR) --systems=$(PERF_SYSTEMS)

.PHONY: perf-all
perf-all: perf-run perf-generate

# Capture a machine's specifications into a profile. With PERF_HOST it captures
# the remote machine; without, this one. Either way the values are derived where
# the benchmarks run, never typed in.
.PHONY: perf-env
perf-env:
	$(REQUIRE_PROFILE)
	@mkdir -p $(PERF_DIR)/$(PROFILE_DIR)
	@if [ -n "$(PERF_HOST)" ]; then \
		ssh -o BatchMode=yes $(PERF_HOST) 'bash -s $(PERF_PROFILE)' \
			< $(PERF_DIR)/capture-env.sh > $(PERF_DIR)/$(PROFILE_DIR)/env.json; \
	else \
		$(PERF_DIR)/capture-env.sh $(PERF_PROFILE) > $(PERF_DIR)/$(PROFILE_DIR)/env.json; \
	fi
	@cat $(PERF_DIR)/$(PROFILE_DIR)/env.json

# The whole pipeline against any machine, in one command: stage the working
# tree, capture the target's specs, run the suite, fetch results AND env.json
# back into profiles/<name>/, regenerate that profile's documents.
#
#   make perf-remote PERF_HOST=user@host PERF_PROFILE=omega
#
# The remote run is detached, so a dropped ssh costs the tail and not the run;
# reattach with 'test/performance/run-remote.sh wait'. Knobs: BENCHTIME,
# PERF_TIMEOUT, PERF_SYSTEMS, BENCH_TMPDIR, REMOTE_DIR.
.PHONY: perf-remote
perf-remote:
	$(REQUIRE_PROFILE)
	@if [ -z "$(PERF_HOST)" ]; then \
		echo "PERF_HOST is required (e.g. PERF_HOST=user@host)"; exit 1; fi
	$(MODULE_AUTH_ENV) \
	PERF_HOST=$(PERF_HOST) PERF_PROFILE=$(PERF_PROFILE) PERF_SYSTEMS=$(PERF_SYSTEMS) \
		BENCHTIME=$(BENCHTIME) PERF_TIMEOUT=$(PERF_TIMEOUT) \
		$(PERF_DIR)/run-remote.sh all

# Reattach to, or inspect, a run already in flight on PERF_HOST.
.PHONY: perf-remote-status
perf-remote-status:
	$(REQUIRE_PROFILE)
	@if [ -z "$(PERF_HOST)" ]; then \
		echo "PERF_HOST is required (e.g. PERF_HOST=user@host)"; exit 1; fi
	PERF_HOST=$(PERF_HOST) PERF_PROFILE=$(PERF_PROFILE) $(PERF_DIR)/run-remote.sh status

# ============================================================================
# Coverage
# ============================================================================

# check-coverage: extract the percentage from a coverage profile and fail if it
# is below the threshold, if the profile does not exist, or if no percentage can
# be read out of it.
# Usage: $(call check-coverage,profile.out,package-name)
#
# The three guards before the comparison are not defensive padding. The previous
# version went straight to
#
#	COVERAGE=$$(go tool cover -func=$(1) | grep total | ...)
#	if [ $$(echo "$${COVERAGE} < $(COVER_MIN)" | bc -l) -eq 1 ]
#
# and on an empty or missing profile COVERAGE is the empty string, `bc` fails
# with a syntax error and prints NOTHING, so the test reads `[ -eq 1 ]`, the
# shell reports "unary operator expected", the if takes the else branch, and the
# gate PASSES. A coverage gate that passes precisely when it has no coverage
# data is the failure mode the gate exists to prevent, so every way of ending up
# without a number is now an explicit failure.
#
# `awk '/^total:/'` rather than `grep total`: the old filter also matched any
# function whose name contains "total", and with more than one match `awk
# {print $$3}` emitted two values, which lands right back in the unparseable
# case above.
define check-coverage
	@if [ ! -s $(1) ]; then \
		echo "FAIL: $(2) coverage profile $(1) is missing or empty; the test run produced no coverage data"; \
		exit 1; \
	fi; \
	COVERAGE=$$($(GO) tool cover -func=$(1) | awk '/^total:/ {print $$3}' | sed 's/%//'); \
	case "$${COVERAGE}" in \
		'' | *[!0-9.]* ) \
			echo "FAIL: $(2) coverage could not be read from $(1) (got '$${COVERAGE}')"; \
			exit 1 ;; \
	esac; \
	echo "$(2) coverage: $${COVERAGE}%"; \
	if [ $$(echo "$${COVERAGE} < $(COVER_MIN)" | bc -l) -eq 1 ]; then \
		echo "FAIL: $(2) coverage $${COVERAGE}% is below minimum $(COVER_MIN)%"; \
		exit 1; \
	fi
endef

# run-coverage: measure one scope's coverage and enforce COVER_MIN on it.
# Usage: $(call run-coverage,<label>,<package pattern>)
#
# Every coverage target below goes through here, so the flags a coverage run
# needs are decided once instead of being copied twenty-three times, and a new
# package target cannot be added with a different set by accident.
#
# On -coverpkg, which the dead COVER_EXCLUDE variable used to hold and which is
# deliberately NOT passed here: it does not do what the variable's presence
# suggests. `go test -coverprofile` already emits an entry for a package that
# has no test file at all, at 0%, so a test-less package is ALREADY in the
# denominator and already drags the total down — measured, not assumed: adding a
# 270-statement package with no tests moved the module total from 91.3% to 89.7%
# without -coverpkg. What -coverpkg adds is credit for statements covered by
# some OTHER package's tests, which raises the same tree from 91.3% to 92.0%.
# Against an unchanged COVER_MIN that is a 0.7-point loosening of the gate, so
# adopting it is a threshold decision for the owner, not a wiring fix.
define run-coverage
	$(GOTEST) -coverprofile=$(COVER_DIR)/$(1).out $(2)
	$(call check-coverage,$(COVER_DIR)/$(1).out,$(1))
endef

.PHONY: coverage
coverage: | $(COVER_DIR)
	$(call run-coverage,total,./...)

.PHONY: coverage-proto
coverage-proto: | $(COVER_DIR)
	$(call run-coverage,proto,$(PKG_PROTO))

.PHONY: coverage-config
coverage-config: | $(COVER_DIR)
	$(call run-coverage,config,$(PKG_CONFIG))

.PHONY: coverage-queue
coverage-queue: | $(COVER_DIR)
	$(call run-coverage,queue,$(PKG_QUEUE))

.PHONY: coverage-invariant
coverage-invariant: | $(COVER_DIR)
	$(call run-coverage,invariant,$(PKG_INVARIANT))

.PHONY: coverage-logdb
coverage-logdb: | $(COVER_DIR)
	$(call run-coverage,logdb,$(PKG_LOGDB))

.PHONY: coverage-raft
coverage-raft: | $(COVER_DIR)
	$(call run-coverage,raft,$(PKG_RAFT))

.PHONY: coverage-waldb
coverage-waldb: | $(COVER_DIR)
	$(call run-coverage,waldb,$(PKG_WALDB))

.PHONY: coverage-seal
coverage-seal: | $(COVER_DIR)
	$(call run-coverage,seal,$(PKG_SEAL))

.PHONY: coverage-rsm
coverage-rsm: | $(COVER_DIR)
	$(call run-coverage,rsm,$(PKG_RSM))

.PHONY: coverage-session
coverage-session: | $(COVER_DIR)
	$(call run-coverage,session,$(PKG_SESSION))

.PHONY: coverage-snapshot
coverage-snapshot: | $(COVER_DIR)
	$(call run-coverage,snapshot,$(PKG_SNAPSHOT))

.PHONY: coverage-engine
coverage-engine: | $(COVER_DIR)
	$(call run-coverage,engine,$(PKG_ENGINE))

.PHONY: coverage-registry
coverage-registry: | $(COVER_DIR)
	$(call run-coverage,registry,$(PKG_REGISTRY))

.PHONY: coverage-transport
coverage-transport: | $(COVER_DIR)
	$(call run-coverage,transport,$(PKG_TRANSPORT))

.PHONY: coverage-quicraft
coverage-quicraft: | $(COVER_DIR)
	$(call run-coverage,quicraft,$(PKG_QUICRAFT))

.PHONY: coverage-errors
coverage-errors: | $(COVER_DIR)
	$(call run-coverage,errors,$(PKG_ERRORS))

.PHONY: coverage-sm
coverage-sm: | $(COVER_DIR)
	$(call run-coverage,sm,$(PKG_SM))

.PHONY: coverage-server
coverage-server: | $(COVER_DIR)
	$(call run-coverage,server,$(PKG_SERVER))

.PHONY: coverage-writemode
coverage-writemode: | $(COVER_DIR)
	$(call run-coverage,writemode,$(PKG_WRITEMODE))

.PHONY: coverage-batch
coverage-batch: | $(COVER_DIR)
	$(call run-coverage,batch,$(PKG_BATCH))

.PHONY: coverage-crypto
coverage-crypto: | $(COVER_DIR)
	$(call run-coverage,crypto,$(PKG_CRYPTO))

$(COVER_DIR):
	mkdir -p $(COVER_DIR)

# ============================================================================
# FIPS
#
# Three checks, deliberately named apart, because they make three different
# claims and only two of them can fail on a non-approved primitive.
#
# build-fips COMPILES. It proves the tree builds under the Go Cryptographic
# Module both with and without the `fips` tag. It produces no artifact and makes
# no compliance claim: this module is a library with no main package, so
# `go build` over a package pattern discards its output. GOFIPS140 is a build
# setting and by itself never executes a cryptographic operation.
#
# verify-fips RUNS the code under GODEBUG=fips140=only, which rejects every
# non-approved algorithm at the point of use. That is the compliance check, and
# `only` rather than `on` is the whole point of it: `on` loads the module but
# still PERMITS non-approved algorithms, so MD5, SHA-1 and arbitrary-IV AES-GCM
# succeed and the gate reports green on exactly the thing it exists to catch.
# Ratified by the owner (conventions/configuration.md section 5); do not weaken
# it, and do not add exclusions to route around a failure.
#
# verify-fips-exempt RUNS pkg/internal/transport/revocation under
# GODEBUG=fips140=on, one notch lower, and that gap is a documented exemption
# rather than an oversight. ONE primitive on that path is mandated by protocol
# and lives in a dependency, so it cannot be made approved from this repository:
#
#   * OCSP CertID is SHA-1 by definition. RFC 6960 section 4.1.1 mandates it for
#     issuerNameHash/issuerKeyHash, where it is an identifier and protects
#     nothing. Inside golang.org/x/crypto/ocsp.
#
# It is permitted under `on`, so this target is a real gate and not a formality:
# it proves a `-tags fips` build of the revocation checker still passes its
# suite, and that nothing OTHER than the CertID hash on that path is
# non-approved.
#
# QUIC PACKET PROTECTION USED TO BE THE SECOND EXEMPTION AND IS NOT ANY MORE.
# quic-go v0.60.0 (go-quicraft#85) routes packet AEADs through Go's TLS 1.3
# AES-GCM implementation instead of the general cipher.NewGCM -- see
# internal/handshake/cipher_suite.go, which dispatches to
# aeadAESGCMTLS13FIPS140 when the module is active. The whole
# pkg/internal/transport tree therefore passes under fips140=only, and it is
# INSIDE the approved-only gate. Do not move it back out; if it goes red, the
# failure is the finding.
#
# The split is a partition, not a hole: verify-fips is a deny-list of exactly
# the one tree verify-fips-exempt then runs, so a package added tomorrow is
# inside the approved-only gate by default rather than silently outside every
# gate. test/buildgates/fips_gate_test.go fails if that stops being true.
#
# So: `-tags fips` makes go-quicraft FIPS at rest AND on the wire — the seal
# layer, the WAL, snapshots and the QUIC transport execute only approved
# primitives and are proven to. The residual is one SHA-1 identifier on the OCSP
# path, and a deployment that configures revocation Mode: crl never reaches it.
# docs/quic-transport/architecture.md carries the citation as the exemption
# record.
# ============================================================================

# The one package tree outside the approved-only gate, and the exact tree
# verify-fips-exempt covers instead. See above.
FIPS_EXCLUDED := github.com/jeremyhahn/go-quicraft/pkg/internal/transport/revocation

.PHONY: build-fips
build-fips:
	GOFIPS140=v1.0.0 CGO_ENABLED=0 $(GO) build -ldflags="$(LDFLAGS)" ./...
	GOFIPS140=v1.0.0 CGO_ENABLED=0 $(GO) build -tags fips -ldflags="$(LDFLAGS)" ./...

.PHONY: verify-fips
verify-fips:
	@echo "==> GODEBUG=fips140=only, -tags fips, excluding $(FIPS_EXCLUDED)/..."
	GOFIPS140=latest GODEBUG=fips140=only $(GO) test -tags fips -timeout=900s \
		$$($(GO) list -tags fips ./... | grep -v '^$(FIPS_EXCLUDED)')

.PHONY: verify-fips-exempt
verify-fips-exempt:
	@echo "==> GODEBUG=fips140=on, -tags fips, $(FIPS_EXCLUDED) (exempt from only; see above)"
	GOFIPS140=latest GODEBUG=fips140=on $(GO) test -tags fips -timeout=900s \
		$(FIPS_EXCLUDED)/...

# ============================================================================
# CI pipeline
# Runs fmt, vet, lint, security scans, build, and tests.
# Requires: golangci-lint, gosec, govulncheck, trivy, nancy
# ============================================================================

.PHONY: ci
# nancy is intentionally NOT in this list: it requires OSS Index credentials
# and fails closed without them, and govulncheck + trivy already cover Go
# dependency vulnerabilities. Run `make nancy` explicitly where credentials
# are configured. This is not a reduction in coverage — with a 401 the scan
# was never running, it was only reporting that it had.
ci: fmt no-mutants check-tool-versions vet lint security build test
	@echo "CI pipeline passed"

# ============================================================================
# Analyser-version guard
#
# GOLANGCI_VERSION is the single source of truth, but the CI workflows and
# devcontainer Dockerfiles cannot read a make variable — ci.yml, release.yml
# (each installing it independently since quality.yml was retired,
# go-quicraft#22) and the two devcontainer Dockerfiles each carry a literal
# pin. They had once drifted to THREE different versions at once
# (Makefile/quality.yml v2.12.2, .devcontainer/Dockerfile v2.11.1,
# Dockerfile.dev and install-tools v2.7.2), which is the failure mode
# the remote-tools comment describes: an analyser of the wrong version is worse
# than none, because it gates on a different rule set while looking like it
# passed. golangci-lint v2 also moved flags and changed which linters are on by
# default, so this is not cosmetic drift.
#
# Mechanical rather than a matter of remembering, for the same reason
# no-mutants is. The floor matters: a discovery that finds nothing must fail,
# or a renamed file silently turns this into a gate over an empty set.
#
# The literal pins are only half of it. A version pin says WHICH ANALYSER, not
# WHICH GO BUILT IT, and the second half is invisible to any amount of grepping
# — so check-lint-toolchain below inspects the installed binary itself. That
# gap cost a CI round trip in #118; the two checks together are the gate.
# ============================================================================
.PHONY: check-tool-versions
check-tool-versions: check-lint-toolchain
	@pins=$$(git grep -hoE 'golangci-lint(@|.* )v[0-9]+\.[0-9]+\.[0-9]+' -- \
		.github .devcontainer Makefile | grep -oE 'v[0-9]+\.[0-9]+\.[0-9]+' | sort -u); \
	count=$$(printf '%s\n' "$$pins" | grep -c . || true); \
	if [ "$$count" -lt 1 ]; then \
		echo "FAIL: found no golangci-lint version pins at all."; \
		echo "      Discovery has broken and this gate is verifying nothing."; \
		exit 1; \
	fi; \
	if [ "$$pins" != "$(GOLANGCI_VERSION)" ]; then \
		echo "FAIL: golangci-lint pins disagree with GOLANGCI_VERSION=$(GOLANGCI_VERSION):"; \
		git grep -nE 'golangci-lint(@|.* )v[0-9]+\.[0-9]+\.[0-9]+' -- \
			.github .devcontainer Makefile; \
		echo ""; \
		echo "Every literal pin must equal GOLANGCI_VERSION. A developer or a"; \
		echo "container linting on a different version gates on a different rule"; \
		echo "set from CI while looking like it passed the same gate."; \
		exit 1; \
	fi; \
	echo "golangci-lint pins agree ($(GOLANGCI_VERSION))"

# ============================================================================
# Analyser build-toolchain guard
#
# A version pin constrains WHICH ANALYSER, not WHICH GO BUILT IT — and an
# analysis tool built by an older Go cannot analyse a module targeting a newer
# one. When go.mod moved to `go 1.27.0` (#83), every locally installed
# golangci-lint refused the module before reading a line of source:
#
#   Error: can't load config: the Go language version (go1.26) used to build
#   golangci-lint is lower than the targeted Go version (1.27.0)
#
# and staticcheck failed differently, choking on the 1.27 standard library
# ("unknown field rfd in struct literal of type splicePipe").
#
# CI never saw it. The runner image bakes v2.13.2 built with its own 1.27
# toolchain and passes; the identically-versioned release binary in ~/go/bin
# was built with go1.26.2 and refuses. Same version string, opposite outcome —
# which is exactly why the literal-pin check above could not see it, and why
# nobody could lint before pushing. Lint findings then surfaced only after a
# full CI round trip on a contended runner pool (#117 burned a cycle on a
# one-line ST1023 a five-second local run would have caught).
#
# So the guard reads the build toolchain out of the binary's own self-report
# ("built with go1.26.2") and compares it to the module's `go` directive, and
# `make lint` runs $(GOLANGCI_BIN) rather than whatever PATH offers first.
# A mismatch prints what to run; the alternative is a developer meeting the
# analyser's own version-comparison error with no idea what produced it.
#
# staticcheck gets NO guard of its own, deliberately: it runs as a
# golangci-lint linter (.golangci.yml `linters.enable`), never as a standalone
# binary — nothing in this Makefile or under .github/ invokes it directly. A
# second guard would gate a tool this repository does not run. Installing the
# analyser with a 1.27 toolchain fixes its half of #118 along with the rest.
# ============================================================================
.PHONY: check-lint-toolchain
check-lint-toolchain:
	@bin='$(GOLANGCI_BIN)'; \
	want='$(GOLANGCI_VERSION)'; \
	need='$(GO_DIRECTIVE)'; \
	if [ -z "$$bin" ]; then \
		echo "FAIL: no golangci-lint found."; \
		echo "      Looked in $(TOOLS_BIN), then on PATH."; \
		echo ""; \
		echo "      Install the pinned analyser, built with this repo's Go:"; \
		echo "          make install-lint"; \
		exit 1; \
	fi; \
	report=$$("$$bin" --version 2>/dev/null); \
	have=$$(printf '%s' "$$report" | sed -n 's/.*has version \([0-9][0-9.]*\).*/v\1/p'); \
	built=$$(printf '%s' "$$report" | sed -n 's/.*built with go\([0-9][0-9.]*\).*/\1/p'); \
	if [ -z "$$have" ] || [ -z "$$built" ]; then \
		echo "FAIL: could not read the version of $$bin."; \
		echo "      It reported: $$report"; \
		echo ""; \
		echo "      Reinstall the pinned analyser:"; \
		echo "          make install-lint"; \
		exit 1; \
	fi; \
	if [ "$$have" != "$$want" ]; then \
		echo "FAIL: $$bin is $$have, this repo pins $$want."; \
		echo "      An analyser of the wrong version gates on a different rule"; \
		echo "      set from CI while looking like it passed the same gate."; \
		echo ""; \
		echo "      Install the pinned analyser:"; \
		echo "          make install-lint"; \
		exit 1; \
	fi; \
	if [ "$$(printf '%s\n%s\n' "$$need" "$$built" | sort -V | head -1)" != "$$need" ]; then \
		echo "FAIL: $$bin was built with go$$built, but this module targets $$need."; \
		echo "      An analyser built by an older Go cannot analyse a newer"; \
		echo "      module: golangci-lint refuses the config outright, and"; \
		echo "      staticcheck chokes parsing the go$$need standard library."; \
		echo "      The version pin ($$want) is satisfied and irrelevant here —"; \
		echo "      what is wrong is the toolchain that BUILT this binary."; \
		echo ""; \
		echo "      Rebuild it with the Go on this PATH ($$($(GO) env GOVERSION)):"; \
		echo "          make install-lint"; \
		exit 1; \
	fi; \
	echo "golangci-lint $$have built with go$$built (module targets $$need): $$bin"

# ============================================================================
# Full CI pipeline, including every build-tagged suite.
#
# `ci` above stops at `test`, which is `go test ./...` with NO build tags. The
# Go toolchain does not warn when a build constraint excludes a file — it just
# compiles a smaller program — so that quietly skipped 147 integration tests,
# 21 errfs fault-injection tests and 35 fuzz targets, and it computed the 91%
# coverage threshold over what was left.
#
# ci-full is the target that matches what .github/workflows/ci.yml now runs.
# `ci` is kept as the fast inner loop; ci-full is what to run before opening a
# PR and what to reproduce a red CI run with. The tagged suites come LAST
# because they are the slow ones (~10 min, dominated by ci-integration) and
# there is no reason to pay for them when gofmt is going to fail anyway.
# ============================================================================

.PHONY: ci-full
ci-full: ci ci-modules ci-errfs ci-fuzz-seeds ci-fuzz-mutation ci-integration ci-netfault \
        build-fips verify-fips verify-fips-exempt
	@echo "Full CI pipeline passed (untagged + integration + netfault + errfs + fuzz seeds + fuzz mutation + all modules + FIPS)"

# ============================================================================
# Heavyweight test offload to omega (198.51.100.90)
#
# Every test target here runs under `-race` (see GOTEST), and ci-integration
# stands up multi-node QUIC clusters. On a developer workstation that is also
# running editors, browsers and other agents, that is enough contention to make
# the 30-second catch-up waits in the snapshot tests flake — the failures are
# environmental, they look like raft defects, and chasing them has cost real
# days (see the CI runner-contention and load-generator incidents).
#
# omega is the department's designated spare-capacity host for exactly this
# (software/CLAUDE.md: "Use it for agent fleets, performance and load testing
# ... rather than loading a busy machine"). It is also where the omega-docker
# Gitea runner lives, so CI already executes there; this target just gives the
# same machine to the local inner loop.
#
#   make remote-ci-full        # the full gate, off-box
#   make remote-test           # any target: remote-<target> runs <target> there
#   make remote-integration-test
#
# The pattern rule means no target needs enrolling by hand. `remote-sync` is an
# explicit rule, so it wins over the pattern and does not recurse.
#
# --delete keeps the remote tree an exact mirror, so a file deleted locally
# cannot linger there and turn a broken build green. .git is excluded
# deliberately: this ships the WORKING TREE, including uncommitted edits, which
# is the whole point during development, and it means nothing on the remote can
# perform a git operation against this repository's history.
# ============================================================================

OMEGA_USER    ?= jhahn
OMEGA_HOST    ?= 198.51.100.90
OMEGA_DIR     ?= ~/ci/go-quicraft
# omega keeps its toolchain in ~/sdk and its go-installed tools in ~/go/bin;
# a non-interactive ssh does not source the profile that would add them.
OMEGA_PATH    := export PATH=$$HOME/sdk/go1.26.5/bin:$$HOME/go/bin:$$PATH
OMEGA_SSH     := ssh -o BatchMode=yes -o ConnectTimeout=10 $(OMEGA_USER)@$(OMEGA_HOST)
RSYNC_EXCLUDE := --exclude .git --exclude .build-cache --exclude $(COVER_DIR) \
                 --exclude $(BUILD_DIR) --exclude '*.test' --exclude '*.prof' \
                 --exclude '*.out' --exclude test/performance/openraft/target

.PHONY: remote-sync
remote-sync:
	@$(OMEGA_SSH) 'mkdir -p $(OMEGA_DIR)'
	@rsync -az --delete $(RSYNC_EXCLUDE) ./ $(OMEGA_USER)@$(OMEGA_HOST):$(OMEGA_DIR)/
	@echo "synced working tree to $(OMEGA_USER)@$(OMEGA_HOST):$(OMEGA_DIR)"

# remote-<target>: sync, then run <target> on omega. Output is streamed back,
# so a failure reads exactly as it would locally.
.PHONY: remote-%
remote-%: remote-sync
	@echo "==> running 'make $*' on $(OMEGA_HOST)"
	@$(OMEGA_SSH) '$(OMEGA_PATH); cd $(OMEGA_DIR) && make $*'

# One-time (or after a toolchain bump) provisioning of the scanners omega does
# not ship with. Kept separate from remote-sync so the common path stays fast.
#
# Every tool is installed UNCONDITIONALLY, not "if absent". omega had a v1
# golangci-lint AND a years-old gosec on PATH, so `command -v` guards skipped
# both installs: `make lint` died on `unknown flag: --timeout` (v2 moved it) and
# `make gosec` died printing its usage, because the old binary has no
# -exclude-dir and a rule set predating G115/G306.
#
# An analyser of the wrong version is worse than none. It gates on a different
# rule set from CI while looking like it passed — the precise failure mode this
# repository's review keeps turning up. golangci-lint is pinned to the version
# quality.yml installs; gosec and govulncheck follow quality.yml's @latest.
.PHONY: remote-tools
remote-tools:
	@$(OMEGA_SSH) '$(OMEGA_PATH); \
		go install golang.org/x/vuln/cmd/govulncheck@latest; \
		go install github.com/securego/gosec/v2/cmd/gosec@latest; \
		go install github.com/golangci/golangci-lint/v2/cmd/golangci-lint@$(GOLANGCI_VERSION); \
		command -v trivy >/dev/null || curl -sfL \
			https://raw.githubusercontent.com/aquasecurity/trivy/main/contrib/install.sh \
			| sh -s -- -b $$HOME/go/bin >/dev/null; \
		echo "omega tools ready ($(GOLANGCI_VERSION))"'
	@# ci-modules builds test/performance/dragonboat, which requires
	@# github.com/jeremyhahn/dragonboat. A global insteadOf rewrites that to the
	@# Gitea host over ssh, so the module download fails with "Host key
	@# verification failed" until the host key is known -- an error that reads
	@# like a missing repository rather than a missing known_hosts entry.
	@$(OMEGA_SSH) 'mkdir -p $$HOME/.ssh && chmod 700 $$HOME/.ssh; \
		for hk in "-p 2222 198.51.100.50" "github.com"; do \
			ssh-keyscan $$hk 2>/dev/null >> $$HOME/.ssh/known_hosts; \
		done; \
		sort -u $$HOME/.ssh/known_hosts -o $$HOME/.ssh/known_hosts; \
		chmod 600 $$HOME/.ssh/known_hosts; echo "  known_hosts seeded"'
	@$(OMEGA_SSH) '$(OMEGA_PATH); for t in go golangci-lint gosec govulncheck trivy; do \
		printf "  %-14s %s\n" "$$t" "$$(command -v $$t || echo MISSING)"; done'

# ============================================================================
# Multi-module build
#
# This repository is not one Go module: contrib/dragonboat and the benchmark
# harnesses under test/performance/ are separate modules that require or
# `replace` the root. Building only the root let a root-module change break a
# sibling silently — contrib/dragonboat sat broken because its `go` directive
# was below what the parent it replaces requires, and nothing built it.
#
# The list is DISCOVERED from the tree rather than written down here, so a new
# go.mod is enrolled the moment it appears. Mirrors the `modules` job in
# .github/workflows/ci.yml, which discovers the same way.
#
# GOWORK=off (exported at the bottom of this file) is load-bearing: inside a
# workspace these modules resolve the root through the workspace instead of
# through their own require/replace directives, and the `go` directive mismatch
# this target exists to catch does not reproduce.
# ============================================================================

#
# Every module is attempted even after one fails, mirroring `fail-fast: false`
# on the CI matrix: one broken sibling must not hide a second one. Stopping at
# the first failure is how contrib/dragonboat's breakage masked the identical
# breakage in three test/performance modules.
.PHONY: ci-modules
ci-modules:
	@rc=0; \
	for dir in $$(find . -name go.mod -not -path './.git/*' -printf '%h\n' | sort); do \
		echo "==> $$dir"; \
		if ! ( cd "$$dir" && $(GO) build ./... && $(GOVET) ./... ); then \
			echo "    FAIL: $$dir"; rc=1; \
		fi; \
	done; \
	if [ $$rc -ne 0 ]; then \
		echo "ci-modules: one or more modules failed to build or vet"; \
	else \
		echo "All modules build and vet clean"; \
	fi; \
	exit $$rc

# ============================================================================
# Linting and static analysis
# ============================================================================

# Runs the analyser resolved by GOLANGCI_BIN, NOT the first golangci-lint on
# PATH, and only after check-lint-toolchain has established that the binary can
# actually analyse this module (#118).
.PHONY: lint
lint: check-lint-toolchain
	$(GOLANGCI_BIN) run --timeout=5m ./...

.PHONY: vet
vet:
	$(GOVET) ./...

# ============================================================================
# Mutation-marker guard
#
# Mutation testing is how correctness is verified in this repository: a fix is
# not trusted until the defect it closes has been re-introduced and the test
# seen to fail. That workflow edits PRODUCTION code and relies on restoring it
# afterwards -- and a restore that never happens is invisible, because a
# disabled guard makes tests pass, not fail.
#
# It has already happened once. A `if false {` around the rollback fsync in
# waldb reached a commit: a failed SaveState whose bytes had reached the medium
# through ordinary writeback could be resurrected by a crash and replayed as
# committed entries the caller had been told were REJECTED. Every suite stayed
# green, because the site had no test -- which is precisely why it was being
# mutated.
#
# So the guard is mechanical rather than a matter of remembering. Test files are
# excluded: `if false` is legitimate there (pinning a compile-time branch), and
# a mutation left in a test file cannot silently weaken production behaviour.
# ============================================================================
.PHONY: no-mutants
no-mutants:
	@found=$$(grep -rnE '\bif false \{|\bMUTANT\b|\bMUTATION\b' \
		--include='*.go' pkg/ test/ contrib/ 2>/dev/null \
		| grep -v '_test\.go:' || true); \
	if [ -n "$$found" ]; then \
		echo "FAIL: mutation markers left in production code:"; \
		echo "$$found"; \
		echo ""; \
		echo "A mutation that is never restored disables a guard silently --"; \
		echo "the suite goes GREEN, which is the opposite of a visible failure."; \
		exit 1; \
	fi; \
	echo "No mutation markers in production code"

.PHONY: fmt
fmt:
	@UNFORMATTED=$$($(GOFMT) -l .); \
	if [ -n "$${UNFORMATTED}" ]; then \
		echo "Files not formatted:"; \
		echo "$${UNFORMATTED}"; \
		exit 1; \
	fi; \
	echo "All files formatted correctly"

# ============================================================================
# Security scanning
# ============================================================================

# Comprehensive security target - the full devops.md §8 suite.
#
# gosec + govulncheck + go mod verify + trivy fs. `trivy-image` is NOT a
# prerequisite here: it builds the devcontainer image, which is slow and needs
# a Docker daemon. Run `make trivy-image` explicitly when an image changes.
.PHONY: security
security: gosec vuln modverify trivy
	@echo "All security scans passed"
	@echo "Note: container images are scanned separately -- run 'make trivy-image'"

.PHONY: modverify
# Module integrity against go.sum (devops.md §8). Catches a tampered or
# corrupted module cache before any of the scanners read it.
modverify:
	$(GO) mod verify

.PHONY: deps-check
# Fail when a DIRECT dependency of any module here is behind its newest release
# (#86). This is the freshness watcher; `deps-latest` beside it is the
# first-party UPDATER, and they are not the same thing -- deps-latest only ever
# looks at github.com/jeremyhahn/*, which is why six third-party dependencies
# drifted a release or more without anything noticing.
#
# NOT a prerequisite of `security`, and not in ci.yml's gate. A dependency
# publishing a release does not make this commit wrong, and a push-path gate
# that fails for something no one in the PR did trains people to ignore it. It
# runs in nightly.yml, which is the channel that actually caught the last two
# (#50's CVE and #85's FIPS fix) -- see the script header for why this had to be
# built on the Gitea plane rather than left to .github/dependabot.yml.
#
# Run it by hand any time; it needs only a go toolchain and network.
deps-check:
	@./scripts/deps-check.sh

.PHONY: gosec
# Deliberately scans MORE than the CI job does. ci.yml excludes test/, and
# contrib/ wholesale; this excludes only test/performance (separate modules
# that never ship). A finding here that CI would not report is still a real
# finding — do not narrow this to match CI.
gosec:
	$(GOSEC) -exclude=$(GOSEC_EXCLUDE) -exclude-dir=test/performance -severity=medium -confidence=medium ./...

.PHONY: vuln
# Fails the build on any vulnerability govulncheck finds reachable from our
# code. It previously swallowed a non-zero exit into a WARNING, which meant a
# real finding printed a message and PASSED — a security gate that cannot fail
# is not a gate. If a stdlib vulnerability is genuinely unfixable without a Go
# upgrade, record that decision explicitly rather than hiding it here.
vuln:
	$(GOVULNCHECK) ./...

.PHONY: trivy
# Scoped to what actually ships. test/performance/* are SEPARATE modules with
# their own go.mod, pulled in only to benchmark against dragonboat and etcd, so
# their transitive CVEs (x/net, x/crypto/ssh, x/text via dragonboat) never
# reach a consumer of this library — `go get github.com/jeremyhahn/go-quicraft`
# does not fetch them. .build-cache is a local build artifact. Excluding them
# is what makes this gate mean "the shipped library is clean" rather than
# failing permanently on benchmark tooling and training everyone to ignore it.
#
# A missing binary FAILS rather than skipping. This target used to print
# "trivy not installed, skipping fs scan" and return success, so `make security`
# announced "All security scans passed" with one of its four scanners absent —
# and that is exactly what happened on the omega test host, silently, until the
# tool inventory was printed. `vuln` and `nancy` were both hardened against this
# same shape earlier ("a security gate that cannot fail is not a gate"); trivy
# was left behind.
#
# The nancy reasoning does NOT transfer here. nancy may skip because govulncheck
# and trivy cover the same ground; nothing else in the suite scans for leaked
# secrets, so a skipped trivy is a real hole rather than a redundant one.
trivy:
	@if ! command -v $(TRIVY) >/dev/null 2>&1; then \
		echo "FAIL: trivy is not installed, so the dependency-CVE and secret scan"; \
		echo "      did not run. This gate does not pass by default."; \
		echo "      Install it with: make install-trivy"; \
		exit 1; \
	fi
	$(TRIVY) fs --scanners vuln,secret --severity CRITICAL,HIGH --exit-code 1 \
		--skip-dirs .build-cache --skip-dirs test/performance .

# Scans the images this repo builds locally: the devcontainer dev image and the
# integration image. Neither is built in CI, so this is the only gate they get
# -- run it when either Dockerfile or its base image changes.
.PHONY: trivy-image
trivy-image: docker-build
	@if command -v $(TRIVY) >/dev/null 2>&1; then \
		$(TRIVY) image --severity CRITICAL,HIGH --exit-code 1 $(DOCKER_IMAGE):$(DOCKER_TAG); \
	else \
		echo "trivy not installed, skipping image scan (install: make install-trivy)"; \
	fi

# Nancy scans go.sum for known vulnerable dependencies
# Note: nancy requires Sonatype OSS Index API access. If it fails, govulncheck
# provides similar coverage for Go module vulnerabilities.
.PHONY: nancy
# nancy exits non-zero BOTH when it finds vulnerabilities and when it cannot
# reach OSS Index, so the previous `|| echo "scan completed"` reported success
# for a real CVE, an expired credential, and a network failure alike. It now
# fails closed. Not-installed is still a legitimate skip, because govulncheck
# and trivy cover the same ground; an installed-but-failing nancy is not.
# OSS Index requires credentials (~/.oss-index-config or OSSI_USERNAME /
# OSSI_TOKEN); a 401 will now stop the build rather than pass silently.
nancy:
	@if command -v nancy >/dev/null 2>&1; then \
		$(GO) list -json -m all | nancy sleuth --quiet; \
	else \
		echo "nancy not installed, skipping (govulncheck and trivy cover this)"; \
	fi

# Generate SARIF reports for all security tools (for CI integration).
#
# Both tools previously ended in `|| true`, so this target reported success for
# a CRITICAL finding, an unparsable config and a missing binary alike — a CI job
# that gates on this target could never fail, exactly the vacuous-gate pattern
# `security` (gosec/vuln/trivy) was already fixed for.
#
# It now fails closed on findings while still WRITING the SARIF files, which is
# the whole point of the target: each tool's exit status is captured, the report
# is left on disk for the CI uploader, and the target exits non-zero afterwards.
# Trivy is scoped identically to the `trivy` gate so the two agree on what
# "clean" means.
.PHONY: security-sarif
security-sarif:
	@mkdir -p $(BUILD_DIR)
	@rc=0; \
	$(GOSEC) -exclude=$(GOSEC_EXCLUDE) -exclude-dir=test/performance -severity=medium -confidence=medium -fmt=sarif -out=$(BUILD_DIR)/gosec-results.sarif ./... || rc=1; \
	$(TRIVY) fs --severity CRITICAL,HIGH --exit-code 1 \
		--skip-dirs .build-cache --skip-dirs test/performance \
		--format sarif --output $(BUILD_DIR)/trivy-results.sarif . || rc=1; \
	echo "SARIF reports generated in $(BUILD_DIR)/"; \
	if [ $$rc -ne 0 ]; then \
		echo "security-sarif: findings reported (or a scanner failed); see the SARIF reports"; \
	fi; \
	exit $$rc

# ============================================================================
# Tool installation
# ============================================================================

.PHONY: install-gosec
install-gosec:
	$(GO) install github.com/securego/gosec/v2/cmd/gosec@latest

.PHONY: install-govulncheck
install-govulncheck:
	$(GO) install golang.org/x/vuln/cmd/govulncheck@latest

.PHONY: install-trivy
install-trivy:
	curl -sfL https://raw.githubusercontent.com/aquasecurity/trivy/main/contrib/install.sh | sh -s -- -b $$(go env GOPATH)/bin

.PHONY: install-nancy
install-nancy:
	$(GO) install github.com/sonatype-nexus-community/nancy@latest

# golangci-lint is installed at $(GOLANGCI_VERSION), not at a second hardcoded
# version. This recipe said v2.7.2 while GOLANGCI_VERSION and quality.yml said
# v2.12.2, so a developer who ran `make install-tools` and then `make lint`
# gated against a different rule set from CI while looking like they had passed
# the same gate. That is the exact failure remote-tools was hardened against —
# see the note above it: an analyser of the wrong version is worse than none.
# `make check-tool-versions` now enforces this mechanically.
#
# It installs golangci-lint by delegating to install-lint rather than fetching
# a release binary through install.sh. The published binaries are built by the
# upstream project's own Go, which is how a correctly pinned analyser came to
# be built with go1.26.2 and refused this 1.27 module (#118). `go install`
# builds it with the toolchain that is about to lint with it, so the two can
# never disagree.
.PHONY: install-tools
install-tools: install-gosec install-govulncheck install-trivy install-nancy install-lint
	@echo "All CI tools installed ($(GOLANGCI_VERSION))"

# Builds the pinned analyser WITH THIS REPO'S GO and puts it where `make lint`
# looks first, which is the whole point: `go install` compiles it against the
# toolchain running the install, so its language version is by construction at
# least the module's `go` directive. Installing into $(TOOLS_BIN) rather than
# GOPATH/bin keeps it out of reach of every other project's `go install`.
.PHONY: install-lint
install-lint:
	@echo "==> golangci-lint $(GOLANGCI_VERSION), built with $$($(GO) env GOVERSION), into $(TOOLS_BIN)"
	GOBIN=$(TOOLS_BIN) $(GO) install github.com/golangci/golangci-lint/v2/cmd/golangci-lint@$(GOLANGCI_VERSION)
	@$(TOOLS_BIN)/golangci-lint --version

# ============================================================================
# Docker
# ============================================================================

.PHONY: docker-build
docker-build:
	docker build -t $(DOCKER_IMAGE):$(DOCKER_TAG) -f $(DOCKER_DIR)/Dockerfile.dev .

.PHONY: docker-build-integration
docker-build-integration:
	docker build -t go-quicraft-integration:$(DOCKER_TAG) -f $(DOCKER_DIR)/Dockerfile .

.PHONY: docker-run
docker-run:
	docker compose -f $(COMPOSE_FILE) run --rm devcontainer

.PHONY: docker-test
docker-test:
	docker compose -f $(COMPOSE_FILE) run --rm devcontainer make test

.PHONY: docker-integration-down
docker-integration-down:
	docker compose -f $(INT_COMPOSE) down -v

# ============================================================================
# Help
# ============================================================================

.PHONY: help
help:
	@echo "go-quicraft Build System"
	@echo ""
	@echo "Primary:"
	@echo "  all                    Build, test, and lint (default)"
	@echo "  build                  Compile all packages"
	@echo "  clean                  Remove build artifacts"
	@echo ""
	@echo "Unit Tests:"
	@echo "  test                   Run all unit tests with race detector"
	@echo "  test-<pkg>             Run tests for a specific package"
	@echo "    packages: proto, config, queue, invariant, logdb,"
	@echo "              raft, waldb, seal, rsm, session, snapshot,"
	@echo "              registry, engine, transport, quicraft, errors, sm,"
	@echo "              server, writemode"
	@echo ""
	@echo "Integration Tests (run in devcontainer automatically):"
	@echo "  integration-test            Run all integration tests in devcontainer"
	@echo "  integration-test-raft       Run raft integration tests in devcontainer"
	@echo "  integration-test-host       Run Host API tests in devcontainer"
	@echo "  integration-test-e2e        Run E2E multi-node cluster tests (Docker Compose)"
	@echo "  integration-test-linearizability  Run porcupine linearizability tests in devcontainer"
	@echo ""
	@echo "Fuzz Testing:"
	@echo "  fuzz-all               Run all fuzz targets (FUZZ_TIME=60s)"
	@echo "  fuzz-proto             Fuzz proto codec round-trips"
	@echo "  fuzz-transport         Fuzz transport frame decoding and the message-frame receive path"
	@echo "  fuzz-waldb             Fuzz WAL record decoding"
	@echo "  fuzz-seal              Fuzz AES-GCM encrypt/decrypt"
	@echo "  fuzz-session           Fuzz session encoding"
	@echo "  fuzz-raft              Fuzz adversarial raft message sequences and log recovery"
	@echo "    Flags: FUZZ_TIME=60s"
	@echo "  ci-fuzz-seeds          Seed + committed-corpus pass only (deterministic, no mutation)"
	@echo ""
	@echo "ErrorFS I/O Error Injection:"
	@echo "  errfs-waldb            Verbose run of the injected-I/O-failure suite"
	@echo "  ci-errfs               Same suite, CI parity (no -v)"
	@echo ""
	@echo "Benchmarks (smoke test only - single run, noise-prone):"
	@echo "  bench-<pkg>            Run benchmarks for a specific package"
	@echo "    packages: proto, raft, waldb, seal, registry, engine, transport, quicraft,"
	@echo "              server, writemode"
	@echo ""
	@echo "Benchmark Regression Gate (see docs/performance/benchmarking.md):"
	@echo "  bench-compare          Interleaved baseline-vs-current comparison (benchstat)"
	@echo "  bench-baseline         Record the working tree as a stored baseline"
	@echo "  install-benchstat      Install benchstat (required for proper statistics)"
	@echo "    Flags: BASELINE=origin/main  PERF_HOST=user@host"
	@echo "           BENCH_PKGS='./pkg/internal/raft/ ...'  BENCH_FILTER=."
	@echo "           BENCH_ROUNDS=8  BENCH_ITERS=100000x  BENCH_TIMEOUT=30m"
	@echo "    PERF_HOST selects where the binaries run. Unset = this machine"
	@echo "    (supported, and what CI uses); set it to a quiet dedicated host"
	@echo "    for tighter confidence intervals. The method is the same either way."
	@echo ""
	@echo "Performance Comparison (via perfcompare orchestrator):"
	@echo "  perf-build                   Build the perfcompare orchestrator"
	@echo "  perf-quicraft                Run QuicRaft benchmarks"
	@echo "  perf-etcd                    Run etcd/raft benchmarks"
	@echo "  perf-dragonboat              Run Dragonboat benchmarks"
	@echo "  perf-openraft                Run OpenRaft (Rust) benchmarks"
	@echo "  perf-compare                 Compare systems (SYSTEMS=\"quicraft etcd\")"
	@echo "  perf-compare-all             Compare all systems"
	@echo "  (the four above store nothing and need no profile)"
	@echo ""
	@echo "Machine profiles (PERF_PROFILE is required; no default):"
	@echo "  perf-run                     Run into profiles/<name>/results"
	@echo "  perf-generate                Generate that profile's documents + charts"
	@echo "  perf-all                     perf-run then perf-generate"
	@echo "  perf-env                     Capture a machine's specs into the profile"
	@echo "  perf-remote                  Stage, capture, run, fetch and generate, on PERF_HOST"
	@echo "  perf-remote-status           Tail a remote run already in flight"
	@echo "    Flags: PERF_PROFILE=omega PERF_HOST=user@host PERF_SYSTEMS=all"
	@echo "           BENCHTIME=30s PERF_TIMEOUT=7200s PERF_EXTRA=--no-build"
	@echo ""
	@echo "Coverage (minimum $(COVER_MIN)%):"
	@echo "  coverage               Run total coverage report"
	@echo "  coverage-<pkg>         Run coverage for a specific package"
	@echo "    packages: proto, config, queue, invariant, logdb,"
	@echo "              raft, waldb, seal, rsm, session, snapshot,"
	@echo "              registry, engine, transport, quicraft, errors, sm,"
	@echo "              server, writemode"
	@echo ""
	@echo "CI Pipeline:"
	@echo "  ci                     Untagged pipeline (fmt→vet→lint→security→build→test)"
	@echo "  ci-full                ci + every build-tagged suite and every module."
	@echo "                         This is what .github/workflows/ci.yml runs."
	@echo "  ci-integration         Integration suite on the host (-tags=integration, -p 1)"
	@echo "  ci-errfs               ErrorFS fault injection (-tags=quicraft_errfs)"
	@echo "  ci-fuzz-seeds          Every fuzz target vs its seeds + committed corpus"
	@echo "  ci-modules             Build and vet every go.mod in the tree"
	@echo ""
	@echo "FIPS:"
	@echo "  build-fips             Compile check: GOFIPS140=v1.0.0, untagged and -tags fips"
	@echo "  verify-fips            Compliance check: run -tags fips under GODEBUG=fips140=only"
	@echo "                         (covers the QUIC transport; excludes only"
	@echo "                         pkg/internal/transport/revocation -- see the Makefile)"
	@echo "  verify-fips-exempt     That excluded tree, -tags fips under GODEBUG=fips140=on"
	@echo "                         (OCSP CertID SHA-1; the exemption is documented)"
	@echo ""
	@echo "Linting:"
	@echo "  lint                   Run golangci-lint (the one from install-lint, not PATH)"
	@echo "  check-tool-versions    Assert every golangci-lint pin matches GOLANGCI_VERSION"
	@echo "  check-lint-toolchain   Assert the installed analyser can analyse this module"
	@echo "  vet                    Run go vet"
	@echo "  fmt                    Check gofmt formatting"
	@echo ""
	@echo "Security (devops.md §8):"
	@echo "  security               Full suite: gosec, govulncheck, go mod verify, trivy fs"
	@echo "  gosec                  Run gosec security scanner"
	@echo "  vuln                   Run govulncheck vulnerability scanner"
	@echo "  modverify              Verify module integrity against go.sum"
	@echo "  deps-check             Fail if a direct dependency of ANY module is behind"
	@echo "                         its newest release (nightly; not in the push gate)"
	@echo "  trivy                  Run Trivy filesystem scan (CRITICAL,HIGH)"
	@echo "  trivy-image            Build and scan the local Docker images with Trivy"
	@echo "                         (not part of 'security' -- needs Docker; run it"
	@echo "                          when a Dockerfile or its base image changes)"
	@echo ""
	@echo "Tool Installation:"
	@echo "  install-tools          Install all CI tools (golangci-lint, gosec, govulncheck, trivy)"
	@echo "  install-lint           Build the pinned golangci-lint with this repo's Go"
	@echo "  install-gosec          Install gosec"
	@echo "  install-govulncheck    Install govulncheck"
	@echo "  install-trivy          Install Trivy"
	@echo ""
	@echo "Docker:"
	@echo "  docker-build                   Build devcontainer image"
	@echo "  docker-build-integration       Build integration test image"
	@echo "  docker-run                     Run devcontainer shell"
	@echo "  docker-test                    Run unit tests in devcontainer"
	@echo "  docker-integration-down        Stop and clean integration containers"

# Builds never resolve through a workspace: modules come from published tags.
# Immunizes against stray go.work files.
export GOWORK=off

# ---------------------------------------------------------------------------
# Release plumbing — software/templates/make/release-alpha.mk (template v2.3.3).
#
# The v1 recipe this replaces auto-incremented PATCH only (so an API addition
# shipped labelled as a bugfix, and a breaking pre-1.0 MINOR could not be cut at
# all -- v0.9.0-alpha had to be tagged by hand), created LIGHTWEIGHT UNSIGNED
# tags as the OWNER's local git identity, pushed EVERY local tag with --tags,
# and let `|| true` swallow a failed VERSION commit so a tag could disagree with
# its own VERSION file. None of it failed loudly (#42, #44).
#
# v2 contract: the version is EXPLICIT -- `make release-alpha VERSION=vX.Y.Z-alpha`
# -- because release-versioning.md decides patch-vs-minor and the recipe refuses
# to guess; every git write goes through agent-git.sh for identity and SSH
# signing; the tag is ANNOTATED; only the release commit and the one named tag
# are pushed, atomically; and there is no `|| true` anywhere, so any failing
# step aborts BEFORE the tag exists.
#
# NOTE, repo-specific: VERSION used to double as this Makefile's build-metadata
# variable, expanded at parse time into LDFLAGS. That made every settable
# `VERSION=` value a parse-time expansion -- a command-injection vector that no
# guard in this recipe could reach, because it fired before target selection.
# The build metadata now reads BUILD_VERSION (top of file) and VERSION belongs
# to this target alone. A bare `make release-alpha` therefore finds VERSION
# UNSET and stops on the first guard with the message that names the fix.
#
# SUBMODULE_TAGS is gone with the v1 recipe: it was empty in this repo, so the
# tag loop it drove was dead code, and the nested modules under contrib/ and
# test/ are not published for consumption.
# ---------------------------------------------------------------------------

# IDENTITY. The recipe's central claim is that every release git write goes
# through agent-git.sh as agent-go-quicraft, signed. As plain `?=` variables
# that claim was falsifiable from the command line: `make release-alpha
# VERSION=v0.9.1-alpha AGENT_GIT=/path/to/anything REPO_NAME=whatever` passed
# every guard -- the path only had to be executable and the name non-empty --
# and then that executable ran add/commit/tag/push, producing a release commit
# that could be unsigned or attributed to the wrong agent.
#
# The two are handled differently, because only one of them has a legitimate
# reason to vary.
#
# REPO_NAME is `override`: it IS this repository's name, no caller value could
# be correct, and it must never be $(notdir $(CURDIR)) -- deriving the signing
# identity from the checkout directory signs a release as the wrong agent from
# any worktree or CI workspace named something else.
#
# AGENT_GIT stays settable, because the company checkout is not at the same
# absolute path on every machine and a hardcoded one makes the target unusable
# elsewhere (and untestable anywhere without it). Its integrity is enforced by
# VERIFYING THE OUTCOME instead of trusting the tool: after the commit and tag
# are made, and BEFORE anything is pushed, the recipe checks that the release
# commit is AUTHORED by agent-<repo> and that the commit and the annotated tag
# both carry signatures that VERIFY against that agent's published key.
#
# Verification is real -- `git verify-commit` / `verify-tag` against
# .release-allowed-signers, an IN-REPO trust anchor. Two earlier attempts were
# both insufficient and are recorded so neither is reintroduced:
#
#   1. Grepping `cat-file` output for `gpgsig` / `SSH SIGNATURE`. Those are
#      marker strings; a substituted helper puts either in a commit MESSAGE and
#      sails past. Only asking git to check the cryptography answers it.
#   2. Building the allowed-signers file from a key found under AGENT_GIT's own
#      tree. That verifies nothing: the helper supplies the key it is checked
#      against, so it signs with its own and drops the matching public half
#      where the recipe looks. An anchor reachable from the thing it checks is
#      not an anchor.
#
# A substituted AGENT_GIT therefore fails here, with everything it produced
# still local and retractable. That is a stronger claim than any path check
# could make -- it holds whatever produced the objects.
AGENT_GIT      ?= /home/jhahn/sources/automatethethings/software/scripts/agent-git.sh
override REPO_NAME := go-quicraft

# RELEASE_SIGNERS is the TRUST ANCHOR, and it is `override` for the reason the
# whole check exists. Deriving the verification key from AGENT_GIT's own tree --
# the first attempt -- verified nothing: a substituted helper can ship its own
# keypair, sign the release with it, and drop the matching public key exactly
# where the recipe looked, and every pre-push check then passes against the
# attacker's key. An anchor reachable from the thing it is meant to check is not
# an anchor. This file is in-repo and settable only by a reviewed commit.
override RELEASE_SIGNERS := $(CURDIR)/.release-allowed-signers

# RELEASE_REMOTE is the git remote release-alpha pushes the release commit and
# tag to: the GITEA remote, named `dev` per conventions/version-control.md 1.
#
# This defaulted to `origin` until 2026-09-01, argued from a measurement of the
# checkout rather than from the convention. The checkout has since been renamed
# and the measurement expired, which is the general trap: a default justified by
# "what the machine currently looks like" needs re-checking whenever the machine
# changes, while a convention-based one does not (#63, reversing the position
# taken on #42).
#
# It stays a VARIABLE, not a constant, so a checkout that has not been renamed
# overrides it in one place. Its existence is asserted before it is trusted --
# an unreachable remote is not an absent tag -- so a wrong value here is a clear
# refusal before anything is written, never a half-completed release.
RELEASE_REMOTE ?= dev

# deps-latest runs before compiling in a consumer repo. It picks the highest tag
# INCLUDING -alpha prereleases -- bare 'go get @latest' ignores prereleases and
# would downgrade to legacy v0.0.0.

.PHONY: release-alpha deps-latest

# Every settable Make variable crosses into the recipe ONCE, via these
# target-specific exports, and the body references only the shell parameters
# ($$REL_*). Without that, the value would be expanded into double-quoted recipe
# TEXT before the grammar guard ran, so a backtick inside it would execute at the
# first guard line -- quoting is no boundary against a value that becomes recipe
# source.
#
# ALL FOUR use $(value ...), which the company template (v2.3.3) applies to
# VERSION alone. Measured on GNU Make 4.3: a plain $(RELEASE_REMOTE) EXECUTES a
# $(shell ...) embedded in a command-line value, while $(value RELEASE_REMOTE)
# does not -- so the three unwrapped reads were live make-function injection on
# a release path. None of the in-file defaults contains a `$`, so taking their
# raw text changes nothing for ordinary use.
# $(origin ...) restricts VERSION to the COMMAND LINE. `$(value VERSION)` alone
# also imports an ambient environment variable, so a bare `make release-alpha`
# in a shell or CI job that happens to export VERSION would sail past the
# explicit-version guard and cut whatever it inherited. An inherited value is
# not an operator stating a version, which is the whole contract here.
release-alpha: export REL_VERSION   := $(if $(filter command line,$(origin VERSION)),$(value VERSION),)
release-alpha: export REL_REMOTE    := $(value RELEASE_REMOTE)
release-alpha: export REL_REPO      := $(value REPO_NAME)
release-alpha: export REL_AGENT_GIT := $(value AGENT_GIT)
release-alpha:
	@test -n "$$REL_VERSION" || { echo 'ERROR: state the version explicitly: make release-alpha VERSION=vX.Y.Z-alpha (release-versioning.md decides patch/minor/major -- the recipe refuses to guess)'; exit 1; }
	@printf '%s' "$$REL_VERSION" | grep -Eq '^v(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)-alpha$$' || { echo "ERROR: VERSION is not of the form vX.Y.Z-alpha (no leading zeroes): $$REL_VERSION"; exit 1; }
	@test "$$(git rev-parse --abbrev-ref HEAD)" = main || { echo "ERROR: releases are cut from main; you are on $$(git rev-parse --abbrev-ref HEAD). 'push $$REL_REMOTE HEAD' would publish this branch."; exit 1; }
	@test -n "$$REL_REPO" || { echo 'ERROR: REPO_NAME is empty -- set it to the literal repo name in the Makefile, never $$(notdir $$(CURDIR))'; exit 1; }
	@test -x "$$REL_AGENT_GIT" || { echo 'ERROR: AGENT_GIT does not point at an executable agent-git.sh -- every git write in this recipe signs through it, and a bad path must fail before VERSION is written'; exit 1; }
	@test -n "$$REL_REMOTE" || { echo 'ERROR: RELEASE_REMOTE is empty -- set it to the Gitea remote'; exit 1; }
	@git remote get-url "$$REL_REMOTE" >/dev/null 2>&1 || { echo "ERROR: no git remote named $$REL_REMOTE. Set RELEASE_REMOTE to the Gitea remote; the duplicate-tag check below cannot answer against a remote that does not resolve"; exit 1; }
	@! git rev-parse -q --verify "refs/tags/$$REL_VERSION" >/dev/null || { echo "ERROR: tag $$REL_VERSION already exists locally"; exit 1; }
	@rc=0; git ls-remote --exit-code --tags "$$REL_REMOTE" "refs/tags/$$REL_VERSION" >/dev/null 2>&1 || rc=$$?; \
	case "$$rc" in \
	  0) echo "ERROR: tag $$REL_VERSION already exists on $$REL_REMOTE (a local check alone would miss this and fail the push AFTER committing VERSION)"; exit 1 ;; \
	  2) : ;; \
	  *) echo "ERROR: could not reach $$REL_REMOTE to check whether tag $$REL_VERSION exists. Refusing to continue: an unreachable remote is not an absent tag"; exit 1 ;; \
	esac
	@test -z "$$(git status --porcelain)" || { echo 'ERROR: working tree not clean'; exit 1; }
	@ver="## [$${REL_VERSION#v}]"; awk -v ver="$$ver" 'index($$0, ver) == 1 { f = 1 } END { exit f ? 0 : 1 }' CHANGELOG.md || { echo "ERROR: CHANGELOG.md has no \"$$ver\" section heading. Matched LITERALLY and anchored at the line start, at least as strictly as .github/workflows/release.yml matches it -- an unanchored prose mention does not count, and neither does a near-miss heading"; exit 1; }
	@printf '%s\n' "$${REL_VERSION#v}" > VERSION
	"$$REL_AGENT_GIT" "$$REL_REPO" add VERSION
	"$$REL_AGENT_GIT" "$$REL_REPO" commit -m "chore(release): $$REL_VERSION"
	"$$REL_AGENT_GIT" "$$REL_REPO" tag -a "$$REL_VERSION" -m "$$REL_VERSION"
	@want="agent-$$REL_REPO"; got="$$(git log -1 --format='%an')"; \
	test "$$got" = "$$want" || { echo "ERROR: the release commit is authored by '$$got', not '$$want'. AGENT_GIT did not write as this repo's agent; nothing has been pushed. Reset with: git reset --hard HEAD~1 && git tag -d $$REL_VERSION"; exit 1; }
	@test "$$(git cat-file -t "$$REL_VERSION")" = tag || { echo "ERROR: $$REL_VERSION is a lightweight tag; releases are annotated and signed. Nothing has been pushed."; exit 1; }
	@test -f "$(RELEASE_SIGNERS)" || { echo 'ERROR: $(RELEASE_SIGNERS) is missing. It is the trust anchor for release signature verification; without it a signature can only be observed, not TRUSTED. Nothing has been pushed.'; exit 1; }
	@rc=0; \
	git -c gpg.ssh.allowedSignersFile="$(RELEASE_SIGNERS)" verify-commit HEAD >/dev/null 2>&1 || { echo "ERROR: the release commit's signature does not verify against the key pinned in $(RELEASE_SIGNERS). Nothing has been pushed. Reset with: git reset --hard HEAD~1 && git tag -d $$REL_VERSION"; rc=1; }; \
	git -c gpg.ssh.allowedSignersFile="$(RELEASE_SIGNERS)" verify-tag "$$REL_VERSION" >/dev/null 2>&1 || { echo "ERROR: tag $$REL_VERSION's signature does not verify against the key pinned in $(RELEASE_SIGNERS). Nothing has been pushed. Reset with: git reset --hard HEAD~1 && git tag -d $$REL_VERSION"; rc=1; }; \
	exit $$rc
	"$$REL_AGENT_GIT" "$$REL_REPO" push --atomic "$$REL_REMOTE" HEAD "refs/tags/$$REL_VERSION"
	@echo "Released $$REL_VERSION (signed, annotated, single tag pushed)"

deps-latest:
	@GOWORK=off sh -c ' \
	SELF=$$(go list -m); \
	for m in $$(go list -m -f "{{if not .Indirect}}{{.Path}}{{end}}" all 2>/dev/null | grep "^github.com/jeremyhahn/" | grep -vx "$$SELF"); do \
	  case "$$(go list -m -f "{{if .Replace}}{{.Replace.Path}}{{end}}" "$$m" 2>/dev/null)" in .*|/*) continue;; esac; \
	  V=$$(go list -m -versions "$$m" 2>/dev/null | tr " " "\n" | tail -1); \
	  if [ -n "$$V" ] && [ "$$V" != "$$m" ]; then go get "$$m@$$V" || exit 1; fi; \
	done; go mod tidy'
