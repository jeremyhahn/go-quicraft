# go-quicraft Build System
# Multi-group Raft library

.DEFAULT_GOAL := build

# Project metadata
MODULE        := github.com/jeremyhahn/go-quicraft
BINARY        := go-quicraft
BUILD_DIR     := build
DOCKER_DIR    := .devcontainer
COVER_DIR     := $(BUILD_DIR)/coverage

# Version injection
VERSION       := $(shell cat VERSION 2>/dev/null || echo "0.0.1-alpha")
GIT_COMMIT    := $(shell git rev-parse --short HEAD 2>/dev/null || echo "unknown")
BUILD_DATE    := $(shell date -u +"%Y-%m-%dT%H:%M:%SZ")
LDFLAGS       := -X main.version=$(VERSION) -X main.gitCommit=$(GIT_COMMIT) -X main.buildDate=$(BUILD_DATE)

# Go tooling
export GOTOOLCHAIN := auto
GO            := go
GOTEST        := $(GO) test -race -count=1 -v
GOBENCH       := $(GO) test -run=^$$ -bench=. -benchmem
GOVET         := $(GO) vet
GOFMT         := gofmt
GOLINT        := golangci-lint
# Must match the version quality.yml installs. A different MAJOR version is not
# a cosmetic difference: v2 moved flags (`--timeout`) and changed which linters
# are enabled by default, so a developer and CI would gate on different rules.
GOLANGCI_VERSION := v2.12.2
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
PKG_ENCLAVE     := ./pkg/enclave/...
PKG_CRYPTO      := ./pkg/crypto/...

# Integration test paths
INT_TEST_DIR    := ./test/integration

# Performance comparison paths
PERF_DIR        := ./test/performance
PERF_DRAGONBOAT := $(PERF_DIR)/dragonboat
PERF_ETCD       := $(PERF_DIR)/etcd
PERF_RESULTS    := ./results

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
	rm -rf test/performance/profiles
	rm -rf test/performance/results/results test/performance/charts/charts
	rm -f test/performance/perfcompare test/performance/cmd/perfcompare/perfcompare
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

.PHONY: test-enclave
test-enclave:
	$(GOTEST) $(PKG_ENCLAVE)

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
SYSTEMS ?= all
BENCHTIME ?= 30s
PERF_TIMEOUT ?= 3600s
PERF_EXTRA ?=

# Internal: common perfcompare flags.
PERF_FLAGS = --benchtime=$(BENCHTIME) --timeout=$(PERF_TIMEOUT) $(PERF_EXTRA)

.PHONY: perf-build
perf-build:
	cd $(PERF_DIR) && $(GO) build -o perfcompare ./cmd/perfcompare/

.PHONY: perf-quicraft
perf-quicraft: perf-build
	cd $(PERF_DIR) && ./perfcompare $(PERF_FLAGS) quicraft

.PHONY: perf-etcd
perf-etcd: perf-build
	cd $(PERF_DIR) && ./perfcompare $(PERF_FLAGS) etcd

.PHONY: perf-dragonboat
perf-dragonboat: perf-build
	cd $(PERF_DIR) && ./perfcompare $(PERF_FLAGS) dragonboat

.PHONY: perf-openraft
perf-openraft: perf-build
	cd $(PERF_DIR) && ./perfcompare $(PERF_FLAGS) openraft

.PHONY: perf-compare
perf-compare: perf-build
	cd $(PERF_DIR) && ./perfcompare $(PERF_FLAGS) $(SYSTEMS)

.PHONY: perf-compare-all
perf-compare-all: perf-build
	cd $(PERF_DIR) && ./perfcompare $(PERF_FLAGS) all

.PHONY: perf-compare-all-visualize
perf-compare-all-visualize: perf-build
	cd $(PERF_DIR) && ./perfcompare $(PERF_FLAGS) --visualize --chart-dir test/performance/charts/ all

# Compare QuicRaft against a specific system (runs both sequentially, then shows side-by-side)
.PHONY: perf-compare-etcd
perf-compare-etcd: perf-build
	cd $(PERF_DIR) && ./perfcompare $(PERF_FLAGS) --visualize --chart-dir test/performance/charts/etcd/ quicraft etcd

.PHONY: perf-compare-dragonboat
perf-compare-dragonboat: perf-build
	cd $(PERF_DIR) && ./perfcompare $(PERF_FLAGS) --visualize --chart-dir test/performance/charts/dragonboat/ quicraft dragonboat

.PHONY: perf-compare-openraft
perf-compare-openraft: perf-build
	cd $(PERF_DIR) && ./perfcompare $(PERF_FLAGS) --visualize --chart-dir test/performance/charts/openraft/ quicraft openraft

# Run all systems, generate charts, and render markdown docs from templates.
# Runs each system exactly once, saves JSON results, generates pairwise SVG charts,
# then renders markdown templates (README.md, etcd.md, dragonboat.md, openraft.md).
# Use BENCHTIME=1s for a quick run, or PERF_EXTRA=--no-build to skip Docker rebuilds.
# Example: make perf-docs BENCHTIME=1s PERF_EXTRA=--no-build
.PHONY: perf-docs
perf-docs: perf-build
	cd $(PERF_DIR) && ./perfcompare $(PERF_FLAGS) --visualize --pairwise-charts --chart-dir=charts/ --json-dir=$(PERF_RESULTS) all
	cd $(PERF_DIR) && ./perfcompare generate --results-dir=$(PERF_RESULTS) --output-dir=. --chart-dir=charts/

# Run-once pipeline: run each system ONCE, save JSON, then generate all docs.
# Phase 1: Run individual systems and save JSON results.
.PHONY: perf-run-quicraft
perf-run-quicraft: perf-build
	cd $(PERF_DIR) && ./perfcompare $(PERF_FLAGS) --json-dir=$(PERF_RESULTS) quicraft

.PHONY: perf-run-etcd
perf-run-etcd: perf-build
	cd $(PERF_DIR) && ./perfcompare $(PERF_FLAGS) --json-dir=$(PERF_RESULTS) etcd

.PHONY: perf-run-dragonboat
perf-run-dragonboat: perf-build
	cd $(PERF_DIR) && ./perfcompare $(PERF_FLAGS) --json-dir=$(PERF_RESULTS) dragonboat

.PHONY: perf-run-openraft
perf-run-openraft: perf-build
	cd $(PERF_DIR) && ./perfcompare $(PERF_FLAGS) --json-dir=$(PERF_RESULTS) openraft

.PHONY: perf-run-all
perf-run-all: perf-build
	cd $(PERF_DIR) && ./perfcompare $(PERF_FLAGS) --json-dir=$(PERF_RESULTS) all

# Phase 2: Generate all markdown docs + SVG charts from stored JSON results.
.PHONY: perf-generate
perf-generate: perf-build
	cd $(PERF_DIR) && ./perfcompare generate --results-dir=$(PERF_RESULTS) --output-dir=. --chart-dir=charts/

# Full pipeline: run all systems once, then generate all docs.
.PHONY: perf-all
perf-all: perf-run-all perf-generate

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

.PHONY: coverage-enclave
coverage-enclave: | $(COVER_DIR)
	$(call run-coverage,enclave,$(PKG_ENCLAVE))

.PHONY: coverage-crypto
coverage-crypto: | $(COVER_DIR)
	$(call run-coverage,crypto,$(PKG_CRYPTO))

$(COVER_DIR):
	mkdir -p $(COVER_DIR)

# ============================================================================
# FIPS build
# ============================================================================

.PHONY: build-fips
build-fips:
	GOFIPS140=v1.0.0 CGO_ENABLED=0 $(GO) build -ldflags="$(LDFLAGS)" ./...

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
ci: fmt vet lint security build test
	@echo "CI pipeline passed"

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
ci-full: ci ci-modules ci-errfs ci-fuzz-seeds ci-integration ci-netfault
	@echo "Full CI pipeline passed (untagged + integration + netfault + errfs + fuzz seeds + all modules)"

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
		for hk in "-p 2222 198.51.100.91" "github.com"; do \
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

.PHONY: lint
lint:
	$(GOLINT) run --timeout=5m ./...

.PHONY: vet
vet:
	$(GOVET) ./...

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

.PHONY: install-tools
install-tools: install-gosec install-govulncheck install-trivy install-nancy
	curl -sSfL https://raw.githubusercontent.com/golangci/golangci-lint/master/install.sh | sh -s -- -b $$(go env GOPATH)/bin v2.7.2
	@echo "All CI tools installed"

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
	@echo "  perf-compare-etcd            Compare QuicRaft vs etcd + generate charts"
	@echo "  perf-compare-dragonboat      Compare QuicRaft vs Dragonboat + generate charts"
	@echo "  perf-compare-openraft        Compare QuicRaft vs OpenRaft + generate charts"
	@echo "  perf-compare-all             Compare all systems"
	@echo "  perf-compare-all-visualize   Compare all + generate SVG charts"
	@echo "  perf-docs                    Run all systems once + generate pairwise charts"
	@echo "    Flags: BENCHTIME=1s PERF_EXTRA=--no-build"
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
	@echo "  build-fips             Build with GOFIPS140=v1.0.0 (PBKDF2-SHA256 KDF)"
	@echo ""
	@echo "Linting:"
	@echo "  lint                   Run golangci-lint"
	@echo "  vet                    Run go vet"
	@echo "  fmt                    Check gofmt formatting"
	@echo ""
	@echo "Security (devops.md §8):"
	@echo "  security               Full suite: gosec, govulncheck, go mod verify, trivy fs"
	@echo "  gosec                  Run gosec security scanner"
	@echo "  vuln                   Run govulncheck vulnerability scanner"
	@echo "  modverify              Verify module integrity against go.sum"
	@echo "  trivy                  Run Trivy filesystem scan (CRITICAL,HIGH)"
	@echo "  trivy-image            Build and scan the local Docker images with Trivy"
	@echo "                         (not part of 'security' -- needs Docker; run it"
	@echo "                          when a Dockerfile or its base image changes)"
	@echo ""
	@echo "Tool Installation:"
	@echo "  install-tools          Install all CI tools (golangci-lint, gosec, govulncheck, trivy)"
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

SUBMODULE_TAGS :=
# Builds never resolve through a workspace: modules come from published tags.
# Immunizes against stray go.work files.
export GOWORK=off

# RELEASE_REMOTE is the git remote release-alpha pushes tags to. Defaults to
# 'origin'; override for a mirror or a private staging remote:
#   make release-alpha RELEASE_REMOTE=staging
RELEASE_REMOTE ?= origin

# SUBMODULE_TAGS lists nested modules tagged in lockstep with the root.
# deps-latest runs before compiling in a consumer repo; release-alpha publishes
# this repo's next -alpha tag. deps-latest picks the highest tag INCLUDING
# -alpha prereleases — bare 'go get @latest' ignores prereleases and would
# downgrade to legacy v0.0.0.

.PHONY: release-alpha deps-latest

release-alpha:
	@V=$$(git describe --tags --abbrev=0 --match 'v*' 2>/dev/null || echo v0.0.0-alpha); \
	NEXT=$$(echo $$V | awk -F'[.-]' '{printf "v%s.%s.%d-alpha", substr($$1,2), $$2, $$3+1}'); \
	echo $${NEXT#v} > VERSION && git add VERSION && git commit -q -m "chore(release): $$NEXT" || true; \
	git tag $$NEXT; \
	for s in $(SUBMODULE_TAGS); do git tag "$$s/$$NEXT"; done; \
	git push $(RELEASE_REMOTE) HEAD --tags && echo "Tagged $$NEXT"

deps-latest:
	@GOWORK=off sh -c ' \
	SELF=$$(go list -m); \
	for m in $$(go list -m -f "{{if not .Indirect}}{{.Path}}{{end}}" all 2>/dev/null | grep "^github.com/jeremyhahn/" | grep -vx "$$SELF"); do \
	  case "$$(go list -m -f "{{if .Replace}}{{.Replace.Path}}{{end}}" "$$m" 2>/dev/null)" in .*|/*) continue;; esac; \
	  V=$$(go list -m -versions "$$m" 2>/dev/null | tr " " "\n" | tail -1); \
	  if [ -n "$$V" ] && [ "$$V" != "$$m" ]; then go get "$$m@$$V" || exit 1; fi; \
	done; go mod tidy'
