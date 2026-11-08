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
# The build reads BUILD_VERSION, not VERSION: VERSION is the release target's
# argument, and a variable read in a `:=` assignment is expanded at parse time,
# so a command-line value such as VERSION='$(shell ...)' would execute before
# any target runs. These values are derived (from a file, git and the clock) and
# are `override` so a command-line value is discarded. Rule: no variable
# expanded at parse time may carry a caller-supplied value. Tool knobs such as
# GO and BUILD_DIR stay settable.
#
# GNU make 4.4 and later expands every EXPORTED variable to build the
# environment of each $(shell ...) call, and a variable given on the command
# line is exported by default. So VERSION='$(shell ...)' would execute at parse
# time during the first $(shell) below, `override` or not. The caller-supplied
# release inputs are therefore unexported: they reach the release recipe only
# through the REL_* target-specific exports, via $(value ...).
#
# `unexport` creates the variable if it is not defined yet, and `?=` leaves a
# defined variable alone, so the RELEASE_* defaults are set first (see the
# Release section below for what they mean).
RELEASE_GIT             ?= git
RELEASE_ALLOWED_SIGNERS ?= .release-allowed-signers
RELEASE_REMOTE          ?= dev
unexport VERSION RELEASE_REMOTE RELEASE_GIT RELEASE_ALLOWED_SIGNERS
unexport BUILD_VERSION GIT_COMMIT BUILD_DATE
override BUILD_VERSION := $(shell cat VERSION 2>/dev/null || echo "0.0.1-alpha")
override GIT_COMMIT    := $(shell git rev-parse --short HEAD 2>/dev/null || echo "unknown")
override BUILD_DATE    := $(shell date -u +"%Y-%m-%dT%H:%M:%SZ")
LDFLAGS       := -X main.version=$(BUILD_VERSION) -X main.gitCommit=$(GIT_COMMIT) -X main.buildDate=$(BUILD_DATE)

# Go tooling
export GOTOOLCHAIN := auto
GO            := go
GOTEST        := $(GO) test -race -count=1 -v
# The repo's build-tag set, space separated, as CI passes it to lint, build,
# coverage, security and integration-test (handbook devcontainer.md §6). Empty:
# no package of the default build is tag-gated; each tagged suite has its own
# ci-* target. GO_TAGS_FLAG is the go tool's form, empty when the set is.
GO_BUILD_TAGS ?=
space         := $(subst ,, )
GO_TAGS_CSV    = $(subst $(space),$(comma),$(strip $(GO_BUILD_TAGS)))
GO_TAGS_FLAG   = $(if $(strip $(GO_BUILD_TAGS)),-tags=$(GO_TAGS_CSV))

# GOFIPS140=v1.0.0 unpacks the frozen FIPS module into the module cache, which
# the toolchain image bakes read-only with GOPROXY=off (devcontainer.md §4).
# Where the cache exists and is not writable, FIPS_GOENV serves it as a file
# proxy into a writable one outside the module (./... would walk it), so the
# snapshot builds offline; elsewhere it is empty and the go tool uses the cache
# as is. A missing cache is not a read-only one: it downloads as usual (#492).
GOMODCACHE_DIR := $(shell $(GO) env GOMODCACHE 2>/dev/null)
FIPS_MODCACHE  ?= $(or $(TMPDIR),/tmp)/go-quicraft-fips-modcache
FIPS_GOENV     := $(if $(shell test -d '$(GOMODCACHE_DIR)' && ! test -w '$(GOMODCACHE_DIR)' && echo ro),GOMODCACHE='$(FIPS_MODCACHE)' GOPROXY='file://$(GOMODCACHE_DIR)/cache/download' GOPRIVATE= GONOPROXY= GOFLAGS='-mod=readonly -modcacherw')
GOBENCH       := $(GO) test -run=^$$ -bench=. -benchmem
GOVET         := $(GO) vet
GOFMT         := gofmt
# The analyser's name on PATH. `make lint` runs GOLANGCI_BIN, which prefers the
# one `make install-lint` builds.
GOLINT        := golangci-lint
# Every pinned tool version: GOLANGCI_VERSION, ACTIONLINT_VERSION,
# TRIVY_VERSION and the rest. One file, also read by the devcontainer image
# (handbook devcontainer.md §2). Every literal golangci-lint pin in the CI
# workflows and devcontainer Dockerfiles must match GOLANGCI_VERSION
# (check-tool-versions enforces this).
TOOL_PINS     := scripts/tool-pins.sh
include $(TOOL_PINS)
# Where `make install-lint` puts the analyser and the first place `make lint`
# looks. Repo-local so another project's `go install` cannot replace it.
TOOLS_BIN     := $(CURDIR)/.tools/bin
# The analyser `make lint` runs: the repo-local build first, then PATH.
# Recursive (`=`) so a missing tool is not a parse-time failure for every target.
GOLANGCI_BIN   = $(shell if [ -x '$(TOOLS_BIN)/golangci-lint' ]; then \
			echo '$(TOOLS_BIN)/golangci-lint'; \
		else command -v $(GOLINT) 2>/dev/null; fi)
# The workflow linter `make lint` runs over .github/workflows, at
# ACTIONLINT_VERSION. `make install-actionlint` builds it into TOOLS_BIN.
# The repo-local build first, then PATH; recursive for the same reason as
# GOLANGCI_BIN.
ACTIONLINT_BIN = $(shell if [ -x '$(TOOLS_BIN)/actionlint' ]; then \
			echo '$(TOOLS_BIN)/actionlint'; \
		else command -v actionlint 2>/dev/null; fi)
# The module's `go` directive, derived from go.mod (hence `override`). The
# analyser's build toolchain is checked against it.
override GO_DIRECTIVE := $(shell awk '$$1=="go"{print $$2; exit}' go.mod 2>/dev/null)
# Every Go module in the tree: each directory holding a go.mod, skipping what
# the go command ignores (names starting with `.` or `_`, and testdata).
# `git ls-files` is not used because remote-% ships the tree without .git.
# The same discovery as the CI jobs that loop over modules. Recursive (`=`) so
# only the targets that use it pay for the walk.
MODULE_DIRS    = $(shell find . -mindepth 1 -type d \( -name '.*' -o -name '_*' -o -name testdata \) -prune \
			-o -name go.mod -printf '%h\n' | sort)
# The platforms the cross-arch CI job builds and vets every module for.
CROSS_PLATFORMS := linux/arm64 linux/386 windows/amd64 darwin/arm64 freebsd/amd64
GOSEC         := gosec
GOVULNCHECK   := govulncheck

# gosec exclusions. Keep identical to the -exclude flag in
# .github/workflows/ci.yml.
#   G103: unsafe calls; the proto codec is a fixed-layout zero-copy encoding
#   G104: unhandled errors; false positive on hash.Write, which returns none
#   G115: integer overflow on conversion; flagged conversions follow a bounds
#         check (MaxFrameSize / MaxReplicationPayloadBytes). This exclusion
#         hides new unbounded narrowing conversions, so review those by hand
#   G301: directory permissions; 0755 is intended for data dirs
#   G304: file path via variable; snapshot/WAL paths are built internally from
#         (shard, replica, index), never from a request
#   G306: file write permissions; 0644 for data files, secrets are written 0600
#   G404: weak random; the only math/rand use is election jitter, seeded from
#         crypto/rand, and is not a security primitive
GOSEC_EXCLUDE := G103,G104,G115,G301,G304,G306,G404

# `make install-trivy` and `make remote-tools` install TRIVY_VERSION into
# TOOLS_BIN; the toolchain image carries the same release.
# The scanner every trivy target runs: the repo-local install first, then PATH.
# Recursive for the same reason as GOLANGCI_BIN; empty when there is none, which
# check-trivy turns into a failure.
TRIVY = $(shell if [ -x '$(TOOLS_BIN)/trivy' ]; then \
			echo '$(TOOLS_BIN)/trivy'; \
		else command -v trivy 2>/dev/null; fi)

# Coverage floor enforced by every coverage target. The flags live in the
# run-coverage recipe below.
COVERAGE_THRESHOLD ?= 91
COVER_MIN     := $(COVERAGE_THRESHOLD)

# Docker
DOCKER_IMAGE  := go-quicraft-dev
DOCKER_INT_IMAGE := go-quicraft-integration
# The docker-* images are tagged per checkout (host and directory), so two
# checkouts sharing a daemon -- including a remote one reached through
# DOCKER_HOST -- never build over each other's images. Derived, hence override.
override DOCKER_TAG := checkout-$(shell printf '%s:%s' "$$(uname -n)" "$$(pwd)" | cksum | cut -d' ' -f1)
# The one file that holds module credentials for the project forge (format:
# .devcontainer/.env.example). Gitignored and excluded from every build context.
MODULE_ENV_FILE := $(CURDIR)/$(DOCKER_DIR)/.env
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
# Snapshot code has no package of its own: it is spread across the root
# package, the engine, the transport, rsm, raft, proto and the log databases.
# test-snapshot therefore selects by test name across all of them.
PKG_SNAPSHOT    := ./pkg/...
RUN_SNAPSHOT    := Snapshot|Import|Export
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
	$(GO) build $(GO_TAGS_FLAG) ./...
	@# The FIPS variant, so CI's build stage compiles both (ci-template v1.21).
	$(FIPS_GOENV) GOFIPS140=v1.0.0 CGO_ENABLED=0 $(GO) build $(GO_TAGS_FLAG) ./...

.PHONY: clean
clean:
	$(GO) clean ./...
	rm -rf $(COVER_DIR) $(BUILD_DIR)/quicraft *.log *.test *.out *.prof .build-cache
	rm -rf test/performance/openraft/target
	@# test/performance/profiles/ holds stored results and is not build output;
	@# clean leaves it alone.
	rm -f test/performance/perfcompare test/performance/perfcompare.remote
	rm -f test/performance/cmd/perfcompare/perfcompare
	rm -f test/performance/quicraft/*.prof test/performance/quicraft/*.test
	rm -f contrib/dragonboat/*.test

# ============================================================================
# Unit tests
# ============================================================================

# run-tests: run one test-<pkg> selection, failing when it selects no tests.
# Usage: $(call run-tests,<package pattern>[,<-run regexp>])
#
# `go test` passes when a -run regexp matches nothing, so the selection is
# listed first (same -race build, reused from cache) and an empty list fails.
define run-tests
	@listing=$$($(GO) test -race -list='$(or $(2),.)' $(1)) || { printf '%s\n' "$$listing"; exit 1; }; \
	if ! printf '%s\n' "$$listing" | grep -qE '^(Test|Example)'; then \
		echo "FAIL: $(1)$(if $(2), -run '$(2)') selects no tests"; exit 1; \
	fi
	$(GOTEST)$(if $(2), -run '$(2)') $(1)
endef

.PHONY: test
test:
	$(GOTEST) ./...

.PHONY: test-proto
test-proto:
	$(call run-tests,$(PKG_PROTO))

.PHONY: test-config
test-config:
	$(call run-tests,$(PKG_CONFIG))

.PHONY: test-queue
test-queue:
	$(call run-tests,$(PKG_QUEUE))

.PHONY: test-invariant
test-invariant:
	$(call run-tests,$(PKG_INVARIANT))

.PHONY: test-logdb
test-logdb:
	$(call run-tests,$(PKG_LOGDB))

.PHONY: test-raft
test-raft:
	$(call run-tests,$(PKG_RAFT))

.PHONY: test-waldb
test-waldb:
	$(call run-tests,$(PKG_WALDB))

.PHONY: test-seal
test-seal:
	$(call run-tests,$(PKG_SEAL))

.PHONY: test-rsm
test-rsm:
	$(call run-tests,$(PKG_RSM))

.PHONY: test-session
test-session:
	$(call run-tests,$(PKG_SESSION))

.PHONY: test-snapshot
test-snapshot:
	$(call run-tests,$(PKG_SNAPSHOT),$(RUN_SNAPSHOT))

.PHONY: test-engine
test-engine:
	$(call run-tests,$(PKG_ENGINE))

.PHONY: test-registry
test-registry:
	$(call run-tests,$(PKG_REGISTRY))

.PHONY: test-transport
test-transport:
	$(call run-tests,$(PKG_TRANSPORT))

.PHONY: test-quicraft
test-quicraft:
	$(call run-tests,$(PKG_QUICRAFT))

.PHONY: test-errors
test-errors:
	$(call run-tests,$(PKG_ERRORS))

.PHONY: test-sm
test-sm:
	$(call run-tests,$(PKG_SM))

.PHONY: test-server
test-server:
	$(call run-tests,$(PKG_SERVER))

.PHONY: test-writemode
test-writemode:
	$(call run-tests,$(PKG_WRITEMODE))

.PHONY: test-batch
test-batch:
	$(call run-tests,$(PKG_BATCH))

.PHONY: test-crypto
test-crypto:
	$(call run-tests,$(PKG_CRYPTO))

# ============================================================================
# Integration tests (always run in devcontainer)
# ============================================================================

# Host targets: build the devcontainer image quietly, then run the tests in it.

# The CI stage: IT_CMD in the devcontainer integration image, under the
# hardened compose service (read-only, no network, no capabilities), in its
# own compose project, torn down with its volumes however the run ends.
.PHONY: integration-test
integration-test: docker-build-it
	@echo "==> Running integration tests in $(IT_IMAGE) (project $(IT_PROJECT))..."
	@status=0; \
	GO_QUICRAFT_IT_IMAGE='$(IT_IMAGE)' docker compose -f $(DEVCONTAINER_COMPOSE) -p '$(IT_PROJECT)' \
		run --rm integration $(IT_CMD) || status=$$?; \
	GO_QUICRAFT_IT_IMAGE='$(IT_IMAGE)' docker compose -f $(DEVCONTAINER_COMPOSE) -p '$(IT_PROJECT)' \
		down --volumes >/dev/null 2>&1 || true; \
	exit $$status

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

# Internal targets, run inside the devcontainer.

# -p 1 serializes packages: concurrent multi-node QUIC clusters under -race
# starve the CPU and make the 30s snapshot catch-up waits flake.
.PHONY: _integration-test
_integration-test:
	# 900s: -p 1 shares one deadline and linearizability alone takes ~305s.
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

# The integration command on the host: the IT_CMD that `make integration-test`
# runs in the image for the `integration` job in .github/workflows/ci.yml, and
# the `integration` gate in release.yml. Unlike _integration-test: no -v (large
# logs), and 1800s because shared runners are slower and -p 1 shares one
# deadline.
.PHONY: ci-integration
ci-integration:
	$(GO) test -race -count=1 -tags=integration -timeout=1800s -p 1 $(INT_TEST_DIR)/...

# Network partitions between live replicas. Needs the quicraft_netfault tag,
# which compiles the transport's send-path drop hook in. Every test asserts
# netfault.BlockedCount() is non-zero, so a missing tag fails loudly. -p 1
# because the injector state is process-global.
#
# NETFAULT_PKGS lists every package with a quicraft_netfault test. Packages run
# whole (no -run filter); the recipe fails if a tagged package is missing here.
NETFAULT_PKGS := $(INT_TEST_DIR)/e2e/... $(INT_TEST_DIR)/host/...

.PHONY: ci-netfault
ci-netfault:
	@missing=""; \
	for d in $$(grep -rl --include='*_test.go' '^//go:build.*quicraft_netfault' $(INT_TEST_DIR) | xargs -r -n1 dirname | sort -u); do \
		case " $(NETFAULT_PKGS) " in *" $$d/... "*) ;; *) missing="$$missing $$d" ;; esac; \
	done; \
	if [ -n "$$missing" ]; then \
		echo "FAIL: quicraft_netfault tests in packages missing from NETFAULT_PKGS:$$missing"; exit 1; \
	fi
	$(GO) test -race -count=1 -tags='integration quicraft_netfault' -timeout=1800s -p 1 \
		$(NETFAULT_PKGS)

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
# The bench-<pkg> targets above are single-run smoke tests and cannot detect a
# regression. bench-compare builds baseline and current test binaries with the
# same toolchain, interleaves them over many rounds at a fixed iteration count,
# and compares the distributions with benchstat. Binaries run locally, or on
# PERF_HOST when set. See docs/testing/benchmarking.md.
# ----------------------------------------------------------------------------

BENCH_SCRIPT   := scripts/bench-compare.sh

# Git ref to compare the working tree against.
BASELINE       ?= origin/main
# Where the benchmark binaries run: unset for this machine, or an ssh
# destination (user@host) for a quiet dedicated host.
PERF_HOST      ?=
# Hot path: append (raft log), replicate (proto codec), apply (engine).
BENCH_PKGS     ?= ./pkg/internal/raft/ ./pkg/internal/engine/ ./pkg/proto/
BENCH_FILTER   ?= .
BENCH_ROUNDS   ?= 8
BENCH_ITERS    ?= 100000x
BENCH_TIMEOUT  ?= 30m

# $(value ...) passes these through unexpanded, so a filter such as
# 'BenchmarkA$|BenchmarkB$' keeps its '$|' instead of make expanding it.
BENCH_ENV = BASELINE='$(value BASELINE)' PERF_HOST='$(value PERF_HOST)' \
            BENCH_PKGS='$(value BENCH_PKGS)' BENCH_FILTER='$(value BENCH_FILTER)' \
            BENCH_ROUNDS='$(value BENCH_ROUNDS)' BENCH_ITERS='$(value BENCH_ITERS)' \
            BENCH_TIMEOUT='$(value BENCH_TIMEOUT)'

.PHONY: bench-compare
bench-compare:
	@$(BENCH_ENV) $(BENCH_SCRIPT) compare

# BASELINE_REF pins the stored baseline to a committed revision; a baseline
# from a dirty tree is not reproducible.
.PHONY: bench-baseline
bench-baseline:
	@$(BENCH_ENV) BASELINE_REF='$(BASELINE_REF)' BASELINE_LABEL='$(BASELINE_LABEL)' \
		$(BENCH_SCRIPT) baseline

.PHONY: install-benchstat
install-benchstat:
	@echo "==> Installing benchstat..."
	$(GO) install golang.org/x/perf/cmd/benchstat@latest

# ============================================================================
# Fuzz Testing
# ============================================================================

# Per-target budget for the fuzz-<pkg> targets: a duration or an execution
# count (`FUZZ_TIME=20000x`).
FUZZ_TIME     ?= 60s
FUZZ_TAGS     := -tags=quicraft_fuzz

# Shell functions shared by the mutation-fuzzing recipes. `go test -fuzz`
# takes one package and a pattern matching one target, so targets run singly:
#
#   fuzz_targets <pkg>              prints the fuzz targets in <pkg>; fails if
#                                   the package does not build or has none
#   fuzz_run <pkg> <target> <flag>… mutation-fuzzes one target; -run='^$'
#                                   skips the unit tests
FUZZ_SH = \
	fuzz_targets() { \
		listing=$$($(GO) test $(FUZZ_TAGS) -list='^Fuzz' "$$1") || return 1; \
		found=""; \
		for name in $$listing; do \
			case "$$name" in Fuzz*) found="$$found $$name" ;; esac; \
		done; \
		if [ -z "$$found" ]; then \
			echo "FAIL: no fuzz targets found in $$1" >&2; return 1; \
		fi; \
		echo $$found; \
	}; \
	fuzz_run() { \
		pkg=$$1; target=$$2; shift 2; \
		$(GO) test $(FUZZ_TAGS) -count=1 -run='^$$' -fuzz="^$$target\$$" "$$@" "$$pkg"; \
	}

# fuzz-pkg: mutation-fuzz every target in one package for FUZZ_TIME each,
# stopping at the first failure so its reproducer is the last thing printed.
# Usage: $(call fuzz-pkg,<package>)
define fuzz-pkg
	@set -u; $(FUZZ_SH); \
	targets=$$(fuzz_targets $(1)) || exit 1; \
	for t in $$targets; do \
		echo "==> $(1) $$t"; \
		fuzz_run $(1) "$$t" -fuzztime=$(FUZZ_TIME) || exit 1; \
	done
endef

.PHONY: fuzz-proto
fuzz-proto:
	$(call fuzz-pkg,./pkg/proto)

.PHONY: fuzz-transport
fuzz-transport:
	$(call fuzz-pkg,./pkg/internal/transport)

.PHONY: fuzz-waldb
fuzz-waldb:
	$(call fuzz-pkg,./pkg/logdb/waldb)

.PHONY: fuzz-seal
fuzz-seal:
	$(call fuzz-pkg,./pkg/seal)

.PHONY: fuzz-session
fuzz-session:
	$(call fuzz-pkg,./pkg/internal/session)

.PHONY: fuzz-raft
fuzz-raft:
	$(call fuzz-pkg,./pkg/internal/raft)

.PHONY: fuzz-all
fuzz-all: fuzz-proto fuzz-transport fuzz-waldb fuzz-seal fuzz-session fuzz-raft

# CI parity: identical to the `fuzz-seeds` job in .github/workflows/ci.yml.
# -run=Fuzz without -fuzz runs each target over its seeds and committed corpus
# once, deterministically. Covers ./... so a new fuzz target is never missed.
.PHONY: ci-fuzz-seeds
ci-fuzz-seeds:
	$(GO) test -race -count=1 $(FUZZ_TAGS) -run=Fuzz ./...

# ============================================================================
# Bounded mutation fuzzing — the release gate
#
# A short regression pass per target, not a search; the nightly fuzz.yml does
# the searching. The budget is an execution count (-fuzztime=<N>x) because a
# duration budget can end in a spurious "context deadline exceeded" at shutdown.
# -parallel=2 keeps concurrent gates on one runner from starving each other.
# ============================================================================

FUZZ_MUT_PKGS ?= ./pkg/proto ./pkg/internal/transport ./pkg/internal/session \
                 ./pkg/internal/raft ./pkg/logdb/waldb ./pkg/seal
# Each budget is the nightly's depth divided by FUZZ_MUT_DIVISOR.
FUZZ_MUT_DIVISOR ?= 6

.PHONY: ci-fuzz-mutation
ci-fuzz-mutation:
	@set -u; overall=0; $(FUZZ_SH); \
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
		targets=$$(fuzz_targets "$$pkg") || { overall=1; continue; }; \
		for t in $$targets; do \
			n=$$(budget "$$pkg" "$$t") || { overall=1; break; }; \
			if ! fuzz_run "$$pkg" "$$t" -parallel=2 -fuzztime=$${n}x; then \
				echo "FAIL: $$pkg $$t"; overall=1; \
			fi; \
		done; \
		echo "ok $$pkg"; \
	done; \
	if [ $$overall -ne 0 ]; then exit 1; fi; \
	echo "All bounded mutation fuzz targets passed"

# ============================================================================
# ErrorFS I/O Error Injection
# ============================================================================

ERRFS_TAGS    := -tags=quicraft_errfs
ERRFS_PKGS    := ./test/errfs/...
GOERRFS       := $(GO) test $(ERRFS_TAGS) -race -count=1 -v

# Verbose developer run of the quicraft_errfs suite; -v shows which injection
# point failed. CI uses ci-errfs.
.PHONY: errfs-waldb
errfs-waldb:
	$(GOERRFS) $(ERRFS_PKGS)

# CI parity: identical to the `errfs` job in .github/workflows/ci.yml and the
# `errfs` gate in release.yml. -race stays on because the tests inject I/O
# failures from concurrent goroutines.
.PHONY: ci-errfs
ci-errfs:
	$(GO) test -race -count=1 $(ERRFS_TAGS) $(ERRFS_PKGS)

# ============================================================================
# Performance Comparison Benchmarks
# ============================================================================

PERF_CMD := $(PERF_DIR)/cmd/perfcompare
# The dragonboat engine is the fork github.com/jeremyhahn/dragonboat/v4, pinned
# in test/performance/dragonboat/go.mod.
#
# Stored results live under a machine profile, test/performance/profiles/<name>/
# (see test/performance/README.md). PERF_PROFILE is required by every target
# that reads or writes stored results and has no default; capture-env.sh
# records the machine's specs.
SYSTEMS ?= all
BENCHTIME ?= 30s
PERF_TIMEOUT ?= 3600s
PERF_EXTRA ?=
PERF_PROFILE ?=
# Systems to run and to cover in generated documents. Generation fails if a
# named system has no results, so narrow it explicitly:
#   make perf-generate PERF_PROFILE=<profile> PERF_SYSTEMS=quicraft,dragonboat,openraft
PERF_SYSTEMS ?= all
# ssh destination for the remote targets, e.g. PERF_HOST=user@bench-host.example.
PERF_HOST ?=

PROFILE_DIR = profiles/$(PERF_PROFILE)

# Internal: common perfcompare flags.
PERF_FLAGS = --benchtime=$(BENCHTIME) --timeout=$(PERF_TIMEOUT) $(PERF_EXTRA)

# Internal: module credentials (GIT_CONFIG_* variables) for the benchmark image
# builds, from the environment or else the gitignored .devcontainer/.env.
# perfcompare passes them as a BuildKit secret and run-remote.sh sends them over
# stdin, so the token never appears on a command line or in an image. Without
# them, modules resolve from the public proxy only.
MODULE_AUTH_ENV = if [ -z "$${GIT_CONFIG_KEY_0:-}" ] && [ -f $(MODULE_ENV_FILE) ]; then \
	set -a; . $(MODULE_ENV_FILE); set +a; fi;
# Internal: every local perfcompare invocation, credentials staged.
PERFCOMPARE = cd $(PERF_DIR) && $(MODULE_AUTH_ENV) ./perfcompare

# Internal: refuse to run without a named profile.
REQUIRE_PROFILE = @if [ -z "$(PERF_PROFILE)" ]; then \
	echo "PERF_PROFILE is required (e.g. PERF_PROFILE=<profile>). Profiles: $$(ls $(PERF_DIR)/profiles 2>/dev/null | tr '\n' ' ')"; \
	exit 1; fi

.PHONY: perf-build
perf-build:
	cd $(PERF_DIR) && $(GO) build -o perfcompare ./cmd/perfcompare/

# ----------------------------------------------------------------------------
# Ad-hoc runs: print a report, store nothing, need no profile.
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
#   make perf-all PERF_PROFILE=<profile>
#   make perf-run PERF_PROFILE=<profile> PERF_SYSTEMS=etcd     # one engine
#   make perf-generate PERF_PROFILE=<profile>
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

# Capture a machine's specs into the profile: PERF_HOST if set, else this one.
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

# The whole pipeline on a remote machine: stage the tree, capture specs, run,
# fetch results and env.json into profiles/<name>/, regenerate documents.
#
#   make perf-remote PERF_HOST=user@host PERF_PROFILE=<profile>
#
# The remote run is detached; reattach with 'test/performance/run-remote.sh
# wait'. Knobs: BENCHTIME, PERF_TIMEOUT, PERF_SYSTEMS, BENCH_TMPDIR, REMOTE_DIR.
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

# check-coverage: fail if the profile is missing or empty, if no percentage can
# be read from it, or if coverage is below COVER_MIN. Without the first two
# checks an empty value makes the `bc` comparison error out and the gate pass.
# `/^total:/` avoids matching functions whose names contain "total".
# Usage: $(call check-coverage,profile.out,package-name)
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
	if awk -v c="$${COVERAGE}" -v m="$(COVER_MIN)" 'BEGIN { exit !(c < m) }'; then \
		echo "FAIL: $(2) coverage $${COVERAGE}% is below minimum $(COVER_MIN)%"; \
		exit 1; \
	fi
endef

# run-coverage: measure one scope's coverage and enforce COVER_MIN on it.
# Usage: $(call run-coverage,<label>,<package pattern>)
#
# -coverpkg is deliberately not passed: packages without tests already count at
# 0%, and -coverpkg would credit cross-package coverage and loosen the gate.
define run-coverage
	$(GOTEST) $(GO_TAGS_FLAG) -coverprofile=$(COVER_DIR)/$(1).out $(2)
	$(call check-coverage,$(COVER_DIR)/$(1).out,$(1))
endef

# SHARD and SHARDS pick one slice of a package list: `make coverage-shard
# SHARD=1 SHARDS=4` runs the second of four. The defaults are the whole list.
SHARD  ?= 0
SHARDS ?= 1

# check_shard is a recipe fragment: it exits 1, naming the target, unless
# SHARD and SHARDS are numbers and SHARD < SHARDS.
check_shard = case "$(SHARD):$(SHARDS)" in *[!0-9:]* | :* | *:) echo "FAIL: $@: SHARD and SHARDS must be numbers, got SHARD=$(SHARD) SHARDS=$(SHARDS)"; exit 1 ;; esac; \
	if [ "$(SHARD)" -ge "$(SHARDS)" ]; then echo "FAIL: $@: SHARD=$(SHARD) must be below SHARDS=$(SHARDS)"; exit 1; fi

# shard_deal is a pipeline stage: it reads "<package> <dir>" lines and prints
# the packages of shard SHARD of SHARDS. A package weighs the bytes of its
# _test.go files. Heaviest first (ties by name), each package goes to the
# lightest shard so far (ties to the lowest index). The deal depends only on
# the tree, so every shard computes the same split: the shards are disjoint
# and together hold every package, untested ones included.
shard_deal = while read -r p d; do printf '%s %s\n' "$$(cat "$$d"/*_test.go 2>/dev/null | wc -c)" "$$p"; done | \
	LC_ALL=C sort -k1,1nr -k2,2 | \
	awk -v s=$(SHARD) -v n=$(SHARDS) 'BEGIN { for (i = 0; i < n; i++) w[i] = 0 } { b = 0; for (i = 1; i < n; i++) if (w[i] < w[b]) b = i; w[b] += $$1; if (b == s) print $$2 }'

# shard_packages is a recipe fragment for $(call shard_packages,<go list
# flags>,<patterns>): it sets the shell variable pkgs to shard SHARD of SHARDS
# of the packages go list finds, dealt by shard_deal.
shard_packages = $(check_shard); \
	dirs=$$($(GO) list $(1) -f '{{.ImportPath}} {{.Dir}}' $(2)) || { echo "FAIL: $@: go list failed, so there is nothing to shard"; exit 1; }; \
	pkgs=$$(printf '%s\n' "$$dirs" | $(shard_deal)); \
	if [ -z "$$pkgs" ]; then echo "FAIL: $@: shard $(SHARD) of $(SHARDS) holds no package"; exit 1; fi

# The per-package -timeout of the -race unit run. Go's own default; CI raises
# it for a 4-core hosted runner.
COVER_TIMEOUT ?= 10m

# cover_shard_profile is shard $(1)'s profile in a run of SHARDS shards.
cover_shard_profile = $(COVER_DIR)/coverage-shard-$(1)-of-$(SHARDS).out

# The test stage is two pieces, so CI can run the -race unit run as parallel
# shards (ci.yml's unit and coverage jobs) and a developer runs the same
# targets in one go:
#   coverage-shard  one shard of the -race unit run, writing its profile
#   coverage-merge  joins the SHARDS profiles into total.out and gates it
# `make coverage` runs both with a single shard.
.PHONY: coverage
coverage: | $(COVER_DIR)
	@$(MAKE) --no-print-directory coverage-shard SHARD=0 SHARDS=1
	@$(MAKE) --no-print-directory coverage-merge SHARDS=1

.PHONY: coverage-shard
coverage-shard: | $(COVER_DIR)
	@$(call shard_packages,$(GO_TAGS_FLAG),./...); \
	echo "==> coverage shard $(SHARD) of $(SHARDS): $$(printf '%s\n' "$$pkgs" | wc -l) packages"; \
	$(GOTEST) $(GO_TAGS_FLAG) -timeout=$(COVER_TIMEOUT) -coverprofile=$(call cover_shard_profile,$(SHARD)) $$pkgs

# The shards hold disjoint packages, so their profiles join by concatenation:
# one mode line, then every shard's blocks. Every one of the SHARDS profiles
# must be present and share the mode line, or the total would be wrong. The
# total is gated as one profile, and every package under pkg/ on its own.
.PHONY: coverage-merge
coverage-merge: | $(COVER_DIR)
	@$(check_shard); \
	out=$(COVER_DIR)/total.out; mode=""; i=0; \
	while [ $$i -lt $(SHARDS) ]; do \
		f=$(call cover_shard_profile,$$i); \
		[ -s "$$f" ] || { echo "FAIL: $@: $$f is missing or empty (make coverage-shard SHARD=$$i SHARDS=$(SHARDS))"; exit 1; }; \
		m=$$(head -n 1 "$$f"); \
		case "$$m" in mode:*) ;; *) echo "FAIL: $@: $$f does not start with a mode line"; exit 1 ;; esac; \
		if [ -z "$$mode" ]; then mode=$$m; printf '%s\n' "$$mode" > "$$out"; \
		elif [ "$$m" != "$$mode" ]; then echo "FAIL: $@: $$f has '$$m', shard 0 has '$$mode'"; exit 1; fi; \
		tail -n +2 "$$f" >> "$$out"; \
		i=$$((i + 1)); \
	done
	$(call check-coverage,$(COVER_DIR)/total.out,total)
	scripts/check-coverage-packages.sh $(COVER_DIR)/total.out $(COVER_MIN) github.com/jeremyhahn/go-quicraft/pkg

# shard-packages prints shard SHARD of SHARDS of SHARD_PKGS under the tags
# SHARD_TAGS, one import path per line, for a workflow that runs go test
# itself (batch.yml's race-full). It deals packages as coverage-shard does.
SHARD_PKGS ?= ./...
SHARD_TAGS ?=

.PHONY: shard-packages
shard-packages:
	@$(call shard_packages,$(if $(strip $(SHARD_TAGS)),-tags='$(SHARD_TAGS)'),$(SHARD_PKGS)); \
	printf '%s\n' "$$pkgs"

# it-shard-packages prints shard SHARD of SHARDS of the integration packages
# (every directory under INT_TEST_DIR with a test file), one per line, for
# ci.yml's integration shards to pass as IT_PKGS. It reads the tree, not go
# list, so the runner needs no Go toolchain: the suite runs in the image.
.PHONY: it-shard-packages
it-shard-packages:
	@$(check_shard); \
	dirs=$$(find $(INT_TEST_DIR) -name '*_test.go' -printf '%h\n' | LC_ALL=C sort -u | while read -r d; do printf '%s %s\n' "$$d" "$$d"; done); \
	pkgs=$$(printf '%s\n' "$$dirs" | $(shard_deal)); \
	if [ -z "$$pkgs" ]; then echo "FAIL: $@: shard $(SHARD) of $(SHARDS) holds no package"; exit 1; fi; \
	printf '%s\n' "$$pkgs"

# `coverage` also holds every package under pkg/ to COVER_MIN, from the profile
# it writes, so no package hides under the total: CI's test stage is
# `make coverage` (ci-template v1.21). coverage-packages is the old name.
.PHONY: coverage-packages
coverage-packages: coverage

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
#
# build-fips compiles the tree under the Go Cryptographic Module with and
# without the `fips` tag. It runs no crypto and makes no compliance claim.
#
# verify-fips runs the tests under GODEBUG=fips140=only, which rejects every
# non-approved algorithm at use. `on` would still permit MD5, SHA-1 and
# arbitrary-IV AES-GCM, so do not weaken it or add exclusions to route around a
# failure. The QUIC transport passes under `only` and stays inside this gate.
#
# verify-fips-exempt runs pkg/internal/transport/revocation under
# GODEBUG=fips140=on. OCSP CertID is SHA-1 by definition (RFC 6960 section
# 4.1.1), an identifier inside golang.org/x/crypto/ocsp. Revocation Mode: crl
# never reaches it. The exemption is recorded in
# docs/quic-transport/architecture.md.
#
# verify-fips excludes exactly the tree verify-fips-exempt runs, so a new
# package is inside the approved-only gate by default
# (test/buildgates/fips_gate_test.go enforces this).
# ============================================================================

# The one package tree outside the approved-only gate.
FIPS_EXCLUDED := github.com/jeremyhahn/go-quicraft/pkg/internal/transport/revocation

.PHONY: build-fips
build-fips:
	$(FIPS_GOENV) GOFIPS140=v1.0.0 CGO_ENABLED=0 $(GO) build -ldflags="$(LDFLAGS)" ./...
	$(FIPS_GOENV) GOFIPS140=v1.0.0 CGO_ENABLED=0 $(GO) build -tags fips -ldflags="$(LDFLAGS)" ./...

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
# nancy is not included: it needs OSS Index credentials, and govulncheck and
# trivy cover the same ground. Run `make nancy` where credentials exist.
ci: fmt no-mutants check-tool-versions vet lint security build test
	@echo "CI pipeline passed"

# ============================================================================
# Analyser-version guard
#
# The CI workflows and devcontainer Dockerfiles carry literal golangci-lint
# pins because they cannot read a make variable. This fails if any pin differs
# from GOLANGCI_VERSION, since an analyser of another version gates on a
# different rule set. Finding no pins at all is also a failure.
# check-lint-toolchain covers the other half: which Go built the binary.
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
# An analyser built by an older Go cannot analyse a module targeting a newer
# one, even at the pinned version. This reads the build toolchain from the
# binary's `--version` output ("built with goX.Y.Z"), compares it to the
# module's `go` directive, and prints the fix on a mismatch. staticcheck runs
# inside golangci-lint, so it needs no separate guard.
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
# `ci` runs untagged tests only, which silently skips the integration, errfs,
# netfault and fuzz suites. ci-full matches .github/workflows/ci.yml; run it
# before opening a PR. The slow tagged suites run last.
# ============================================================================

.PHONY: ci-full
ci-full: ci ci-modules ci-cross ci-errfs ci-fuzz-seeds ci-fuzz-mutation ci-integration ci-netfault \
        build-fips verify-fips verify-fips-exempt
	@echo "Full CI pipeline passed (untagged + integration + netfault + errfs + fuzz seeds + fuzz mutation + all modules on every platform + FIPS)"

# ============================================================================
# Heavyweight test offload to a remote build host
#
# The -race suites and multi-node integration clusters are sensitive to load; on
# a busy workstation the 30-second snapshot catch-up waits can flake. These
# targets run any target on REMOTE_HOST instead. There is no default host.
#
#   make remote-ci-full REMOTE_HOST=build-host.example        # the full gate, off-box
#   make remote-test REMOTE_HOST=build-host.example           # remote-<target> runs <target> there
#   make remote-integration-test REMOTE_HOST=build-host.example REMOTE_USER=ci
#
# REMOTE_USER is optional. REMOTE_WORKDIR is where the tree lands; it is not
# named REMOTE_DIR because test/performance/run-remote.sh already uses that.
# `remote-sync` is an explicit rule, so it wins over the remote-% pattern.
#
# rsync --delete keeps the remote tree an exact mirror. .git is excluded: the
# working tree, including uncommitted edits, is what ships.
# ============================================================================

REMOTE_HOST    ?=
REMOTE_USER    ?=
REMOTE_WORKDIR ?= ~/ci/go-quicraft
# The ssh destination: user@host when REMOTE_USER is set, the bare host otherwise.
REMOTE_DEST     = $(if $(REMOTE_USER),$(REMOTE_USER)@)$(REMOTE_HOST)
# The remote PATH, which a non-interactive ssh does not set up. The toolchain is
# the go.mod `go` directive version installed via golang.org/dl into
# ~/sdk/go<ver>; a host without it is refused.
REMOTE_GO_SDK   := $$HOME/sdk/go$(GO_DIRECTIVE)
REMOTE_ENV_PATH := test -x $(REMOTE_GO_SDK)/bin/go || { \
	echo "remote host has no $(REMOTE_GO_SDK): install go$(GO_DIRECTIVE) with golang.org/dl" >&2; exit 1; }; \
	export PATH=$(REMOTE_GO_SDK)/bin:$$HOME/go/bin:$$PATH
REMOTE_SSH      = ssh -o BatchMode=yes -o ConnectTimeout=10 $(REMOTE_DEST)
RSYNC_EXCLUDE  := --exclude .git --exclude .build-cache --exclude $(COVER_DIR) \
                  --exclude $(BUILD_DIR) --exclude '*.test' --exclude '*.prof' \
                  --exclude '*.out' --exclude test/performance/openraft/target

# Internal: refuse to run a remote target with no host named.
REQUIRE_REMOTE_HOST = @if [ -z "$(REMOTE_HOST)" ]; then \
	echo "REMOTE_HOST is required: the remote targets have no default host."; \
	echo "usage: make remote-<target> REMOTE_HOST=<host> [REMOTE_USER=<user>] [REMOTE_WORKDIR=<dir>]"; \
	echo "       make remote-tools REMOTE_HOST=<host> [FORGE_SSH_HOST=<forge> FORGE_SSH_PORT=<port>]"; \
	exit 1; fi

.PHONY: remote-sync
remote-sync:
	$(REQUIRE_REMOTE_HOST)
	@$(REMOTE_SSH) 'mkdir -p $(REMOTE_WORKDIR)'
	@rsync -az --delete $(RSYNC_EXCLUDE) ./ $(REMOTE_DEST):$(REMOTE_WORKDIR)/
	@echo "synced working tree to $(REMOTE_DEST):$(REMOTE_WORKDIR)"

# remote-<target>: sync, then run <target> on REMOTE_HOST, streaming output.
.PHONY: remote-%
remote-%: remote-sync
	@echo "==> running 'make $*' on $(REMOTE_HOST)"
	@$(REMOTE_SSH) '$(REMOTE_ENV_PATH); cd $(REMOTE_WORKDIR) && make $*'

# Install the scanners on the remote host (once, or after a toolchain bump).
# Tools are installed unconditionally so a stale binary already on PATH is
# replaced; golangci-lint is pinned to GOLANGCI_VERSION.
#
# FORGE_SSH_HOST optionally names a private forge that the remote host's git
# insteadOf rewrites module fetches to; its host key is seeded with github.com's.
FORGE_SSH_HOST ?=
FORGE_SSH_PORT ?= 22

.PHONY: remote-tools
remote-tools:
	$(REQUIRE_REMOTE_HOST)
	@$(REMOTE_SSH) '$(REMOTE_ENV_PATH); \
		go install golang.org/x/vuln/cmd/govulncheck@$(GOVULNCHECK_VERSION); \
		$(call GOSEC_INSTALL,go); \
		go install github.com/golangci/golangci-lint/v2/cmd/golangci-lint@$(GOLANGCI_VERSION); \
		command -v trivy >/dev/null || curl -sfL \
			https://raw.githubusercontent.com/aquasecurity/trivy/$(TRIVY_VERSION)/contrib/install.sh \
			| sh -s -- -b $$HOME/go/bin $(TRIVY_VERSION) >/dev/null; \
		echo "remote tools ready ($(GOLANGCI_VERSION))"'
	@# Seed known_hosts so module fetches rewritten to ssh do not fail with
	@# "Host key verification failed".
	@$(REMOTE_SSH) 'mkdir -p $$HOME/.ssh && chmod 700 $$HOME/.ssh; \
		for hk in $(if $(FORGE_SSH_HOST),"-p $(FORGE_SSH_PORT) $(FORGE_SSH_HOST)") "github.com"; do \
			ssh-keyscan $$hk 2>/dev/null >> $$HOME/.ssh/known_hosts; \
		done; \
		sort -u $$HOME/.ssh/known_hosts -o $$HOME/.ssh/known_hosts; \
		chmod 600 $$HOME/.ssh/known_hosts; echo "  known_hosts seeded"'
	@$(REMOTE_SSH) '$(REMOTE_ENV_PATH); for t in go golangci-lint gosec govulncheck trivy; do \
		printf "  %-14s %s\n" "$$t" "$$(command -v $$t || echo MISSING)"; done'

# ============================================================================
# Multi-module build
#
# contrib/dragonboat and the harnesses under test/performance/ are separate
# modules that require or replace the root, so a root change can break them.
# Every go.mod is discovered from the tree (MODULE_DIRS). ci-modules mirrors the
# `modules` job in .github/workflows/ci.yml; ci-cross mirrors the `cross-arch`
# job, building and vetting every module for each of CROSS_PLATFORMS.
# ci-cross sets CGO_ENABLED=0 because the toolchain image enables cgo for
# -race, and a cgo cross build needs a C cross compiler the image lacks.
#
# GOWORK=off (exported below) matters: in a workspace the modules would resolve
# the root through it rather than their own require/replace directives.
#
# Every module is attempted even after a failure, so one broken module cannot
# hide another.
# ============================================================================
# MODULE_FLOOR fails both targets when discovery comes back short, so a broken
# walk cannot pass over an empty set. Lower it in the commit that removes a
# module.
MODULE_FLOOR := 6
define check-module-floor
	n=$$(printf '%s\n' $(MODULE_DIRS) | grep -c .); \
	echo "Discovered $$n module(s)"; \
	if [ "$$n" -lt $(MODULE_FLOOR) ]; then \
		echo "FAIL: discovered $$n modules but expected at least $(MODULE_FLOOR)."; exit 1; \
	fi
endef

# Nested modules also run their tests (perfresult and benchutil gate
# `make bench-compare`); the root's run in `make coverage`. Every module is
# vetted with -tags=benchmark too, which nothing else compiles.
.PHONY: ci-modules
ci-modules:
	@$(check-module-floor); \
	rc=0; \
	for dir in $(MODULE_DIRS); do \
		echo "==> $$dir"; \
		test_cmd=true; [ "$$dir" = . ] || test_cmd="$(GO) test -count=1 ./..."; \
		if ! ( cd "$$dir" && $(GO) build ./... && $(GOVET) ./... && $(GOVET) -tags=benchmark ./... \
				&& $$test_cmd ); then \
			echo "    FAIL: $$dir"; rc=1; \
		fi; \
	done; \
	if [ $$rc -ne 0 ]; then \
		echo "ci-modules: one or more modules failed to build or vet"; \
	else \
		echo "All modules build and vet clean"; \
	fi; \
	exit $$rc

.PHONY: ci-cross
ci-cross:
	@$(check-module-floor); \
	rc=0; \
	for platform in $(CROSS_PLATFORMS); do \
		goos=$${platform%/*}; goarch=$${platform#*/}; \
		for dir in $(MODULE_DIRS); do \
			echo "==> $$platform $$dir"; \
			if ! ( cd "$$dir" && CGO_ENABLED=0 GOOS=$$goos GOARCH=$$goarch $(GO) build ./... \
					&& CGO_ENABLED=0 GOOS=$$goos GOARCH=$$goarch $(GOVET) ./... ); then \
				echo "    FAIL: $$platform $$dir"; rc=1; \
			fi; \
		done; \
	done; \
	if [ $$rc -ne 0 ]; then \
		echo "ci-cross: one or more modules failed to build or vet for another platform"; \
	else \
		echo "All modules build and vet clean for: $(CROSS_PLATFORMS)"; \
	fi; \
	exit $$rc

# ============================================================================
# Linting and static analysis
# ============================================================================

# Runs the analyser resolved by GOLANGCI_BIN in every module (MODULE_DIRS),
# after check-lint-toolchain has confirmed it can analyse this module. Nested
# modules share the root's .golangci.yml, and -tags=benchmark brings in the
# harness entry points under test/performance (the tag selects nothing in the
# root), without which their helpers read as unused. Every module is attempted
# even after a failure. gofmt and go vet run first, and actionlint over the
# workflows last, each also after an earlier failure: this is CI's lint stage
# (ci-template v1.21).
.PHONY: lint
lint: check-lint-toolchain
	@rc=0; \
	$(MAKE) --no-print-directory fmt || rc=1; \
	$(GOVET) $(GO_TAGS_FLAG) ./... || rc=1; \
	for dir in $(MODULE_DIRS); do \
		echo "==> $$dir"; \
		( cd "$$dir" && '$(GOLANGCI_BIN)' run --timeout=5m --config '$(CURDIR)/.golangci.yml' \
			--build-tags benchmark$(if $(strip $(GO_BUILD_TAGS)),$(comma)$(GO_TAGS_CSV)) ./... ) || rc=1; \
	done; \
	$(MAKE) --no-print-directory actionlint || rc=1; \
	$(MAKE) --no-print-directory api-check || rc=1; \
	exit $$rc

# Compares the exported API of every public package (root module and
# contrib/dragonboat) with the last release tag, using apidiff pinned by the
# root go.mod's tool directive. Before v1.0.0 incompatible changes are
# printed and pass; from v1.0.0 one fails unless VERSION is a new major. See
# scripts/api-check.sh and docs/compatibility/engineering.md.
.PHONY: api-check
api-check:
	@./scripts/api-check.sh

# Validates workflow syntax, expressions and runner labels before CI does.
# actionlint reads .github/actionlint.yaml by itself where one exists (the
# place to declare self-hosted runner labels; every job here runs on
# ubuntu-latest, so there is none). Its shell checks come from shellcheck, and
# without shellcheck on PATH actionlint skips them and still passes, so a
# missing shellcheck fails like a missing actionlint.
.PHONY: actionlint
actionlint:
	@bin='$(ACTIONLINT_BIN)'; \
	want='$(ACTIONLINT_VERSION)'; \
	if [ -z "$$bin" ]; then \
		echo "FAIL: no actionlint found, so the workflows were not linted."; \
		echo "      Looked in $(TOOLS_BIN), then on PATH."; \
		echo "      Install the pinned version with: make install-actionlint"; \
		exit 1; \
	fi; \
	have=$$("$$bin" --version 2>/dev/null | head -1); \
	if [ "$$have" != "$$want" ]; then \
		echo "FAIL: $$bin is '$$have', this repo pins $$want."; \
		echo "      Install the pinned version with: make install-actionlint"; \
		exit 1; \
	fi; \
	if ! command -v shellcheck >/dev/null 2>&1; then \
		echo "FAIL: shellcheck is not on PATH, so actionlint would skip every"; \
		echo "      run: script. Install shellcheck (apt install shellcheck)."; \
		exit 1; \
	fi; \
	echo "==> actionlint $$have: .github/workflows"; \
	"$$bin" && echo "Workflows pass actionlint"

.PHONY: vet
vet:
	$(GOVET) ./...

# ============================================================================
# Mutation-marker guard
#
# Mutation testing edits production code to confirm a test fails, and an
# unrestored mutation keeps tests green while disabling a guard. This fails on
# `if false {`, MUTANT or MUTATION in non-test Go files. Test files are excluded
# because `if false` is legitimate there.
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

# gosec, govulncheck, go mod verify and trivy fs. trivy-image is excluded
# because it builds the devcontainer image and needs Docker; run it explicitly.
.PHONY: security
security: gosec vuln modverify trivy
	@echo "All security scans passed"
	@echo "Note: container images are scanned separately -- run 'make trivy-image'"

.PHONY: modverify
# Verify the module cache against go.sum before the scanners read it.
modverify:
	$(GO) mod verify

.PHONY: deps-check
# Fail when a direct dependency of any module here is behind its newest
# release, once that release is 7 days old (a younger one is reported as NEW
# and does not fail). deps-latest, by contrast, only updates github.com/jeremyhahn/*.
# Runs nightly rather than in the push gate, since an upstream release does not
# make a commit wrong. Needs only a go toolchain and network.
deps-check:
	@./scripts/deps-check.sh

.PHONY: gosec
# gosec alone, no network (handbook devops.md §4, §8). CI's lint job runs it
# on the PR tier and `make security` on the full tier (ci-template v1.30):
# only test/performance (separate modules that never ship) is excluded.
gosec:
	$(GOSEC) $(GO_TAGS_FLAG) -exclude=$(GOSEC_EXCLUDE) -exclude-dir=test/performance -severity=medium -confidence=medium ./...

.PHONY: vuln
# Fails on any reachable vulnerability govulncheck finds. Record any accepted
# exception explicitly rather than hiding it here.
vuln:
	$(GOVULNCHECK) $(GO_TAGS_FLAG) ./...

.PHONY: trivy
# Scoped to what ships: test/performance/* are separate benchmark modules that
# consumers never fetch, and .build-cache is a local artifact. A missing binary
# fails rather than skips (check-trivy), because trivy is the only secret
# scanner here.
trivy: check-trivy
	$(TRIVY) fs --scanners vuln,secret --severity CRITICAL,HIGH --exit-code 1 \
		--skip-dirs .build-cache --skip-dirs test/performance .

# Scans the devcontainer integration image (handbook devops.md §8). CI's `image`
# job runs the same scan on the image it builds at the commit under test.
.PHONY: trivy-image
trivy-image: check-trivy docker-build-it
	$(TRIVY) image --severity CRITICAL,HIGH --exit-code 1 $(IT_IMAGE)

# Every trivy target fails rather than skips without the binary: a scan that
# did not run must not read as a scan that passed.
.PHONY: check-trivy
check-trivy:
	@if [ -z '$(TRIVY)' ] || ! command -v '$(TRIVY)' >/dev/null 2>&1; then \
		echo "FAIL: trivy is not installed, so the scan did not run."; \
		echo "      This gate does not pass by default."; \
		echo "      Install trivy $(TRIVY_VERSION) with: make install-trivy"; \
		exit 1; \
	fi

# Nancy scans dependencies against Sonatype OSS Index, which needs credentials
# (~/.oss-index-config or OSSI_USERNAME / OSSI_TOKEN). An installed nancy fails
# closed on findings or errors; a missing one is skipped because govulncheck and
# trivy cover the same ground.
.PHONY: nancy
nancy:
	@if command -v nancy >/dev/null 2>&1; then \
		$(GO) list -json -m all | nancy sleuth --quiet; \
	else \
		echo "nancy not installed, skipping (govulncheck and trivy cover this)"; \
	fi

# Write gosec and trivy SARIF reports, then exit non-zero if either reported
# findings or failed. Trivy is scoped the same as the `trivy` target.
.PHONY: security-sarif
security-sarif: check-trivy
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

# gosec builds in a throwaway module so x/tools can be raised to XTOOLS_VERSION
# (see scripts/tool-pins.sh): a plain `go install gosec@GOSEC_VERSION` links
# the x/tools its go.mod names, which cannot read Go 1.27 export data. The
# Dockerfile tools stage does the same. $(1) is the go command. One shell line
# with no single quotes, so remote-tools can embed it in its ssh command.
GOSEC_INSTALL = ( d="$$(mktemp -d)" && cd "$$d" && export GOWORK=off && \
	$(1) mod init gosec-build && \
	$(1) get github.com/securego/gosec/v2/cmd/gosec@$(GOSEC_VERSION) golang.org/x/tools@$(XTOOLS_VERSION) && \
	$(1) install github.com/securego/gosec/v2/cmd/gosec; \
	rc=$$?; cd / && rm -rf "$$d"; exit $$rc )

.PHONY: install-gosec
install-gosec:
	$(call GOSEC_INSTALL,$(GO))

.PHONY: install-govulncheck
install-govulncheck:
	$(GO) install golang.org/x/vuln/cmd/govulncheck@$(GOVULNCHECK_VERSION)

# Installs the pinned scanner into $(TOOLS_BIN), where every trivy target looks
# first. The installer comes from the same tag and checks the release
# checksum. The closing version check is what fails the target on a failed
# download, since `curl | sh` exits 0 on an empty one and a binary from an
# earlier install would otherwise answer.
.PHONY: install-trivy
install-trivy:
	@mkdir -p $(TOOLS_BIN)
	curl -sfL https://raw.githubusercontent.com/aquasecurity/trivy/$(TRIVY_VERSION)/contrib/install.sh \
		| sh -s -- -b $(TOOLS_BIN) $(TRIVY_VERSION)
	@v=$$($(TOOLS_BIN)/trivy --version 2>/dev/null); \
	case "$$v" in \
		*"Version: $(patsubst v%,%,$(TRIVY_VERSION))"*) echo "trivy $(TRIVY_VERSION) installed in $(TOOLS_BIN)" ;; \
		*) echo "FAIL: trivy $(TRIVY_VERSION) was not installed in $(TOOLS_BIN)"; exit 1 ;; \
	esac

.PHONY: install-nancy
install-nancy:
	$(GO) install github.com/sonatype-nexus-community/nancy@latest

# golangci-lint comes from install-lint, which builds it with the local Go
# rather than downloading a release binary built by an older toolchain.
.PHONY: install-tools
install-tools: install-gosec install-govulncheck install-trivy install-nancy install-lint install-actionlint
	@echo "All CI tools installed ($(GOLANGCI_VERSION), actionlint $(ACTIONLINT_VERSION), trivy $(TRIVY_VERSION))"

# Builds the pinned workflow linter into $(TOOLS_BIN), where `make actionlint`
# looks first. shellcheck, which it runs on every run: script, comes from the
# system package manager.
.PHONY: install-actionlint
install-actionlint:
	GOBIN=$(TOOLS_BIN) $(GO) install github.com/rhysd/actionlint/cmd/actionlint@$(ACTIONLINT_VERSION)
	@$(TOOLS_BIN)/actionlint --version

# Builds the pinned analyser with this repo's Go into $(TOOLS_BIN), where
# `make lint` looks first.
.PHONY: install-lint
install-lint:
	@echo "==> golangci-lint $(GOLANGCI_VERSION), built with $$($(GO) env GOVERSION), into $(TOOLS_BIN)"
	GOBIN=$(TOOLS_BIN) $(GO) install github.com/golangci/golangci-lint/v2/cmd/golangci-lint@$(GOLANGCI_VERSION)
	@$(TOOLS_BIN)/golangci-lint --version

# ============================================================================
# Docker
# ============================================================================

# Every docker target works against a remote daemon through the standard
# DOCKER_HOST variable (e.g. DOCKER_HOST=ssh://user@bench-host.example): the
# images COPY the source, nothing is bind-mounted, and the credential file is
# read by the local docker CLI.
#
# Internal: MODULE_ENV_FILE, when present, as the gomodauth BuildKit secret the
# Dockerfiles source for `go mod download` (never a layer or a command line).
# Absent, modules resolve from the public proxy and a forge-only module fails
# the build.
comma := ,
DOCKER_SECRET_ARGS = $(if $(wildcard $(MODULE_ENV_FILE)),--secret id=gomodauth$(comma)src=$(MODULE_ENV_FILE))
# Internal: the same file as the container's run-time environment.
DOCKER_ENV_ARGS = $(if $(wildcard $(MODULE_ENV_FILE)),--env-file $(MODULE_ENV_FILE))
# Internal: the secret mount needs BuildKit; the legacy builder rejects it.
REQUIRE_BUILDX = @docker buildx version >/dev/null 2>&1 || { \
	echo "docker buildx is required: the devcontainer Dockerfiles mount module"; \
	echo "credentials as a BuildKit secret. Install the docker-buildx plugin."; \
	exit 1; }

# The devcontainer (handbook devcontainer.md): one Dockerfile whose toolchain
# stage runs every CI stage but integration, and whose integration stage runs
# that, under the hardened compose service beside it.
DEVCONTAINER_DIR     := build/docker/devcontainer
DEVCONTAINER_COMPOSE := $(DEVCONTAINER_DIR)/compose.yaml
# The toolchain image tag, hashed from everything the image is built from, so a
# changed pin, Dockerfile or module list is a new tag (`make toolchain-image`).
# Recursive: only the targets that use it pay for the hash.
TOOLCHAIN_TAG   = $(shell cat $(DEVCONTAINER_DIR)/Dockerfile $(TOOL_PINS) go.mod go.sum | sha256sum | cut -c1-16)
TOOLCHAIN_IMAGE = go-quicraft-toolchain:$(TOOLCHAIN_TAG)
# The integration image tag, hashed from everything the integration stage is
# built from (handbook ci-standard.md §9): the GO_IMAGE build argument, the
# Dockerfile and every path its stages COPY from the context, tracked or
# untracked but not ignored, so a changed file is a new tag and runs of one
# tree on one daemon share one image (`make it-image`). A deleted tracked file
# drops out of the hash. Recursive: only the targets that use it pay for it.
# ci-template v1.29 (ruling 220(5), devcontainer.md §6): APK_INDEX_SUM, the
# Alpine package index checksum CI reads with scripts/apk-index-sum.sh, joins
# the hash, so a new Alpine package release is a new tag and a rebuild. Unset,
# as on a workstation that has not read it, it hashes as empty.
IT_INPUTS = $(DEVCONTAINER_DIR)/Dockerfile $(TOOL_PINS) go.mod go.sum pkg test
IT_TAG    = $(shell { echo 'GO_IMAGE=$(GO_IMAGE)'; echo 'APK_INDEX_SUM=$(APK_INDEX_SUM)'; git ls-files -co --exclude-standard -- $(IT_INPUTS) | LC_ALL=C sort -u | xargs -r -d '\n' sha256sum 2>/dev/null; } | sha256sum | cut -c1-16)
# The integration image and the compose project it runs under. CI names the
# project per run so that concurrent runs on one daemon never meet, and
# scripts/reap-stale-ci-stacks.sh removes what a killed run left, the image
# included once no container uses it.
IT_IMAGE   ?= go-quicraft-ci-it:$(IT_TAG)
IT_PROJECT ?= go-quicraft-it-$(DOCKER_TAG)
# What `make integration-test` runs in the integration image: the CI job's
# command, -race and serialized packages (see _integration-test). IT_PKGS is
# the package scope; ci.yml's integration shards pass a slice of it
# (`make it-shard-packages`).
IT_PKGS    ?= $(INT_TEST_DIR)/...
IT_CMD     ?= go test -race -count=1 -tags=integration$(if $(strip $(GO_BUILD_TAGS)),$(comma)$(GO_TAGS_CSV)) -timeout=1800s -p 1 $(IT_PKGS)
# Internal: a devcontainer build of one stage. GO_IMAGE comes from the pin file;
# GO_BUILD_PROXY, when set in the caller's environment, replaces the public
# module proxy for the build-time fetches. Quoted: CI passes a `|`-separated
# list (ci-template v1.29.5), which unquoted is a shell pipe.
DEVCONTAINER_BUILD = docker buildx build --load $(DOCKER_SECRET_ARGS) \
	--build-arg GO_IMAGE=$(GO_IMAGE) \
	$(if $(GO_BUILD_PROXY),--build-arg "GO_BUILD_PROXY=$(GO_BUILD_PROXY)") \
	-f $(DEVCONTAINER_DIR)/Dockerfile

# The make target docker-make runs inside the dev image.
DOCKER_MAKE_TARGET ?= ci

# Builds both devcontainer images: toolchain, tagged by content, and integration.
.PHONY: docker-build
docker-build: docker-build-toolchain docker-build-it

# Built only when the daemon lacks the tag, as docker-build-it (handbook
# devcontainer.md §6). Two jobs on one daemon can both miss the tag and race
# to build it; the loser's build fails with the tag already held, so a failed
# build re-inspects and succeeds when the tag is now there.
.PHONY: docker-build-toolchain
docker-build-toolchain:
	$(REQUIRE_BUILDX)
	@if docker image inspect '$(TOOLCHAIN_IMAGE)' >/dev/null 2>&1; then \
		echo "==> $(TOOLCHAIN_IMAGE) is already on the daemon"; \
	elif ! $(DEVCONTAINER_BUILD) --target toolchain -t '$(TOOLCHAIN_IMAGE)' .; then \
		if docker image inspect '$(TOOLCHAIN_IMAGE)' >/dev/null 2>&1; then \
			echo "==> $(TOOLCHAIN_IMAGE) was built by a parallel job; using it"; \
		else \
			exit 1; \
		fi; \
	fi

# Built only when the daemon lacks the tag: the tag is the content, so a held
# image is this tree's (handbook devcontainer.md §6).
# APK_INDEX_SUM reaches the integration stage only, whose `apk upgrade` it
# re-runs when the index moves (ci-template v1.29).
.PHONY: docker-build-it
docker-build-it:
	$(REQUIRE_BUILDX)
	@if docker image inspect '$(IT_IMAGE)' >/dev/null 2>&1; then \
		echo "==> $(IT_IMAGE) is already on the daemon"; \
	else \
		$(DEVCONTAINER_BUILD) --build-arg APK_INDEX_SUM=$(APK_INDEX_SUM) \
			--target integration -t '$(IT_IMAGE)' .; \
	fi

# Prints the toolchain image tag; `make docker-build` builds it.
.PHONY: toolchain-image
toolchain-image:
	@echo $(TOOLCHAIN_IMAGE)

# Prints the integration image tag, go-quicraft-ci-it:<hash>, from the content
# (never from IT_IMAGE); `make docker-build` builds it when the daemon lacks it.
.PHONY: it-image
it-image:
	@echo go-quicraft-ci-it:$(IT_TAG)

# The legacy dev image (.devcontainer/Dockerfile.dev), which docker-make runs.
.PHONY: docker-build-dev
docker-build-dev:
	$(REQUIRE_BUILDX)
	docker buildx build --load $(DOCKER_SECRET_ARGS) -t $(DOCKER_IMAGE):$(DOCKER_TAG) -f $(DOCKER_DIR)/Dockerfile.dev .

.PHONY: docker-build-integration
docker-build-integration:
	$(REQUIRE_BUILDX)
	docker buildx build --load $(DOCKER_SECRET_ARGS) -t $(DOCKER_INT_IMAGE):$(DOCKER_TAG) -f $(DOCKER_DIR)/Dockerfile .

# Runs `make $(DOCKER_MAKE_TARGET)` in a freshly built dev image:
#   make docker-make DOCKER_MAKE_TARGET=test
#   DOCKER_HOST=ssh://user@bench-host.example make docker-make
.PHONY: docker-make
docker-make: docker-build-dev
	docker run --rm --network host --cap-add NET_ADMIN $(DOCKER_ENV_ARGS) $(DOCKER_IMAGE):$(DOCKER_TAG) make $(DOCKER_MAKE_TARGET)

# Removes the images the docker-build* targets made for this checkout.
.PHONY: docker-clean
docker-clean:
	@for image in $(TOOLCHAIN_IMAGE) $(IT_IMAGE) $(DOCKER_IMAGE):$(DOCKER_TAG) $(DOCKER_INT_IMAGE):$(DOCKER_TAG); do \
		if docker image inspect "$$image" >/dev/null 2>&1; then docker image rm "$$image"; fi; \
	done

# Runs CMD in the toolchain image as the invoking user, with the checkout
# bind-mounted (compose service `toolchain`), offline and under the integration
# service's posture: make docker-run CMD='make lint'. A workstation target; CI
# copies the checkout in instead (scripts/in-toolchain-image.sh).
CMD ?= make build
.PHONY: docker-run
docker-run: docker-build-toolchain
	GO_QUICRAFT_TOOLCHAIN_TAG=$(TOOLCHAIN_TAG) GO_QUICRAFT_UID=$$(id -u) GO_QUICRAFT_GID=$$(id -g) \
		docker compose -f $(DEVCONTAINER_COMPOSE) -p go-quicraft-tc-$(DOCKER_TAG) \
		run --rm toolchain sh -c '$(CMD)'

# The legacy devcontainer shell (.devcontainer/docker-compose.yml).
.PHONY: docker-shell
docker-shell:
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
	@echo "Benchmark Regression Gate (see docs/testing/benchmarking.md):"
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
	@echo "    Flags: PERF_PROFILE=<profile> PERF_HOST=user@host PERF_SYSTEMS=all"
	@echo "           BENCHTIME=30s PERF_TIMEOUT=7200s PERF_EXTRA=--no-build"
	@echo ""
	@echo "Coverage (minimum $(COVER_MIN)%):"
	@echo "  coverage               Run total coverage report"
	@echo "  coverage-<pkg>         Run coverage for a specific package"
	@echo "    packages: proto, config, queue, invariant, logdb,"
	@echo "              raft, waldb, seal, rsm, session,"
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
	@echo "  ci-cross               Build and vet every module for each of CROSS_PLATFORMS"
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
	@echo "  lint                   Run golangci-lint in every module (install-lint's build, not PATH)"
	@echo "  check-tool-versions    Assert every golangci-lint pin matches GOLANGCI_VERSION"
	@echo "  check-lint-toolchain   Assert the installed analyser can analyse this module"
	@echo "  vet                    Run go vet"
	@echo "  fmt                    Check gofmt formatting"
	@echo ""
	@echo "Security:"
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
	@echo "  install-trivy          Install the pinned Trivy into .tools/bin"
	@echo ""
	@echo "Docker:"
	@echo "  docker-build                   Build the devcontainer toolchain and integration images"
	@echo "  docker-run CMD='make lint'     Run CMD in the toolchain image, checkout bind-mounted"
	@echo "  toolchain-image                Print the toolchain image tag (content-hashed)"
	@echo "  docker-build-dev               Build the legacy dev image (Dockerfile.dev)"
	@echo "  docker-build-integration       Build the legacy integration image (.devcontainer/Dockerfile)"
	@echo "  docker-make                    Run make DOCKER_MAKE_TARGET (default ci) in a fresh"
	@echo "                                 dev image; honours DOCKER_HOST for a remote daemon"
	@echo "  docker-clean                   Remove this checkout's docker-build* images"
	@echo "  docker-shell                   Run the legacy devcontainer shell"
	@echo "  docker-test                    Run unit tests in devcontainer"
	@echo "  docker-integration-down        Stop and clean integration containers"

# Builds never resolve through a workspace; this ignores stray go.work files.
export GOWORK=off

# ---------------------------------------------------------------------------
# Release
#
# `make release-alpha VERSION=vX.Y.Z-alpha`, `make release-candidate
# VERSION=vX.Y.Z-rc.N` or `make release VERSION=vX.Y.Z`.
# The version is explicit; the recipe never guesses. Every git write goes
# through RELEASE_GIT, the tag is annotated, only the release commit and its
# tags are pushed (atomically, by object ID), any failing step aborts before
# anything is pushed, and the push is read back from the remote before the
# release is reported.
# ---------------------------------------------------------------------------

# RELEASE_GIT is the git command every write runs through. The default is plain
# git, which signs per the releaser's own configuration (gpg.format=ssh,
# user.signingkey, commit.gpgsign, tag.gpgsign). A signing wrapper is named with
# any leading arguments it takes, e.g. RELEASE_GIT='/path/to/wrapper project';
# the value is split on whitespace into a command and its arguments.
#
# Trust model. RELEASE_GIT is trusted to authenticate and nothing else; in the
# company flow it is the wrapper that carries the push credentials, so the push
# has to run through it. Every read is plain git. After the writes, plain git
# resolves the release commit and its tag objects to object IDs, checks that
# the commit is exactly the VERSION bump (plus the contrib/dragonboat parent
# pin) on top of the main it started from and that each tag object targets it
# under its own name, and verifies every signature against RELEASE_ALLOWED_SIGNERS. The push names those object IDs,
# never a ref a helper could move. A helper can still ignore what it is asked
# to push, so afterwards plain git reads the remote back, at the URL resolved
# before RELEASE_GIT first ran, and the release fails unless the branch and every
# tag name exactly the verified objects. The read-back uses the releaser's own
# transport, which the duplicate-tag check already requires. A helper that
# rewrites the releaser's git configuration or binaries is outside what any
# recipe can check.

# (RELEASE_GIT defaults to plain git; the default is set at the top of this
# file, ahead of the `unexport` that keeps command-line values out of $(shell).)
# RELEASE_ALLOWED_SIGNERS is the trust anchor, a git allowed-signers file
# (gpg.ssh.allowedSignersFile format), relative to the checkout root. The
# release commit's author e-mail must be a principal in it, and the commit and
# both tags must carry SSH signatures that verify against a key it lists for
# that principal. A missing file fails the release before anything is written.

# (Defaulted at the top of this file, like RELEASE_GIT.)
# The git remote the release commit and tags are pushed to. Its existence is
# checked before anything is written.

# (Defaulted at the top of this file, like RELEASE_GIT.)
# deps-latest updates first-party dependencies to their highest tag, including
# -alpha prereleases, which a bare 'go get @latest' ignores.

.PHONY: release release-alpha release-candidate deps-latest

# Each settable variable reaches the recipe once, through these target-specific
# exports, and the body reads only the $$REL_* shell parameters, so a value is
# never expanded into recipe text. $(value ...) keeps make from expanding a
# $(shell ...) in a command-line value. $(origin ...) accepts VERSION only from
# the command line, not from an inherited environment variable.
#
# release-body is shared by release-alpha (vX.Y.Z-alpha), release-candidate
# (vX.Y.Z-rc.N) and release (vX.Y.Z); they differ only in REL_PATTERN /
# REL_FORM, so every guard applies to all three.
# release.yml publishes a tag with no alpha/beta/rc suffix as a full release.
#
# The write, verify, push and read-back steps run as one shell, so the object
# IDs resolved after the writes are the ones verified, pushed and compared.
#
# Verification runs with an allowed-signers file holding only the lines that
# name the commit's author, so a key the anchor lists for someone else does not
# verify. The gpg.*program settings disable OpenPGP and X.509 verification: only
# an SSH signature checked against the anchor can pass, never a key that merely
# happens to be trusted in a local keyring.
#
# contrib/dragonboat is its own module. A consumer ignores its replace of the
# parent and resolves the parent through its require, so the release commit
# pins that require to the version being cut and the contrib/dragonboat/<version>
# tag is made beside the root and test/performance tags, signed and pushed the
# same way. release-contrib-pin rewrites the one require line on stdin or in the
# named file to $$REL_VERSION and fails unless exactly one such line exists; the
# guard runs it read-only before anything is written, and the verification
# re-derives the pinned go.mod from main to compare it with the release commit.
release-contrib-pin = awk -v v="$$REL_VERSION" '$$1 == "require" && $$2 == "github.com/jeremyhahn/go-quicraft" && NF == 3 { $$3 = v; n++ } { print } END { exit n == 1 ? 0 : 1 }'

define release-body
@test -n "$$REL_VERSION" || { echo "ERROR: state the version explicitly: make $$REL_TARGET VERSION=$$REL_FORM (release-versioning.md decides patch/minor/major -- the recipe refuses to guess)"; exit 1; }
@printf '%s' "$$REL_VERSION" | grep -Eq "$$REL_PATTERN" || { echo "ERROR: VERSION is not of the form $$REL_FORM (no leading zeroes): $$REL_VERSION"; exit 1; }
@test "$$(git rev-parse --abbrev-ref HEAD)" = main || { echo "ERROR: releases are cut from main; you are on $$(git rev-parse --abbrev-ref HEAD). 'push $$REL_REMOTE HEAD' would publish this branch."; exit 1; }
@set -- $$REL_GIT; test $$# -gt 0 && command -v "$$1" >/dev/null 2>&1 || { echo 'ERROR: RELEASE_GIT does not name an executable command -- every git write in this recipe runs through it, and a bad command must fail before VERSION is written'; exit 1; }
@test -f "$$REL_SIGNERS" && test -r "$$REL_SIGNERS" || { echo "ERROR: the allowed-signers file $$REL_SIGNERS is missing or unreadable. It is the trust anchor the release is verified against; set RELEASE_ALLOWED_SIGNERS to a git allowed-signers file listing your signing key. Nothing has been written."; exit 1; }
@test -n "$$REL_REMOTE" || { echo 'ERROR: RELEASE_REMOTE is empty -- set it to the remote the release tag is pushed to'; exit 1; }
@git remote get-url "$$REL_REMOTE" >/dev/null 2>&1 || { echo "ERROR: no git remote named $$REL_REMOTE. Set RELEASE_REMOTE to the remote the release tag is pushed to; the duplicate-tag check below cannot answer against a remote that does not resolve"; exit 1; }
@! git rev-parse -q --verify "refs/tags/$$REL_VERSION" >/dev/null || { echo "ERROR: tag $$REL_VERSION already exists locally"; exit 1; }
@rc=0; git ls-remote --exit-code --tags "$$REL_REMOTE" "refs/tags/$$REL_VERSION" >/dev/null 2>&1 || rc=$$?; \
case "$$rc" in \
  0) echo "ERROR: tag $$REL_VERSION already exists on $$REL_REMOTE (a local check alone would miss this and fail the push AFTER committing VERSION)"; exit 1 ;; \
  2) : ;; \
  *) echo "ERROR: could not reach $$REL_REMOTE to check whether tag $$REL_VERSION exists. Refusing to continue: an unreachable remote is not an absent tag"; exit 1 ;; \
esac
@test -z "$$(git status --porcelain)" || { echo 'ERROR: working tree not clean'; exit 1; }
@ver="## [$${REL_VERSION#v}]"; awk -v ver="$$ver" 'index($$0, ver) == 1 { f = 1 } END { exit f ? 0 : 1 }' CHANGELOG.md || { echo "ERROR: CHANGELOG.md has no \"$$ver\" section heading. Matched LITERALLY and anchored at the line start, at least as strictly as .github/workflows/release.yml matches it -- an unanchored prose mention does not count, and neither does a near-miss heading"; exit 1; }
@$(release-contrib-pin) contrib/dragonboat/go.mod >/dev/null || { echo "ERROR: contrib/dragonboat/go.mod does not hold exactly one 'require github.com/jeremyhahn/go-quicraft <version>' line. The release commit pins it to $$REL_VERSION so the contrib/dragonboat/$$REL_VERSION tag is importable; nothing has been written."; exit 1; }
@! git rev-parse -q --verify "refs/tags/contrib/dragonboat/$$REL_VERSION" >/dev/null || { echo "ERROR: tag contrib/dragonboat/$$REL_VERSION already exists locally"; exit 1; }
@url="$$(git remote get-url "$$REL_REMOTE")" && base="$$(git rev-parse --verify -q 'refs/heads/main^{commit}')" || { echo "ERROR: could not resolve $$REL_REMOTE's URL or main before writing anything"; exit 1; }; \
relgit() { printf '%s\n' "$$REL_GIT $$*"; $$REL_GIT "$$@"; }; \
cmod=contrib/dragonboat/go.mod; \
printf '%s\n' "$${REL_VERSION#v}" > VERSION || exit 1; \
$(release-contrib-pin) "$$cmod" > "$$cmod.release" && mv "$$cmod.release" "$$cmod" || { rm -f "$$cmod.release"; echo "ERROR: could not pin $$cmod to $$REL_VERSION. Nothing has been committed. Reset with: git checkout -- VERSION $$cmod"; exit 1; }; \
relgit add VERSION "$$cmod" || exit 1; \
relgit commit -m "chore(release): $$REL_VERSION" || exit 1; \
relgit tag -a "$$REL_VERSION" -m "$$REL_VERSION" || exit 1; \
relgit tag -a "test/performance/$$REL_VERSION" -m "test/performance $$REL_VERSION (lockstep with root $$REL_VERSION)" || exit 1; \
relgit tag -a "contrib/dragonboat/$$REL_VERSION" -m "contrib/dragonboat $$REL_VERSION (lockstep with root $$REL_VERSION)" || exit 1; \
reset="git reset --hard $$base && git tag -d $$REL_VERSION test/performance/$$REL_VERSION contrib/dragonboat/$$REL_VERSION"; \
commit="$$(git rev-parse --verify -q 'refs/heads/main^{commit}')" && \
tag="$$(git rev-parse --verify -q "refs/tags/$$REL_VERSION")" && \
ptag="$$(git rev-parse --verify -q "refs/tags/test/performance/$$REL_VERSION")" && \
ctag="$$(git rev-parse --verify -q "refs/tags/contrib/dragonboat/$$REL_VERSION")" || { echo "ERROR: the release commit or one of its tags does not resolve. Nothing has been pushed. Reset with: $$reset"; exit 1; }; \
for t in "$$tag" "$$ptag" "$$ctag"; do \
  test "$$(git cat-file -t "$$t")" = tag || { echo "ERROR: $$t is not a tag object -- a lightweight tag; releases are annotated and signed. Nothing has been pushed. Reset with: $$reset"; exit 1; }; \
done; \
test "$$(git rev-list --parents -n 1 "$$commit")" = "$$commit $$base" || { echo "ERROR: the release commit $$commit is not a single commit on top of main $$base. Nothing has been pushed. Reset with: $$reset"; exit 1; }; \
test "$$(git diff-tree -r --no-commit-id --name-only "$$base" "$$commit")" = "$$(printf 'VERSION\n%s' "$$cmod")" && test "$$(git cat-file blob "$$commit:VERSION")" = "$${REL_VERSION#v}" && \
pinned_mod="$$(git cat-file blob "$$base:$$cmod" | $(release-contrib-pin))" && test "$$(git cat-file blob "$$commit:$$cmod")" = "$$pinned_mod" || { echo "ERROR: the release commit $$commit changes more than VERSION and the parent requirement in $$cmod, or VERSION does not read $${REL_VERSION#v}, or $$cmod does not require the parent at $$REL_VERSION. Nothing has been pushed. Reset with: $$reset"; exit 1; }; \
for pair in "$$tag $$REL_VERSION" "$$ptag test/performance/$$REL_VERSION" "$$ctag contrib/dragonboat/$$REL_VERSION"; do \
  set -- $$pair; \
  git cat-file tag "$$1" | awk -v o="object $$commit" -v n="tag $$2" 'NR == 1 { a = ($$0 == o) } NR == 2 { b = ($$0 == "type commit") } NR == 3 { c = ($$0 == n) } END { exit !(a && b && c) }' || { echo "ERROR: tag object $$1 does not point at the release commit $$commit under the name $$2. Nothing has been pushed. Reset with: $$reset"; exit 1; }; \
done; \
author="$$(git log -1 --format='%ae' "$$commit")"; \
pinned="$$(mktemp)" || exit 1; trap 'rm -f "$$pinned"' EXIT; \
awk -v who="$$author" '/^[[:space:]]*(#|$$)/ { next } { n = split($$1, p, ","); for (i = 1; i <= n; i++) if (p[i] == who) { print; next } }' "$$REL_SIGNERS" > "$$pinned" || exit 1; \
test -s "$$pinned" || { echo "ERROR: the release commit's author <$$author> is not a principal in $$REL_SIGNERS. Nothing has been pushed. Reset with: $$reset"; exit 1; }; \
verify() { git -c gpg.program=false -c gpg.openpgp.program=false -c gpg.x509.program=false -c gpg.ssh.allowedSignersFile="$$pinned" "$$@" >/dev/null 2>&1; }; \
rc=0; \
verify verify-commit "$$commit" || { echo "ERROR: the release commit's signature does not verify against a key $$REL_SIGNERS lists for <$$author>. Nothing has been pushed. Reset with: $$reset"; rc=1; }; \
for pair in "$$tag $$REL_VERSION" "$$ptag test/performance/$$REL_VERSION" "$$ctag contrib/dragonboat/$$REL_VERSION"; do \
  set -- $$pair; \
  verify verify-tag "$$1" || { echo "ERROR: tag $$2's signature does not verify against a key $$REL_SIGNERS lists for <$$author>. Nothing has been pushed. Reset with: $$reset"; rc=1; }; \
done; \
test $$rc -eq 0 || exit 1; \
relgit push --atomic "$$REL_REMOTE" "$$commit:refs/heads/main" "$$tag:refs/tags/$$REL_VERSION" "$$ptag:refs/tags/test/performance/$$REL_VERSION" "$$ctag:refs/tags/contrib/dragonboat/$$REL_VERSION" || { echo "ERROR: the push through RELEASE_GIT failed. An atomic push publishes nothing on failure, but RELEASE_GIT is not trusted to have been atomic: inspect $$url before running the release again."; exit 1; }; \
published="$$(git ls-remote "$$url" refs/heads/main "refs/tags/$$REL_VERSION" "refs/tags/test/performance/$$REL_VERSION" "refs/tags/contrib/dragonboat/$$REL_VERSION")" || { echo "ERROR: RELEASE_GIT reported a successful push, but $$url could not be read back, so it does not hold the verified release as far as this recipe can tell. Inspect it before announcing anything."; exit 1; }; \
for pair in "refs/heads/main $$commit" "refs/tags/$$REL_VERSION $$tag" "refs/tags/test/performance/$$REL_VERSION $$ptag" "refs/tags/contrib/dragonboat/$$REL_VERSION $$ctag"; do \
  set -- $$pair; \
  got="$$(printf '%s\n' "$$published" | awk -v r="$$1" '$$2 == r { print $$1 }')"; \
  test "$$got" = "$$2" || { echo "ERROR: $$url has $$1 at $${got:-nothing}; the verified release has it at $$2."; rc=1; }; \
done; \
test $$rc -eq 0 || { echo "ERROR: RELEASE_GIT reported a successful push, but $$url does not hold the verified release. Do not announce it; inspect the remote."; exit 1; }; \
echo "Released $$REL_VERSION (signed, annotated; root, test/performance and contrib/dragonboat tags pushed atomically and read back from $$url)"
endef

release-alpha: export override REL_VERSION := $(if $(filter command line,$(origin VERSION)),$(value VERSION),)
release-alpha: export override REL_REMOTE  := $(value RELEASE_REMOTE)
release-alpha: export override REL_GIT     := $(value RELEASE_GIT)
release-alpha: export override REL_SIGNERS := $(value RELEASE_ALLOWED_SIGNERS)
release-alpha: export override REL_TARGET  := release-alpha
release-alpha: export override REL_FORM    := vX.Y.Z-alpha
release-alpha: export override REL_PATTERN := ^v(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)-alpha$$
release-alpha:
	$(release-body)

# release cuts a stable version (no -alpha suffix).
release: export override REL_VERSION := $(if $(filter command line,$(origin VERSION)),$(value VERSION),)
release: export override REL_REMOTE  := $(value RELEASE_REMOTE)
release: export override REL_GIT     := $(value RELEASE_GIT)
release: export override REL_SIGNERS := $(value RELEASE_ALLOWED_SIGNERS)
release: export override REL_TARGET  := release
release: export override REL_FORM    := vX.Y.Z
release: export override REL_PATTERN := ^v(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)$$
release:
	$(release-body)

# release-candidate cuts a release candidate, vX.Y.Z-rc.N (release-versioning.md
# §1a). N is a positive integer without leading zeroes. A candidate is never
# inferred: VERSION must be stated on the command line like every other release.
# VERSION and the CHANGELOG heading carry the unprefixed form (X.Y.Z-rc.N).
release-candidate: export override REL_VERSION := $(if $(filter command line,$(origin VERSION)),$(value VERSION),)
release-candidate: export override REL_REMOTE  := $(value RELEASE_REMOTE)
release-candidate: export override REL_GIT     := $(value RELEASE_GIT)
release-candidate: export override REL_SIGNERS := $(value RELEASE_ALLOWED_SIGNERS)
release-candidate: export override REL_TARGET  := release-candidate
release-candidate: export override REL_FORM    := vX.Y.Z-rc.N
release-candidate: export override REL_PATTERN := ^v(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)-rc\.[1-9][0-9]*$$
release-candidate:
	$(release-body)

deps-latest:
	@GOWORK=off sh -c ' \
	SELF=$$(go list -m); \
	for m in $$(go list -m -f "{{if not .Indirect}}{{.Path}}{{end}}" all 2>/dev/null | grep "^github.com/jeremyhahn/" | grep -vx "$$SELF"); do \
	  case "$$(go list -m -f "{{if .Replace}}{{.Replace.Path}}{{end}}" "$$m" 2>/dev/null)" in .*|/*) continue;; esac; \
	  V=$$(go list -m -versions "$$m" 2>/dev/null | tr " " "\n" | tail -1); \
	  if [ -n "$$V" ] && [ "$$V" != "$$m" ]; then go get "$$m@$$V" || exit 1; fi; \
	done; go mod tidy'
