# go-quicraft Build System
# Multi-group Raft library

.DEFAULT_GOAL := build

# Project metadata
MODULE        := github.com/jeremyhahn/go-quicraft
BINARY        := go-quicraft
BUILD_DIR     := build
DOCKER_DIR    := .devcontainer
COVER_DIR     := $(BUILD_DIR)/coverage

# Version injection: derived values are `override`, caller-supplied release inputs are unexported so no parse-time $(shell) sees them.
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
# The repo's build-tag set as CI passes it; GO_TAGS_FLAG is the go tool's form, empty when the set is.
GO_BUILD_TAGS ?=
space         := $(subst ,, )
GO_TAGS_CSV    = $(subst $(space),$(comma),$(strip $(GO_BUILD_TAGS)))
GO_TAGS_FLAG   = $(if $(strip $(GO_BUILD_TAGS)),-tags=$(GO_TAGS_CSV))

# FIPS_GOENV serves a read-only module cache as a file proxy so the FIPS snapshot builds offline.
GOMODCACHE_DIR := $(shell $(GO) env GOMODCACHE 2>/dev/null)
FIPS_MODCACHE  ?= $(or $(TMPDIR),/tmp)/go-quicraft-fips-modcache
FIPS_GOENV     := $(if $(shell test -d '$(GOMODCACHE_DIR)' && ! test -w '$(GOMODCACHE_DIR)' && echo ro),GOMODCACHE='$(FIPS_MODCACHE)' GOPROXY='file://$(GOMODCACHE_DIR)/cache/download' GOPRIVATE= GONOPROXY= GOFLAGS='-mod=readonly -modcacherw')
GOBENCH       := $(GO) test -run=^$$ -bench=. -benchmem
GOVET         := $(GO) vet
GOFMT         := gofmt
# The analyser's name on PATH. `make lint` runs GOLANGCI_BIN, which prefers the
# one `make install-lint` builds.
GOLINT        := golangci-lint
# Every pinned tool version; CI and devcontainer golangci-lint pins must match GOLANGCI_VERSION (check-tool-versions).
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
# Every Go module directory in the tree, skipping what the go command ignores.
MODULE_DIRS    = $(shell find . -mindepth 1 -type d \( -name '.*' -o -name '_*' -o -name testdata \) -prune \
			-o -name go.mod -printf '%h\n' | sort)
# The platforms the cross-arch CI job builds and vets every module for.
CROSS_PLATFORMS := linux/arm64 linux/386 windows/amd64 darwin/arm64 freebsd/amd64
GOSEC         := gosec
GOVULNCHECK   := govulncheck

# gosec exclusions; keep identical to the -exclude flag in .github/workflows/ci.yml.
GOSEC_EXCLUDE := G103,G104,G115,G301,G304,G306,G404

# The trivy binary: TOOLS_BIN first, then PATH; empty when absent, which check-trivy fails.
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

# run-tests: run one test selection, failing when it selects no tests. Usage: $(call run-tests,<pkg pattern>[,<-run regexp>])
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
	if [ $$status -eq 0 ]; then :; $(IT_WALSYNC_RUN) fi; \
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

# The CI integration command on the host: no -v, 1800s timeout for slower shared runners.
.PHONY: ci-integration
ci-integration:
	$(GO) test -race -count=1 -tags=integration -timeout=1800s -p 1 $(INT_TEST_DIR)/...

# Network-partition suites under the quicraft_netfault tag, -p 1 because the injector is process-global; NETFAULT_PKGS must list every tagged package.
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

# Regression benchmarking (release gate): bench-compare interleaves baseline and current binaries and compares with benchstat; see docs/testing/benchmarking.md.

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

# Shell helpers for mutation fuzzing: fuzz_targets <pkg> lists targets; fuzz_run <pkg> <target> <flag>... fuzzes one.
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

.PHONY: fuzz-batch
fuzz-batch:
	$(call fuzz-pkg,./pkg/batch)

.PHONY: fuzz-shamir
fuzz-shamir:
	$(call fuzz-pkg,./pkg/crypto/shamir)

.PHONY: fuzz-engine
fuzz-engine:
	$(call fuzz-pkg,./pkg/internal/engine)

.PHONY: fuzz-revocation
fuzz-revocation:
	$(call fuzz-pkg,./pkg/internal/transport/revocation)

.PHONY: fuzz-kv
fuzz-kv:
	$(call fuzz-pkg,./pkg/sm/kv)

.PHONY: fuzz-all
fuzz-all: fuzz-proto fuzz-transport fuzz-waldb fuzz-seal fuzz-session fuzz-raft \
          fuzz-batch fuzz-shamir fuzz-engine fuzz-revocation fuzz-kv

# CI parity: identical to the `fuzz-seeds` job in .github/workflows/ci.yml.
# -run=Fuzz without -fuzz runs each target over its seeds and committed corpus
# once, deterministically. Covers ./... so a new fuzz target is never missed.
.PHONY: ci-fuzz-seeds
ci-fuzz-seeds:
	$(GO) test -race -count=1 $(FUZZ_TAGS) -run=Fuzz ./...

# Bounded mutation fuzzing (release gate): an execution-count budget per target; nightly fuzz.yml does the searching.

FUZZ_MUT_PKGS ?= ./pkg/proto ./pkg/internal/transport ./pkg/internal/session \
                 ./pkg/internal/raft ./pkg/logdb/waldb ./pkg/seal ./pkg/batch \
                 ./pkg/crypto/shamir ./pkg/internal/engine \
                 ./pkg/internal/transport/revocation ./pkg/sm/kv
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
		  ./pkg/batch)              base=700000 ;; \
		  ./pkg/crypto/shamir)      base=450000 ;; \
		  ./pkg/internal/engine)    base=480000 ;; \
		  ./pkg/internal/transport/revocation) base=380000 ;; \
		  ./pkg/sm/kv)              base=1500000 ;; \
		  *) echo "FAIL: no execution budget is calibrated for $$1 -- add one before adding the package to FUZZ_MUT_PKGS" >&2; return 1 ;; \
		esac; \
		case "$$2" in \
		  FuzzMessageFrameDecode) base=160000 ;; \
		  FuzzFrameHeaderUnmarshalCrash) base=4500 ;; \
		  FuzzSplitCombine) base=25000 ;; \
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
# Benchmark results live under test/performance/profiles/<name>/; PERF_PROFILE is required and has no default.
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

# Module credentials for benchmark image builds, passed as a BuildKit secret or over stdin, never on a command line.
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

# Profile pipeline: make perf-all|perf-run|perf-generate PERF_PROFILE=<profile> [PERF_SYSTEMS=...]
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

# Whole pipeline on PERF_HOST: make perf-remote PERF_HOST=user@host PERF_PROFILE=<profile>; reattach with run-remote.sh wait.
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

# check-coverage: fail on a missing or empty profile, an unreadable total, or coverage below COVER_MIN. Usage: $(call check-coverage,profile.out,package-name)
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

# run-coverage: measure one scope and enforce COVER_MIN, without -coverpkg. Usage: $(call run-coverage,<label>,<package pattern>)
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

# shard_deal: print the packages of shard SHARD of SHARDS, heaviest test bytes first to the lightest shard; deterministic per tree.
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

# coverage-shard runs one -race unit shard; coverage-merge joins and gates the profiles; coverage runs both with one shard.
.PHONY: coverage
coverage: | $(COVER_DIR)
	@$(MAKE) --no-print-directory coverage-shard SHARD=0 SHARDS=1
	@$(MAKE) --no-print-directory coverage-merge SHARDS=1

.PHONY: coverage-shard
coverage-shard: | $(COVER_DIR)
	@$(call shard_packages,$(GO_TAGS_FLAG),./...); \
	echo "==> coverage shard $(SHARD) of $(SHARDS): $$(printf '%s\n' "$$pkgs" | wc -l) packages"; \
	$(GOTEST) $(GO_TAGS_FLAG) -timeout=$(COVER_TIMEOUT) -coverprofile=$(call cover_shard_profile,$(SHARD)) $$pkgs

# Join shard profiles (one mode line, then every block); all SHARDS must be present. Gate the total and each pkg/ package.
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

# it-shard-packages: print shard SHARD of SHARDS of the integration packages, read from the tree, not go list.
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

# FIPS: build-fips builds with and without the fips tag; verify-fips runs under fips140=only; verify-fips-exempt runs revocation (OCSP SHA-1) under fips140=on.

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

# CI pipeline: fmt, vet, lint, security scans, build and tests.

.PHONY: ci
# nancy is not included: it needs OSS Index credentials, and govulncheck and
# trivy cover the same ground. Run `make nancy` where credentials exist.
ci: fmt no-mutants check-tool-versions vet lint lint-gates security build test
	@echo "CI pipeline passed"

# Fail if any CI or devcontainer golangci-lint pin differs from GOLANGCI_VERSION, or if none is found.
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

# Fail if golangci-lint was built by a Go older than the module's go directive.
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

# Full CI pipeline including every build-tagged suite, matching .github/workflows/ci.yml.

.PHONY: ci-full
ci-full: ci ci-modules ci-cross ci-errfs ci-fuzz-seeds ci-fuzz-mutation ci-integration ci-netfault \
        build-fips verify-fips verify-fips-exempt
	@echo "Full CI pipeline passed (untagged + integration + netfault + errfs + fuzz seeds + fuzz mutation + all modules on every platform + FIPS)"

# Run any target on REMOTE_HOST (rsync mirror without .git): make remote-<target> REMOTE_HOST=host [REMOTE_USER=user].

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

# Install the pinned scanners on the remote host; FORGE_SSH_HOST optionally seeds a private forge's host key.
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

# Multi-module build: ci-modules and ci-cross build and vet every discovered module (GOWORK=off, CGO_ENABLED=0 for cross), attempting all.
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

# gofmt, go vet and golangci-lint (-tags=benchmark) in every module, each attempted after a failure.
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
	exit $$rc

# The repository's own gates: actionlint and the API check, both attempted after a failure.
.PHONY: lint-gates
lint-gates:
	@rc=0; \
	$(MAKE) --no-print-directory actionlint || rc=1; \
	$(MAKE) --no-print-directory api-check || rc=1; \
	exit $$rc

# Compare every public package's exported API with the last release tag (apidiff); see scripts/api-check.sh.
.PHONY: api-check
api-check:
	@./scripts/api-check.sh

# Validate workflows with actionlint; a missing shellcheck fails, since actionlint would silently skip shell checks.
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

# Fail on unrestored mutation markers (`if false {`, MUTANT, MUTATION) in non-test Go files.
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
# deps-check fails a direct dependency behind a release over 7 days old or vulnerable; deps-bump moves failing requirements to latest.
DEPS_CHECK_MODULES ?= . contrib/dragonboat test/performance test/performance/dragonboat test/performance/etcd test/performance/quicraft

deps-check:
	$(GO) run ./scripts/depscheck -go $(GO) $(if $(DEPS_CHECK_BASE),-base $(DEPS_CHECK_BASE)) -vuln -govulncheck $(GOVULNCHECK) $(if $(DEPS_CHECK_VULN_TAGS),-vuln-tags $(DEPS_CHECK_VULN_TAGS)) $(DEPS_CHECK_MODULES)

.PHONY: deps-bump
deps-bump:
	$(GO) run ./scripts/depscheck -go $(GO) -bump $(DEPS_CHECK_MODULES)

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
# Trivy is scoped to what ships; a missing binary fails, since trivy is the only secret scanner.
trivy: check-trivy
	$(TRIVY) fs --scanners vuln,secret --severity CRITICAL,HIGH --exit-code 1 \
		--skip-dirs .build-cache --skip-dirs test/performance --skip-dirs scripts/depscheck/testdata .

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

# Nancy needs OSS Index credentials; installed it fails closed, missing it is skipped.
.PHONY: nancy
nancy:
	@if command -v nancy >/dev/null 2>&1; then \
		$(GO) list -json -m all | nancy sleuth --quiet; \
	else \
		echo "nancy not installed, skipping (govulncheck and trivy cover this)"; \
	fi

# Write gosec and trivy SARIF reports, then fail if either found anything or failed.
.PHONY: security-sarif
security-sarif: check-trivy
	@mkdir -p $(BUILD_DIR)
	@rc=0; \
	$(GOSEC) -exclude=$(GOSEC_EXCLUDE) -exclude-dir=test/performance -severity=medium -confidence=medium -fmt=sarif -out=$(BUILD_DIR)/gosec-results.sarif ./... || rc=1; \
	$(TRIVY) fs --severity CRITICAL,HIGH --exit-code 1 \
		--skip-dirs .build-cache --skip-dirs test/performance --skip-dirs scripts/depscheck/testdata \
		--format sarif --output $(BUILD_DIR)/trivy-results.sarif . || rc=1; \
	echo "SARIF reports generated in $(BUILD_DIR)/"; \
	if [ $$rc -ne 0 ]; then \
		echo "security-sarif: findings reported (or a scanner failed); see the SARIF reports"; \
	fi; \
	exit $$rc

# ============================================================================
# Tool installation
# ============================================================================

# Build gosec in a throwaway module so x/tools can be raised to XTOOLS_VERSION; one shell line without single quotes for remote-tools.
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

# Install the pinned trivy into TOOLS_BIN; the closing version check fails a failed download.
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

# Docker targets honour DOCKER_HOST; MODULE_ENV_FILE, when present, is the gomodauth BuildKit secret.
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
# Integration image tag: a hash of GO_IMAGE, the Dockerfile, every COPY path and APK_INDEX_SUM.
IT_INPUTS = $(DEVCONTAINER_DIR)/Dockerfile $(TOOL_PINS) go.mod go.sum pkg test
IT_TAG    = $(shell { echo 'GO_IMAGE=$(GO_IMAGE)'; echo 'APK_INDEX_SUM=$(APK_INDEX_SUM)'; git ls-files -co --exclude-standard -- $(IT_INPUTS) | LC_ALL=C sort -u | xargs -r -d '\n' sha256sum 2>/dev/null; } | sha256sum | cut -c1-16)
# Integration image and compose project; CI names the project per run.
IT_IMAGE   ?= go-quicraft-ci-it:$(IT_TAG)
IT_PROJECT ?= go-quicraft-it-$(DOCKER_TAG)
# What make integration-test runs in the image; IT_PKGS is the package scope.
IT_PKGS    ?= $(INT_TEST_DIR)/...
IT_CMD     ?= go test -race -count=1 -tags=integration$(if $(strip $(GO_BUILD_TAGS)),$(comma)$(GO_TAGS_CSV)) -timeout=1800s -p 1 $(IT_PKGS)
# WAL-sync mode: rerun the linearizability and e2e Partition packages in IT_PKGS with QUICRAFT_IT_WAL_SYNC=1.
IT_WALSYNC_ALL     = $(filter $(INT_TEST_DIR)/...,$(IT_PKGS))
IT_WALSYNC_LIN     = $(if $(IT_WALSYNC_ALL)$(filter $(INT_TEST_DIR)/linearizability $(INT_TEST_DIR)/linearizability/...,$(IT_PKGS)),$(INT_TEST_DIR)/linearizability/...)
IT_WALSYNC_E2E     = $(if $(IT_WALSYNC_ALL)$(filter $(INT_TEST_DIR)/e2e $(INT_TEST_DIR)/e2e/...,$(IT_PKGS)),$(INT_TEST_DIR)/e2e)
IT_WALSYNC_TEST    = go test -race -count=1 -tags=integration$(if $(strip $(GO_BUILD_TAGS)),$(comma)$(GO_TAGS_CSV)) -timeout=1800s -p 1
IT_WALSYNC_COMPOSE = GO_QUICRAFT_IT_IMAGE='$(IT_IMAGE)' docker compose -f $(DEVCONTAINER_COMPOSE) -p '$(IT_PROJECT)' \
	run --rm -e QUICRAFT_IT_WAL_SYNC=1 integration $(IT_WALSYNC_TEST)
# A shell fragment, each command ended by `;`: it leaves the first failure's
# exit code in $$status and runs nothing after a failure.
IT_WALSYNC_RUN = \
	$(if $(IT_WALSYNC_LIN),echo "==> WAL-sync mode: $(IT_WALSYNC_LIN)"; \
	$(IT_WALSYNC_COMPOSE) $(IT_WALSYNC_LIN) || status=$$?;) \
	$(if $(IT_WALSYNC_E2E),[ $$status -ne 0 ] || echo "==> WAL-sync mode: $(IT_WALSYNC_E2E) -run Partition"; \
	[ $$status -ne 0 ] || $(IT_WALSYNC_COMPOSE) -run 'Partition' $(IT_WALSYNC_E2E) || status=$$?;)

.PHONY: integration-test-walsync
integration-test-walsync: $(if $(strip $(IT_WALSYNC_LIN)$(IT_WALSYNC_E2E)),docker-build-it)
	@$(if $(strip $(IT_WALSYNC_LIN)$(IT_WALSYNC_E2E)),status=0; $(IT_WALSYNC_RUN) \
	GO_QUICRAFT_IT_IMAGE='$(IT_IMAGE)' docker compose -f $(DEVCONTAINER_COMPOSE) -p '$(IT_PROJECT)' \
		down --volumes >/dev/null 2>&1 || true; \
	exit $$status,echo "==> WAL-sync mode: no linearizability or e2e package in IT_PKGS; nothing to run")
# Build one devcontainer stage; GO_BUILD_PROXY, quoted, replaces the module proxy when set.
DEVCONTAINER_BUILD = docker buildx build --load $(DOCKER_SECRET_ARGS) \
	--build-arg GO_IMAGE=$(GO_IMAGE) \
	$(if $(GO_BUILD_PROXY),--build-arg "GO_BUILD_PROXY=$(GO_BUILD_PROXY)") \
	-f $(DEVCONTAINER_DIR)/Dockerfile

# The make target docker-make runs inside the dev image.
DOCKER_MAKE_TARGET ?= ci

# Builds both devcontainer images: toolchain, tagged by content, and integration.
.PHONY: docker-build
docker-build: docker-build-toolchain docker-build-it

# Built only when the daemon lacks the tag; a failed build that lost a race re-inspects the tag.
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

# Built only when the daemon lacks the tag; APK_INDEX_SUM reaches the integration stage only.
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

# Run CMD in the toolchain image with the checkout bind-mounted: make docker-run CMD='make lint'.
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
	@echo "  integration-test-walsync    Linearizability + e2e Partition with WAL fsync on"
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

# Release: make release-alpha|release-candidate|release VERSION=...; the version is explicit and nothing is pushed until every step passes.

# RELEASE_GIT runs every git write (default plain git); every pushed object is verified against RELEASE_ALLOWED_SIGNERS and read back from the remote.

# RELEASE_ALLOWED_SIGNERS: allowed-signers file whose key for the author must verify the commit and both tags.

# (Defaulted at the top of this file, like RELEASE_GIT.)
# The git remote the release commit and tags are pushed to. Its existence is
# checked before anything is written.

# (Defaulted at the top of this file, like RELEASE_GIT.)
# deps-latest updates first-party dependencies to their highest tag, including
# -alpha prereleases, which a bare 'go get @latest' ignores.

.PHONY: release release-alpha release-candidate deps-latest

# Settable variables reach release-body only as $$REL_* exports; write, verify, push and read-back run in one shell.
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

# release-candidate cuts vX.Y.Z-rc.N; VERSION must be stated on the command line.
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
