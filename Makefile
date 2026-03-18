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
GOSEC         := gosec
GOVULNCHECK   := govulncheck

# gosec exclusions (documented):
#   G103: unsafe calls (low-level binary encoding)
#   G104: unhandled errors (false positive on hash.Write)
#   G115: integer overflow conversions (protocol-spec bounded)
#   G301: directory permissions (0755 standard for data dirs)
#   G304: file path via variable (all paths constructed internally)
#   G306: file write permissions (0644 standard for data files)
#   G404: weak random (election jitter, not security-sensitive)
GOSEC_EXCLUDE := G103,G104,G115,G301,G304,G306,G404

# Trivy
TRIVY         := trivy

# Coverage
COVER_MIN     := 91
COVER_FLAGS   := -coverprofile
COVER_EXCLUDE := -coverpkg=./...

# Docker
DOCKER_IMAGE  := go-quicraft-dev
DOCKER_TAG    := latest
COMPOSE_FILE  := $(DOCKER_DIR)/docker-compose.yml
INT_COMPOSE   := $(DOCKER_DIR)/docker-compose.integration.yml

# Package paths
PKG_PROTO       := ./pkg/proto/...
PKG_CONFIG      := ./pkg/config/...
PKG_QUEUE       := ./pkg/internal/queue/...
PKG_STOPPER     := ./pkg/internal/stopper/...
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
PKG_DISCOVERY   := ./pkg/discovery/...
PKG_BOOTSTRAP   := ./pkg/bootstrap/...
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

.PHONY: test-stopper
test-stopper:
	$(GOTEST) $(PKG_STOPPER)

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

.PHONY: test-discovery
test-discovery:
	$(GOTEST) $(PKG_DISCOVERY)

.PHONY: test-bootstrap
test-bootstrap:
	$(GOTEST) $(PKG_BOOTSTRAP)

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

.PHONY: integration-test-bootstrap
integration-test-bootstrap:
	@echo "==> Building integration container..."
	@docker compose -f $(COMPOSE_FILE) build --quiet integration
	@echo "==> Running bootstrap integration tests..."
	docker compose -f $(COMPOSE_FILE) run --rm integration make _integration-test-bootstrap

# Internal targets: executed inside the devcontainer. Not intended for host use.

.PHONY: _integration-test
_integration-test:
	$(GOTEST) -tags=integration -timeout=300s $(INT_TEST_DIR)/...

.PHONY: _integration-test-raft
_integration-test-raft:
	$(GOTEST) -tags=integration $(INT_TEST_DIR)/raft/...

.PHONY: _integration-test-host
_integration-test-host:
	$(GOTEST) -tags=integration $(INT_TEST_DIR)/host/...

.PHONY: _integration-test-e2e
_integration-test-e2e:
	$(GOTEST) -tags=integration $(INT_TEST_DIR)/e2e/...

.PHONY: _integration-test-bootstrap
_integration-test-bootstrap:
	$(GOTEST) -tags=integration $(INT_TEST_DIR)/bootstrap/...

.PHONY: integration-test-linearizability
integration-test-linearizability:
	@echo "==> Building integration container..."
	@docker compose -f $(COMPOSE_FILE) build --quiet integration
	@echo "==> Running linearizability tests..."
	docker compose -f $(COMPOSE_FILE) run --rm integration make _integration-test-linearizability

.PHONY: _integration-test-linearizability
_integration-test-linearizability:
	$(GOTEST) -tags=integration -timeout=600s $(INT_TEST_DIR)/linearizability/...

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

.PHONY: bench-discovery
bench-discovery:
	$(GOBENCH) $(PKG_DISCOVERY)

.PHONY: bench-bootstrap
bench-bootstrap:
	$(GOBENCH) $(PKG_BOOTSTRAP)

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

.PHONY: fuzz-all
fuzz-all: fuzz-proto fuzz-transport fuzz-waldb fuzz-seal fuzz-session

# ============================================================================
# ErrorFS I/O Error Injection (Phase 3)
# ============================================================================

ERRFS_TAGS    := -tags=quicraft_errfs
GOERRFS       := $(GO) test $(ERRFS_TAGS) -race -count=1 -v

.PHONY: errfs-waldb
errfs-waldb:
	$(GOERRFS) ./test/errfs/...

# ============================================================================
# Performance Comparison Benchmarks
# ============================================================================

PERF_CMD := $(PERF_DIR)/cmd/perfcompare
DRAGONBOAT_SRC ?= /home/jhahn/sources/dragonboat
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

# check-coverage: extract percentage from coverage profile and fail if below threshold
# Usage: $(call check-coverage,profile.out,package-name)
define check-coverage
	@COVERAGE=$$($(GO) tool cover -func=$(1) | grep total | awk '{print $$3}' | sed 's/%//'); \
	echo "$(2) coverage: $${COVERAGE}%"; \
	if [ $$(echo "$${COVERAGE} < $(COVER_MIN)" | bc -l) -eq 1 ]; then \
		echo "FAIL: $(2) coverage $${COVERAGE}% is below minimum $(COVER_MIN)%"; \
		exit 1; \
	fi
endef

.PHONY: coverage
coverage: | $(COVER_DIR)
	$(GOTEST) -coverprofile=$(COVER_DIR)/total.out ./...
	$(call check-coverage,$(COVER_DIR)/total.out,total)

.PHONY: coverage-proto
coverage-proto: | $(COVER_DIR)
	$(GOTEST) -coverprofile=$(COVER_DIR)/proto.out $(PKG_PROTO)
	$(call check-coverage,$(COVER_DIR)/proto.out,proto)

.PHONY: coverage-config
coverage-config: | $(COVER_DIR)
	$(GOTEST) -coverprofile=$(COVER_DIR)/config.out $(PKG_CONFIG)
	$(call check-coverage,$(COVER_DIR)/config.out,config)

.PHONY: coverage-queue
coverage-queue: | $(COVER_DIR)
	$(GOTEST) -coverprofile=$(COVER_DIR)/queue.out $(PKG_QUEUE)
	$(call check-coverage,$(COVER_DIR)/queue.out,queue)

.PHONY: coverage-stopper
coverage-stopper: | $(COVER_DIR)
	$(GOTEST) -coverprofile=$(COVER_DIR)/stopper.out $(PKG_STOPPER)
	$(call check-coverage,$(COVER_DIR)/stopper.out,stopper)

.PHONY: coverage-invariant
coverage-invariant: | $(COVER_DIR)
	$(GOTEST) -coverprofile=$(COVER_DIR)/invariant.out $(PKG_INVARIANT)
	$(call check-coverage,$(COVER_DIR)/invariant.out,invariant)

.PHONY: coverage-logdb
coverage-logdb: | $(COVER_DIR)
	$(GOTEST) -coverprofile=$(COVER_DIR)/logdb.out $(PKG_LOGDB)
	$(call check-coverage,$(COVER_DIR)/logdb.out,logdb)

.PHONY: coverage-raft
coverage-raft: | $(COVER_DIR)
	$(GOTEST) -coverprofile=$(COVER_DIR)/raft.out $(PKG_RAFT)
	$(call check-coverage,$(COVER_DIR)/raft.out,raft)

.PHONY: coverage-waldb
coverage-waldb: | $(COVER_DIR)
	$(GOTEST) -coverprofile=$(COVER_DIR)/waldb.out $(PKG_WALDB)
	$(call check-coverage,$(COVER_DIR)/waldb.out,waldb)

.PHONY: coverage-seal
coverage-seal: | $(COVER_DIR)
	$(GOTEST) -coverprofile=$(COVER_DIR)/seal.out $(PKG_SEAL)
	$(call check-coverage,$(COVER_DIR)/seal.out,seal)

.PHONY: coverage-rsm
coverage-rsm: | $(COVER_DIR)
	$(GOTEST) -coverprofile=$(COVER_DIR)/rsm.out $(PKG_RSM)
	$(call check-coverage,$(COVER_DIR)/rsm.out,rsm)

.PHONY: coverage-session
coverage-session: | $(COVER_DIR)
	$(GOTEST) -coverprofile=$(COVER_DIR)/session.out $(PKG_SESSION)
	$(call check-coverage,$(COVER_DIR)/session.out,session)

.PHONY: coverage-snapshot
coverage-snapshot: | $(COVER_DIR)
	$(GOTEST) -coverprofile=$(COVER_DIR)/snapshot.out $(PKG_SNAPSHOT)
	$(call check-coverage,$(COVER_DIR)/snapshot.out,snapshot)

.PHONY: coverage-engine
coverage-engine: | $(COVER_DIR)
	$(GOTEST) -coverprofile=$(COVER_DIR)/engine.out $(PKG_ENGINE)
	$(call check-coverage,$(COVER_DIR)/engine.out,engine)

.PHONY: coverage-registry
coverage-registry: | $(COVER_DIR)
	$(GOTEST) -coverprofile=$(COVER_DIR)/registry.out $(PKG_REGISTRY)
	$(call check-coverage,$(COVER_DIR)/registry.out,registry)

.PHONY: coverage-transport
coverage-transport: | $(COVER_DIR)
	$(GOTEST) -coverprofile=$(COVER_DIR)/transport.out $(PKG_TRANSPORT)
	$(call check-coverage,$(COVER_DIR)/transport.out,transport)

.PHONY: coverage-quicraft
coverage-quicraft: | $(COVER_DIR)
	$(GOTEST) -coverprofile=$(COVER_DIR)/quicraft.out $(PKG_QUICRAFT)
	$(call check-coverage,$(COVER_DIR)/quicraft.out,quicraft)

.PHONY: coverage-errors
coverage-errors: | $(COVER_DIR)
	$(GOTEST) -coverprofile=$(COVER_DIR)/errors.out $(PKG_ERRORS)
	$(call check-coverage,$(COVER_DIR)/errors.out,errors)

.PHONY: coverage-sm
coverage-sm: | $(COVER_DIR)
	$(GOTEST) -coverprofile=$(COVER_DIR)/sm.out $(PKG_SM)
	$(call check-coverage,$(COVER_DIR)/sm.out,sm)

.PHONY: coverage-discovery
coverage-discovery: | $(COVER_DIR)
	$(GOTEST) -coverprofile=$(COVER_DIR)/discovery.out $(PKG_DISCOVERY)
	$(call check-coverage,$(COVER_DIR)/discovery.out,discovery)

.PHONY: coverage-bootstrap
coverage-bootstrap: | $(COVER_DIR)
	$(GOTEST) -coverprofile=$(COVER_DIR)/bootstrap.out $(PKG_BOOTSTRAP)
	$(call check-coverage,$(COVER_DIR)/bootstrap.out,bootstrap)

.PHONY: coverage-server
coverage-server: | $(COVER_DIR)
	$(GOTEST) -coverprofile=$(COVER_DIR)/server.out $(PKG_SERVER)
	$(call check-coverage,$(COVER_DIR)/server.out,server)

.PHONY: coverage-writemode
coverage-writemode: | $(COVER_DIR)
	$(GOTEST) -coverprofile=$(COVER_DIR)/writemode.out $(PKG_WRITEMODE)
	$(call check-coverage,$(COVER_DIR)/writemode.out,writemode)

.PHONY: coverage-batch
coverage-batch: | $(COVER_DIR)
	$(GOTEST) -coverprofile=$(COVER_DIR)/batch.out $(PKG_BATCH)
	$(call check-coverage,$(COVER_DIR)/batch.out,batch)

.PHONY: coverage-enclave
coverage-enclave: | $(COVER_DIR)
	$(GOTEST) -coverprofile=$(COVER_DIR)/enclave.out $(PKG_ENCLAVE)
	$(call check-coverage,$(COVER_DIR)/enclave.out,enclave)

.PHONY: coverage-crypto
coverage-crypto: | $(COVER_DIR)
	$(GOTEST) -coverprofile=$(COVER_DIR)/crypto.out $(PKG_CRYPTO)
	$(call check-coverage,$(COVER_DIR)/crypto.out,crypto)

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
ci: fmt vet lint gosec vuln trivy nancy build test
	@echo "CI pipeline passed"

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

# Comprehensive security target - runs all security analysis tools
.PHONY: security
security: gosec vuln trivy nancy
	@echo "All security scans passed"

.PHONY: gosec
gosec:
	$(GOSEC) -exclude=$(GOSEC_EXCLUDE) -exclude-dir=test/performance -severity=medium -confidence=medium ./...

.PHONY: vuln
vuln:
	$(GOVULNCHECK) ./... || echo "WARNING: govulncheck found stdlib vulnerabilities (upgrade Go to fix)"

.PHONY: trivy
trivy:
	$(TRIVY) fs --severity CRITICAL,HIGH --exit-code 1 --skip-dirs contrib/qrdb .

.PHONY: trivy-image
trivy-image: docker-build
	$(TRIVY) image --severity CRITICAL,HIGH --exit-code 1 $(DOCKER_IMAGE):$(DOCKER_TAG)

# Nancy scans go.sum for known vulnerable dependencies
# Note: nancy requires Sonatype OSS Index API access. If it fails, govulncheck
# provides similar coverage for Go module vulnerabilities.
.PHONY: nancy
nancy:
	@if command -v nancy >/dev/null 2>&1; then \
		$(GO) list -json -m all 2>/dev/null | nancy sleuth --quiet 2>/dev/null || \
		echo "nancy scan completed (check ~/.ossindex/nancy.combined.log for details)"; \
	else \
		echo "nancy not installed, skipping (govulncheck provides similar coverage)"; \
	fi

# Generate SARIF reports for all security tools (for CI integration)
.PHONY: security-sarif
security-sarif:
	$(GOSEC) -exclude=$(GOSEC_EXCLUDE) -exclude-dir=test/performance -severity=medium -confidence=medium -fmt=sarif -out=$(BUILD_DIR)/gosec-results.sarif ./... || true
	$(TRIVY) fs --severity CRITICAL,HIGH --format sarif --output $(BUILD_DIR)/trivy-results.sarif . || true
	@echo "SARIF reports generated in $(BUILD_DIR)/"

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
	@echo "    packages: proto, config, queue, stopper, invariant, logdb,"
	@echo "              raft, waldb, seal, rsm, session, snapshot,"
	@echo "              registry, engine, transport, quicraft, errors, sm,"
	@echo "              discovery, bootstrap, server, writemode"
	@echo ""
	@echo "Integration Tests (run in devcontainer automatically):"
	@echo "  integration-test            Run all integration tests in devcontainer"
	@echo "  integration-test-raft       Run raft integration tests in devcontainer"
	@echo "  integration-test-host       Run Host API tests in devcontainer"
	@echo "  integration-test-e2e        Run E2E multi-node cluster tests (Docker Compose)"
	@echo "  integration-test-bootstrap  Run bootstrap integration tests in devcontainer"
	@echo "  integration-test-linearizability  Run porcupine linearizability tests in devcontainer"
	@echo ""
	@echo "Fuzz Testing:"
	@echo "  fuzz-all               Run all fuzz targets (FUZZ_TIME=60s)"
	@echo "  fuzz-proto             Fuzz proto codec round-trips"
	@echo "  fuzz-transport         Fuzz transport frame decoding"
	@echo "  fuzz-waldb             Fuzz WAL record decoding"
	@echo "  fuzz-seal              Fuzz AES-GCM encrypt/decrypt"
	@echo "  fuzz-session           Fuzz session encoding"
	@echo "    Flags: FUZZ_TIME=60s"
	@echo ""
	@echo "Benchmarks:"
	@echo "  bench-<pkg>            Run benchmarks for a specific package"
	@echo "    packages: proto, raft, waldb, seal, registry, engine, transport, quicraft,"
	@echo "              discovery, bootstrap, server, writemode"
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
	@echo "    packages: proto, config, queue, stopper, invariant, logdb,"
	@echo "              raft, waldb, seal, rsm, session, snapshot,"
	@echo "              registry, engine, transport, quicraft, errors, sm,"
	@echo "              discovery, bootstrap, server, writemode"
	@echo ""
	@echo "CI Pipeline:"
	@echo "  ci                     Run full CI pipeline (fmt→vet→lint→gosec→vuln→trivy→build→test)"
	@echo ""
	@echo "FIPS:"
	@echo "  build-fips             Build with GOFIPS140=v1.0.0 (PBKDF2-SHA256 KDF)"
	@echo ""
	@echo "Linting:"
	@echo "  lint                   Run golangci-lint"
	@echo "  vet                    Run go vet"
	@echo "  fmt                    Check gofmt formatting"
	@echo ""
	@echo "Security:"
	@echo "  gosec                  Run gosec security scanner"
	@echo "  vuln                   Run govulncheck vulnerability scanner"
	@echo "  trivy                  Run Trivy filesystem scan (CRITICAL,HIGH)"
	@echo "  trivy-image            Build and scan Docker image with Trivy"
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
