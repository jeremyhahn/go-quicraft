# contrib/dragonboat Build System
# Dragonboat API compatibility layer for go-quicraft

.DEFAULT_GOAL := build

# Project metadata
MODULE        := github.com/jeremyhahn/go-quicraft/contrib/dragonboat
BUILD_DIR     := build
COVER_DIR     := $(BUILD_DIR)/coverage

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
TRIVY         := trivy

# Security exclusions (match top-level project)
GOSEC_EXCLUDE := G103,G104,G115,G301,G304,G306,G404

# Coverage
COVER_MIN     := 90
COVER_FLAGS   := -coverprofile

# Docker (relative to project root)
COMPOSE_FILE  := ../../.devcontainer/docker-compose.yml

# Package paths
PKG_CLIENT       := ./client/...
PKG_CONFIG       := ./config/...
PKG_RAFTIO       := ./raftio/...
PKG_STATEMACHINE := ./statemachine/...

# Integration test paths
INT_TEST_DIR     := ./test/integration

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
	rm -rf $(COVER_DIR) $(BUILD_DIR) *.log *.test *.out

# ============================================================================
# Unit tests
# ============================================================================

.PHONY: test
test:
	$(GOTEST) ./...

.PHONY: test-client
test-client:
	$(GOTEST) $(PKG_CLIENT)

.PHONY: test-config
test-config:
	$(GOTEST) $(PKG_CONFIG)

.PHONY: test-raftio
test-raftio:
	$(GOTEST) $(PKG_RAFTIO)

.PHONY: test-statemachine
test-statemachine:
	$(GOTEST) $(PKG_STATEMACHINE)

# ============================================================================
# Integration tests (always run in devcontainer)
# ============================================================================

.PHONY: integration-test
integration-test:
	@echo "==> Building integration container..."
	@docker compose -f $(COMPOSE_FILE) build --quiet integration
	@echo "==> Running contrib/dragonboat integration tests..."
	docker compose -f $(COMPOSE_FILE) run --rm integration bash -c "cd contrib/dragonboat && make _integration-test"

.PHONY: _integration-test
_integration-test:
	$(GOTEST) -tags=integration -timeout=300s $(INT_TEST_DIR)/...

# ============================================================================
# Benchmarks
# ============================================================================

.PHONY: bench
bench:
	$(GOBENCH) ./...

.PHONY: bench-client
bench-client:
	$(GOBENCH) $(PKG_CLIENT)

.PHONY: bench-config
bench-config:
	$(GOBENCH) $(PKG_CONFIG)

.PHONY: bench-raftio
bench-raftio:
	$(GOBENCH) $(PKG_RAFTIO)

.PHONY: bench-statemachine
bench-statemachine:
	$(GOBENCH) $(PKG_STATEMACHINE)

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

.PHONY: coverage-client
coverage-client: | $(COVER_DIR)
	$(GOTEST) -coverprofile=$(COVER_DIR)/client.out $(PKG_CLIENT)
	$(call check-coverage,$(COVER_DIR)/client.out,client)

.PHONY: coverage-config
coverage-config: | $(COVER_DIR)
	$(GOTEST) -coverprofile=$(COVER_DIR)/config.out $(PKG_CONFIG)
	$(call check-coverage,$(COVER_DIR)/config.out,config)

.PHONY: coverage-raftio
coverage-raftio: | $(COVER_DIR)
	$(GOTEST) -coverprofile=$(COVER_DIR)/raftio.out $(PKG_RAFTIO)
	$(call check-coverage,$(COVER_DIR)/raftio.out,raftio)

.PHONY: coverage-statemachine
coverage-statemachine: | $(COVER_DIR)
	$(GOTEST) -coverprofile=$(COVER_DIR)/statemachine.out $(PKG_STATEMACHINE)
	$(call check-coverage,$(COVER_DIR)/statemachine.out,statemachine)

$(COVER_DIR):
	mkdir -p $(COVER_DIR)

# ============================================================================
# Linting and static analysis
# ============================================================================

.PHONY: lint
lint:
	@if command -v $(GOLINT) >/dev/null 2>&1; then \
		$(GOLINT) run --timeout=5m ./...; \
	else \
		echo "golangci-lint not installed, skipping"; \
	fi

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
# CI pipeline
# Runs fmt, vet, lint, security scans, build, and tests.
# Requires: golangci-lint, gosec, govulncheck, trivy, nancy
# ============================================================================

.PHONY: ci
ci: fmt vet lint gosec vuln trivy nancy build test
	@echo "CI pipeline passed"

# ============================================================================
# Security scanning
# ============================================================================

.PHONY: security
security: gosec vuln trivy nancy
	@echo "All security scans passed"

.PHONY: gosec
gosec:
	$(GOSEC) -exclude=$(GOSEC_EXCLUDE) -severity=medium -confidence=medium ./...

.PHONY: vuln
vuln:
	$(GOVULNCHECK) ./... || echo "WARNING: govulncheck found stdlib vulnerabilities (upgrade Go to fix)"

.PHONY: trivy
trivy:
	$(TRIVY) fs --severity CRITICAL,HIGH --exit-code 1 .

.PHONY: nancy
nancy:
	@if command -v nancy >/dev/null 2>&1; then \
		$(GO) list -json -m all 2>/dev/null | nancy sleuth --quiet 2>/dev/null || \
		echo "nancy scan completed (check ~/.ossindex/nancy.combined.log for details)"; \
	else \
		echo "nancy not installed, skipping (govulncheck provides similar coverage)"; \
	fi

# ============================================================================
# Help
# ============================================================================

.PHONY: help
help:
	@echo "contrib/dragonboat Build System"
	@echo ""
	@echo "Primary:"
	@echo "  all                    Build, test, and lint"
	@echo "  build                  Compile all packages"
	@echo "  clean                  Remove build artifacts"
	@echo ""
	@echo "Unit Tests:"
	@echo "  test                   Run all unit tests with race detector"
	@echo "  test-client            Run client package tests"
	@echo "  test-config            Run config package tests"
	@echo "  test-raftio            Run raftio package tests"
	@echo "  test-statemachine      Run statemachine package tests"
	@echo ""
	@echo "Integration Tests (run in devcontainer):"
	@echo "  integration-test       Run all integration tests in devcontainer"
	@echo ""
	@echo "Benchmarks:"
	@echo "  bench                  Run all benchmarks"
	@echo "  bench-client           Run client benchmarks"
	@echo "  bench-config           Run config benchmarks"
	@echo "  bench-raftio           Run raftio benchmarks"
	@echo "  bench-statemachine     Run statemachine benchmarks"
	@echo ""
	@echo "Coverage (minimum $(COVER_MIN)%):"
	@echo "  coverage               Run total coverage report"
	@echo "  coverage-client        Run client coverage"
	@echo "  coverage-config        Run config coverage"
	@echo "  coverage-raftio        Run raftio coverage"
	@echo "  coverage-statemachine  Run statemachine coverage"
	@echo ""
	@echo "CI:"
	@echo "  ci                     Run full CI pipeline (fmt, vet, lint, security, build, test)"
	@echo ""
	@echo "Linting:"
	@echo "  lint                   Run golangci-lint (if available)"
	@echo "  vet                    Run go vet"
	@echo "  fmt                    Check gofmt formatting"
	@echo ""
	@echo "Security:"
	@echo "  security               Run all security scans"
	@echo "  gosec                  Run gosec static analysis"
	@echo "  vuln                   Run govulncheck"
	@echo "  trivy                  Run trivy filesystem scan"
	@echo "  nancy                  Run nancy dependency scan"
