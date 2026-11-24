FROM golang:1.27.4-bookworm

# Install build, test, and debugging dependencies.
RUN apt-get update && apt-get install -y --no-install-recommends \
    make \
    git \
    bc \
    curl \
    net-tools \
    iproute2 \
    iputils-ping \
    && rm -rf /var/lib/apt/lists/*

# Install CI tooling. The golangci-lint version must equal GOLANGCI_VERSION in
# the Makefile; `make check-tool-versions` (part of `make ci`) asserts it.
RUN go install github.com/golangci/golangci-lint/v2/cmd/golangci-lint@v2.14.0 && \
    go install github.com/securego/gosec/v2/cmd/gosec@latest && \
    go install golang.org/x/vuln/cmd/govulncheck@latest && \
    go install github.com/sonatype-nexus-community/nancy@latest && \
    curl -sfL https://raw.githubusercontent.com/aquasecurity/trivy/main/contrib/install.sh | sh -s -- -b /usr/local/bin

WORKDIR /app

# Download modules in their own layer; forge credentials come from the optional gomodauth BuildKit secret, never a layer (kept identical across images, see test/buildgates).
COPY go.mod go.sum* ./
RUN --mount=type=secret,id=gomodauth \
    set -eu; \
    if [ -s /run/secrets/gomodauth ]; then \
      set -a; . /run/secrets/gomodauth; set +a; \
      echo "==> go mod download: Gitea module credentials mounted"; \
    else \
      echo "==> go mod download: no module credentials mounted; github.com/jeremyhahn/* resolves from the public proxy only"; \
    fi; \
    go mod download

# Copy the full project source tree.
COPY . .

# Entrypoint configures QUIC UDP buffer sizes at container startup.
COPY .devcontainer/entrypoint.sh /usr/local/bin/entrypoint.sh
ENTRYPOINT ["/usr/local/bin/entrypoint.sh"]

# Default command runs all integration tests.
CMD ["make", "_integration-test"]
