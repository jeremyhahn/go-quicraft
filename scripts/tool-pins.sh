# shellcheck shell=sh disable=SC2034 # read by make and sourced; no use here
# Tool pins: the one file every pinned tool version lives in (handbook
# devcontainer.md §2). The Makefile `include`s it and the devcontainer
# Dockerfile (build/docker/devcontainer/Dockerfile) sources it, so the host
# install targets, the toolchain image and the CI that runs in that image use
# the same versions.
#
# Format: plain KEY=value lines, no spaces, no quotes, so that both make and sh
# read them identically. Sourced, never executed.
#
# Every download carries its SHA-256. The `go install` tools need none: the Go
# checksum database verifies the module they are built from.

# The Go toolchain: the version go.mod names, pinned by index digest.
GO_IMAGE=golang:1.27.3-alpine3.24@sha256:f2c872bcee949b7be9c490efd02b48a8a2fbf947cdf533f1c58525ec9d8170fd

# The golangci-lint version. Every literal pin in the CI workflows and
# devcontainer Dockerfiles must match it (check-tool-versions enforces this).
GOLANGCI_VERSION=v2.14.0
# The workflow linter `make lint` runs over .github/workflows.
ACTIONLINT_VERSION=v1.7.12
GOSEC_VERSION=v2.29.0
# gosec is built against this x/tools: the v0.49.0 its go.mod names cannot read
# the export data (version 5) Go 1.27 writes, so every import fails to type-check.
XTOOLS_VERSION=v0.52.0
GOVULNCHECK_VERSION=v1.8.0

# trivy: a release archive, checked against these digests (upstream's
# trivy_<version>_checksums.txt) before it is unpacked.
TRIVY_VERSION=v0.75.0
TRIVY_SHA256_LINUX_AMD64=c6e65abddb348e25f10549df887045629cf28cc72453cd1c63acb717316b3f3f
TRIVY_SHA256_LINUX_ARM64=a1ee9f6ffb7d112b64ff726a2a0717c21175c1114361391f4a132956751a13b3
# The same two archives by release-asset id, the fallback when the release
# download URL fails: api.github.com/repos/aquasecurity/trivy/releases/assets/<id>.
# The digests above still check whatever either URL returns.
TRIVY_ASSET_ID_LINUX_AMD64=603335432
TRIVY_ASSET_ID_LINUX_ARM64=603335583
