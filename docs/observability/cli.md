# Observability: CLI

> **Status: NORMATIVE** · Scope: go-quicraft metrics, logging and event
> notifications · Last verified: 2026-10-13
> Conflicts with this document are defects. Fix them here first.

No CLI surface: go-quicraft is a library and ships no binaries (there is no
`cmd/` directory, and `make build` runs `go build ./...`), so this feature
contributes no `CommandDefinition` to any parity suite.

The library serves no scrape endpoint and writes no log file. It hands the
embedder a `prometheus.Registry` and an `*slog.Logger`; a `--metrics-port` flag
here would be the library opening a socket in someone else's process. The
consuming surface is the Go API in [`api.md`](api.md), and task-level wiring is
in [`user.md`](user.md).

Tests for this feature run under `make test-quicraft`.
