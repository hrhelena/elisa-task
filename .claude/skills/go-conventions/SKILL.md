---
name: go-conventions
description: Go coding, testing and tooling conventions for this repository. Use when writing or reviewing Go code here.
paths: ["**/*.go", "go.mod", "Makefile", ".golangci.yml", ".pre-commit-config.yaml"]
---

# Go conventions

## Layout
```
cmd/enron-service/       main: config, wiring, graceful shutdown
internal/config/         env-based config (defaults that run locally)
internal/httpapi/        handlers, routing (net/http ServeMux with method patterns), middleware
internal/ingest/         maildir walker, parser, ingestion orchestration
internal/store/          Store interface + postgres implementation + migrations
internal/fetch/          dataset download + extraction
deploy/                  k8s manifests (kustomize base + overlays), kind config
Docs/ADR/                architecture decision records
```
Interfaces are defined where they are consumed, and kept small. Keep `internal/` packages free of `main`-level globals.

## Code
- Go version is whatever `go.mod` says. Use the stdlib first: `net/http` routing, `log/slog` JSON logging, `context` everywhere.
- Prefer `pgx/v5` (+ `pgxpool`) and embedded SQL migrations (`golang-migrate` or `goose`; pick whichever the ADR says).
- Errors: wrap with `fmt.Errorf("doing x: %w", err)`, and use sentinel errors or `errors.Is/As`. Never `panic` outside `main`.
- Malformed data is never fatal: count it, log it at a sampled rate, and continue.
- Every goroutine has an owner and a way to stop (context). No leaks: tests may use `go.uber.org/goleak`.
- Configuration comes from env vars, each with a sensible local default, and is documented in the README.

## Tooling (project-local, pinned; no global installs)
- Dev tools are pinned via `tool` directives in `go.mod` (for example `go get -tool github.com/golangci/golangci-lint/v2/cmd/golangci-lint@vX`)
  and run with `go tool <name>`.
- pre-commit is pinned in a project venv (`make tools` creates `.venv` with a pinned `pre-commit`). Hooks run gofmt/goimports,
  golangci-lint, go vet, go mod tidy check, trailing whitespace and yaml checks.
- The `Makefile` is the single entry point: `make tools fmt lint test itest build docker kind-up deploy`.

## Tests
- Unit tests: table-driven, with `t.Parallel()` where safe, `testing` + `github.com/stretchr/testify/require` only if already in use.
- Integration tests: in `*_integration_test.go` behind the `//go:build integration` tag, using testcontainers-go for Postgres.
  Run them with `make itest`.
- Use `-race` in CI. Test fixtures (small maildir samples, including malformed ones) go in `testdata/`.
- Name tests after the Gherkin scenario they cover where practical.
