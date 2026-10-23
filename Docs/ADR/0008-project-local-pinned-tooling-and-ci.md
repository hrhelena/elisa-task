# 0008. Project-local pinned tooling, pre-commit hooks and GitHub Actions CI

- **Status:** Accepted
- **Date:** 2026-10-23
- **Author:** Helena Rivas (with product-owner agent)
- **Deciders:** Helena Rivas
- **Related:** [#1](https://github.com/hrhelena/elisa-task/issues/1), [#2](https://github.com/hrhelena/elisa-task/issues/2), [#13](https://github.com/hrhelena/elisa-task/issues/13), [0006](0006-cloud-native-deployment-with-kustomize-and-kind.md)

## Summary

All development tools are pinned and installed inside the project (never globally). Pre-commit hooks enforce formatting
and linting, and GitHub Actions runs lint, tests, integration tests and the image build on every PR.

## Context

- Several agents work in parallel worktrees; they need identical tool versions.
- `prompt.md` asks for pre-commit hooks for linting and formatting, and a CI pipeline.

## Decision

- **Go tools** are pinned with `tool` directives in `go.mod` and run with `go tool <name>`: `golangci-lint` v2,
  `goimports`, the migration tool if it has a CLI.
- **pre-commit** is pinned in `requirements-dev.txt` and installed into a project venv (`.venv/`) by `make tools`, which
  also runs `pre-commit install`.
- **Hooks:** `gofmt`/`goimports`, `golangci-lint run`, `go vet ./...`, a `go mod tidy` check, trailing whitespace,
  end-of-file, YAML checks. Hooks call the pinned tools through `go tool` (local `repo: local` hooks).
- **Makefile** is the single entry point: `tools`, `fmt`, `lint`, `test`, `itest`, `build`, `docker`, `compose-up`,
  `kind-up`, `deploy`.
- **CI (GitHub Actions)** on pull requests and pushes to `main`: `lint` (golangci-lint), `test` (`go test -race ./...`),
  `itest` (`go test -race -tags integration ./...` with testcontainers on the runner's Docker), `docker` (build the image),
  and manifest validation (`kubectl kustomize`). A kind smoke test is a nice-to-have.
- The Go version comes from `go.mod` (`actions/setup-go` with `go-version-file: go.mod`).

## Rationale

Pinned project-local tools give reproducible results across agents, the developer machine and CI, and keep the host clean.
`go tool` directives (Go 1.24+) avoid a separate `tools.go` and install step.

## Consequences

- Positive: `make tools && make lint test` works on a fresh clone with only Go, Python 3 and Docker installed.
- Negative: the first `make tools` takes a while; `go.mod` carries tool dependencies.
- Follow-ups: repo skeleton ticket, CI ticket.
