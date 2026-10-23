# Enron email metrics service

Go service that ingests the Enron maildir dataset into Postgres and exposes metrics over HTTP.
Full documentation lands with #14; architecture decisions are in [`Docs/ADR/`](Docs/ADR/README.md).

## Quick start

Prerequisites: Go (version in `go.mod`), Python 3 and Docker. All other tools are project-local and pinned.

```bash
make tools       # .venv with pinned pre-commit, git pre-commit hook, Go modules
make lint test   # golangci-lint + go vet, unit tests with -race
make build       # bin/enron-service
make help        # all targets
```

Run `make tools` in every checkout and git worktree before committing. The pre-commit hook lives in the
hooks directory that all worktrees share, but it uses the `.venv` of the worktree being committed to,
and refuses the commit (with a hint) if that `.venv` is missing.

Python dev tools are locked with hashes in `requirements-dev.txt`. To change them, edit `requirements-dev.in`
and run `make py-lock`.

## CI

GitHub Actions (`.github/workflows/ci.yml`) runs on every pull request and on pushes to `main`, using the same
Makefile targets: `lint` (all pre-commit hooks, including the `go mod tidy` and formatting checks, then `make lint`),
`test` (`make test` with `-race`; `coverage.out` is uploaded as an artifact) and `itest` (`make itest` with
testcontainers on the runner's Docker). A new push to a branch cancels that branch's older run.
