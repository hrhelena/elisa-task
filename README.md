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
