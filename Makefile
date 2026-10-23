# Single entry point for development, CI and deployment (ADR 0008).
# All tools are project-local and pinned: Go tools via `tool` directives in go.mod (`go tool ...`),
# pre-commit via requirements-dev.txt in .venv. Nothing is installed globally.

SHELL := bash
.SHELLFLAGS := -eu -o pipefail -c
.DEFAULT_GOAL := help

MODULE  := github.com/hrhelena/elisa-task
BINARY  := bin/enron-service
PYTHON  ?= python3
VENV    := .venv
# Dev tools are pure Go; building them without cgo keeps them independent of the host C toolchain
# (upstream golangci-lint releases are built the same way). Tests and the service build are unaffected.
GO_TOOL := CGO_ENABLED=0 go tool
# Keep pre-commit's own cache inside the repository (the git hook sets the same path).
export PRE_COMMIT_HOME := $(CURDIR)/$(VENV)/pre-commit-home
# Go files tracked or new (respects .gitignore, so other worktrees under .claude/ are never touched).
GO_FILES = $(shell git ls-files --cached --others --exclude-standard '*.go')

##@ Setup

.PHONY: tools
tools: $(VENV)/.installed ## Install project-local tools (.venv with pre-commit, git hook, Go modules)
	@# Worktree-aware replacement for `pre-commit install` (see scripts/git-hooks/pre-commit).
	hooks_dir=$$(git rev-parse --git-path hooks); mkdir -p "$$hooks_dir"; \
		install -m 0755 scripts/git-hooks/pre-commit "$$hooks_dir/pre-commit"; \
		echo "git pre-commit hook installed at $$hooks_dir/pre-commit"
	go mod download

$(VENV)/.installed: requirements-dev.txt
	$(PYTHON) -m venv $(VENV)
	$(VENV)/bin/pip install --quiet --disable-pip-version-check --no-cache-dir -r requirements-dev.txt
	touch $@

##@ Development

.PHONY: fmt
fmt: ## Format Go code (goimports with local prefix, gofmt -s)
	@files="$(GO_FILES)"; if [ -n "$$files" ]; then \
		$(GO_TOOL) goimports -w -local $(MODULE) $$files; \
		gofmt -s -w $$files; \
	fi

.PHONY: lint
lint: ## Run golangci-lint and go vet
	$(GO_TOOL) golangci-lint run ./...
	go vet -tags integration ./...

.PHONY: test
test: ## Run unit tests with the race detector
	go test -race -count=1 ./...

.PHONY: itest
itest: ## Run unit and integration tests (needs Docker for testcontainers)
	go test -race -count=1 -tags integration ./...

.PHONY: build
build: ## Build a static binary into bin/enron-service
	CGO_ENABLED=0 go build -trimpath -o $(BINARY) ./cmd/enron-service

.PHONY: pre-commit
pre-commit: ## Run all pre-commit hooks on all files
	$(VENV)/bin/pre-commit run --all-files

.PHONY: clean
clean: ## Remove build outputs
	rm -rf bin/

##@ Containers and deployment

define not_implemented
	@echo "make $@: not implemented yet, see #$(1) (https://github.com/hrhelena/elisa-task/issues/$(1))" >&2
	@exit 1
endef

.PHONY: docker
docker: ## Build the container image (#11)
	$(call not_implemented,11)

.PHONY: compose-up
compose-up: ## Start the docker compose dev stack (#11)
	$(call not_implemented,11)

.PHONY: compose-down
compose-down: ## Stop the docker compose dev stack (#11)
	$(call not_implemented,11)

.PHONY: kind-up
kind-up: ## Create the local kind cluster (#12)
	$(call not_implemented,12)

.PHONY: kind-down
kind-down: ## Delete the local kind cluster (#12)
	$(call not_implemented,12)

.PHONY: deploy
deploy: ## Deploy to the current kube context with kustomize (#12)
	$(call not_implemented,12)

##@ Help

.PHONY: help
help: ## List available targets
	@awk 'BEGIN {FS = ":.*## "; printf "Usage: make <target>\n"} \
		/^##@/ {printf "\n%s\n", substr($$0, 5)} \
		/^[a-zA-Z0-9_-]+:.*## / {printf "  %-14s %s\n", $$1, $$2}' $(MAKEFILE_LIST)
