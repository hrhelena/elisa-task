# Single entry point for development, CI and deployment (ADR 0008).
# All tools are project-local and pinned: Go tools via `tool` directives in go.mod (`go tool ...`),
# pre-commit via the hashed lock requirements-dev.txt in .venv. Nothing is installed globally.

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
# Tracked files deleted from the working tree but not yet staged are skipped.
GO_FILES = $(shell git ls-files --cached --others --exclude-standard '*.go' | while read -r f; do [ -f "$$f" ] && echo "$$f"; done || true)
# pip-tools is only used by `make py-lock`, in a throwaway venv under the gitignored .tools/.
PIP_TOOLS_VERSION := 7.6.2
LOCK_VENV := .tools/pip-tools
PIP_FLAGS := --quiet --disable-pip-version-check --no-cache-dir
# Extra `go test` flags, e.g. `make test TEST_FLAGS=-coverprofile=coverage.out` (CI uses this).
TEST_FLAGS ?=

##@ Setup

.PHONY: tools
tools: $(VENV)/.installed ## Install project-local tools (.venv with pre-commit, git hook, Go modules)
	scripts/install-git-hook.sh
	go mod download

# requirements-dev.txt is the fully pinned, hashed lock. Rebuild .venv from scratch whenever it changes,
# so no package from an older lock survives.
$(VENV)/.installed: requirements-dev.txt
	rm -rf $(VENV)
	$(PYTHON) -m venv $(VENV)
	$(VENV)/bin/pip install $(PIP_FLAGS) --require-hashes -r requirements-dev.txt
	touch $@

.PHONY: py-lock
py-lock: ## Re-resolve requirements-dev.in into the hashed lock requirements-dev.txt
	rm -rf $(LOCK_VENV)
	$(PYTHON) -m venv $(LOCK_VENV)
	$(LOCK_VENV)/bin/pip install $(PIP_FLAGS) pip-tools==$(PIP_TOOLS_VERSION)
	CUSTOM_COMPILE_COMMAND="make py-lock" PIP_NO_CACHE_DIR=1 $(LOCK_VENV)/bin/pip-compile --quiet \
		--generate-hashes --allow-unsafe --strip-extras --no-emit-index-url --upgrade \
		--cache-dir $(LOCK_VENV)/cache --output-file requirements-dev.txt requirements-dev.in
	rm -rf $(LOCK_VENV)

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
	go test -race -count=1 $(TEST_FLAGS) ./...

.PHONY: itest
itest: ## Run unit and integration tests (needs Docker for testcontainers)
	go test -race -count=1 -tags integration $(TEST_FLAGS) ./...

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
