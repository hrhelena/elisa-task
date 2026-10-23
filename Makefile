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
# Container image (#11), overridable: `make docker IMAGE=... VERSION=...`.
IMAGE    ?= enron-service:dev
VERSION  ?= $(shell git describe --tags --always --dirty 2>/dev/null || echo dev)
REVISION ?= $(shell git rev-parse HEAD 2>/dev/null || echo unknown)
# Image timestamps come from the commit, not the wall clock, so rebuilding the same commit yields the same image ID.
# A wall-clock "created" label made every `make deploy` roll the pod (and kill an in-flight /fetch).
# SOURCE_DATE_EPOCH is also passed to BuildKit (image config timestamps), and --provenance=false drops the
# provenance attestation, whose per-build data would otherwise change the image ID (containerd image store).
SOURCE_DATE_EPOCH ?= $(shell git log -1 --format=%ct 2>/dev/null || echo 0)
CREATED  ?= $(shell TZ=UTC git log -1 --date=format-local:%Y-%m-%dT%H:%M:%SZ --format=%cd 2>/dev/null || echo unknown)
COMPOSE  ?= docker compose
# Host directory compose mounts at /data. compose-up creates it as the invoking user first: if compose's
# create_host_path made it, a rootful Linux daemon would create it root-owned and the app could not write to it.
DATASET_DIR ?= ./Dataset
# Swagger UI served at /docs (#54, ADR 0016): the single place its version is pinned. A unit test checks that
# internal/httpapi/swaggerui/VERSION matches. Bump with `make swagger-ui SWAGGER_UI_VERSION=x.y.z`.
SWAGGER_UI_VERSION ?= 5.34.0
# kind (#12): a dedicated cluster name, and its context passed explicitly to every kubectl call, so no other cluster
# is ever targeted. kind-up restores the previous current context after creating the cluster.
# kind and kubectl are host prerequisites.
KIND_CLUSTER ?= elisa-enron
KIND_CONTEXT := kind-$(KIND_CLUSTER)
# Opt-in (#40): mount the local dataset into kind read-only instead of using POST /fetch. Pass it to both kind-up
# (cluster created with the mount) and deploy (overlays/kind-local-dataset). 1 means Dataset/ of the main checkout
# (also from a worktree); a path means that directory, which must contain maildir/. Empty: unchanged behaviour.
LOCAL_DATASET ?=

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

.PHONY: swagger-ui
swagger-ui: ## Re-vendor Swagger UI at SWAGGER_UI_VERSION into internal/httpapi/swaggerui (needs network)
	scripts/update-swagger-ui.sh $(SWAGGER_UI_VERSION)

.PHONY: pre-commit
pre-commit: ## Run all pre-commit hooks on all files
	$(VENV)/bin/pre-commit run --all-files

.PHONY: clean
clean: ## Remove build outputs
	rm -rf bin/

##@ Containers and deployment

.PHONY: docker
docker: ## Build the container image $(IMAGE) (IMAGE and VERSION overridable)
	docker build \
		--build-arg VERSION=$(VERSION) \
		--build-arg REVISION=$(REVISION) \
		--build-arg CREATED=$(CREATED) \
		--build-arg SOURCE_DATE_EPOCH=$(SOURCE_DATE_EPOCH) \
		--provenance=false \
		-t $(IMAGE) .

.PHONY: docker-smoke
docker-smoke: docker ## Build the image, then smoke-test it and the compose stack (ports 18080/15432)
	IMAGE=$(IMAGE) scripts/docker-smoke.sh

.PHONY: compose-up
compose-up: ## Build the image and start Postgres plus the service (docker compose)
	mkdir -p "$(DATASET_DIR)"
	IMAGE=$(IMAGE) VERSION=$(VERSION) REVISION=$(REVISION) CREATED=$(CREATED) \
		DATASET_DIR="$(DATASET_DIR)" $(COMPOSE) up -d --build

.PHONY: compose-down
compose-down: ## Stop the docker compose stack (keeps the database volume)
	$(COMPOSE) down

.PHONY: compose-clean
compose-clean: ## Stop the docker compose stack and delete the database volume
	$(COMPOSE) down -v

.PHONY: k8s-validate
k8s-validate: ## Render the kustomize base and overlays, kubeconform them and assert the ticket's values
	scripts/k8s-validate.sh

.PHONY: kind-up
kind-up: ## Create the kind cluster $(KIND_CLUSTER) (no-op if it exists; LOCAL_DATASET=1 mounts Dataset/)
	@KIND_CLUSTER="$(KIND_CLUSTER)" LOCAL_DATASET="$(LOCAL_DATASET)" scripts/kind-up.sh

.PHONY: kind-down
kind-down: ## Delete the kind cluster $(KIND_CLUSTER) (no-op if it does not exist)
	@if kind get clusters 2>/dev/null | grep -qx "$(KIND_CLUSTER)"; then \
		kind delete cluster --name "$(KIND_CLUSTER)"; \
	else \
		echo "kind cluster $(KIND_CLUSTER) does not exist"; \
	fi

.PHONY: deploy
deploy: docker ## Build the image, load it into kind $(KIND_CLUSTER) and apply the kind overlay (LOCAL_DATASET=1: local Dataset/)
	IMAGE=$(IMAGE) KIND_CLUSTER=$(KIND_CLUSTER) LOCAL_DATASET="$(LOCAL_DATASET)" scripts/kind-deploy.sh

.PHONY: undeploy
undeploy: ## Delete the kind overlay's resources from $(KIND_CLUSTER), including the namespace and its volumes
	$(GO_TOOL) kustomize build deploy/k8s/overlays/kind | \
		kubectl --context "$(KIND_CONTEXT)" delete --ignore-not-found --wait=true -f -
	@# The cluster-scoped PersistentVolume of the LOCAL_DATASET variant (Retain: the host files are never touched).
	kubectl --context "$(KIND_CONTEXT)" delete persistentvolume enron-local-dataset --ignore-not-found --wait=true

# End-to-end test (#13) on its own throwaway cluster, so it never touches the KIND_CLUSTER used for development.
E2E_KIND_CLUSTER ?= elisa-enron-e2e

.PHONY: e2e-kind
e2e-kind: docker ## Build the image, then fetch, ingest and restart-test it on a throwaway kind cluster
	IMAGE=$(IMAGE) KIND_CLUSTER=$(E2E_KIND_CLUSTER) scripts/kind-e2e.sh

##@ Help

.PHONY: help
help: ## List available targets
	@awk 'BEGIN {FS = ":.*## "; printf "Usage: make <target>\n"} \
		/^##@/ {printf "\n%s\n", substr($$0, 5)} \
		/^[a-zA-Z0-9_-]+:.*## / {printf "  %-14s %s\n", $$1, $$2}' $(MAKEFILE_LIST)
