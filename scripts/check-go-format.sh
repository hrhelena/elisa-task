#!/usr/bin/env bash
# Fails (exit 1) if any of the given Go files is not formatted. Used by pre-commit.
#   check-go-format.sh gofmt     FILE...   -> gofmt -s
#   check-go-format.sh goimports FILE...   -> goimports with the module's local prefix
# Tools come from go.mod (`go tool ...`), so versions are pinned.
set -euo pipefail

tool=${1:?usage: check-go-format.sh gofmt|goimports FILE...}
shift
[ "$#" -gt 0 ] || exit 0

case "$tool" in
  gofmt) out=$(gofmt -s -l "$@") ;;
  goimports) out=$(CGO_ENABLED=0 go tool goimports -l -local github.com/hrhelena/elisa-task "$@") ;;
  *) echo "unknown tool: $tool" >&2; exit 2 ;;
esac

if [ -n "$out" ]; then
  echo "$tool: these files need formatting (run 'make fmt'):" >&2
  echo "$out" >&2
  exit 1
fi
