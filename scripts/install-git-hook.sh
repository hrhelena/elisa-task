#!/usr/bin/env bash
# Installs scripts/git-hooks/stub as the git pre-commit hook (called by `make tools`).
#
# This replaces `pre-commit install`, which hard-codes one virtualenv's path into the hooks
# directory shared by all git worktrees. An existing hook that is not ours is moved to
# pre-commit.legacy (it is not chained), with a warning.
set -euo pipefail

root=$(git rev-parse --show-toplevel)
stub="$root/scripts/git-hooks/stub"
hooks_dir=$(git rev-parse --git-path hooks)
target="$hooks_dir/pre-commit"
# Both the stub and the earlier full hook carry this phrase, so either is upgraded in place.
marker='installed by `make tools`'

mkdir -p "$hooks_dir"

if [ -e "$target" ] && ! cmp -s "$stub" "$target"; then
  if grep -qF "$marker" "$target"; then
    echo "git pre-commit hook: updating an older version of the stub"
  else
    backup="$target.legacy"
    [ -e "$backup" ] && backup="$target.legacy.$(date +%Y%m%d%H%M%S)"
    mv "$target" "$backup"
    echo "WARNING: an existing git pre-commit hook was moved to $backup and will no longer run." >&2
  fi
fi

install -m 0755 "$stub" "$target"
echo "git pre-commit hook installed at $target"
