#!/usr/bin/env bash
# Smoke test for the container image and the docker compose dev loop (#11). Run via `make docker-smoke`
# (which builds the image first) and in CI's `docker` job. Each check is named after the Gherkin scenario
# it covers. The stack runs as its own compose project on non-default host ports, so it never touches a
# running dev stack, and is always removed (with its volume) on exit.
#
# Environment:
#   IMAGE          image to test (default enron-service:dev); must already exist locally
#   APP_PORT       host port for the service (default 18080)
#   POSTGRES_PORT  host port for Postgres (default 15432)
#   DATASET_DIR    dataset dir to mount (default: a fresh empty temp dir)
#   MAX_IMAGE_MB   image size limit in MB (default 40)
set -euo pipefail

IMAGE=${IMAGE:-enron-service:dev}
export APP_PORT=${APP_PORT:-18080}
export POSTGRES_PORT=${POSTGRES_PORT:-15432}
MAX_IMAGE_MB=${MAX_IMAGE_MB:-40}
PROJECT=enron-smoke
BASE=http://localhost:${APP_PORT}

cleanup_dir=""
if [ -z "${DATASET_DIR:-}" ]; then
  DATASET_DIR=$(mktemp -d)
  cleanup_dir=$DATASET_DIR
fi
export DATASET_DIR IMAGE

compose() { docker compose -p "$PROJECT" "$@"; }

failures=0
pass() { echo "PASS  $*"; }
fail() { echo "FAIL  $*" >&2; failures=$((failures + 1)); }
skip() { echo "SKIP  $*"; }

cleanup() {
  if [ "$failures" -gt 0 ]; then
    echo "--- app logs ---" >&2
    compose logs app >&2 || true
  fi
  compose down -v --remove-orphans >/dev/null 2>&1 || true
  if [ -n "$cleanup_dir" ]; then rm -rf "$cleanup_dir"; fi
}
trap cleanup EXIT

echo "== Scenario: Dataset not baked into the image"
if grep -qxE '/?Dataset/?' .dockerignore; then
  pass ".dockerignore excludes Dataset/"
else
  fail ".dockerignore does not exclude Dataset/"
fi

echo "== Scenario: Image builds and runs as non-root ($IMAGE)"
user=$(docker image inspect "$IMAGE" --format '{{.Config.User}}')
if [ "$user" = "65532:65532" ]; then pass "user is $user"; else fail "user is '$user', want 65532:65532"; fi
size=$(docker image inspect "$IMAGE" --format '{{.Size}}')
size_mb=$((size / 1000 / 1000))
if [ "$size" -lt $((MAX_IMAGE_MB * 1000 * 1000)) ]; then
  pass "size is ${size_mb} MB (< ${MAX_IMAGE_MB} MB)"
else
  fail "size is ${size_mb} MB, want < ${MAX_IMAGE_MB} MB"
fi

echo "== Scenario: Compose dev loop end to end (app on $BASE)"
compose down -v --remove-orphans >/dev/null 2>&1 || true
# compose prints progress to stderr; keep it to check the start order below.
up_log=$(compose --progress plain up -d --no-build --quiet-pull 2>&1) || { echo "$up_log" >&2; fail "compose up"; exit 1; }
echo "$up_log"
ready=""
for _ in $(seq 1 60); do
  if [ "$(curl -s -o /dev/null -w '%{http_code}' --max-time 2 "$BASE/readyz")" = 200 ]; then ready=1; break; fi
  sleep 1
done
if [ -n "$ready" ]; then pass "GET /readyz returned 200 within 60s"; else fail "GET /readyz not 200 within 60s"; fi
for path in /healthz /metrics /status; do
  code=$(curl -s -o /dev/null -w '%{http_code}' --max-time 5 "$BASE$path")
  if [ "$code" = 200 ]; then pass "GET $path returned 200"; else fail "GET $path returned $code"; fi
done
start_body=$(curl -s -w '\n%{http_code}' --max-time 15 -X POST "$BASE/start")
code=$(echo "$start_body" | tail -n1)
if [ -d "$DATASET_DIR/maildir" ]; then
  if [ "$code" = 202 ]; then pass "POST /start returned 202"; else fail "POST /start returned $code, want 202"; fi
else
  # No dataset mounted (the default): the endpoint must say so instead of starting a run.
  if [ "$code" = 409 ] && echo "$start_body" | grep -q '"code":"dataset_missing"'; then
    pass "POST /start without a maildir returned 409 dataset_missing"
  else
    fail "POST /start without a maildir returned $code, want 409 dataset_missing"
  fi
fi

echo "== Scenario: App waits for Postgres"
condition=$(compose config app | grep -A2 '^    depends_on:' | grep -A1 '^      postgres:' |
  sed -n 's/^ *condition: *//p' || true)
if [ "$condition" = service_healthy ]; then
  pass "app depends_on postgres with condition service_healthy"
else
  fail "app depends_on postgres condition is '${condition:-unset}', want service_healthy"
fi
healthy_line=$(echo "$up_log" | grep -n 'postgres-1 *Healthy' | head -n1 | cut -d: -f1)
starting_line=$(echo "$up_log" | grep -n 'app-1 *Start' | head -n1 | cut -d: -f1)
if [ -n "$healthy_line" ] && [ -n "$starting_line" ] && [ "$healthy_line" -lt "$starting_line" ]; then
  pass "compose started the app only after postgres reported Healthy"
else
  fail "compose did not report postgres Healthy before starting the app"
fi
if compose logs --no-log-prefix app | grep -q '"level":"ERROR"'; then
  fail "app logged errors"
else
  pass "app logged no errors"
fi

if [ "$failures" -gt 0 ]; then
  echo "$failures check(s) failed" >&2
  exit 1
fi
echo "all checks passed"
