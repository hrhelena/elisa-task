#!/usr/bin/env bash
# End-to-end test on a throwaway kind cluster (#13); run by `make e2e-kind` after `make docker`, and by the CI job
# e2e-kind. It exercises the real deployment path and the whole API against a small generated dataset:
#   1. Generates a fixture maildir (46 files: 44 messages from 12 senders, 1 empty file, 1 binary file) and packs it
#      as a tar.gz with the same layout as the real archive (maildir/<mailbox>/<folder>/<n>.).
#   2. Creates the kind cluster $KIND_CLUSTER (`make kind-up`) and deploys with scripts/kind-deploy.sh, as
#      `make deploy` does.
#   3. Serves the tarball from an in-cluster busybox httpd (the tarball is a ConfigMap) and points the service's
#      DATASET_URL at it, so POST /fetch downloads and extracts it like the real 443 MB archive, without the internet.
#   4. Asserts, through a kubectl port-forward:
#      - POST /fetch: 202, then GET /status .dataset.state reaches "ready" (an empty volume before);
#      - POST /start: 202, then GET /status reaches finished=true with exact counts, skip reasons and field errors;
#      - GET /top-senders: the expected top 10, in order (ties by address), and all 12 with ?limit=12;
#      - deleting the enron-service pod: once the new pod is ready, /status reports the same completed run and
#        /top-senders is unchanged;
#      - a second POST /start: a new run that completes with the same totals and the same /top-senders.
# On failure it prints the namespace's resources, pod descriptions, container logs and events.
# Always targets the context kind-$KIND_CLUSTER explicitly, never the current kube context. It refuses to run on an
# existing cluster (the counts are exact, so the cluster must start empty) and deletes the cluster it created on
# exit unless E2E_KEEP_CLUSTER=1.
set -euo pipefail

cd "$(dirname "$0")/.."
export CGO_ENABLED=0

KIND_CLUSTER=${KIND_CLUSTER:-elisa-enron-e2e}
CONTEXT="kind-$KIND_CLUSTER"
IMAGE=${IMAGE:-enron-service:dev}
NAMESPACE=enron
E2E_PORT=${E2E_PORT:-18090}
E2E_KEEP_CLUSTER=${E2E_KEEP_CLUSTER:-0}
# Upper bounds for each wait; the fixture fetches and ingests in seconds.
WAIT_TIMEOUT=${WAIT_TIMEOUT:-120}
ROLLOUT_TIMEOUT=${ROLLOUT_TIMEOUT:-180s}
# Pinned like every other image (ADR 0008): busybox's httpd serves the fixture inside the cluster.
FIXTURE_SERVER_IMAGE=busybox:1.37.0@sha256:bdf57e528e45e4433820e045b29b4597825a1c9e38353532d90a01445013f82e
FIXTURE_NAME=enron-fixture.tar.gz
FIXTURE_URL="http://e2e-fixture-server.$NAMESPACE.svc.cluster.local/$FIXTURE_NAME"
APP_SELECTOR=app.kubernetes.io/name=enron-service
BASE="http://127.0.0.1:$E2E_PORT"

kc() { kubectl --context "$CONTEXT" "$@"; }
log() { printf '\n==> %s\n' "$*"; }
fail() {
	echo "FAIL: $*" >&2
	exit 1
}

for bin in docker kind kubectl curl jq tar gzip; do
	command -v "$bin" >/dev/null || fail "$bin not found on PATH (prerequisite, see README)"
done
if kind get clusters 2>/dev/null | grep -qx "$KIND_CLUSTER"; then
	fail "kind cluster '$KIND_CLUSTER' already exists; the e2e needs an empty cluster. Delete it with" \
		"'make kind-down KIND_CLUSTER=$KIND_CLUSTER' or pick another KIND_CLUSTER."
fi

work=.tools/kind-e2e
rm -rf "$work"
mkdir -p "$work"
pf_pid=""
created=0

diagnostics() {
	log "diagnostics (context $CONTEXT, namespace $NAMESPACE)"
	kc -n "$NAMESPACE" get all,pvc,configmap -o wide || true
	kc -n "$NAMESPACE" describe pods || true
	local pod
	for pod in $(kc -n "$NAMESPACE" get pods -o name 2>/dev/null); do
		echo "--- logs $pod"
		kc -n "$NAMESPACE" logs "$pod" --all-containers --tail=200 || true
		echo "--- previous logs $pod (if it restarted)"
		kc -n "$NAMESPACE" logs "$pod" --all-containers --previous --tail=200 2>/dev/null || true
	done
	echo "--- events"
	kc -n "$NAMESPACE" get events --sort-by=.lastTimestamp || true
	echo "--- port-forward log"
	cat "$work/port-forward.log" 2>/dev/null || true
}

cleanup() {
	local rc=$?
	if [ -n "$pf_pid" ]; then
		kill "$pf_pid" 2>/dev/null || true
	fi
	if [ "$rc" -ne 0 ] && [ "$created" -eq 1 ]; then
		diagnostics
	fi
	if [ "$created" -eq 1 ]; then
		if [ "$E2E_KEEP_CLUSTER" = 1 ]; then
			echo "kind-e2e: keeping cluster $KIND_CLUSTER (delete it with: make kind-down KIND_CLUSTER=$KIND_CLUSTER)"
		else
			kind delete cluster --name "$KIND_CLUSTER" || true
		fi
	fi
	if [ "$rc" -eq 0 ]; then
		echo "kind-e2e: PASS"
	else
		echo "kind-e2e: FAIL (exit $rc)" >&2
	fi
	exit "$rc"
}
trap cleanup EXIT

# --- Fixture -------------------------------------------------------------------------------------------------------
# Senders in the expected /top-senders order: count descending, then address ascending (ADR 0011). Every message
# from a sender goes into that sender's mailbox; some From headers use a display name and mixed case, which the
# lenient normalization (ADR 0012) lowercases into the same sender.
EXPECTED_SENDERS=(
	"jeff.dasovich@enron.com 9 dasovich-j"
	"phillip.allen@enron.com 7 allen-p"
	"kay.mann@enron.com 5 mann-k"
	"eric.bass@enron.com 4 bass-e"
	"sean.crandell@enron.com 4 crandell-s"
	"sara.shackleton@enron.com 3 shackleton-s"
	"tana.jones@enron.com 3 jones-t"
	"kate.symes@enron.com 2 symes-k"
	"vince.kaminski@enron.com 2 kaminski-v"
	"chris.germany@enron.com 1 germany-c"
	"debra.perlingiere@enron.com 1 perlingiere-d"
	"john.arnold@enron.com 1 arnold-j"
)
# Plus 2 stored messages without a usable sender (field errors, excluded from /top-senders) and 2 malformed files.
EXPECTED_PROCESSED=44
EXPECTED_SKIPPED=2
EXPECTED_FILES_SEEN=46

# message <path> <from-header-line> <subject>: writes one Enron-style message.
message() {
	mkdir -p "$(dirname "$1")"
	{
		printf 'Message-ID: <%s.JavaMail.evans@thyme>\r\n' "$(echo "$1" | tr '/.' '--')"
		printf 'Date: Mon, 14 May 2001 16:39:00 -0700 (PDT)\r\n'
		[ -n "$2" ] && printf '%s\r\n' "$2"
		printf 'To: tim.belden@enron.com\r\n'
		printf 'Subject: %s\r\n' "$3"
		printf 'Mime-Version: 1.0\r\n'
		printf 'Content-Type: text/plain; charset=us-ascii\r\n'
		printf 'Content-Transfer-Encoding: 7bit\r\n'
		printf '\r\n'
		printf 'Body of %s.\r\n' "$3"
	} >"$1"
}

generate_fixture() {
	local dir=$1 line email count mailbox i from
	for line in "${EXPECTED_SENDERS[@]}"; do
		read -r email count mailbox <<<"$line"
		for ((i = 1; i <= count; i++)); do
			from="From: $email"
			# Every other message: display name and an upper-cased address.
			if ((i % 2 == 0)); then
				from="From: \"${mailbox%-*}\" <$(echo "$email" | tr '[:lower:]' '[:upper:]')>"
			fi
			message "$dir/maildir/$mailbox/sent_items/$i." "$from" "$email message $i"
		done
	done
	# Stored, but no sender: field_errors.missing_sender and field_errors.invalid_sender.
	message "$dir/maildir/allen-p/inbox/1." "" "no From header"
	message "$dir/maildir/mann-k/inbox/1." "From: undisclosed-recipients" "unusable From header"
	# Malformed: skipped with empty_file and not_an_email, never fatal (ADR 0007).
	mkdir -p "$dir/maildir/bass-e/inbox"
	: >"$dir/maildir/bass-e/inbox/1."
	printf '\211PNG\r\n\032\n\000\000\000\rIHDR\000\000\001\000' >"$dir/maildir/bass-e/inbox/2."

	local files
	files=$(find "$dir/maildir" -type f | wc -l | tr -d ' ')
	[ "$files" -eq "$EXPECTED_FILES_SEEN" ] || fail "fixture has $files files, expected $EXPECTED_FILES_SEEN"
}

log "generating the fixture dataset"
generate_fixture "$work/fixture"
# ustar (no pax or xattr records) and COPYFILE_DISABLE (no macOS ._ files): the same archive from GNU and BSD tar.
COPYFILE_DISABLE=1 tar --format ustar -C "$work/fixture" -cf - maildir | gzip -9n >"$work/$FIXTURE_NAME"
ls -l "$work/$FIXTURE_NAME"

# --- Cluster and deployment ----------------------------------------------------------------------------------------
log "creating kind cluster $KIND_CLUSTER"
created=1
make --no-print-directory kind-up KIND_CLUSTER="$KIND_CLUSTER"

log "deploying $IMAGE (scripts/kind-deploy.sh)"
IMAGE="$IMAGE" KIND_CLUSTER="$KIND_CLUSTER" ROLLOUT_TIMEOUT="$ROLLOUT_TIMEOUT" scripts/kind-deploy.sh

log "serving the fixture in-cluster at $FIXTURE_URL"
kc -n "$NAMESPACE" create configmap e2e-fixture --from-file="$FIXTURE_NAME=$work/$FIXTURE_NAME" \
	--dry-run=client -o yaml | kc apply -f -
# The namespace enforces the "restricted" Pod Security profile, so the server runs as non-root with no capabilities.
kc apply -f - <<EOF
apiVersion: apps/v1
kind: Deployment
metadata:
  name: e2e-fixture-server
  namespace: $NAMESPACE
  labels:
    app.kubernetes.io/name: e2e-fixture-server
spec:
  replicas: 1
  selector:
    matchLabels:
      app.kubernetes.io/name: e2e-fixture-server
  template:
    metadata:
      labels:
        app.kubernetes.io/name: e2e-fixture-server
    spec:
      automountServiceAccountToken: false
      securityContext:
        runAsNonRoot: true
        runAsUser: 65532
        runAsGroup: 65532
        seccompProfile:
          type: RuntimeDefault
      containers:
        - name: httpd
          image: $FIXTURE_SERVER_IMAGE
          command: ["httpd", "-f", "-v", "-p", "8080", "-h", "/www"]
          ports:
            - name: http
              containerPort: 8080
          readinessProbe:
            tcpSocket:
              port: http
            periodSeconds: 2
          securityContext:
            allowPrivilegeEscalation: false
            readOnlyRootFilesystem: true
            capabilities:
              drop: ["ALL"]
          volumeMounts:
            - name: www
              mountPath: /www
              readOnly: true
      volumes:
        - name: www
          configMap:
            name: e2e-fixture
---
apiVersion: v1
kind: Service
metadata:
  name: e2e-fixture-server
  namespace: $NAMESPACE
spec:
  selector:
    app.kubernetes.io/name: e2e-fixture-server
  ports:
    - name: http
      port: 80
      targetPort: http
EOF
kc -n "$NAMESPACE" rollout status deployment/e2e-fixture-server --timeout="$ROLLOUT_TIMEOUT"

log "pointing DATASET_URL at the fixture"
# The container env overrides the ConfigMap's DATASET_URL (envFrom); this rolls the pod once.
kc -n "$NAMESPACE" set env deployment/enron-service DATASET_URL="$FIXTURE_URL"
kc -n "$NAMESPACE" rollout status deployment/enron-service --timeout="$ROLLOUT_TIMEOUT"

# --- HTTP helpers --------------------------------------------------------------------------------------------------
pf_start() {
	pf_stop
	kc -n "$NAMESPACE" port-forward svc/enron-service "$E2E_PORT:80" >"$work/port-forward.log" 2>&1 &
	pf_pid=$!
	local i
	for ((i = 0; i < 30; i++)); do
		if curl -fsS --max-time 2 "$BASE/healthz" >/dev/null 2>&1; then
			return 0
		fi
		kill -0 "$pf_pid" 2>/dev/null || break
		sleep 1
	done
	fail "port-forward to svc/enron-service on port $E2E_PORT did not become reachable"
}

pf_stop() {
	if [ -n "$pf_pid" ]; then
		kill "$pf_pid" 2>/dev/null || true
		wait "$pf_pid" 2>/dev/null || true
		pf_pid=""
	fi
}

# call <METHOD> <path>: sets $code and writes the body to $work/body.json.
call() {
	code=$(curl -sS --max-time 10 -X "$1" -o "$work/body.json" -w '%{http_code}' "$BASE$2") || code=000
}

# expect_code <METHOD> <path> <code>
expect_code() {
	call "$1" "$2"
	if [ "$code" != "$3" ]; then
		cat "$work/body.json" >&2 2>/dev/null || true
		fail "$1 $2: HTTP $code, expected $3"
	fi
	echo "$1 $2 -> $code"
}

# wait_for <description> <jq filter on GET /status>: polls until the filter is true; fails fast on a failure state.
wait_for() {
	local deadline=$((SECONDS + WAIT_TIMEOUT))
	while ((SECONDS < deadline)); do
		call GET /status
		if [ "$code" = 200 ]; then
			if jq -e "$2" "$work/body.json" >/dev/null; then
				echo "$1: ok after $((WAIT_TIMEOUT - deadline + SECONDS))s"
				return 0
			fi
			if jq -e '.state == "failed" or .dataset.state == "failed"' "$work/body.json" >/dev/null; then
				jq . "$work/body.json" >&2
				fail "$1: GET /status reports a failure"
			fi
		fi
		sleep 1
	done
	jq . "$work/body.json" >&2 2>/dev/null || true
	fail "$1: not reached within ${WAIT_TIMEOUT}s"
}

# assert_json <description> <file> <jq filter>: the filter must evaluate to true.
assert_json() {
	if ! jq -e "$3" "$2" >/dev/null; then
		jq . "$2" >&2
		fail "$1 (filter: $3)"
	fi
	echo "assert: $1"
}

senders_json() { # top <n> expected senders as the API's JSON array
	local line email count _mb
	for line in "${EXPECTED_SENDERS[@]:0:$1}"; do
		read -r email count _mb <<<"$line"
		jq -cn --arg e "$email" --argjson c "$count" '{email: $e, count: $c}'
	done | jq -cs .
}

# assert_completed_run <file>: the exact counts of a finished ingestion of the fixture.
assert_completed_run() {
	assert_json "state completed, finished" "$1" '.state == "completed" and .finished == true and .error == null'
	assert_json "messages_processed $EXPECTED_PROCESSED" "$1" ".messages_processed == $EXPECTED_PROCESSED"
	assert_json "total_messages $EXPECTED_PROCESSED" "$1" ".total_messages == $EXPECTED_PROCESSED"
	assert_json "messages_skipped $EXPECTED_SKIPPED" "$1" ".messages_skipped == $EXPECTED_SKIPPED"
	assert_json "files_seen $EXPECTED_FILES_SEEN" "$1" ".files_seen == $EXPECTED_FILES_SEEN"
	assert_json "skip_reasons: 1 empty_file, 1 not_an_email, nothing else" "$1" \
		'.skip_reasons.empty_file == 1 and .skip_reasons.not_an_email == 1
		 and ([.skip_reasons[]] | add) == 2'
	assert_json "field_errors: 1 missing_sender, 1 invalid_sender, nothing else" "$1" \
		'.field_errors.missing_sender == 1 and .field_errors.invalid_sender == 1
		 and ([.field_errors[]] | add) == 2'
}

# check_top_senders <tag>: asserts the default top 10 and ?limit=12, saving both as $work/top-<tag>-*.json.
check_top_senders() {
	expect_code GET /top-senders 200
	cp "$work/body.json" "$work/top-$1-10.json"
	assert_json "/top-senders: the expected top 10 in order" "$work/top-$1-10.json" \
		".senders == $(senders_json 10)"
	expect_code GET '/top-senders?limit=12' 200
	cp "$work/body.json" "$work/top-$1-12.json"
	assert_json "/top-senders?limit=12: all 12 senders in order, none without a sender" "$work/top-$1-12.json" \
		".senders == $(senders_json 12)"
}

# --- Scenario: Fetch and ingest inside kind ------------------------------------------------------------------------
pf_start

log "scenario: fetch and ingest inside kind"
call GET /status
assert_json "empty volume and database: dataset absent, ingestion idle" "$work/body.json" \
	'.dataset.state == "absent" and .state == "idle"'
expect_code POST /fetch 202
wait_for 'dataset.state "ready"' '.dataset.state == "ready"'
assert_json "dataset: $EXPECTED_FILES_SEEN files extracted" "$work/body.json" \
	".dataset.files_extracted == $EXPECTED_FILES_SEEN"
expect_code POST /fetch 200

expect_code POST /start 202
run1=$(jq -r .run_id "$work/body.json")
wait_for "ingestion run $run1 finished" '.finished == true'
cp "$work/body.json" "$work/status-run1.json"
jq . "$work/status-run1.json"
assert_json "the status is for run $run1" "$work/status-run1.json" ".run_id == $run1"
assert_completed_run "$work/status-run1.json"
check_top_senders run1

# --- Scenario: Pod restarts keep state -----------------------------------------------------------------------------
log "scenario: pod restarts keep state"
old_pod=$(kc -n "$NAMESPACE" get pods -l "$APP_SELECTOR" -o jsonpath='{.items[0].metadata.name}')
pf_stop
kc -n "$NAMESPACE" delete pod "$old_pod" --wait=true
new_pod=""
deadline=$((SECONDS + WAIT_TIMEOUT))
while [ -z "$new_pod" ] && ((SECONDS < deadline)); do
	new_pod=$(kc -n "$NAMESPACE" get pods -l "$APP_SELECTOR" -o name |
		grep -v "^pod/$old_pod\$" | head -n1 || true)
	[ -n "$new_pod" ] || sleep 1
done
[ -n "$new_pod" ] || fail "no replacement for pod $old_pod within ${WAIT_TIMEOUT}s"
kc -n "$NAMESPACE" wait --for=condition=Ready "$new_pod" --timeout="${WAIT_TIMEOUT}s"
echo "pod $old_pod replaced by ${new_pod#pod/}"
pf_start

expect_code GET /status 200
cp "$work/body.json" "$work/status-restart.json"
assert_json "same completed run $run1 after the restart" "$work/status-restart.json" ".run_id == $run1"
assert_completed_run "$work/status-restart.json"
# The run (counts, reasons, timestamps) comes from Postgres, so it is identical; only the dataset block is
# re-derived by the new pod from the volume.
assert_same() { # <description> <file-a> <file-b> [jq filter applied to both]
	if ! jq -e -n --slurpfile a "$2" --slurpfile b "$3" "(\$a[0] | ${4:-.}) == (\$b[0] | ${4:-.})" >/dev/null; then
		diff <(jq -S "${4:-.}" "$2") <(jq -S "${4:-.}" "$3") >&2 || true
		fail "$1"
	fi
	echo "assert: $1"
}
assert_same "GET /status reports the same run as before the restart" \
	"$work/status-run1.json" "$work/status-restart.json" 'del(.dataset)'
assert_json "the dataset is still ready on the volume" "$work/status-restart.json" '.dataset.state == "ready"'
check_top_senders restart
assert_same "GET /top-senders is unchanged by the restart" "$work/top-run1-10.json" "$work/top-restart-10.json"

# --- Re-ingestion is idempotent ------------------------------------------------------------------------------------
log "scenario: a second POST /start re-ingests idempotently"
expect_code POST /start 202
run2=$(jq -r .run_id "$work/body.json")
[ "$run2" != "$run1" ] || fail "the second POST /start reused run id $run1"
wait_for "ingestion run $run2 finished" ".run_id == $run2 and .finished == true"
cp "$work/body.json" "$work/status-run2.json"
jq . "$work/status-run2.json"
assert_completed_run "$work/status-run2.json"
check_top_senders run2
assert_same "GET /top-senders is unchanged by the re-ingestion" "$work/top-run1-12.json" "$work/top-run2-12.json"
