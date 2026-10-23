#!/usr/bin/env bash
# Validates the Kubernetes manifests (#12): renders the base and both overlays with the pinned kustomize,
# validates every resource against the Kubernetes JSON schema with kubeconform, and asserts the values that
# the ticket's Gherkin scenarios and ADRs 0014/0015 fix, so a regression fails `make k8s-validate` and CI.
# All three tools are pinned in go.mod (tool directives) and run with `go tool`.
#
# Environment:
#   K8S_VERSION     Kubernetes version of the schemas (default: the kind node version in deploy/kind/cluster.yaml)
#   KUBECONFORM_CACHE  schema cache directory (default .tools/kubeconform-cache; empty disables the cache)
#   RENDER_DIR      keep the rendered YAML here instead of a temporary directory
set -euo pipefail

cd "$(dirname "$0")/.."
# The tools are pure Go; building them without cgo keeps them independent of the host C toolchain.
export CGO_ENABLED=0

K8S_VERSION=${K8S_VERSION:-1.37.0}
KUBECONFORM_CACHE=${KUBECONFORM_CACHE-.tools/kubeconform-cache}

if [[ -n "${RENDER_DIR:-}" ]]; then
	out=$RENDER_DIR
	mkdir -p "$out"
else
	out=$(mktemp -d)
	trap 'rm -rf "$out"' EXIT
fi

fails=0
ok() { printf 'ok    %s\n' "$1"; }
fail() {
	printf 'FAIL  %s\n' "$1" >&2
	fails=$((fails + 1))
}

# expect <description> <rendered file> <yq expression over all documents> <expected output>
expect() {
	local got
	# stderr is kept out of the value: on a cold module cache `go tool` prints "go: downloading ..." there.
	if ! got=$(go tool yq eval-all "$3" "$2" 2>"$out/yq.stderr"); then
		fail "$1: yq error: $(cat "$out/yq.stderr")"
		return
	fi
	if [[ "$got" == "$4" ]]; then
		ok "$1"
	else
		fail "$1: got '$got', want '$4'"
	fi
}

# seconds <duration> converts a whole-second Go duration such as "60s" to 60 (and fails on anything else).
seconds() {
	if [[ "$1" =~ ^([0-9]+)s$ ]]; then
		echo "${BASH_REMATCH[1]}"
	else
		echo "not a whole-second duration: '$1'" >&2
		return 1
	fi
}

# Build the pinned tools up front, so module downloads happen (and fail) here rather than inside a check.
for tool in kustomize kubeconform yq; do
	go tool -n "$tool" >/dev/null
done

echo "== Render (kustomize $(go tool kustomize version))"
for target in base overlays/kind overlays/cloud; do
	name=${target#overlays/}
	if ! go tool kustomize build "deploy/k8s/$target" >"$out/$name.yaml"; then
		fail "deploy/k8s/$target does not render"
		exit 1
	fi
	ok "deploy/k8s/$target renders ($(grep -c '^kind:' "$out/$name.yaml") resources)"
done

echo "== Schema validation (kubeconform, Kubernetes $K8S_VERSION)"
kc_args=(-strict -summary -kubernetes-version "$K8S_VERSION")
if [[ -n "$KUBECONFORM_CACHE" ]]; then
	mkdir -p "$KUBECONFORM_CACHE"
	kc_args+=(-cache "$KUBECONFORM_CACHE")
fi
# -strict rejects unknown fields; kubeconform exits non-zero on any invalid resource or missing schema.
if ! go tool kubeconform "${kc_args[@]}" "$out/base.yaml" "$out/kind.yaml" "$out/cloud.yaml"; then
	fail "kubeconform reported invalid resources"
fi

deploy='select(.kind == "Deployment" and .metadata.name == "enron-service")'
pod="$deploy | .spec.template.spec"
ctr="$pod | .containers[] | select(.name == \"enron-service\")"
cm='select(.kind == "ConfigMap" and .metadata.name == "enron-service-config")'
pvc='select(.kind == "PersistentVolumeClaim" and .metadata.name == "enron-data")'

# Checks that hold for the base and must survive both overlays unchanged.
for name in base kind cloud; do
	f="$out/$name.yaml"
	echo "== $name: shutdown and probe timing, hardening, topology"
	expect "$name: namespace enron" "$f" '[.metadata.namespace // "enron"] | unique | join(",")' "enron"
	expect "$name: env from ConfigMap enron-service-config" "$f" \
		"$ctr | [.envFrom[].configMapRef.name | select(. != null)] | join(\",\")" "enron-service-config"
	expect "$name: SHUTDOWN_DRAIN_DELAY 5s" "$f" "$cm | .data.SHUTDOWN_DRAIN_DELAY" "5s"
	expect "$name: SHUTDOWN_TIMEOUT 20s" "$f" "$cm | .data.SHUTDOWN_TIMEOUT" "20s"
	expect "$name: DB_STARTUP_TIMEOUT 60s" "$f" "$cm | .data.DB_STARTUP_TIMEOUT" "60s"
	expect "$name: DATA_DIR /data" "$f" "$cm | .data.DATA_DIR" "/data"
	expect "$name: terminationGracePeriodSeconds 40" "$f" "$pod | .terminationGracePeriodSeconds" "40"
	expect "$name: readinessProbe GET /readyz" "$f" "$ctr | .readinessProbe.httpGet.path" "/readyz"
	expect "$name: readinessProbe timeoutSeconds 3" "$f" "$ctr | .readinessProbe.timeoutSeconds" "3"
	expect "$name: livenessProbe GET /healthz" "$f" "$ctr | .livenessProbe.httpGet.path" "/healthz"
	expect "$name: startupProbe GET /healthz" "$f" "$ctr | .startupProbe.httpGet.path" "/healthz"
	expect "$name: startupProbe periodSeconds 5" "$f" "$ctr | .startupProbe.periodSeconds" "5"
	expect "$name: startupProbe failureThreshold 30" "$f" "$ctr | .startupProbe.failureThreshold" "30"

	# The invariants behind those numbers, so a later edit to one of them cannot silently break the others.
	grace=$(go tool yq eval-all "$pod | .terminationGracePeriodSeconds" "$f")
	drain=$(seconds "$(go tool yq eval-all "$cm | .data.SHUTDOWN_DRAIN_DELAY" "$f")")
	timeout=$(seconds "$(go tool yq eval-all "$cm | .data.SHUTDOWN_TIMEOUT" "$f")")
	dbstart=$(seconds "$(go tool yq eval-all "$cm | .data.DB_STARTUP_TIMEOUT" "$f")")
	budget=$(go tool yq eval-all "$ctr | .startupProbe.periodSeconds * .startupProbe.failureThreshold" "$f")
	if ((grace > drain + timeout + 5)); then
		ok "$name: grace ${grace}s > drain ${drain}s + shutdown ${timeout}s + 5s closers"
	else
		fail "$name: grace ${grace}s must exceed drain ${drain}s + shutdown ${timeout}s + 5s closers"
	fi
	if ((budget > dbstart)); then
		ok "$name: startup budget ${budget}s > DB_STARTUP_TIMEOUT ${dbstart}s"
	else
		fail "$name: startup budget ${budget}s must exceed DB_STARTUP_TIMEOUT ${dbstart}s"
	fi

	expect "$name: replicas 1" "$f" "$deploy | .spec.replicas" "1"
	expect "$name: strategy Recreate" "$f" "$deploy | .spec.strategy.type" "Recreate"
	expect "$name: PVC enron-data ReadWriteOnce" "$f" "$pvc | .spec.accessModes | join(\",\")" "ReadWriteOnce"
	expect "$name: PVC enron-data 10Gi" "$f" "$pvc | .spec.resources.requests.storage" "10Gi"
	expect "$name: PVC enron-data mounted at /data" "$f" \
		"$pod as \$p | \$p.volumes[] | select(.persistentVolumeClaim.claimName == \"enron-data\") | .name as \$v | \$p.containers[0].volumeMounts[] | select(.name == \$v) | .mountPath" \
		"/data"
	expect "$name: runAsNonRoot" "$f" "$pod | .securityContext.runAsNonRoot" "true"
	expect "$name: runAsUser 65532" "$f" "$pod | .securityContext.runAsUser" "65532"
	expect "$name: fsGroup 65532" "$f" "$pod | .securityContext.fsGroup" "65532"
	expect "$name: seccomp RuntimeDefault" "$f" "$pod | .securityContext.seccompProfile.type" "RuntimeDefault"
	expect "$name: readOnlyRootFilesystem" "$f" "$ctr | .securityContext.readOnlyRootFilesystem" "true"
	expect "$name: no privilege escalation" "$f" "$ctr | .securityContext.allowPrivilegeEscalation" "false"
	expect "$name: drops ALL capabilities, adds none" "$f" \
		"$ctr | (.securityContext.capabilities.drop | join(\",\")) + \"/\" + ((.securityContext.capabilities.add // []) | length | tostring)" \
		"ALL/0"
	expect "$name: Service enron-service ClusterIP 80 -> http" "$f" \
		'select(.kind == "Service" and .metadata.name == "enron-service") | .spec.type + " " + (.spec.ports[0].port | tostring) + " " + .spec.ports[0].targetPort' \
		"ClusterIP 80 http"
	expect "$name: no custom resources (core and apps/v1 only)" "$f" \
		'[select(.apiVersion != "v1" and .apiVersion != "apps/v1")] | length' "0"
done

kind="$out/kind.yaml"
echo "== kind overlay"
expect "kind: Postgres is StatefulSet postgres" "$kind" \
	'[select(.kind == "StatefulSet") | .metadata.name] | join(",")' "postgres"
expect "kind: Postgres headless Service" "$kind" \
	'select(.kind == "Service" and .metadata.name == "postgres") | .spec.clusterIP' "None"
expect "kind: Postgres volume 2Gi" "$kind" \
	'select(.kind == "StatefulSet") | .spec.volumeClaimTemplates[0].spec.resources.requests.storage' "2Gi"
expect "kind: image enron-service:dev, IfNotPresent" "$kind" "$ctr | .image + \" \" + .imagePullPolicy" \
	"enron-service:dev IfNotPresent"
expect "kind: generated Secret enron-db is the one the Deployment uses" "$kind" \
	"([select(.kind == \"Secret\") | .metadata.name | select(test(\"^enron-db-\"))] | join(\",\")) == ([$ctr | .envFrom[].secretRef.name | select(. != null)] | join(\",\"))" \
	"true"
expect "kind: enron-db Secret has DATABASE_URL" "$kind" \
	'select(.kind == "Secret" and (.metadata.name | test("^enron-db-"))) | .data | has("DATABASE_URL")' "true"

cloud="$out/cloud.yaml"
echo "== cloud overlay"
expect "cloud: no StatefulSet" "$cloud" '[select(.kind == "StatefulSet")] | length' "0"
expect "cloud: defines no Secret" "$cloud" '[select(.kind == "Secret")] | length' "0"
expect "cloud: references Secret enron-db" "$cloud" \
	"$ctr | [.envFrom[].secretRef.name | select(. != null)] | join(\",\")" "enron-db"
expect "cloud: PVC enron-data sets no storageClassName" "$cloud" "$pvc | (.spec | has(\"storageClassName\") | not)" "true"
expect "cloud: registry image placeholder" "$cloud" "$ctr | .image" "REGISTRY/enron-service:VERSION"

if ((fails > 0)); then
	echo "k8s-validate: $fails check(s) failed" >&2
	exit 1
fi
echo "k8s-validate: all checks passed"
