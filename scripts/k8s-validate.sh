#!/usr/bin/env bash
# Validates the Kubernetes manifests (#12): renders the base and the overlays (kind, cloud, and the opt-in
# kind-local-dataset variant of #40) with the pinned kustomize, validates every resource against the Kubernetes JSON
# schema with kubeconform, and asserts the values that the tickets' Gherkin scenarios and ADRs 0014/0015 fix, so a
# regression fails `make k8s-validate` and CI. It also checks the kind cluster configs that scripts/kind-up.sh uses.
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
for target in base overlays/kind overlays/cloud overlays/kind-local-dataset; do
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
if ! go tool kubeconform "${kc_args[@]}" "$out/base.yaml" "$out/kind.yaml" "$out/cloud.yaml" \
	"$out/kind-local-dataset.yaml"; then
	fail "kubeconform reported invalid resources"
fi

deploy='select(.kind == "Deployment" and .metadata.name == "enron-service")'
pod="$deploy | .spec.template.spec"
ctr="$pod | .containers[] | select(.name == \"enron-service\")"
cm='select(.kind == "ConfigMap" and .metadata.name == "enron-service-config")'
pvc='select(.kind == "PersistentVolumeClaim" and .metadata.name == "enron-data")'

# Checks that hold for the base and must survive every overlay unchanged (kind-local-dataset only replaces the
# dataset volume; its own checks are below).
for name in base kind cloud kind-local-dataset; do
	f="$out/$name.yaml"
	echo "== $name: shutdown and probe timing, hardening, topology"
	expect "$name: namespace enron" "$f" '[select(.kind != "PersistentVolume") | .metadata.namespace // "enron"] | unique | join(",")' "enron"
	expect "$name: Namespace enron enforces Pod Security restricted" "$f" \
		'select(.kind == "Namespace" and .metadata.name == "enron") | .metadata.labels["pod-security.kubernetes.io/enforce"]' \
		"restricted"
	# "restricted" rejects hostPath volumes; catch one here rather than at admission time on a cluster.
	expect "$name: no hostPath volume in any pod template" "$f" \
		'[select(.spec.template.spec.volumes != null) | .spec.template.spec.volumes[] | select(has("hostPath"))] | length' "0"
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
	if [[ "$name" != kind-local-dataset ]]; then
		expect "$name: PVC enron-data ReadWriteOnce" "$f" "$pvc | .spec.accessModes | join(\",\")" "ReadWriteOnce"
		expect "$name: PVC enron-data 10Gi" "$f" "$pvc | .spec.resources.requests.storage" "10Gi"
		expect "$name: PVC enron-data mounted at /data" "$f" \
			"$pod as \$p | \$p.volumes[] | select(.persistentVolumeClaim.claimName == \"enron-data\") | .name as \$v | \$p.containers[0].volumeMounts[] | select(.name == \$v) | .mountPath" \
			"/data"
		expect "$name: no PersistentVolume (dynamically provisioned)" "$f" '[select(.kind == "PersistentVolume")] | length' "0"
	fi
	expect "$name: runAsNonRoot" "$f" "$pod | .securityContext.runAsNonRoot" "true"
	expect "$name: runAsUser 65532" "$f" "$pod | .securityContext.runAsUser" "65532"
	expect "$name: fsGroup 65532" "$f" "$pod | .securityContext.fsGroup" "65532"
	expect "$name: fsGroupChangePolicy OnRootMismatch" "$f" "$pod | .securityContext.fsGroupChangePolicy" "OnRootMismatch"
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
# PGDATA must live on the persistent volume (a subdirectory of its mountPath); anywhere else, the data would be on
# an emptyDir or the container filesystem and vanish with the pod.
sts='select(.kind == "StatefulSet" and .metadata.name == "postgres")'
pg="$sts | .spec.template.spec"
expect "kind: PGDATA is under the Postgres volume mountPath" "$kind" \
	"$sts | .spec.volumeClaimTemplates[0].metadata.name as \$v | .spec.template.spec.containers[] | select(.name == \"postgres\") | ([.volumeMounts[] | select(.name == \$v) | .mountPath] | .[0]) as \$m | ([.env[] | select(.name == \"PGDATA\") | .value] | .[0]) as \$d | (\$m != null and \$d != null and (\$d | test(\"^\" + \$m + \"/[^/]+\")))" \
	"true"
expect "kind: Postgres fsGroup 70, OnRootMismatch" "$kind" \
	"$pg | .securityContext | (.fsGroup | tostring) + \" \" + .fsGroupChangePolicy" "70 OnRootMismatch"
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

local="$out/kind-local-dataset.yaml"
pv='select(.kind == "PersistentVolume" and .metadata.name == "enron-local-dataset")'
lpvc='select(.kind == "PersistentVolumeClaim" and .metadata.name == "enron-local-dataset")'
echo "== kind-local-dataset overlay (#40)"
# Everything but the dataset volume is exactly overlays/kind: drop the PV/PVCs and the Deployment's "data" volume
# and mount from both renders and compare the rest.
same='[select(.kind != "PersistentVolumeClaim" and .kind != "PersistentVolume") | (select(.kind == "Deployment" and .metadata.name == "enron-service") |= (del(.spec.template.spec.volumes[] | select(.name == "data")) | del(.spec.template.spec.containers[].volumeMounts[] | select(.name == "data"))))] | sort_by(.kind + "/" + .metadata.name) | to_json(0)'
if [[ "$(go tool yq eval-all "$same" "$kind")" == "$(go tool yq eval-all "$same" "$local")" ]]; then
	ok "kind-local-dataset: identical to overlays/kind except the dataset volume"
else
	fail "kind-local-dataset: differs from overlays/kind beyond the dataset volume"
fi
expect "kind-local-dataset: no PVC enron-data" "$local" "[$pvc] | length" "0"
expect "kind-local-dataset: PersistentVolumeClaims" "$local" \
	'[select(.kind == "PersistentVolumeClaim") | .metadata.name] | join(",")' "enron-local-dataset"
expect "kind-local-dataset: PV is cluster-scoped" "$local" "$pv | .metadata | has(\"namespace\") | not" "true"
expect "kind-local-dataset: PV is a static local volume at /var/local/enron-dataset" "$local" "$pv | .spec.local.path" \
	"/var/local/enron-dataset"
expect "kind-local-dataset: PV ReadOnlyMany, Retain, no StorageClass" "$local" \
	"$pv | (.spec.accessModes | join(\",\")) + \" \" + .spec.persistentVolumeReclaimPolicy + \" '\" + .spec.storageClassName + \"'\"" \
	"ReadOnlyMany Retain ''"
expect "kind-local-dataset: PV pinned to nodes labelled elisa-enron/local-dataset=true" "$local" \
	"$pv | .spec.nodeAffinity.required.nodeSelectorTerms[0].matchExpressions[0] | .key + \" \" + .operator + \" \" + (.values | join(\",\"))" \
	"elisa-enron/local-dataset In true"
expect "kind-local-dataset: PVC binds the PV by name, ReadOnlyMany, no StorageClass" "$local" \
	"$lpvc | .spec.volumeName + \" \" + (.spec.accessModes | join(\",\")) + \" '\" + .spec.storageClassName + \"'\"" \
	"enron-local-dataset ReadOnlyMany ''"
expect "kind-local-dataset: pod volume data is the PVC, read-only" "$local" \
	"$pod | .volumes[] | select(.name == \"data\") | .persistentVolumeClaim.claimName + \" \" + (.persistentVolumeClaim.readOnly | tostring)" \
	"enron-local-dataset true"
expect "kind-local-dataset: /data mounted read-only" "$local" \
	"$ctr | .volumeMounts[] | select(.name == \"data\") | .mountPath + \" \" + (.readOnly | tostring)" "/data true"

echo "== kind cluster configs (scripts/kind-up.sh)"
# Default unchanged: without LOCAL_DATASET the tracked config is used as is.
if PRINT_CONFIG=1 LOCAL_DATASET= scripts/kind-up.sh >"$out/kind-cluster.yaml" &&
	cmp -s "$out/kind-cluster.yaml" deploy/kind/cluster.yaml; then
	ok "kind config without LOCAL_DATASET is deploy/kind/cluster.yaml unchanged"
else
	fail "kind config without LOCAL_DATASET differs from deploy/kind/cluster.yaml"
fi
expect "kind config without LOCAL_DATASET has no extraMounts" deploy/kind/cluster.yaml \
	'[.nodes[] | select(has("extraMounts"))] | length' "0"
# With LOCAL_DATASET: a stand-in dataset directory, so the check does not depend on the developer's Dataset/.
mkdir -p "$out/dataset/maildir"
dataset=$(cd "$out/dataset" && pwd -P)
if PRINT_CONFIG=1 LOCAL_DATASET="$out/dataset" scripts/kind-up.sh >"$out/kind-cluster-local.yaml"; then
	cfg="$out/kind-cluster-local.yaml"
	expect "kind config with LOCAL_DATASET: only extraMounts and labels added" "$cfg" \
		'del(.nodes[0].extraMounts, .nodes[0].labels) | to_json(0)' \
		"$(go tool yq eval 'to_json(0)' deploy/kind/cluster.yaml)"
	expect "kind config with LOCAL_DATASET: mounts the dataset read-only at the PV's node path" "$cfg" \
		'.nodes[0].extraMounts | length as $n | .[0] | ($n | tostring) + " " + .hostPath + " " + .containerPath + " " + (.readOnly | tostring)' \
		"1 $dataset $(go tool yq eval-all "$pv | .spec.local.path" "$local") true"
	expect "kind config with LOCAL_DATASET: node label matches the PV's nodeAffinity" "$cfg" \
		".nodes[0].labels[\"$(go tool yq eval-all "$pv | .spec.nodeAffinity.required.nodeSelectorTerms[0].matchExpressions[0].key" "$local")\"]" \
		"true"
else
	fail "scripts/kind-up.sh cannot render the LOCAL_DATASET config"
fi
if PRINT_CONFIG=1 LOCAL_DATASET="$out/no-such-dir" scripts/kind-up.sh >/dev/null 2>&1; then
	fail "kind config with LOCAL_DATASET: a directory without maildir/ must be rejected"
else
	ok "kind config with LOCAL_DATASET: a directory without maildir/ is rejected"
fi

if ((fails > 0)); then
	echo "k8s-validate: $fails check(s) failed" >&2
	exit 1
fi
echo "k8s-validate: all checks passed"
