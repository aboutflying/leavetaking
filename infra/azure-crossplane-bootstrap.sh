#!/usr/bin/env bash
#
# Idempotent day-1 bootstrap for an Azure + Crossplane + CAPI + Talos cluster.
#
# Provisions:
#   1. A local kind management cluster
#   2. Crossplane with Azure providers (networking layer)
#   3. CAPI + CAPZ + Talos bootstrap/control-plane providers
#   4. A Talos-based workload cluster on Azure
#
# Re-running is safe: uses helm upgrade --install, kubectl apply, and
# status checks to skip already-completed steps.
#
# Prerequisites: kind, helm, kubectl, clusterctl, talosctl, envsubst
#
# Required environment variables:
#   AZURE_SUBSCRIPTION_ID
#   AZURE_TENANT_ID
#   AZURE_CLIENT_ID
#   AZURE_CLIENT_SECRET
#
set -euo pipefail

# ──────────────────────────────────────────────────────────────────────────────
# Configuration (override via environment)
# ──────────────────────────────────────────────────────────────────────────────
CLUSTER_NAME="${CLUSTER_NAME:-talos-workload}"
MGMT_CLUSTER_NAME="${MGMT_CLUSTER_NAME:-crossplane-mgmt}"
AZURE_LOCATION="${AZURE_LOCATION:-eastus2}"
RESOURCE_GROUP="${RESOURCE_GROUP:-${CLUSTER_NAME}-rg}"
KUBERNETES_VERSION="${KUBERNETES_VERSION:-v1.30.4}"
TALOS_VERSION="${TALOS_VERSION:-v1.7}"
CONTROL_PLANE_COUNT="${CONTROL_PLANE_COUNT:-3}"
CONTROL_PLANE_VM_SIZE="${CONTROL_PLANE_VM_SIZE:-Standard_D2s_v3}"
WORKER_COUNT="${WORKER_COUNT:-3}"
WORKER_VM_SIZE="${WORKER_VM_SIZE:-Standard_D4s_v3}"

CROSSPLANE_VERSION="${CROSSPLANE_VERSION:-1.17.1}"
CAPZ_VERSION="${CAPZ_VERSION:-v1.16.0}"
CABPT_VERSION="${CABPT_VERSION:-v0.6.7}"
CACPPT_VERSION="${CACPPT_VERSION:-v0.5.7}"

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
MANIFESTS_DIR="${SCRIPT_DIR}/manifests"
OUTPUT_DIR="${SCRIPT_DIR}/output"

export CLUSTER_NAME AZURE_LOCATION RESOURCE_GROUP KUBERNETES_VERSION TALOS_VERSION
export CONTROL_PLANE_COUNT CONTROL_PLANE_VM_SIZE WORKER_COUNT WORKER_VM_SIZE
export AZURE_SUBSCRIPTION_ID AZURE_TENANT_ID AZURE_CLIENT_ID AZURE_CLIENT_SECRET

# ──────────────────────────────────────────────────────────────────────────────
# Helpers
# ──────────────────────────────────────────────────────────────────────────────
RED='\033[0;31m'; GREEN='\033[0;32m'; YELLOW='\033[1;33m'; NC='\033[0m'

log()  { echo -e "${GREEN}[INFO]${NC}  $*"; }
warn() { echo -e "${YELLOW}[WARN]${NC}  $*"; }
err()  { echo -e "${RED}[ERR]${NC}   $*" >&2; }
die()  { err "$@"; exit 1; }

check_bin() {
  command -v "$1" &>/dev/null || die "'$1' is required but not found in PATH"
}

wait_for_condition() {
  local description="$1" check_cmd="$2" timeout="${3:-300}" interval="${4:-10}"
  local elapsed=0
  log "Waiting for: ${description} (timeout ${timeout}s)"
  while ! eval "${check_cmd}" &>/dev/null; do
    if (( elapsed >= timeout )); then
      die "Timed out waiting for: ${description}"
    fi
    sleep "${interval}"
    elapsed=$((elapsed + interval))
    printf '.'
  done
  echo
  log "Ready: ${description}"
}

render_template() {
  envsubst < "$1"
}

# ──────────────────────────────────────────────────────────────────────────────
# Preflight
# ──────────────────────────────────────────────────────────────────────────────
preflight() {
  log "Running preflight checks…"
  for bin in kind helm kubectl clusterctl talosctl envsubst; do
    check_bin "${bin}"
  done

  for var in AZURE_SUBSCRIPTION_ID AZURE_TENANT_ID AZURE_CLIENT_ID AZURE_CLIENT_SECRET; do
    [[ -n "${!var:-}" ]] || die "Required env var ${var} is not set"
  done

  mkdir -p "${OUTPUT_DIR}"
  log "Preflight OK"
}

# ──────────────────────────────────────────────────────────────────────────────
# Step 1: Management cluster (kind)
# ──────────────────────────────────────────────────────────────────────────────
ensure_mgmt_cluster() {
  if kind get clusters 2>/dev/null | grep -qx "${MGMT_CLUSTER_NAME}"; then
    log "Kind cluster '${MGMT_CLUSTER_NAME}' already exists, reusing"
    kubectl config use-context "kind-${MGMT_CLUSTER_NAME}"
  else
    log "Creating kind management cluster '${MGMT_CLUSTER_NAME}'…"
    kind create cluster --config "${MANIFESTS_DIR}/kind-cluster.yaml" \
      --name "${MGMT_CLUSTER_NAME}" --wait 120s
  fi

  wait_for_condition "management cluster reachable" \
    "kubectl cluster-info" 60
}

# ──────────────────────────────────────────────────────────────────────────────
# Step 2: Crossplane
# ──────────────────────────────────────────────────────────────────────────────
install_crossplane() {
  log "Installing Crossplane ${CROSSPLANE_VERSION}…"
  helm repo add crossplane-stable https://charts.crossplane.io/stable 2>/dev/null || true
  helm repo update crossplane-stable

  helm upgrade --install crossplane crossplane-stable/crossplane \
    --namespace crossplane-system --create-namespace \
    --version "${CROSSPLANE_VERSION}" \
    --set args='{"--enable-usages"}' \
    --wait --timeout 5m

  wait_for_condition "Crossplane pods ready" \
    "kubectl -n crossplane-system get deploy crossplane -o jsonpath='{.status.readyReplicas}' | grep -q '[1-9]'" \
    180
}

install_crossplane_providers() {
  log "Applying Crossplane Azure providers…"
  kubectl apply -f "${MANIFESTS_DIR}/crossplane/provider-azure.yaml"

  log "Waiting for providers to become healthy…"
  for provider in provider-azure-network provider-azure-compute provider-azure-managedidentity provider-azure-azure; do
    wait_for_condition "Provider ${provider} healthy" \
      "kubectl get provider.pkg.crossplane.io ${provider} -o jsonpath='{.status.conditions[?(@.type==\"Healthy\")].status}' | grep -q True" \
      300 15
  done
}

configure_crossplane_azure_creds() {
  log "Ensuring Azure credentials secret for Crossplane…"
  local creds_json
  creds_json=$(cat <<CREDS
{
  "clientId": "${AZURE_CLIENT_ID}",
  "clientSecret": "${AZURE_CLIENT_SECRET}",
  "subscriptionId": "${AZURE_SUBSCRIPTION_ID}",
  "tenantId": "${AZURE_TENANT_ID}"
}
CREDS
  )

  kubectl -n crossplane-system create secret generic azure-creds \
    --from-literal=credentials="${creds_json}" \
    --dry-run=client -o yaml | kubectl apply -f -

  log "Applying Crossplane ProviderConfig…"
  kubectl apply -f "${MANIFESTS_DIR}/crossplane/provider-config.yaml"
}

# ──────────────────────────────────────────────────────────────────────────────
# Step 3: Crossplane Compositions (Azure networking)
# ──────────────────────────────────────────────────────────────────────────────
apply_crossplane_compositions() {
  log "Applying XRD and Composition for Azure networking…"
  kubectl apply -f "${MANIFESTS_DIR}/crossplane/xrd-network.yaml"

  wait_for_condition "XRD established" \
    "kubectl get xrd xazurenetworks.infrastructure.example.com -o jsonpath='{.status.conditions[?(@.type==\"Established\")].status}' | grep -q True" \
    120

  kubectl apply -f "${MANIFESTS_DIR}/crossplane/composition-network.yaml"

  log "Applying network claim…"
  render_template "${MANIFESTS_DIR}/crossplane/network-claim.yaml" | kubectl apply -f -
}

# ──────────────────────────────────────────────────────────────────────────────
# Step 4: CAPI + CAPZ + Talos providers
# ──────────────────────────────────────────────────────────────────────────────
install_capi() {
  log "Initializing CAPI with CAPZ and Talos providers…"

  # clusterctl init is largely idempotent — it skips already-installed components.
  export AZURE_SUBSCRIPTION_ID_B64="$(echo -n "${AZURE_SUBSCRIPTION_ID}" | base64 -w0)"
  export AZURE_TENANT_ID_B64="$(echo -n "${AZURE_TENANT_ID}" | base64 -w0)"
  export AZURE_CLIENT_ID_B64="$(echo -n "${AZURE_CLIENT_ID}" | base64 -w0)"
  export AZURE_CLIENT_SECRET_B64="$(echo -n "${AZURE_CLIENT_SECRET}" | base64 -w0)"

  # clusterctl idempotency: check if providers already exist
  local existing
  existing="$(clusterctl config repositories 2>/dev/null || true)"

  clusterctl init \
    --infrastructure "azure:${CAPZ_VERSION}" \
    --bootstrap "talos:${CABPT_VERSION}" \
    --control-plane "talos:${CACPPT_VERSION}" \
    --wait-providers \
    --wait-provider-timeout 600 \
    2>&1 || {
      # clusterctl exits non-zero if providers already installed; check for that
      if kubectl get deploy -n capz-system capz-controller-manager &>/dev/null; then
        warn "CAPI providers appear already installed, continuing"
      else
        die "clusterctl init failed"
      fi
    }

  wait_for_condition "CAPZ controller ready" \
    "kubectl -n capz-system get deploy capz-controller-manager -o jsonpath='{.status.readyReplicas}' | grep -q '[1-9]'" \
    300

  wait_for_condition "Talos bootstrap controller ready" \
    "kubectl -n cabpt-system get deploy cabpt-controller-manager -o jsonpath='{.status.readyReplicas}' | grep -q '[1-9]'" \
    300

  wait_for_condition "Talos control-plane controller ready" \
    "kubectl -n cacppt-system get deploy cacppt-controller-manager -o jsonpath='{.status.readyReplicas}' | grep -q '[1-9]'" \
    300
}

# ──────────────────────────────────────────────────────────────────────────────
# Step 5: Azure identity secret for CAPZ
# ──────────────────────────────────────────────────────────────────────────────
ensure_capz_identity_secret() {
  log "Ensuring CAPZ identity secret…"
  kubectl create secret generic azure-cluster-identity-secret \
    --namespace default \
    --from-literal=clientSecret="${AZURE_CLIENT_SECRET}" \
    --dry-run=client -o yaml | kubectl apply -f -
}

# ──────────────────────────────────────────────────────────────────────────────
# Step 6: Workload cluster (Talos on Azure via CAPI)
# ──────────────────────────────────────────────────────────────────────────────
apply_workload_cluster() {
  log "Applying workload cluster manifests…"

  render_template "${MANIFESTS_DIR}/capi/cluster.yaml" | kubectl apply -f -
  render_template "${MANIFESTS_DIR}/capi/control-plane.yaml" | kubectl apply -f -
  render_template "${MANIFESTS_DIR}/capi/workers.yaml" | kubectl apply -f -

  log "Cluster resources applied. Waiting for control plane initialization…"
  wait_for_condition "Cluster '${CLUSTER_NAME}' control plane initialized" \
    "kubectl get cluster ${CLUSTER_NAME} -o jsonpath='{.status.conditions[?(@.type==\"ControlPlaneInitialized\")].status}' | grep -q True" \
    900 20

  log "Waiting for control plane to be fully ready…"
  wait_for_condition "Cluster '${CLUSTER_NAME}' control plane ready" \
    "kubectl get cluster ${CLUSTER_NAME} -o jsonpath='{.status.conditions[?(@.type==\"ControlPlaneReady\")].status}' | grep -q True" \
    900 20
}

# ──────────────────────────────────────────────────────────────────────────────
# Step 7: Retrieve kubeconfig and talosconfig
# ──────────────────────────────────────────────────────────────────────────────
retrieve_configs() {
  log "Retrieving workload cluster kubeconfig…"
  clusterctl get kubeconfig "${CLUSTER_NAME}" > "${OUTPUT_DIR}/${CLUSTER_NAME}.kubeconfig"
  log "Kubeconfig written to ${OUTPUT_DIR}/${CLUSTER_NAME}.kubeconfig"

  log "Retrieving talosconfig…"
  kubectl get secret "${CLUSTER_NAME}-talosconfig" -o jsonpath='{.data.talosconfig}' \
    | base64 -d > "${OUTPUT_DIR}/${CLUSTER_NAME}.talosconfig" 2>/dev/null || {
      warn "talosconfig secret not yet available — retrieve manually once ready:"
      warn "  kubectl get secret ${CLUSTER_NAME}-talosconfig -o jsonpath='{.data.talosconfig}' | base64 -d > ${OUTPUT_DIR}/${CLUSTER_NAME}.talosconfig"
    }

  log "Waiting for worker MachineDeployment to be ready…"
  wait_for_condition "Workers ready" \
    "kubectl get machinedeployment ${CLUSTER_NAME}-workers -o jsonpath='{.status.readyReplicas}' | grep -q '${WORKER_COUNT}'" \
    600 20 || warn "Workers not fully ready yet — check 'kubectl get machines' for status"
}

# ──────────────────────────────────────────────────────────────────────────────
# Step 8: Install CNI on workload cluster
# ──────────────────────────────────────────────────────────────────────────────
install_cni() {
  log "Applying Calico CNI to workload cluster…"
  local wk_kubeconfig="${OUTPUT_DIR}/${CLUSTER_NAME}.kubeconfig"

  if [[ ! -f "${wk_kubeconfig}" ]]; then
    warn "Kubeconfig not found at ${wk_kubeconfig}, skipping CNI install"
    return
  fi

  kubectl --kubeconfig="${wk_kubeconfig}" apply -f \
    https://raw.githubusercontent.com/projectcalico/calico/v3.27.0/manifests/calico.yaml \
    2>/dev/null || warn "CNI apply failed — cluster may not be reachable yet. Apply manually."

  log "CNI applied. Nodes should become Ready shortly."
}

# ──────────────────────────────────────────────────────────────────────────────
# Summary
# ──────────────────────────────────────────────────────────────────────────────
print_summary() {
  cat <<EOF

${GREEN}══════════════════════════════════════════════════════════════${NC}
  Bootstrap complete!
${GREEN}══════════════════════════════════════════════════════════════${NC}

  Management cluster : ${MGMT_CLUSTER_NAME} (kind)
  Workload cluster   : ${CLUSTER_NAME}
  Azure region       : ${AZURE_LOCATION}
  Resource group     : ${RESOURCE_GROUP}
  Control plane      : ${CONTROL_PLANE_COUNT}x ${CONTROL_PLANE_VM_SIZE}
  Workers            : ${WORKER_COUNT}x ${WORKER_VM_SIZE}
  Kubernetes         : ${KUBERNETES_VERSION}
  Talos              : ${TALOS_VERSION}

  Outputs:
    kubeconfig  → ${OUTPUT_DIR}/${CLUSTER_NAME}.kubeconfig
    talosconfig → ${OUTPUT_DIR}/${CLUSTER_NAME}.talosconfig

  Next steps:
    export KUBECONFIG=${OUTPUT_DIR}/${CLUSTER_NAME}.kubeconfig
    kubectl get nodes
    talosctl --talosconfig ${OUTPUT_DIR}/${CLUSTER_NAME}.talosconfig health

EOF
}

# ──────────────────────────────────────────────────────────────────────────────
# Main
# ──────────────────────────────────────────────────────────────────────────────
main() {
  preflight

  ensure_mgmt_cluster            # kind
  install_crossplane             # helm
  install_crossplane_providers   # Azure family providers
  configure_crossplane_azure_creds
  apply_crossplane_compositions  # XRD, Composition, Claim → networking

  install_capi                   # clusterctl init
  ensure_capz_identity_secret

  apply_workload_cluster         # Cluster, TalosControlPlane, MachineDeployment
  retrieve_configs               # kubeconfig + talosconfig
  install_cni                    # Calico on workload cluster

  print_summary
}

main "$@"
