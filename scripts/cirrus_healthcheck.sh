#!/bin/bash
# cirrus_healthcheck.sh — opinionated probe for the cirrus-csg-postgres release
#
# Inspects the CloudNativePG cluster deployed by helm/ (CNPG cluster
# `csg-postgres` in namespace `pg-testing` by default). Designed to be read
# top-to-bottom by someone new to Kubernetes: each section prints what it's
# checking, the raw kubectl/psql output, a short explanation, and a
# PASS / WARN / FAIL line.
#
# Read-only. Never modifies cluster state.
#
# Usage:
#   scripts/cirrus_healthcheck.sh [options]
#
# Options:
#   -n, --namespace NS    Namespace the release lives in   (default: pg-testing)
#   -r, --release    REL  Helm release name                (default: cirrus-csg-postgres)
#   -c, --cluster    CL   CNPG cluster name                (default: csg-postgres)
#   -d, --database   DB   Application database name       (default: csg-pg)
#       --context    CTX  kubectl context to target       (default: current)
#       --no-color        Disable ANSI color
#   -v, --verbose         Extra detail per section
#   -h, --help            Show this help

set -euo pipefail

NAMESPACE="pg-testing"
RELEASE="cirrus-csg-postgres"
CLUSTER="csg-postgres"
DATABASE="csg-pg"
CONTEXT=""
USE_COLOR=1
VERBOSE=0

PASS_COUNT=0
WARN_COUNT=0
FAIL_COUNT=0
TUNING_HINTS=()

while [[ $# -gt 0 ]]; do
    case "$1" in
        -n|--namespace) NAMESPACE="$2"; shift 2;;
        -r|--release)   RELEASE="$2";   shift 2;;
        -c|--cluster)   CLUSTER="$2";   shift 2;;
        -d|--database)  DATABASE="$2";  shift 2;;
        --context)      CONTEXT="$2";   shift 2;;
        --no-color)     USE_COLOR=0;    shift;;
        -v|--verbose)   VERBOSE=1;      shift;;
        -h|--help)      sed -n '2,22p' "$0" | sed 's/^# \{0,1\}//'; exit 0;;
        *) echo "Unknown option: $1" >&2; exit 2;;
    esac
done

if [[ $USE_COLOR -eq 1 && -t 1 ]]; then
    BLUE=$'\033[0;34m'; GREEN=$'\033[0;32m'; YELLOW=$'\033[1;33m'
    RED=$'\033[0;31m';  CYAN=$'\033[0;36m';  BOLD=$'\033[1m'; NC=$'\033[0m'
else
    BLUE=""; GREEN=""; YELLOW=""; RED=""; CYAN=""; BOLD=""; NC=""
fi

KCTL=(kubectl)
[[ -n "$CONTEXT" ]] && KCTL+=(--context "$CONTEXT")
KCTL_NS=("${KCTL[@]}" -n "$NAMESPACE")

# --- helpers ----------------------------------------------------------------

section() {
    echo
    echo -e "${BOLD}${BLUE}═══ $* ═══${NC}"
}

explain() {
    echo -e "  ${CYAN}↳ $*${NC}"
}

pass() { echo -e "  ${GREEN}✔ PASS${NC} — $*"; PASS_COUNT=$((PASS_COUNT+1)); }
warn() { echo -e "  ${YELLOW}⚠ WARN${NC} — $*"; WARN_COUNT=$((WARN_COUNT+1)); }
fail() { echo -e "  ${RED}✘ FAIL${NC} — $*"; FAIL_COUNT=$((FAIL_COUNT+1)); }
info() { echo -e "  ${CYAN}ℹ${NC} $*"; }
hint() { TUNING_HINTS+=("$1"); }

# Run a kubectl/exec command and indent its output for readability.
run() {
    "$@" 2>&1 | sed 's/^/    /' || true
}

# psql -tAc against the primary pod. Echoes raw rows on stdout (no indent).
# PRIMARY_POD is resolved lazily in section 2/3.
PRIMARY_POD=""
pg_query() {
    local sql="$1"
    local pod="${PRIMARY_POD:-${CLUSTER}-1}"
    "${KCTL_NS[@]}" exec "$pod" -c postgres -- \
        psql -U postgres -d "$DATABASE" -tAXqc "$sql" 2>/dev/null
}

# Same, but against an arbitrary database name (for cross-DB introspection).
pg_query_db() {
    local db="$1"
    local sql="$2"
    local pod="${PRIMARY_POD:-${CLUSTER}-1}"
    "${KCTL_NS[@]}" exec "$pod" -c postgres -- \
        psql -U postgres -d "$db" -tAXqc "$sql" 2>/dev/null
}

require_cmd() {
    command -v "$1" >/dev/null 2>&1 || { fail "$1 not found in PATH"; exit 1; }
}

human_bytes() {
    awk -v b="$1" 'BEGIN{
        split("B KB MB GB TB PB",u);
        i=1; while (b>=1024 && i<6){ b/=1024; i++ }
        printf "%.1f%s", b, u[i]
    }'
}

# Strip CNPG resource units (Ki, Mi, Gi, m, n, u) to a plain number.
# CPU: 16 -> 16 cores, 250m -> 0.25, 100000000n -> 0.1
# Mem: 128Gi -> 128*1024^3, 4096Mi -> 4096*1024^2
to_cores() {
    local v="$1"
    case "$v" in
        *n) awk -v x="${v%n}" 'BEGIN{printf "%.3f", x/1e9}';;
        *u) awk -v x="${v%u}" 'BEGIN{printf "%.3f", x/1e6}';;
        *m) awk -v x="${v%m}" 'BEGIN{printf "%.3f", x/1000}';;
        *)  awk -v x="$v"     'BEGIN{printf "%.3f", x+0}';;
    esac
}
to_bytes() {
    local v="$1"
    case "$v" in
        *Ki) awk -v x="${v%Ki}" 'BEGIN{printf "%.0f", x*1024}';;
        *Mi) awk -v x="${v%Mi}" 'BEGIN{printf "%.0f", x*1024*1024}';;
        *Gi) awk -v x="${v%Gi}" 'BEGIN{printf "%.0f", x*1024*1024*1024}';;
        *Ti) awk -v x="${v%Ti}" 'BEGIN{printf "%.0f", x*1024*1024*1024*1024}';;
        *K)  awk -v x="${v%K}"  'BEGIN{printf "%.0f", x*1000}';;
        *M)  awk -v x="${v%M}"  'BEGIN{printf "%.0f", x*1000000}';;
        *G)  awk -v x="${v%G}"  'BEGIN{printf "%.0f", x*1000000000}';;
        *)   awk -v x="$v"      'BEGIN{printf "%.0f", x+0}';;
    esac
}

# ============================================================================
section "0. Preflight"
# ============================================================================

require_cmd kubectl
require_cmd helm
require_cmd awk

CUR_CTX=$("${KCTL[@]}" config current-context 2>/dev/null || echo "")
echo "  kubectl context: ${CUR_CTX:-<none>}"
if [[ -z "$CUR_CTX" ]]; then
    fail "no kubectl context selected"
    exit 1
elif [[ "$CUR_CTX" != "nwc1" && -z "$CONTEXT" ]]; then
    warn "current context is '$CUR_CTX' (expected 'nwc1'); pass --context to override"
else
    pass "kubectl reachable, context='$CUR_CTX'"
fi
explain "kubectl needs a 'context' to know which cluster to talk to. We expect 'nwc1'."

if "${KCTL[@]}" get namespace "$NAMESPACE" >/dev/null 2>&1; then
    pass "namespace '$NAMESPACE' exists"
else
    fail "namespace '$NAMESPACE' not found"
    exit 1
fi

if helm status -n "$NAMESPACE" "$RELEASE" >/dev/null 2>&1; then
    HAS_HELM_RELEASE=1
    pass "helm release '$RELEASE' present in '$NAMESPACE'"
else
    HAS_HELM_RELEASE=0
fi

# Detect deployment manager. ArgoCD renders Helm via 'helm template' and applies
# the rendered manifests as plain k8s objects; no helm release Secret is ever
# created. Argo tracks ownership via the 'app.kubernetes.io/instance' label.
ARGO_INSTANCE=$("${KCTL_NS[@]}" get pods -l "cnpg.io/cluster=$CLUSTER" \
                -o jsonpath='{.items[0].metadata.labels.app\.kubernetes\.io/instance}' 2>/dev/null || echo "")
# Robust check: try to look up the Application CRD by name. Forbidden also
# counts as "present" since it means the CRD is installed but we can't read it.
crd_probe=$("${KCTL[@]}" get crd applications.argoproj.io 2>&1 || true)
if echo "$crd_probe" | grep -qiE 'NAME|forbidden|^applications.argoproj.io'; then
    ARGO_PRESENT=1
else
    ARGO_PRESENT=0
fi

if [[ -n "$ARGO_INSTANCE" && $ARGO_PRESENT -eq 1 ]]; then
    pass "deployment managed by ArgoCD (app.kubernetes.io/instance='$ARGO_INSTANCE'; Application CRD installed cluster-wide)"
    explain "Argo renders Helm via 'helm template' then applies as plain manifests — 'helm list' is empty by design. Use the Argo WebUI for sync/health/last-sync."
    if [[ $HAS_HELM_RELEASE -eq 0 ]]; then
        info "expected: no helm-tracked release in '$NAMESPACE' (Argo is the owner)"
    fi
    tenant_out=$("${KCTL[@]}" get tenant --no-headers 2>/dev/null || true)
    if [[ -n "$tenant_out" ]]; then
        info "Capsule multi-tenancy active (tenants: $(echo "$tenant_out" | awk '{print $1}' | tr '\n' ',' | sed 's/,$//')) — cluster-scoped resources (Application CR, VolumeSnapshots, etc.) live outside your tenant scope; use the WebUI for those"
    fi
elif [[ $HAS_HELM_RELEASE -eq 1 ]]; then
    :  # already PASSed above
else
    warn "no helm release and no ArgoCD tracking label detected — chart may have been applied as raw manifests"
    explain "Neither 'helm list' nor the app.kubernetes.io/instance label found this deployment; ownership is unclear."
fi

crd_err=$("${KCTL[@]}" get crd clusters.postgresql.cnpg.io 2>&1 >/dev/null || true)
if [[ -z "$crd_err" ]]; then
    pass "CNPG CRD installed (clusters.postgresql.cnpg.io)"
elif echo "$crd_err" | grep -qi forbidden; then
    # Indirect proof: pods exist with cnpg.io/cluster label — operator is running
    if "${KCTL_NS[@]}" get pods -l "cnpg.io/cluster=$CLUSTER" -o name >/dev/null 2>&1; then
        pass "CNPG operator inferred from cnpg.io/cluster-labeled pods (CRD check Forbidden)"
    else
        warn "cannot verify CNPG operator (CRD list Forbidden, no labeled pods found)"
    fi
else
    fail "CNPG CRD not installed — operator missing from this cluster"
fi
explain "CNPG is the Kubernetes operator that manages our PostgreSQL cluster CR."

# ============================================================================
section "1. Helm release summary"
# ============================================================================

if [[ $HAS_HELM_RELEASE -eq 1 ]]; then
    run helm status -n "$NAMESPACE" "$RELEASE" --show-resources
    explain "Lists the chart version, release status, and the objects helm owns."
    if [[ $VERBOSE -eq 1 ]]; then
        echo "  Non-default values (helm get values):"
        run helm get values -n "$NAMESPACE" "$RELEASE"
    fi
    pass "helm release inspected"
elif [[ -n "$ARGO_INSTANCE" ]]; then
    info "deployment is Argo-managed; sync/health/history live in the Argo WebUI"
    info "search for Application name (or label): ${BOLD}${ARGO_INSTANCE}${NC}"
    explain "Things to check in the WebUI: Sync Status (Synced), Health Status (Healthy), Last Sync timestamp, and Source repo+path+revision (which branch is the source of truth)."
else
    info "no helm release named '$RELEASE' — skipping"
fi

# ============================================================================
section "2. CNPG cluster status"
# ============================================================================

cnpg_err=$("${KCTL_NS[@]}" get cluster.postgresql.cnpg.io "$CLUSTER" -o json 2>&1 >/dev/null || true)
CLUSTER_JSON=$("${KCTL_NS[@]}" get cluster.postgresql.cnpg.io "$CLUSTER" -o json 2>/dev/null || echo "")
HAS_CLUSTER_CR=0
if [[ -z "$CLUSTER_JSON" ]]; then
    if echo "$cnpg_err" | grep -qi forbidden; then
        warn "RBAC: not permitted to read cluster.postgresql.cnpg.io — falling back to pod-level checks"
        explain "Ask cluster admin for: get/list on clusters.postgresql.cnpg.io in '$NAMESPACE' (and ideally volumesnapshots) for richer healthchecks."
    else
        fail "CNPG Cluster '$CLUSTER' not found in namespace '$NAMESPACE'"
    fi
else
    HAS_CLUSTER_CR=1
    phase=$(echo "$CLUSTER_JSON"     | awk -F'"' '/"phase":/{print $4; exit}')
    cur=$(echo "$CLUSTER_JSON"       | awk -F'"' '/"currentPrimary":/{print $4; exit}')
    tgt=$(echo "$CLUSTER_JSON"       | awk -F'"' '/"targetPrimary":/{print $4; exit}')
    ready=$(echo "$CLUSTER_JSON"     | awk -F'[:,]' '/"readyInstances":/{gsub(/ /,""); print $2; exit}')
    instances=$(echo "$CLUSTER_JSON" | awk -F'[:,]' '/"instances":/{gsub(/ /,""); print $2; exit}')

    echo "  phase            : $phase"
    echo "  primary (cur/tgt): $cur / $tgt"
    echo "  ready / total    : ${ready:-?} / ${instances:-?}"
    explain "phase='Cluster in healthy state' is the good case. cur==tgt means no failover in flight."

    if [[ "$phase" == "Cluster in healthy state" && "$cur" == "$tgt" \
          && "${ready:-0}" == "${instances:-0}" && -n "${ready:-}" ]]; then
        pass "CNPG cluster healthy ($ready/$instances ready, primary=$cur)"
    else
        warn "CNPG cluster not fully healthy — see fields above"
    fi
fi

# ============================================================================
section "3. Pods & restarts"
# ============================================================================

run "${KCTL_NS[@]}" get pods -l "cnpg.io/cluster=$CLUSTER" \
    -o 'custom-columns=NAME:.metadata.name,ROLE:.metadata.labels.cnpg\.io/instanceRole,READY:.status.containerStatuses[?(@.name=="postgres")].ready,RESTARTS:.status.containerStatuses[?(@.name=="postgres")].restartCount,AGE:.metadata.creationTimestamp,NODE:.spec.nodeName'
explain "Each pod is one PostgreSQL instance. ROLE=primary/replica. RESTARTS > 0 means the process died at least once."

# Resolve actual primary pod via CNPG instanceRole label
PRIMARY_POD=$("${KCTL_NS[@]}" get pods -l "cnpg.io/cluster=$CLUSTER,cnpg.io/instanceRole=primary" \
              -o jsonpath='{.items[0].metadata.name}' 2>/dev/null || echo "")
if [[ -z "$PRIMARY_POD" ]]; then
    PRIMARY_POD="${CLUSTER}-1"
    warn "could not identify primary by label — defaulting to ${PRIMARY_POD}"
else
    info "current primary pod: ${PRIMARY_POD}"
fi

# parse restart counts + last-restart time
restarts=$("${KCTL_NS[@]}" get pods -l "cnpg.io/cluster=$CLUSTER" \
            -o jsonpath='{range .items[*]}{.metadata.name}={.status.containerStatuses[?(@.name=="postgres")].restartCount};{.status.containerStatuses[?(@.name=="postgres")].lastState.terminated.finishedAt}{"\n"}{end}' \
            2>/dev/null)
bad_restart=0
while IFS= read -r line; do
    [[ -z "$line" ]] && continue
    pod="${line%%=*}"
    rest="${line#*=}"; n="${rest%%;*}"; when="${rest#*;}"
    [[ "${n:-0}" -eq 0 ]] && continue
    if [[ -n "$when" ]]; then
        age_days=$(awk -v d="$when" 'BEGIN{
            cmd="date -u +%s"; cmd | getline now; close(cmd);
            cmd="date -u -d \"" d "\" +%s 2>/dev/null || date -u -j -f %Y-%m-%dT%H:%M:%SZ \"" d "\" +%s 2>/dev/null"
            cmd | getline t; close(cmd);
            printf "%.0f", (now-t)/86400
        }')
        if [[ "${age_days:-0}" -lt 7 ]]; then
            warn "pod $pod restarted $n time(s); last restart ${age_days}d ago — investigate"
            bad_restart=1
        else
            info "pod $pod has $n restart(s); most recent ${age_days}d ago (old, likely benign)"
        fi
    else
        warn "pod $pod has $n restart(s) — could not determine when"
        bad_restart=1
    fi
done <<< "$restarts"
[[ $bad_restart -eq 0 ]] && pass "no recent (<7d) restarts on postgres containers"

# ============================================================================
section "4. Resource usage vs limits"
# ============================================================================

if [[ $HAS_CLUSTER_CR -eq 1 ]]; then
    LIMITS=$("${KCTL_NS[@]}" get cluster.postgresql.cnpg.io "$CLUSTER" \
              -o jsonpath='{.spec.resources.limits.cpu} {.spec.resources.limits.memory}' 2>/dev/null)
else
    # Fall back: read limits straight off the postgres container in pod -1
    LIMITS=$("${KCTL_NS[@]}" get pod "${CLUSTER}-1" \
              -o jsonpath='{.spec.containers[?(@.name=="postgres")].resources.limits.cpu} {.spec.containers[?(@.name=="postgres")].resources.limits.memory}' 2>/dev/null)
fi
read -r LIM_CPU LIM_MEM <<< "$LIMITS"
echo "  configured limits: cpu=${LIM_CPU:-?}  memory=${LIM_MEM:-?}"
explain "These come from helm/values.yaml (db.resource.limits). If real usage is far below, we're over-provisioned."

if ! "${KCTL_NS[@]}" top pods -l "cnpg.io/cluster=$CLUSTER" --no-headers >/dev/null 2>&1; then
    warn "kubectl top unavailable (metrics-server not installed?) — skipping live usage"
else
    echo
    echo "  live usage snapshot:"
    run "${KCTL_NS[@]}" top pods -l "cnpg.io/cluster=$CLUSTER" --containers

    lim_cpu_cores=$(to_cores "${LIM_CPU:-0}")
    lim_mem_bytes=$(to_bytes "${LIM_MEM:-0}")

    while read -r pod ctr cpu mem; do
        [[ -z "$pod" || "$ctr" != "postgres" ]] && continue
        u_cpu=$(to_cores "$cpu")
        u_mem=$(to_bytes "$mem")
        if [[ "$(awk -v a="$lim_cpu_cores" 'BEGIN{print (a>0)}')" == 1 ]]; then
            cpu_pct=$(awk -v u="$u_cpu" -v l="$lim_cpu_cores" 'BEGIN{printf "%.1f", 100*u/l}')
            mem_pct=$(awk -v u="$u_mem" -v l="$lim_mem_bytes" 'BEGIN{printf "%.1f", 100*u/l}')
            echo "  $pod/postgres: cpu=${u_cpu}c (${cpu_pct}% of $lim_cpu_cores) | mem=$(human_bytes "$u_mem") (${mem_pct}% of $(human_bytes "$lim_mem_bytes"))"

            band_cpu=$(awk -v p="$cpu_pct" 'BEGIN{
                if (p<10) print "low"; else if (p<60) print "ok"; else if (p<80) print "warm"; else print "hot"
            }')
            band_mem=$(awk -v p="$mem_pct" 'BEGIN{
                if (p<10) print "low"; else if (p<60) print "ok"; else if (p<80) print "warm"; else print "hot"
            }')
            case "$band_cpu" in
                low)  hint "CPU on $pod is ${cpu_pct}% of $lim_cpu_cores — consider lowering db.resource.limits.cpu in helm/values.yaml";;
                hot)  warn "$pod CPU at ${cpu_pct}% of limit — close to ceiling";;
            esac
            case "$band_mem" in
                low)  hint "Memory on $pod is ${mem_pct}% of $(human_bytes "$lim_mem_bytes") — consider lowering db.resource.limits.memory";;
                hot)  warn "$pod memory at ${mem_pct}% of limit — close to ceiling";;
            esac
        fi
    done < <("${KCTL_NS[@]}" top pods -l "cnpg.io/cluster=$CLUSTER" --containers --no-headers 2>/dev/null)
    pass "live usage sampled (this is a single instant — re-run later for trend)"
fi

# ============================================================================
section "5. Storage / PVCs"
# ============================================================================

run "${KCTL_NS[@]}" get pvc -l "cnpg.io/cluster=$CLUSTER"
explain "Each instance has its own PVC sized per helm db.size (default 512Gi)."

DF=$("${KCTL_NS[@]}" exec "${CLUSTER}-1" -c postgres -- df -B1 --output=size,used,pcent /var/lib/postgresql/data 2>/dev/null \
        | awk 'NR==2{print $1" "$2" "$3}' || echo "")
if [[ -n "$DF" ]]; then
    read -r SZ USED PCT <<< "$DF"
    echo "  ${CLUSTER}-1 data dir: $(human_bytes "$USED") used of $(human_bytes "$SZ") (${PCT})"
    pnum=${PCT%\%}
    if   [[ ${pnum:-0} -ge 85 ]]; then fail "PVC ${PCT} full — at risk"
    elif [[ ${pnum:-0} -ge 70 ]]; then warn "PVC ${PCT} full — plan capacity"
    elif [[ ${pnum:-0} -lt 5 ]];  then
        pass "PVC only ${PCT} used"
        hint "PVC only ${PCT} used of $(human_bytes "$SZ") — consider shrinking db.size in helm/values.yaml on next deploy (note: PVC shrink usually requires recreate)"
    else                              pass "PVC ${PCT} used — healthy"
    fi
else
    warn "could not stat data dir inside ${CLUSTER}-1"
fi

# ============================================================================
section "6. Backups (VolumeSnapshots)"
# ============================================================================

vs_err=$("${KCTL_NS[@]}" get volumesnapshot -l "cnpg.io/cluster=$CLUSTER" 2>&1 >/dev/null || true)
if echo "$vs_err" | grep -qi forbidden; then
    warn "RBAC: not permitted to list volumesnapshots — skipping backup check"
    explain "Ask admin for get/list on volumesnapshots.snapshot.storage.k8s.io in '$NAMESPACE'."
    NEWEST=""
else
    run "${KCTL_NS[@]}" get volumesnapshot -l "cnpg.io/cluster=$CLUSTER" --sort-by=.metadata.creationTimestamp
    explain "CNPG creates VolumeSnapshots for backup (retention 4w). readyToUse=true means good."
    NEWEST=$("${KCTL_NS[@]}" get volumesnapshot -l "cnpg.io/cluster=$CLUSTER" \
              -o jsonpath='{.items[-1:].metadata.creationTimestamp}' 2>/dev/null || echo "")
fi
if [[ -z "$NEWEST" && -z "$vs_err" ]]; then
    warn "no VolumeSnapshots found — has a backup run yet?"
elif [[ -z "$NEWEST" ]]; then
    :  # already handled above
else
    age_days=$(awk -v d="$NEWEST" 'BEGIN{
        cmd="date -u +%s"; cmd | getline now; close(cmd);
        cmd="date -u -d \"" d "\" +%s 2>/dev/null || date -u -j -f %Y-%m-%dT%H:%M:%SZ \"" d "\" +%s 2>/dev/null"
        cmd | getline t; close(cmd);
        printf "%.1f", (now-t)/86400
    }')
    echo "  newest snapshot: $NEWEST (${age_days}d old)"
    if   awk -v a="$age_days" 'BEGIN{exit !(a>14)}'; then fail "newest backup is ${age_days}d old"
    elif awk -v a="$age_days" 'BEGIN{exit !(a>8)}';  then warn "newest backup is ${age_days}d old"
    else                                                  pass "recent backup present (${age_days}d old)"
    fi
fi

# ============================================================================
section "7. TLS certificate"
# ============================================================================

CERT="${CLUSTER}-server-cert"
if "${KCTL_NS[@]}" get certificate "$CERT" >/dev/null 2>&1; then
    NOTAFTER=$("${KCTL_NS[@]}" get certificate "$CERT" -o jsonpath='{.status.notAfter}' 2>/dev/null)
    echo "  $CERT notAfter: $NOTAFTER"
    days_left=$(awk -v d="$NOTAFTER" 'BEGIN{
        cmd="date -u +%s"; cmd | getline now; close(cmd);
        cmd="date -u -d \"" d "\" +%s 2>/dev/null || date -u -j -f %Y-%m-%dT%H:%M:%SZ \"" d "\" +%s 2>/dev/null"
        cmd | getline t; close(cmd);
        printf "%.1f", (t-now)/86400
    }')
    if   awk -v a="$days_left" 'BEGIN{exit !(a<7)}';  then fail "cert expires in ${days_left}d"
    elif awk -v a="$days_left" 'BEGIN{exit !(a<30)}'; then warn "cert expires in ${days_left}d"
    else                                                   pass "cert valid for ${days_left}d"
    fi
else
    warn "Certificate '$CERT' not found"
fi
explain "Self-signed cert managed by cert-manager (helm/templates/cert.yaml)."

# ============================================================================
section "8. ExternalSecrets (OpenBao sync)"
# ============================================================================

if "${KCTL_NS[@]}" get externalsecret >/dev/null 2>&1; then
    run "${KCTL_NS[@]}" get externalsecret -o wide
    explain "ExternalSecrets sync DB credentials from OpenBao every hour. STATUS should be 'SecretSynced'."
    not_ready=$("${KCTL_NS[@]}" get externalsecret \
                  -o jsonpath='{range .items[*]}{.metadata.name}={.status.conditions[?(@.type=="Ready")].status}{"\n"}{end}' \
                  | awk -F= '$2!="True"{print $1}')
    if [[ -z "$not_ready" ]]; then
        pass "all ExternalSecrets Ready=True"
    else
        for s in $not_ready; do warn "ExternalSecret '$s' not Ready"; done
    fi
else
    warn "ExternalSecret CRD not present (external-secrets operator?)"
fi

# ============================================================================
section "9. In-pod psql probes"
# ============================================================================

if ! "${KCTL_NS[@]}" exec "${CLUSTER}-1" -c postgres -- true >/dev/null 2>&1; then
    warn "cannot exec into ${CLUSTER}-1 — skipping psql probes"
else
    # Connections vs max
    conns=$(pg_query "SELECT count(*) FROM pg_stat_activity;" | tr -d '[:space:]')
    maxc=$(pg_query "SHOW max_connections;" | tr -d '[:space:]')
    if [[ -n "$conns" && -n "$maxc" ]]; then
        pct=$(awk -v c="$conns" -v m="$maxc" 'BEGIN{printf "%.1f", 100*c/m}')
        echo "  connections: $conns / $maxc (${pct}%)"
        explain "max_connections is set in helm/templates/postgres_cluster.yaml (currently 500)."
        if   awk -v p="$pct" 'BEGIN{exit !(p>80)}'; then warn "connection pool ${pct}% full"
        elif awk -v p="$pct" 'BEGIN{exit !(p<5)}';  then
            pass "${conns}/${maxc} connections"
            hint "Only $conns/$maxc connections in use — max_connections=$maxc may be far higher than needed"
        else                                             pass "${conns}/${maxc} connections"
        fi
    fi

    # DB sizes
    echo
    echo "  database sizes:"
    pg_query "SELECT datname || '  ' || pg_size_pretty(pg_database_size(datname)) FROM pg_database WHERE datistemplate=false ORDER BY pg_database_size(datname) DESC;" \
        | sed 's/^/    /'
    explain "Total bytes on disk per database. Compare against PVC size from section 5."

    # Per-DB history span: scan every timestamp/date column and report earliest record.
    echo
    echo "  history span per database (oldest → newest record, per timestamp column):"
    explain "Tells you how many days/years of history each DB retains. Built by introspecting information_schema for every timestamp/date column and running MIN/MAX."

    SPAN_SQL=$(cat <<'PLSQL'
DO $$
DECLARE
    rec RECORD;
    min_ts timestamp; max_ts timestamp; n bigint;
BEGIN
    CREATE TEMP TABLE IF NOT EXISTS _hist(
        schema_name text, table_name text, column_name text,
        oldest timestamp, newest timestamp, n bigint
    );
    DELETE FROM _hist;
    FOR rec IN
        SELECT c.table_schema, c.table_name, c.column_name
        FROM information_schema.columns c
        JOIN information_schema.tables t
          ON t.table_schema=c.table_schema AND t.table_name=c.table_name
        WHERE (c.data_type LIKE 'timestamp%' OR c.data_type='date')
          AND c.table_schema NOT IN ('pg_catalog','information_schema','pg_toast')
          AND t.table_type='BASE TABLE'
    LOOP
        BEGIN
            EXECUTE format(
                'SELECT MIN(%I)::timestamp, MAX(%I)::timestamp, COUNT(*) FROM %I.%I WHERE %I IS NOT NULL',
                rec.column_name, rec.column_name,
                rec.table_schema, rec.table_name, rec.column_name)
              INTO min_ts, max_ts, n;
            IF min_ts IS NOT NULL THEN
                INSERT INTO _hist VALUES (rec.table_schema, rec.table_name, rec.column_name, min_ts, max_ts, n);
            END IF;
        EXCEPTION WHEN OTHERS THEN NULL;
        END;
    END LOOP;
END $$;
SELECT  schema_name
     || E'\t' || table_name || '.' || column_name
     || E'\t' || to_char(oldest,'YYYY-MM-DD')
     || E'\t' || to_char(newest,'YYYY-MM-DD')
     || E'\t' || EXTRACT(EPOCH FROM (newest - oldest))::bigint
     || E'\t' || n
FROM _hist
ORDER BY oldest ASC, n DESC;
PLSQL
)

    DBS=$(pg_query "SELECT datname FROM pg_database WHERE datistemplate=false AND datname<>'postgres' ORDER BY pg_database_size(datname) DESC;")
    while read -r db; do
        [[ -z "$db" ]] && continue
        echo "    ${BOLD}${db}${NC}"
        rows=$(pg_query_db "$db" "$SPAN_SQL")
        if [[ -z "$rows" ]]; then
            echo "      (no timestamp/date columns with data)"
            continue
        fi
        printed_oldest=""
        row_idx=0
        total_rows=$(echo "$rows" | grep -c .)
        while IFS=$'\t' read -r schema tabcol oldest newest secs nrows; do
            [[ -z "$tabcol" ]] && continue
            row_idx=$((row_idx+1))
            if [[ $VERBOSE -eq 0 && $row_idx -gt 5 ]]; then
                if [[ $row_idx -eq 6 ]]; then
                    echo "      … $((total_rows-5)) more column(s) — re-run with --verbose to see all"
                fi
                continue
            fi
            # Format span: years if >=365 days, else days
            span_str=$(awk -v s="$secs" 'BEGIN{
                d=s/86400;
                if (d>=365) printf "%.2fy", d/365.25;
                else        printf "%.1fd",  d
            }')
            qual=""
            if [[ "$schema" != "public" ]]; then qual="${schema}."; fi
            printf "      %-32s  %s → %s  (%s, %s rows)\n" \
                "${qual}${tabcol}" "$oldest" "$newest" "$span_str" "$nrows"
            if [[ -z "$printed_oldest" ]]; then
                printed_oldest="$oldest"
                # Surface the earliest-overall date for this DB as a tuning hint candidate
                age_y=$(awk -v s="$secs" 'BEGIN{printf "%.2f", (s/86400)/365.25}')
                if awk -v y="$age_y" 'BEGIN{exit !(y>5)}'; then
                    hint "$db retains ${age_y}y of history (oldest=${oldest} via ${qual}${tabcol}) — consider archiving rows older than your retention policy"
                fi
            fi
        done <<< "$rows"
    done <<< "$DBS"

    # Replication
    echo
    echo "  replication state (from primary):"
    rep=$(pg_query "SELECT application_name || '  ' || state || '  lag_bytes=' || COALESCE(pg_wal_lsn_diff(pg_current_wal_lsn(), replay_lsn)::text, 'n/a') FROM pg_stat_replication;")
    if [[ -z "$rep" ]]; then
        warn "no replicas reporting to primary"
    else
        echo "$rep" | sed 's/^/    /'
        bad=$(echo "$rep" | awk '/lag_bytes=[0-9]+/{ match($0,/lag_bytes=[0-9]+/); s=substr($0,RSTART+10,RLENGTH-10)+0; if (s>16777216) print }')
        if [[ -n "$bad" ]]; then warn "replication lag >16MiB observed"
        else                     pass "replication looks healthy"
        fi
    fi
    explain "Standby pulls WAL from primary. Lag should stay near zero on an idle DB."

    # Long-running queries
    echo
    echo "  queries running >5min:"
    long=$(pg_query "SELECT pid || '  ' || COALESCE(usename,'?') || '  ' || state || '  ' || (now()-query_start) || '  ' || left(replace(query, E'\n',' '),80) FROM pg_stat_activity WHERE state<>'idle' AND query_start IS NOT NULL AND now()-query_start > interval '5 minutes' AND backend_type='client backend';")
    if [[ -z "$long" ]]; then
        pass "no queries running >5min"
    else
        echo "$long" | sed 's/^/    /'
        warn "$(echo "$long" | wc -l | tr -d ' ') long-running quer(y/ies) — investigate"
    fi

    # Top tables in app DB
    if [[ $VERBOSE -eq 1 ]]; then
        echo
        echo "  top 5 tables in $DATABASE by total size:"
        pg_query "SELECT relname || '  ' || pg_size_pretty(pg_total_relation_size(c.oid)) FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace WHERE relkind='r' AND n.nspname NOT IN ('pg_catalog','information_schema') ORDER BY pg_total_relation_size(c.oid) DESC LIMIT 5;" \
            | sed 's/^/    /'
    fi
fi

# ============================================================================
section "10. Recent log lines"
# ============================================================================

if "${KCTL_NS[@]}" logs "${CLUSTER}-1" -c postgres --tail=200 >/dev/null 2>&1; then
    # CNPG emits one JSON object per line: {"level":"...","record":{"error_severity":"ERROR",...}}
    matches=$("${KCTL_NS[@]}" logs "${CLUSTER}-1" -c postgres --tail=500 2>/dev/null \
              | grep -E '"error_severity":"(ERROR|FATAL|PANIC)"|"level":"(error|fatal)"|duration: [0-9]{6,}' || true)
    if [[ -z "$matches" ]]; then
        pass "no ERROR/FATAL/long-duration lines in last 500 log lines"
    else
        echo "$matches" | tail -10 | sed 's/^/    /'
        warn "$(echo "$matches" | wc -l | tr -d ' ') notable log line(s) in last 500"
    fi
    explain "log_min_duration_statement=120000 means queries >120s are logged with 'duration:'."
else
    warn "could not read logs for ${CLUSTER}-1"
fi

# ============================================================================
section "11. Recent namespace events"
# ============================================================================

EV=$("${KCTL_NS[@]}" get events --sort-by=.lastTimestamp 2>/dev/null | tail -20 || echo "")
if [[ -z "$EV" ]]; then
    info "no events"
else
    echo "$EV" | sed 's/^/    /'
    warn_lines=$(echo "$EV" | awk '$2=="Warning"' | wc -l | tr -d ' ')
    if [[ "$warn_lines" -gt 0 ]]; then warn "$warn_lines Warning event(s) in last 20"
    else                                pass "no Warning events in last 20"
    fi
fi
explain "Events are short-lived (~1h). 'Warning' rows are worth scanning."

# ============================================================================
section "Summary"
# ============================================================================

echo "  Results: ${GREEN}${PASS_COUNT} PASS${NC}  ${YELLOW}${WARN_COUNT} WARN${NC}  ${RED}${FAIL_COUNT} FAIL${NC}"
echo
if [[ ${#TUNING_HINTS[@]} -gt 0 ]]; then
    echo "  ${BOLD}Tuning hints${NC} (review helm/values.yaml on next chart bump):"
    for h in "${TUNING_HINTS[@]}"; do
        echo "    • $h"
    done
else
    echo "  No tuning hints from this run."
fi
echo
echo "  Re-run later (cpu/mem are one-shot samples). For continuous logs:"
echo "    kubectl logs -n $NAMESPACE -f ${PRIMARY_POD:-${CLUSTER}-1} -c postgres"

if [[ $FAIL_COUNT -gt 0 ]]; then exit 2
elif [[ $WARN_COUNT -gt 0 ]]; then exit 1
else                               exit 0
fi
