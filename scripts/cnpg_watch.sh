#!/bin/bash
# cnpg_watch.sh — read-only DELTA tick for the cirrus-csg-postgres CNPG cluster
#
# The lean companion to cirrus_healthcheck.sh: that script is a full snapshot;
# this one is a recurring *tick*. It probes the same CNPG cluster (`csg-postgres`
# in namespace `pg-testing`, context `nwc1`) but keeps state between runs under
# $XDG_STATE_HOME/cnpg-watch/ and prints ONE terse line per section — expanding
# only what changed since the last tick. Meant to be driven on a ~30-min
# Claude-side timer (see the watch-cnpg skill); a quiet hour stays a few lines.
#
# It also nudges once a day: after ~24h it prints `deep-sweep: due`, the cue to
# run the full cirrus_healthcheck.sh for a routine baseline + tuning review.
#
# Read-only. Every DB probe is a SELECT; every kubectl call is get/logs/exec-psql.
# Never modifies cluster state.
#
# Usage:
#   scripts/cnpg_watch.sh [options]
#
# Options:
#   -n, --namespace NS     Namespace the cluster lives in   (default: pg-testing)
#   -c, --cluster    CL    CNPG cluster name                (default: csg-postgres)
#   -d, --database   DB    Application database name        (default: csg-pg)
#       --context    CTX   kubectl context to target        (default: current)
#       --db-host    HOST  TCP preflight host               (default: csg-postgres.k8s.ucar.edu)
#       --db-port    PORT  TCP preflight port               (default: 5432)
#       --state      FILE  State file path                  (default: $XDG_STATE_HOME/cnpg-watch/state)
#       --deep-interval S  Seconds between deep-sweep nudges (default: 86400)
#       --reset-baseline   Seed state from current values, print nothing, exit 0
#       --mark-deep-sweep  Stamp the deep-sweep timestamp to now, exit 0
#       --no-color         Disable ANSI color
#   -v, --verbose          Extra detail per section
#   -h, --help             Show this help

set -euo pipefail

NAMESPACE="pg-testing"
CLUSTER="csg-postgres"
DATABASE="csg-pg"
CONTEXT=""
DBHOST="csg-postgres.k8s.ucar.edu"
DBPORT="5432"
STATE="${XDG_STATE_HOME:-$HOME/.local/state}/cnpg-watch/state"
DEEP_INTERVAL=86400
RESET_BASELINE=0
MARK_DEEP=0
USE_COLOR=1
VERBOSE=0

# thresholds
CONN_PCT_WARN=80          # per-instance connections vs max_connections
FS_PCT_WARN=40            # fs_scans family share of max_connections
REPL_LAG_WARN=$((16*1024*1024))   # replication lag bytes
LOG_WINDOW_FALLBACK="35m" # first-tick log window before a marker exists

PASS_COUNT=0
WARN_COUNT=0
FAIL_COUNT=0

while [[ $# -gt 0 ]]; do
    case "$1" in
        -n|--namespace)     NAMESPACE="$2"; shift 2;;
        -c|--cluster)       CLUSTER="$2";   shift 2;;
        -d|--database)      DATABASE="$2";  shift 2;;
        --context)          CONTEXT="$2";   shift 2;;
        --db-host)          DBHOST="$2";    shift 2;;
        --db-port)          DBPORT="$2";    shift 2;;
        --state)            STATE="$2";     shift 2;;
        --deep-interval)    DEEP_INTERVAL="$2"; shift 2;;
        --reset-baseline)   RESET_BASELINE=1; shift;;
        --mark-deep-sweep)  MARK_DEEP=1;    shift;;
        --no-color)         USE_COLOR=0;    shift;;
        -v|--verbose)       VERBOSE=1;      shift;;
        -h|--help)          sed -n '2,32p' "$0" | sed 's/^# \{0,1\}//'; exit 0;;
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

NOW=$(date -u +%s)
NOW_ISO=$(date -u +%Y-%m-%dT%H:%M:%SZ)

# --- state helpers (flat KEY=VALUE file; portable, not sourced) --------------
# We rewrite the whole file each tick, re-emitting every key we read, so there
# is no carry-forward bookkeeping. NEW_STATE accumulates; committed atomically.
NEW_STATE=""
get_state() { grep "^$1=" "$STATE" 2>/dev/null | tail -1 | cut -d= -f2- || true; }
put_state() { NEW_STATE+="$1=$2"$'\n'; }
commit_state() {
    mkdir -p "$(dirname "$STATE")"
    printf '%s' "$NEW_STATE" > "$STATE"
}
# state keys are alphanumerics + underscore; sanitize pod names for keys
key_of() { echo "$1" | tr -c 'A-Za-z0-9' '_'; }

# --- output helpers ----------------------------------------------------------
line() { echo -e "$*"; }
pass() { PASS_COUNT=$((PASS_COUNT+1)); }
warn() { WARN_COUNT=$((WARN_COUNT+1)); }
fail() { FAIL_COUNT=$((FAIL_COUNT+1)); }
tag_ok()   { echo -e "${GREEN}$*${NC}"; }
tag_warn() { echo -e "${YELLOW}$*${NC}"; }
tag_fail() { echo -e "${RED}$*${NC}"; }

# psql -tAc against an explicit pod + db. Raw rows on stdout, errors swallowed.
pg_query_pod() {
    "${KCTL_NS[@]}" exec "$1" -c postgres -- \
        psql -U postgres -d "$2" -tAXqc "$3" 2>/dev/null || true
}

epoch_of() {  # RFC3339 -> epoch, GNU or BSD date
    awk -v d="$1" 'BEGIN{
        cmd="date -u -d \"" d "\" +%s 2>/dev/null || date -u -j -f %Y-%m-%dT%H:%M:%SZ \"" d "\" +%s 2>/dev/null"
        cmd | getline t; close(cmd); print t+0
    }'
}

# --- --mark-deep-sweep: stamp only, no cluster calls -------------------------
if [[ $MARK_DEEP -eq 1 ]]; then
    if [[ -f "$STATE" ]]; then
        NEW_STATE=$(grep -v '^LAST_DEEP_SWEEP=' "$STATE" 2>/dev/null || true)
        [[ -n "$NEW_STATE" ]] && NEW_STATE+=$'\n'
    fi
    NEW_STATE+="LAST_DEEP_SWEEP=$NOW"$'\n'
    commit_state
    echo "deep-sweep timestamp stamped ($NOW_ISO)"
    exit 0
fi

# --- 0. connectivity preflight (VPN-safe) ------------------------------------
# A fully-down VPN blackholes DNS/SYN; kubectl/psql timeouts don't cover it.
# TCP-probe the endpoint first and treat unreachable as a clean no-op, not a
# DB fault — so the timer survives an overnight VPN outage without alarms.
command -v kubectl >/dev/null 2>&1 || { echo "OFFLINE: kubectl not found in PATH"; exit 0; }
if command -v python3 >/dev/null 2>&1; then
    if ! python3 -c "import socket,sys; socket.create_connection(('$DBHOST',$DBPORT),4).close()" 2>/dev/null; then
        echo "OFFLINE: cannot reach ${DBHOST}:${DBPORT} (VPN down?) — no-op"
        exit 0
    fi
fi

CUR_CTX=$("${KCTL[@]}" config current-context 2>/dev/null || echo "")

# --reset-baseline seeds state from current values but prints no report. Probe
# and commit exactly as a normal tick, just with the human-facing output muted.
[[ $RESET_BASELINE -eq 1 ]] && exec >/dev/null

if [[ "$CUR_CTX" != "nwc1" && -z "$CONTEXT" ]]; then
    line "${YELLOW}ctx${NC}: current context '$CUR_CTX' (expected 'nwc1'; pass --context nwc1)"
    warn
fi

echo -e "${BOLD}${BLUE}── cnpg-watch ${NOW_ISO} (${CLUSTER}/${NAMESPACE}) ──${NC}"

# Resolve instances (pod=role). Read-only fs_scans load lands on the replica, so
# connection / temp-spill probes run PER-INSTANCE, not just against the primary.
INSTANCES=$("${KCTL_NS[@]}" get pods -l "cnpg.io/cluster=$CLUSTER" \
    -o jsonpath='{range .items[*]}{.metadata.name}={.metadata.labels.cnpg\.io/instanceRole}{"\n"}{end}' 2>/dev/null || true)
PRIMARY_POD=$("${KCTL_NS[@]}" get pods -l "cnpg.io/cluster=$CLUSTER,cnpg.io/instanceRole=primary" \
    -o jsonpath='{.items[0].metadata.name}' 2>/dev/null || echo "")
[[ -z "$PRIMARY_POD" ]] && PRIMARY_POD="${CLUSTER}-1"

# ============================================================================
# 1. cluster phase / failover
# ============================================================================
# PREV keys hold the last committed tick's values; we compare, then re-emit.
old_phase=$(get_state PHASE_PREV); old_primary=$(get_state PRIMARY_PREV)
CLUSTER_JSON=$("${KCTL_NS[@]}" get cluster.postgresql.cnpg.io "$CLUSTER" -o json 2>/dev/null || echo "")
if [[ -n "$CLUSTER_JSON" ]]; then
    # Rich path: the CNPG Cluster CR is readable.
    phase=$(echo "$CLUSTER_JSON" | awk -F'"' '/"phase":/{print $4; exit}')
    cur=$(echo "$CLUSTER_JSON"   | awk -F'"' '/"currentPrimary":/{print $4; exit}')
    tgt=$(echo "$CLUSTER_JSON"   | awk -F'"' '/"targetPrimary":/{print $4; exit}')
    ready=$(echo "$CLUSTER_JSON" | awk -F'[:,]' '/"readyInstances":/{gsub(/ /,""); print $2; exit}')
    inst=$(echo "$CLUSTER_JSON"  | awk -F'[:,]' '/"instances":/{gsub(/ /,""); print $2; exit}')
    healthy=0
    [[ "$phase" == "Cluster in healthy state" && "$cur" == "$tgt" && "${ready:-0}" == "${inst:-0}" && -n "${ready:-}" ]] && healthy=1
    changed=""
    [[ -n "$old_phase" && "$old_phase" != "$phase" ]] && changed=" (was: $old_phase)"
    [[ -n "$old_primary" && "$old_primary" != "$cur" ]] && changed="$changed ${RED}FAILOVER: primary $old_primary → $cur${NC}"
    if [[ $healthy -eq 1 && -z "$changed" ]]; then
        line "cluster: $(tag_ok healthy) (${ready}/${inst}, primary $cur)"; pass
    elif [[ $healthy -eq 1 ]]; then
        line "cluster: $(tag_warn changed) →$changed now healthy (${ready}/${inst}, primary $cur)"; warn
    else
        line "cluster: $(tag_fail unhealthy) phase='$phase' cur/tgt=$cur/$tgt ready=${ready:-?}/${inst:-?}$changed"; fail
    fi
    put_state PHASE_PREV "$phase"; put_state PRIMARY_PREV "$cur"
else
    # Fallback: reading the cluster-scoped CR is RBAC-forbidden in this Capsule
    # tenant (expected — not a fault). Derive health from the pods we CAN read:
    # every postgres container ready + a primary present == healthy.
    ready=$(echo "$INSTANCES" | grep -c '=' || true)
    total="$ready"
    notready=$("${KCTL_NS[@]}" get pods -l "cnpg.io/cluster=$CLUSTER" \
        -o jsonpath='{range .items[*]}{.status.containerStatuses[?(@.name=="postgres")].ready}{"\n"}{end}' 2>/dev/null \
        | grep -c '^false$' || true)
    cur="$PRIMARY_POD"
    changed=""
    [[ -n "$old_primary" && "$old_primary" != "$cur" ]] && changed=" ${RED}FAILOVER: primary $old_primary → $cur${NC}"
    if [[ "$total" -gt 0 && "${notready:-0}" -eq 0 && -n "$cur" && -z "$changed" ]]; then
        line "cluster: $(tag_ok healthy) (${total}/${total} pods, primary $cur) [pod-level]"; pass
    elif [[ "$total" -gt 0 && "${notready:-0}" -eq 0 && -n "$cur" ]]; then
        line "cluster: $(tag_warn changed)$changed now healthy (${total}/${total} pods, primary $cur) [pod-level]"; warn
    else
        line "cluster: $(tag_fail unhealthy) ${notready:-?} pod(s) not ready, primary='${cur:-none}' [pod-level]$changed"; fail
    fi
    put_state PHASE_PREV "pod-level"; put_state PRIMARY_PREV "$cur"
fi

# ============================================================================
# 2. pod restarts + image sha (deploy detection)
# ============================================================================
restart_flag=0; restart_msg=""
while IFS= read -r rl; do
    [[ -z "$rl" ]] && continue
    pod="${rl%%=*}"; rest="${rl#*=}"; n="${rest%%;*}"
    k=$(key_of "$pod")
    old_n=$(get_state "RESTART_$k")
    put_state "RESTART_$k" "${n:-0}"
    if [[ -n "$old_n" && "${n:-0}" -gt "$old_n" ]]; then
        restart_flag=1
        restart_msg="$restart_msg ${pod}:$((n-old_n)) new"
    fi
done <<< "$("${KCTL_NS[@]}" get pods -l "cnpg.io/cluster=$CLUSTER" \
    -o jsonpath='{range .items[*]}{.metadata.name}={.status.containerStatuses[?(@.name=="postgres")].restartCount};{"\n"}{end}' 2>/dev/null || true)"

# CNPG images are pinned by digest (postgresql@sha256:…) or tag (postgresql:18.3),
# not by a per-build sha-<hex> tag. Compare the full image reference so ANY change
# (tag or digest) is caught; display a short form on the terse line.
sha=$("${KCTL_NS[@]}" get pods -l "cnpg.io/cluster=$CLUSTER" \
    -o jsonpath='{.items[0].status.containerStatuses[?(@.name=="postgres")].imageID}' 2>/dev/null | tr -d '[:space:]' || true)
[[ -z "$sha" ]] && sha=$("${KCTL_NS[@]}" get pods -l "cnpg.io/cluster=$CLUSTER" \
    -o jsonpath='{.items[0].spec.containers[?(@.name=="postgres")].image}' 2>/dev/null | tr -d '[:space:]' || true)
img_short=$(echo "$sha" | grep -oE 'sha256:[0-9a-f]{12}' | head -1 || true)
[[ -z "$img_short" ]] && img_short="${sha##*/}"
old_sha=$(get_state IMAGE_REF)
put_state IMAGE_REF "$sha"
sha_changed=0
[[ -n "$old_sha" && -n "$sha" && "$old_sha" != "$sha" ]] && sha_changed=1

if [[ $restart_flag -eq 1 || $sha_changed -eq 1 ]]; then
    m="pods:"
    [[ $restart_flag -eq 1 ]] && m="$m $(tag_warn "restarts:")$restart_msg"
    [[ $sha_changed -eq 1 ]] && m="$m $(tag_warn "image changed") (now ${img_short}) — a deploy?"
    line "$m"; warn
else
    line "pods: $(tag_ok 'no new restarts') (image ${img_short})"; pass
fi

# ============================================================================
# 3. connections by consumer family (per instance)
# ============================================================================
CONN_CASE="CASE
  WHEN application_name LIKE '%:fs_scans:%'    THEN 'fs_scans'
  WHEN application_name LIKE '%:job_history:%' THEN 'job_history'
  WHEN application_name LIKE '%system_status%' THEN 'system_status'
  WHEN application_name LIKE 'sam-webapp%'     THEN 'sam-webapp:other'
  WHEN coalesce(application_name,'')=''        THEN '(untagged)'
  ELSE application_name END"
maxc=$(pg_query_pod "$PRIMARY_POD" "$DATABASE" "SHOW max_connections;" | tr -d '[:space:]')
[[ -z "$maxc" ]] && maxc=0
conn_flag=0; conn_lines=""
while IFS='=' read -r pod role; do
    [[ -z "$pod" ]] && continue
    total=$(pg_query_pod "$pod" "$DATABASE" "SELECT count(*) FROM pg_stat_activity WHERE backend_type='client backend';" | tr -d '[:space:]')
    [[ -z "$total" ]] && continue
    fs=$(pg_query_pod "$pod" "$DATABASE" "SELECT count(*) FROM pg_stat_activity WHERE backend_type='client backend' AND application_name LIKE '%:fs_scans:%';" | tr -d '[:space:]')
    pct=$(awk -v c="$total" -v m="$maxc" 'BEGIN{printf "%.0f",(m>0)?100*c/m:0}')
    fspct=$(awk -v c="${fs:-0}" -v m="$maxc" 'BEGIN{printf "%.0f",(m>0)?100*c/m:0}')
    tagpod="${pod##*-}(${role:-?})"
    conn_lines="$conn_lines ${tagpod}=${total}/${maxc}[fs:${fs:-0}]"
    if awk -v p="$pct" -v w="$CONN_PCT_WARN" 'BEGIN{exit !(p>=w)}'; then
        conn_flag=1; conn_lines="$conn_lines$(tag_warn "(${pct}%!)")"
    elif awk -v p="$fspct" -v w="$FS_PCT_WARN" 'BEGIN{exit !(p>=w)}'; then
        conn_flag=1; conn_lines="$conn_lines$(tag_warn "(fs ${fspct}%!)")"
    fi
    if [[ $VERBOSE -eq 1 ]]; then
        echo "  ${pod} (${role:-?}):"
        pg_query_pod "$pod" "$DATABASE" "SELECT '    '||fam||E'\t'||n FROM (SELECT $CONN_CASE AS fam, count(*) n FROM pg_stat_activity WHERE backend_type='client backend' GROUP BY 1 ORDER BY 2 DESC) s;"
    fi
done <<< "$INSTANCES"
if [[ $conn_flag -eq 1 ]]; then line "conns:$conn_lines"; warn
else line "conns: $(tag_ok ok)$conn_lines"; pass; fi

# ============================================================================
# 4. temp-file spill RATE (per instance) — work_mem pressure proxy
# ============================================================================
# pg_stat_database.temp_bytes is CUMULATIVE since stats reset; the signal is the
# DELTA between ticks. Growth points at slow-path aggregation exceeding work_mem
# (64MB) — usually the fs_scans recursive directory_stats walk on the replica.
spill_flag=0; spill_lines=""
while IFS='=' read -r pod role; do
    [[ -z "$pod" ]] && continue
    tb=$(pg_query_pod "$pod" "$DATABASE" "SELECT coalesce(sum(temp_bytes),0) FROM pg_stat_database;" | tr -d '[:space:]')
    [[ -z "$tb" ]] && continue
    k=$(key_of "$pod")
    old_tb=$(get_state "TEMP_$k")
    put_state "TEMP_$k" "$tb"
    if [[ -n "$old_tb" ]]; then
        d=$(( tb - old_tb ))
        (( d < 0 )) && d=0   # negative => stats reset; treat as no growth
        if (( d > 0 )); then
            pretty=$(awk -v b="$d" 'BEGIN{split("B KB MB GB TB",u);i=1;while(b>=1024&&i<5){b/=1024;i++}printf "%.1f%s",b,u[i]}')
            spill_lines="$spill_lines ${pod##*-}(${role:-?})+${pretty}"
            if (( d > 1024*1024*1024 )); then spill_flag=1; spill_lines="$spill_lines$(tag_warn '!')"; fi
        fi
    fi
done <<< "$INSTANCES"
if [[ -z "$spill_lines" ]]; then line "tempspill: $(tag_ok 'none since last tick')"; pass
elif [[ $spill_flag -eq 1 ]]; then line "tempspill:$(tag_warn "$spill_lines") — heavy slow-path; durable fix is query-side rollups, not work_mem"; warn
else line "tempspill:$spill_lines (minor)"; pass; fi

# ============================================================================
# 5. long-running queries (>5min) + replication lag
# ============================================================================
long=$(pg_query_pod "$PRIMARY_POD" "$DATABASE" "SELECT count(*) FROM pg_stat_activity WHERE state<>'idle' AND query_start IS NOT NULL AND now()-query_start>interval '5 minutes' AND backend_type='client backend';" | tr -d '[:space:]')
if [[ "${long:-0}" -gt 0 ]]; then
    line "longq: $(tag_warn "${long} quer(y/ies) >5min")"; warn
    [[ $VERBOSE -eq 1 ]] && pg_query_pod "$PRIMARY_POD" "$DATABASE" "SELECT '    pid '||pid||' '||(now()-query_start)||'  '||left(replace(query,E'\n',' '),70) FROM pg_stat_activity WHERE state<>'idle' AND now()-query_start>interval '5 minutes' AND backend_type='client backend';"
else
    line "longq: $(tag_ok 'none >5min')"; pass
fi

rep=$(pg_query_pod "$PRIMARY_POD" "$DATABASE" "SELECT coalesce(max(pg_wal_lsn_diff(pg_current_wal_lsn(), replay_lsn)),0)::bigint FROM pg_stat_replication;" | tr -d '[:space:]')
if [[ -n "$rep" ]] && awk -v r="$rep" -v w="$REPL_LAG_WARN" 'BEGIN{exit !(r>w)}'; then
    prettylag=$(awk -v b="$rep" 'BEGIN{split("B KB MB GB",u);i=1;while(b>=1024&&i<4){b/=1024;i++}printf "%.1f%s",b,u[i]}')
    line "repl: $(tag_warn "lag ${prettylag}")"; warn
else
    line "repl: $(tag_ok 'in sync')"; pass
fi

# ============================================================================
# 6. new notable log lines since last tick
# ============================================================================
last_log=$(get_state LAST_LOG_TIME)
if [[ -n "$last_log" ]]; then LOG_SINCE=(--since-time="$last_log"); else LOG_SINCE=(--since="$LOG_WINDOW_FALLBACK"); fi
logmatches=$("${KCTL_NS[@]}" logs "$PRIMARY_POD" -c postgres "${LOG_SINCE[@]}" --tail=-1 2>/dev/null \
    | grep -E '"error_severity":"(ERROR|FATAL|PANIC)"|"level":"(error|fatal)"|duration: [0-9]{5,}' || true)
put_state LAST_LOG_TIME "$NOW_ISO"
if [[ -z "$logmatches" ]]; then
    line "logs: $(tag_ok 'no new ERROR/FATAL/≥10s')"; pass
else
    n=$(echo "$logmatches" | grep -c . || true)
    sev=$(echo "$logmatches" | grep -cE '"error_severity":"(ERROR|FATAL|PANIC)"|"level":"(error|fatal)"' || true)
    if [[ "${sev:-0}" -gt 0 ]]; then line "logs: $(tag_fail "${sev} error/fatal") + $((n-sev)) slow(≥10s) since last tick"; fail
    else line "logs: $(tag_warn "${n} slow(≥10s)") since last tick (≥2s is expected)"; warn; fi
    echo "$logmatches" | tail -"${VERBOSE:+8}" | tail -3 | sed 's/^/    /'
fi

# ============================================================================
# 7. capacity & expiry (report only on band change)
# ============================================================================
band_report() {  # name value warn_thresh fail_thresh dir(hi|lo) prev_key
    local name="$1" val="$2" wt="$3" ft="$4" dir="$5" pk="$6"
    local band="ok"
    if [[ "$dir" == "hi" ]]; then
        awk -v v="$val" -v t="$ft" 'BEGIN{exit !(v>=t)}' && band="fail"
        [[ "$band" == "ok" ]] && awk -v v="$val" -v t="$wt" 'BEGIN{exit !(v>=t)}' && band="warn"
    else
        awk -v v="$val" -v t="$ft" 'BEGIN{exit !(v<=t)}' && band="fail"
        [[ "$band" == "ok" ]] && awk -v v="$val" -v t="$wt" 'BEGIN{exit !(v<=t)}' && band="warn"
    fi
    local prev; prev=$(get_state "$pk"); put_state "$pk" "$band"
    local delta=""; [[ -n "$prev" && "$prev" != "$band" ]] && delta=" (was $prev)"
    case "$band" in
        fail) echo "$(tag_fail "${name}=${val} [fail]")$delta"; fail;;
        warn) echo "$(tag_warn "${name}=${val} [warn]")$delta"; warn;;
        ok)   echo "${name}=${val}$delta"; pass;;
    esac
}
cap=""
PCT=$("${KCTL_NS[@]}" exec "$PRIMARY_POD" -c postgres -- df --output=pcent /var/lib/postgresql/data 2>/dev/null | awk 'NR==2{gsub(/%/,"");print $1}' || true)
[[ -n "$PCT" ]] && cap="$cap $(band_report pvc% "$PCT" 70 85 hi PVC_BAND)"
NEWEST=$("${KCTL_NS[@]}" get volumesnapshot -l "cnpg.io/cluster=$CLUSTER" -o jsonpath='{.items[-1:].metadata.creationTimestamp}' 2>/dev/null || true)
if [[ -n "$NEWEST" ]]; then
    bage=$(awk -v now="$NOW" -v t="$(epoch_of "$NEWEST")" 'BEGIN{printf "%.1f",(now-t)/86400}')
    cap="$cap $(band_report backup_d "$bage" 8 14 hi BACKUP_BAND)"
fi
CERT="${CLUSTER}-server-cert"
NOTAFTER=$("${KCTL_NS[@]}" get certificate "$CERT" -o jsonpath='{.status.notAfter}' 2>/dev/null || true)
if [[ -n "$NOTAFTER" ]]; then
    cdays=$(awk -v now="$NOW" -v t="$(epoch_of "$NOTAFTER")" 'BEGIN{printf "%.0f",(t-now)/86400}')
    cap="$cap $(band_report cert_d "$cdays" 30 7 lo CERT_BAND)"
fi
line "capacity:$cap"

# ============================================================================
# 8. daily deep-sweep nudge (informational; does not affect verdict)
# ============================================================================
last_deep=$(get_state LAST_DEEP_SWEEP)
if [[ $RESET_BASELINE -eq 1 ]]; then
    put_state LAST_DEEP_SWEEP "$NOW"   # baseline resets the deep-sweep clock too
elif [[ -z "$last_deep" ]]; then
    put_state LAST_DEEP_SWEEP "$NOW"
    line "deep-sweep: baseline stamped"
else
    put_state LAST_DEEP_SWEEP "$last_deep"
    age=$(( NOW - last_deep ))
    if (( age >= DEEP_INTERVAL )); then
        hrs=$(awk -v a="$age" 'BEGIN{printf "%.0f",a/3600}')
        line "deep-sweep: $(tag_warn 'due') (last ran ${hrs}h ago) — run scripts/cirrus_healthcheck.sh, then cnpg_watch.sh --mark-deep-sweep"
    else
        hrs=$(awk -v a="$age" 'BEGIN{printf "%.0f",a/3600}')
        [[ $VERBOSE -eq 1 ]] && line "deep-sweep: ok (last ran ${hrs}h ago)"
    fi
fi

# ============================================================================
# commit + verdict
# ============================================================================
commit_state

if [[ $RESET_BASELINE -eq 1 ]]; then
    echo "cnpg-watch: baseline seeded ($NOW_ISO)" >&2
    exit 0
fi

echo -e "→ ${GREEN}${PASS_COUNT} ok${NC}  ${YELLOW}${WARN_COUNT} warn${NC}  ${RED}${FAIL_COUNT} fail${NC}"
if   [[ $FAIL_COUNT -gt 0 ]]; then exit 2
elif [[ $WARN_COUNT -gt 0 ]]; then exit 1
else                               exit 0
fi
