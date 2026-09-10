---
name: watch-cnpg
description: >-
  Watching the deployed CloudNativePG cluster (csg-postgres, namespace
  pg-testing, context nwc1) that backs fs_scans and job_history on CIRRUS, for
  anomalies on a recurring tick. Load before starting or resuming a cnpg-watch
  loop to run scripts/cnpg_watch.sh, read and classify what it reports (cluster
  phase / failover, pod restarts / image deploys, connections by consumer,
  temp-file spill, long queries, replication lag, error logs, capacity), fire
  the once-a-day full sweep when it's due, and (re)start the Claude-side tick
  timer without spawning a duplicate — so a real regression is caught and a
  quiet hour stays a few lines.
---

# Watch CNPG

An ordered procedure for keeping an eye on the CloudNativePG database on CIRRUS
— cluster `csg-postgres`, namespace `pg-testing`, context `nwc1` — that backs
fs_scans (reads on the replica) and job_history / system_status (writes on the
primary). The mechanics live in `scripts/cnpg_watch.sh` — a read-only *delta*
tick. This skill carries the judgment the script can't: what a line means, what
to flag vs let ride, when to escalate to the full sweep, and how to run the
recurring wake without piling up duplicate timers.

Work top to bottom. Step 1 runs a tick; steps 2–5 read it; step 6 is the daily
deep sweep; step 7 schedules the recurring wake; step 8 is the traps.

## 1. Run a tick

```bash
scripts/cnpg_watch.sh                    # one tick; prints only what changed
scripts/cnpg_watch.sh --reset-baseline   # seed a fresh baseline, no report
scripts/cnpg_watch.sh --mark-deep-sweep  # stamp the deep-sweep clock (see step 6)
```

Prerequisites: the VPN is up (the script TCP-probes `csg-postgres.k8s.ucar.edu:5432`
first and exits `0` with `OFFLINE:` if it can't reach it — VPN down is not a DB
fault); `kubectl` targets the `nwc1` context (pass `--context nwc1` if it isn't
your current one); and you can `exec` into the CNPG pods (RBAC). Exit code is
`0` quiet / `1` warn / `2` fail, so a scheduler can alert on it. State (last
cluster phase / primary, per-pod restart counts, image ref, per-instance
`temp_bytes`, log marker, capacity bands, deep-sweep clock) lives outside the
repo under `$XDG_STATE_HOME/cnpg-watch/`.

**Report only what changed since the last tick.** A quiet tick is one terse line
per section; escalate a *trend across ticks*, not a single outlier. Everything
is read-only — every DB probe is a `SELECT`, every kubectl call is
`get`/`logs`/`exec … psql`. Never add a write path, and never modify cluster
state (see step 6).

## 2. Cluster & pods

- **`cluster:`** — `healthy (2/2, primary csg-postgres-N)` is the good case. A
  `phase` change, or `cur/tgt` divergence, means a **failover is in flight**;
  the script prints `FAILOVER: primary X → Y` when the primary moves. A settled
  failover (new primary, back to healthy) is worth one WARN line, not alarm; a
  *stuck* phase (not healthy across several ticks) is the real problem. Note the
  **`[pod-level]`** suffix: reading the cluster-scoped CNPG CR is RBAC-forbidden
  for the operator's OIDC user in this Capsule tenant (expected — not a fault),
  so the script falls back to pod readiness (every postgres container ready + a
  primary present == healthy) and detects failover from the primary pod's role
  label. If the CR ever becomes readable, the line drops the suffix and reports
  the richer `phase`/`readyInstances` directly.
- **`pods:`** — a restart-count delta (`csg-postgres-1:2 new`) or an **image
  changed** line (a Postgres image bump / operator-driven redeploy). Both are
  WARN. A single old restart won't re-flag — the script compares counts tick to
  tick. Correlate an image change with an intended deploy; an *unexpected* one
  is worth surfacing to Ben (deploy mechanics are his).

## 3. The performance line

- **`conns:`** — per-instance `count/max_connections` with the fs_scans share in
  `[fs:N]`. Remember the split: **fs_scans connections land on the replica**
  (it reads the `-ro` service), **job_history / system_status writers on the
  primary**. The script WARNs when an instance crosses ~80% of `max_connections`
  (300), or when fs_scans alone crosses ~40% — that's per-collection SQLAlchemy
  pools (default 5+10/collection/pod) growing under concurrency; the lever is
  bounding `pool_size`, not raising `max_connections`. `-v` prints the full
  consumer breakdown.
- **`tempspill:`** — the **delta** in `temp_bytes` since the last tick (the raw
  counter is cumulative). Growth is work_mem (64MB) pressure, almost always the
  fs_scans recursive `directory_stats` walk on the **replica**. A big jump
  WARNs. The durable fix is **query-side (pre-aggregated rollups)**, *not* a
  higher `work_mem` — the script says so on the line. Flag sustained growth
  across ticks, not one spike.
- **`longq:`** — client queries running >5min. `-v` prints pid / age / query
  head. One long fs_scans slow-path scan can be expected; several, or a
  *stuck* one, is the signal.

## 4. The log line

`logs: no new ERROR/FATAL/≥10s` is the normal case. The script reads only lines
*since the last tick* (`--since-time`), drops benign noise, and sorts the rest
into three buckets — the line reads `logs: N cluster-err + N app-err + N slow(≥10s)`
with only the non-zero buckets shown:

- **cluster-err → FAIL** (drives exit 2). Any ERROR/FATAL/PANIC whose sqlstate is
  *not* in the app-caused set below — connection/auth (`28…`), insufficient
  resources (`53…`), operator action (`57…`), system/internal (`58…`/`XX…`), and
  crucially **any unknown sqlstate**. This is the bucket that means the cluster (or
  its infra) needs you. Read the message; a `FATAL` here is usually a
  connection/auth issue or an OOM.
- **app-err → WARN** (exit 1, not 2). Client-caused SQL errors the server
  *correctly* rejected — sqlstate classes `22…` (data), `23…` (integrity, e.g.
  `23505` duplicate-key), `42…` (syntax/access), via `APP_ERR_RE`. Surfaced and
  worth chasing on the *app* side, but a healthy cluster serving a buggy client
  must not page a scheduler watching the exit code. (This is why the ~hourly SAM
  `xras_notices 23505` held the tick at WARN, never FAIL, during that incident.)
  Widen `APP_ERR_RE` only for a class that is *always* client-caused; when unsure,
  leave it in the FAIL bucket.
- **slow (≥10s) → WARN**. Note: `log_min_duration_statement=2000`, so every `≥2s`
  statement is logged — those 4-digit-ms durations are the *expected* fs_scans
  slow-path tail and are **not** surfaced here. Flag a NEW slow shape or a
  sustained rise in the ≥10s count, not the ambient noise. Admin/diagnostic
  `application_name=psql` sessions — chiefly `cirrus_healthcheck.sh`'s own
  history-span / `pg_stat_statements` probes — are excluded from this bucket
  (`SLOW_SELF_RE`), so running the deep sweep doesn't make the next tick WARN on
  the sweep's own ≥10s queries.
- **benign idle-timeout** (`sql_state_code 57P05`, "terminating connection due to
  idle-session timeout") is the error-path analogue of that ≥2s noise: the server
  healthily reaping idle pooled connections (chiefly SAM's `system_status`
  writers), high-volume and permanent — hundreds since the last tick is normal.
  The script filters these *out* of all three buckets (see `BENIGN_LOG_RE`) and
  reports them as a `[+N benign idle-timeout]` tail, so they never drive the exit
  code but a genuine surge (pool churn, a mass restart) is still visible as a
  trend. A *different* sqlstate confirmed benign can be added to the alternation.

The sample lines under the `logs:` line show **one representative per distinct
error shape** (sqlstate + message head), cluster-err first, capped at 3 (8 under
`-v`) — so a mixed batch never buries a distinct shape behind repeats of a noisier
one.

## 5. Capacity & expiry

`capacity:` reports PVC fill %, newest backup (VolumeSnapshot) age in days, and
TLS cert days-left. These move slowly, so the script only speaks up on a **band
change** (`[warn]`/`[fail]`, with `(was ok)`): PVC ≥70/85%, backup >8/14d,
cert <30/7d. `repl:` reports replication lag and WARNs past ~16MiB. A crossing
here is a plan-ahead item (capacity, a missed backup, a cert renewal), not a
fire — but a `[fail]` PVC or a stale backup does need action.

**`backup_d` is silently absent when VolumeSnapshots are RBAC-forbidden** —
they're cluster-scoped and currently outside this tenant's scope, so neither the
tick nor the deep sweep can see them for the operator's user. Its absence from
the line is *not* a "backups OK" signal; backup health has to be confirmed via
the Argo/cluster-admin path, not this watch.

## 6. The daily deep sweep

The tick is deliberately terse; once a day, take a full-picture reading with the
comprehensive probe:

```bash
scripts/cirrus_healthcheck.sh            # full snapshot: 12 sections + tuning hints
```

Run it in **two** situations:

1. **Reactively** — whenever a tick flags something worth investigating (a
   failover, a spill/conn trend, a FATAL log). The healthcheck's deeper
   per-instance psql probes, `pg_stat_statements` top-shape ranking, history
   spans, and event/log tail are where you diagnose.
2. **Routine, once a day** — the tick tracks when the deep sweep last ran and
   prints `deep-sweep: due (last ran Nh ago)` after ~24h. That line is your cue:
   run `cirrus_healthcheck.sh`, skim its **Tuning hints** block (over/under-
   provisioned CPU/mem, PVC sizing, temp-spill, long history), then stamp the
   clock so the nudge clears:

   ```bash
   scripts/cnpg_watch.sh --mark-deep-sweep
   ```

Tuning is a **recommendation**, never an action: the knobs live in
`helm/values.yaml` and `helm/templates/postgres_cluster.yaml`
(`db.resource.limits`, `db.size`, `max_connections`, `work_mem`,
`log_min_duration_statement`). Surface hints to Ben; **never modify cluster
state** and never edit the chart as part of a watch tick.

## 7. Scheduling the recurring wake ("restart only if necessary")

The tick repeats on a **Claude-side timer** (a scheduled cron, or a `/loop`),
NOT a cluster CronJob. Cadence ~30 min. Because the tick emits `deep-sweep: due`
after ~24h (step 6), that single timer also yields exactly one deep sweep per
day — there is no second timer to manage.

**Before starting a watch timer, check one isn't already running, and only
(re)start if none is live** — never spawn a duplicate:

1. `CronList` (or check `/tasks`) for an existing cnpg-watch cron/loop.
2. If one is already scheduled and healthy, leave it — do nothing.
3. If none exists (or it died), start one: a `CronCreate` cron at ~30-min
   cadence (or `/loop` in dynamic mode) whose prompt runs `scripts/cnpg_watch.sh`,
   reports only the deltas per steps 2–5, and runs the deep sweep (step 6) when
   the tick says it's due.

A dropped VPN makes a tick a clean no-op (step 1), so the timer survives an
overnight VPN outage without alarms.

## 8. Traps

- **TCP preflight is load-bearing.** A fully-down VPN blackholes DNS/SYN and the
  kubectl/psql connect timeouts don't cover it. The script probes TCP first and
  exits `0` with `OFFLINE:` — that is not a DB fault, don't escalate it.
- **`kubectl logs -l <sel>` defaults to `--tail=10` per pod.** The script passes
  `--tail=-1` with `--since-time`; without it the log section silently
  undercounts.
- **`temp_bytes` / restart counts are cumulative.** The tick reports the
  **delta** between ticks; after a stats reset (a pod restart) the delta reads
  as a drop and is treated as no growth, not a negative.
- **Primary vs replica is not cosmetic.** fs_scans read load, its temp-spill,
  and its `pg_stat_statements` costs accumulate on the **replica** (`-ro`), not
  the primary — the tick and the healthcheck both probe per-instance for exactly
  this reason. Don't diagnose fs_scans load off the primary alone.
- **Everything is read-only.** `SELECT`/`kubectl exec … psql` probes only —
  never a write path, never a cluster mutation, never a chart edit from a tick.
- **State lives outside the repo.** Don't commit a state file; `--reset-baseline`
  re-seeds it after a long gap so a stale phase/primary/count isn't reported as
  a change.
- **`pg_stat_statements` needs `CREATE EXTENSION` in `campaign`.** The
  healthcheck's top-shape ranking is empty until that one-time manual post-deploy
  step is done; the tick doesn't depend on it, but the deep sweep's tuning view
  does.
