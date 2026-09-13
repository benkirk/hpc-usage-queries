# CNPG backups: current gap and a path to a mature backup policy

**Status:** Future work, **not implemented.** The `csg-postgres` CNPG cluster
configures a backup *mechanism* but has **no resource that triggers a backup**, so
it is almost certainly taking **zero automated backups** today. This doc records
the gap, the constraints that make the obvious quick fixes wrong, and the
recommended path — so the analysis does not have to be re-derived when it becomes
urgent.

**Recommendation:** when SAM approaches living in CNPG for prod *exclusively* (or
any irreplaceable tenant is added), adopt the **barman object-store → Boreas S3
path** (plugin-managed retention + point-in-time recovery) as the primary
mechanism. Whole-cluster volume snapshots and a per-database logical-dump tier are
secondary/interim tools, each with a real caveat (below). Until then this is a
deliberate, low-risk deferral: the current tenants are all recreatable or pre-prod.

> **Why this is safe to defer today.** Every database on the cluster right now is
> either fully recreatable (`campaign` is rebuilt weekly from SQLite;
> `casper_jobs`/`derecho_jobs` re-sync from PBS logs) or still pre-prod (SAM). The
> only irreplaceable state — SAM's `system_status` (~527 MB) — is not yet the
> production system of record. The trigger for acting is SAM going CNPG-prod, not
> a date.

## 1. Current state (the gap)

Backups are configured in two places, and the configuration is inert without a
trigger:

- `helm/values.yaml` — `db.backups.volumeSnapshot.enabled: true`
  (`snapshotClassName: csi-rbdplugin-snapclass`); `db.backups.s3.enabled: false`
  with the Boreas endpoint already present
  (`https://boreas.hpc.ucar.edu:6443`, `destinationPath: s3://csg`).
- `helm/templates/postgres_cluster.yaml` — a `backup:` stanza (volumeSnapshot
  `className`; `retentionPolicy: "4w"` hardcoded; a `barmanObjectStore` block
  gated entirely on `s3.enabled`, so currently rendered off).

**The missing piece:** in CNPG, `.spec.backup` only configures *how* a backup
would be taken. A backup is only ever performed when a `Backup` or
`ScheduledBackup` object exists. **There is no `ScheduledBackup` anywhere in
`helm/templates/`** (the templates are only `cert.yaml`, `pg_service.yaml`,
`pg_service_ro.yaml`, `postgres_cluster.yaml`, and the two external-secret
templates). So: no periodic base backup, and — because `s3.enabled: false` — no
WAL archiving either. Net: **no automated backups of any kind.**

**Backup health is also unobservable to us.** VolumeSnapshots and CNPG `Backup`
objects are Forbidden to the operator's OIDC user under the Capsule tenant, so
neither `scripts/cnpg_watch.sh` nor `scripts/cirrus_healthcheck.sh` can confirm
backup health — see the watch-cnpg skill's "`backup_d` is silently absent" note.
Absence of a backup warning is *not* a "backups OK" signal.

## 2. Hard constraints (verified 2026-09-13 against the live cluster)

These are why the intuitive quick fixes are wrong:

- **Operator CNPG 1.28.0** (operand `ghcr.io/cloudnative-pg/postgresql:18.3`),
  read from `pod/csg-postgres-1` annotation `cnpg.io/operatorVersion`.
- **Volume snapshots do not support retention in 1.28.** The operator does not
  prune them, and `spec.backup.retentionPolicy` is **deprecated** — the CNPG 1.28
  backup docs say to "rely on the retention mechanisms provided by the backup
  plugin." A daily `volumeSnapshot` `ScheduledBackup` would therefore accumulate
  snapshots **unbounded**. (This killed the first instinct — "just add a daily
  ScheduledBackup, retain 7.")
- **Physical CNPG backups are whole-cluster, not per-database.** Both volume
  snapshots and barman capture the entire cluster (all databases), so on the
  single shared cluster you cannot back up only `system_status`. "Back up some,
  not all" cannot be expressed with physical backups on one cluster (see §4/§5).
- **No Prometheus/Alertmanager scrapes `pg-testing`** (confirmed with the
  operator). `enablePodMonitor: true` would therefore create an object nothing
  consumes — metric-based backup visibility is inert here and is deferred (§6).
- **RBAC:** the operator's OIDC user cannot create or list `scheduledbackups`,
  `backups`, `podmonitors`, or `prometheusrules` in `pg-testing`. The ArgoCD
  deploy ServiceAccount is a separate identity and already creates the `Cluster`
  CR in the `postgresql.cnpg.io` group, so chart-created CNPG CRs are *likely*
  fine — but this is unverified, and `monitoring.coreos.com` create rights are
  unverified.

## 3. Databases on the cluster (backup priority is inverse to size)

| Database | Module | Size | Recreatable? |
|----------|--------|------|--------------|
| `campaign` | fs_scans | largest (tens of M rows/collection) | **Yes** — rebuilt weekly from SQLite `.db` via consolidation |
| `casper_jobs` / `derecho_jobs` | job_history | ~43 GB / ~30 GB | **Yes** — re-syncable from PBS logs |
| `system_status` (+ `sam_dev`) | SAM | ~527 MB | **No — irreplaceable** |

The large databases are the *least* important to back up. Worse, `campaign` is
rebuilt weekly via an atomic `ALTER SCHEMA RENAME` swap (a full DROP+recreate of
block content), so any *long* snapshot retention is actively wasteful: every
weekly snapshot boundary straddles a rebuild and captures a full-size delta of
regenerable data. This is the core reason snapshots are the wrong tool for
long-retention of the data that actually matters.

## 4. Reference: how NCAR/rda-python-dbms does it

`NCAR/rda-python-dbms` runs production PostgreSQL on the same Cirrus provider and
is the worked example for the pieces we lack. Per `pgdb01-cirrus/templates/`:

- `weekly-backup.yaml` — a `kind: ScheduledBackup` (`method: volumeSnapshot`,
  `target`, `backupOwnerReference: self`, six-field cron) that actually *triggers*
  backups. **This is the trigger we are missing.**
- `objectstore.yaml` + `s3_backup.yaml` — barman WAL archiving to the **same
  Boreas endpoint our chart already points at** (retention on the object store,
  WAL bzip2 compression) → the PITR path.
- `backups_external_secret.yaml` — S3 credentials via OpenBao (ExternalSecret).
- `alert-rule.yaml` + `alert-email.yaml` — `enablePodMonitor: true` plus a
  `PrometheusRule` (replication lag, cluster-down) with email routing — **only
  useful with a Prometheus stack, which we do not have** (§6).
- **Tiering by recreatability:** they run four separate clusters, and
  `pgdb04-cirrus` has **no `db.backups` section at all** — validating the
  "back up some, not all" instinct as *cluster-per-tier* rather than
  per-database (§5, prod end-state).

Do not copy verbatim (four clusters, their alerting stack). Mine the object-store
+ ScheduledBackup pattern.

## 5. Recommended target and the interim options

### Primary target — barman object-store → Boreas S3 (PITR + real retention)
The path CNPG steers you toward now that snapshot retention is deprecated. Our
chart already has the endpoint and a gated in-tree `barmanObjectStore` stanza, so
the delta is small:
1. Create the S3 credentials secret for Boreas (`s3://csg`) via an ExternalSecret,
   modeled on rda's `backups_external_secret.yaml`.
2. Enable archiving — either flip the existing in-tree `barmanObjectStore` stanza
   (`s3.enabled: true`) or move to the newer `barman-cloud.cloudnative-pg.io`
   plugin (rda uses the plugin; it is the forward-looking form).
3. Add a `ScheduledBackup` for periodic base backups (the object-store plugin then
   manages base-backup retention; WAL archiving gives continuous PITR, ~seconds
   RPO).

This gives real, plugin-managed retention **and** point-in-time recovery for
SAM's irreplaceable writes — which whole-cluster snapshots (daily RPO, no
retention story in 1.28) cannot.

### Interim / alternative options (each with its caveat)
- **Daily `volumeSnapshot` ScheduledBackup + a prune CronJob.** Keeps snapshots
  as the mechanism, but *requires* an external pruner (delete `Backup` objects
  older than N days → garbage-collects their snapshots) because 1.28 will not
  prune. Extra RBAC for the CronJob's SA (delete on `backups.postgresql.cnpg.io`).
  Cheapest "real backups now," but the ugliest retention story.
- **Logical `pg_dump` tier for the SAM DBs only.** A `CronJob` dumping just
  `system_status` (+ `sam_dev`) to S3, retained weeks/months. ~527 MB, per-database
  granularity, cheap long retention — the clean way to honor "back up some, not
  all" on one shared cluster. No PITR; slower restore; acceptable for this tier.
- **Cluster-per-tier (rda pattern) — prod end-state.** When SAM goes
  CNPG-prod-exclusive: put SAM on a dedicated *backed-up* cluster and leave
  job_history/fs_scans on an *unbacked-up* cluster (like rda `pgdb04-cirrus`).
  Cleanest separation of backup policy by criticality; most infrastructure.

## 6. Observability (deferred, coupled to a monitoring stack)

`enablePodMonitor: true` on the Cluster would expose CNPG's metrics, including
`cnpg_collector_last_available_backup_timestamp` (backup age — the exact signal
our RBAC-blind scripts cannot see), `cnpg_pg_replication_lag`, and
`cnpg_collector_up`. This is the natural way to close the backup-visibility gap
*even under Capsule RBAC* (metrics come from the exporter, not from listing
cluster-scoped objects). **But it requires a Prometheus that scrapes `pg-testing`
plus Alertmanager routing, which this cluster lacks.** Record it as coupled to any
future monitoring-stack rollout; shipping it alone would produce a dangling,
unscraped PodMonitor.

## 7. RBAC asks for the platform team

- VolumeSnapshot / `Backup` get+list in the tenant (to *observe* backups), **or**
  a Prometheus metric path (§6). Either closes the visibility gap.
- `clusters.postgresql.cnpg.io` get/patch, or the `cnpg` kubectl plugin — to read
  the Cluster CR directly (today the watch falls back to pod-level) and to run
  true `switchover` tests.
- Confirm the ArgoCD deploy SA can create `ScheduledBackup` and object-store
  resources before relying on the chart path (§2).

## 8. Related / cross-references

- Cluster tuning and the two manual post-deploy steps:
  [`implemented/CNPG_fs_scans.md`](implemented/CNPG_fs_scans.md).
- **Separate hardening item (not part of backups):** the effective `pg_hba` rule
  is `host all all 0.0.0.0/0 md5` (all curated rules commented out) and
  `enableSuperuserAccess: true`. Worth its own hardening review; noted here only so
  it is not lost.

## 9. If pursued (sketch, not for now)

1. ExternalSecret for Boreas S3 creds (model on rda `backups_external_secret.yaml`).
2. Enable archiving: in-tree `barmanObjectStore` (`s3.enabled: true`) **or** the
   `barman-cloud.cloudnative-pg.io` plugin on the Cluster; set object-store
   retention.
3. Add `helm/templates/scheduled_backup.yaml` — a `ScheduledBackup` for periodic
   base backups, gated on a values flag.
4. (If a monitoring stack lands) `monitoring.enablePodMonitor: true` + a minimal
   `PrometheusRule` (stale-backup age, replication lag, cluster down).
5. (For "some, not all" long retention) a `pg_dump` `CronJob` for the SAM DBs.
6. **Verify:** a `Backup` object appears on schedule; WAL is archived to Boreas;
   run a **restore drill** (recover to a scratch cluster / PITR to a timestamp)
   before declaring the policy real. Update `scripts/cirrus_healthcheck.sh` /
   watch expectations once the RBAC or metric path makes backups observable.
