# QHist Database Schema

This document describes the optimized database schema for NCAR's Casper and Derecho HPC job history.

## Overview

- **Databases**: Separate SQLite files per machine (`data/casper.db`, `data/derecho.db`), or per-machine PostgreSQL databases (`casper_jobs`, `derecho_jobs`)
- **Schema**: Normalized with foreign keys and composite indexes
- **Timestamps**: Naive UTC — epoch seconds converted to UTC then stored without timezone info so they round-trip correctly through both PostgreSQL (any server timezone) and SQLite
- **Performance**: 5-10x query speedup via optimization
- **1:1 invariant**: Every `jobs` row is guaranteed to have a `job_charges` row, enforced by the `trg_ensure_job_charge` database trigger

## Schema Design

### Normalization Strategy

The schema uses **foreign key normalization** with **hybrid properties** for frequently-queried text fields:

- **users** table: Maps `user_id` → `username` (~3,500 entries)
- **accounts** table: Maps `account_id` → `account_name` (~1,300 entries)
- **queues** table: Maps `queue_id` → `queue_name` (~150 entries)
- **job_qos** table: Maps `qos_id` → (`name`, `factor`) — canonical priority class with its charging multiplier (4 seed rows)

The `user`, `account`, `queue`, and `qos` attributes are implemented as SQLAlchemy `@hybrid_property` decorators that:
- Return text values from relationships (e.g., `job.user` → `"alice"`, `job.qos` → `"premium"`)
- Accept text assignments via setters (e.g., `job.user = "alice"`, `job.qos = "premium"`)
- Generate SQL subqueries for filtering (e.g., `Job.user == "alice"`)
- Maintain 100% backward compatibility with denormalized schema

Benefits:
- Integer joins vastly faster than text comparisons
- Reduced storage (IDs vs repeated strings)
- Referential integrity enforced
- Transparent access pattern (looks like text columns to application code)

### Materialized Charges

The **job_charges** table stores pre-computed resource hours:
- Eliminates on-the-fly calculation overhead
- Machine-specific charging rules applied during import
- 1:1 correspondence with jobs table

### Composite Indexes

> **Corrected 2026-07-28.** This section previously listed `(queue_id, end)`,
> `(queue_id, user_id, end)` and `(queue_id, account_id, end)`, and claimed a
> planner trace through `ix_jobs_queue_end`. **None of those indexes exist** —
> `database/models.py` is the only inventory, and every entity composite there
> is on `submit`, not `end`. The accurate list is below and in
> § *Indexes* further down; do not plan against the old text.

`Job.__table_args__` defines seven composites:

- `(user_id, account_id)` — `ix_jobs_user_account`
- `(submit, end)` — `ix_jobs_submit_end`
- `(user_id, submit)` / `(account_id, submit)` / `(queue_id, submit)`
- `(account_id, end)` — `ix_jobs_account_end`; `(user_id, end)` — `ix_jobs_user_end`

plus single-column btrees on `job_id`, `short_id`, `status`, `submit`,
`start`, **`end`** (`ix_jobs_end`), and the four lookup FKs.

`DailySummary` adds `ix_daily_summary_date` and
`ix_daily_summary_user_account`.

**Aggregates do not need the `end` composites.** PostgreSQL resolves a scoped
aggregate with `BitmapAnd(ix_jobs_account_id, ix_jobs_end)` — on casper_jobs
(21.0M rows) a 30-day account-scoped aggregate runs in 119 ms, user-scoped in 202 ms, and with `owners_limit=10` in 257 ms.
In that plan the BitmapAnd costs ~23 ms of 105 ms while the nested loop into
`job_charges` accounts for 638k of 684k buffers.

The join, not the index, is where the time goes: on a wide unscoped aggregate
it is **45 %** of total runtime (`EXPLAIN` shows a full seq scan of `jobs`
hash-joined against a full seq scan of *all* of `job_charges`). The cheap win
there is not an index but avoiding the scan altogether — see
`JobQueries._timeseries_uses_summary`, which serves qualifying series off
`daily_summary` ~800× faster. Full numbers in
`docs/plans/JOBS_TIMESERIES.md` § *Where the time actually goes*.

**Top-N does.** `jobs_search(account=…, start=…, end=…, limit=50)` (SAM's jobs
table) is `ORDER BY end DESC LIMIT 50`, and without `(account_id, end)` the
planner walks `ix_jobs_end` backward filtering on `account_id`. That is fast for
an account that ran recently and pathological for one whose jobs sit early in a
wide window: it reads every other account's newer rows first. Measured in
production 2026-09-30 (derecho 16.3M rows, casper 27.5M; window
2025-10-01..2026-09-30, top 50). "Before" ran on the primary, first touch; the
long cases were cold and setting hint bits (`dirtied` buffers):

| scope | before | rows filtered | after |
|---|---|---|---|
| derecho P86850054 (active) | 6,014 ms | 384,878 | 1.2 ms |
| derecho WYOM0224 (stalled) | 58,123 ms | 3,997,555 | 0.5 ms |
| derecho UCUB0056 (stalled, warm) | 1,200 ms | 3,398,566 | 0.3 ms |
| derecho user, stopped early (warm) | 1,049 ms | 2,472,537 | 0.3 ms |
| casper NMMM0073 (stalled) | 108,897 ms | 7,287,219 | 4.1 ms |
| casper user, stopped early (warm) | 2,093 ms | 5,769,647 | 8.8 ms |
| casper 2-account tree (`IN`), replica, indexes built | 81,934 ms | 6,790,889 | 1.2 ms (split) |

The local PG 18 mirror reproduces the shape (0.8-1.5 s warm for a stalled
account). 30-day scoped aggregates did not regress there (42 → 20 ms, 8 → 7 ms,
21 → 3 ms; the planner switches to the new composites). In production each
index built in 9.5-19 s: 368 MB each on derecho, 508 MB each on casper.

**A multi-account tree still needs the query split.** With several leading
`account_id` values the composite cannot return rows in `end` order, so an
`account_id IN (...)` top-N keeps walking `ix_jobs_end` (the 81,934 ms row).
`jobs_search` therefore runs a multi-account search ordered by `end` as one
`ORDER BY end LIMIT offset+limit` branch per account, `UNION ALL`-ed and
re-sorted (`_account_fanout_ids`); each branch is a short walk of
`ix_jobs_account_end`.

`ix_jobs_user_end` covers the same shape for a user-scoped table. There is no
`(queue_id, end)`: no top-N surface scopes by queue alone.

**Adding them to an existing database** — `create_all` only builds indexes for
new tables, so run on the primary, per machine DB (applied to production
`derecho_jobs` and `casper_jobs` 2026-09-30):

```sql
CREATE INDEX CONCURRENTLY IF NOT EXISTS ix_jobs_account_end ON jobs (account_id, "end");
CREATE INDEX CONCURRENTLY IF NOT EXISTS ix_jobs_user_end    ON jobs (user_id, "end");
SELECT indexrelid::regclass, indisvalid FROM pg_index
 WHERE indexrelid::regclass::text IN ('ix_jobs_account_end', 'ix_jobs_user_end');
```

An interrupted concurrent build leaves `indisvalid = f`, and `IF NOT EXISTS`
then skips it silently — drop and rebuild.

**Follow-up: drop the unused `submit` composites.** Candidates are
`ix_jobs_account_submit` and `ix_jobs_submit_end` (1.4 GB derecho, 2.0 GB casper together) — no query
filters on `submit` except the `uq_jobs_job_id_submit` dedup, and both showed 0
scans on both production DBs on 2026-09-30. `ix_jobs_user_submit` showed 2
(derecho), so it stays. Those counters only span back to the 2026-09-29
failover, and `submit` is a sort key in SAM's jobs table, so re-check
`pg_stat_user_indexes.idx_scan` after a real period before dropping
(`DROP INDEX CONCURRENTLY`).

## Table Schemas

### jobs

Core job records with foreign keys to normalized tables.

| Column | Type | Index | Description |
|--------|------|-------|-------------|
| `id` | INTEGER | PK, AUTO | Primary key (handles scheduler ID wrap) |
| `job_id` | TEXT | YES | Scheduler job ID (e.g., "2712367.desched1") |
| `short_id` | INTEGER | YES | Base job number (array index stripped) |
| `priority` | TEXT | NO | Raw PBS priority string (e.g., premium, regular, economy) — see `qos` below for the normalized form |
| `user` | HYBRID | - | Username (hybrid property → user_obj.username) |
| `account` | HYBRID | - | Account name (hybrid property → account_obj.account_name) |
| `queue` | HYBRID | - | Queue name (hybrid property → queue_obj.queue_name) |
| `qos` | HYBRID | - | Canonical QoS name (hybrid property → qos_obj.name) |
| `user_id` | INTEGER | FK, YES | → users.id |
| `account_id` | INTEGER | FK, YES | → accounts.id |
| `queue_id` | INTEGER | FK, YES | → queues.id |
| `qos_id` | INTEGER | FK, YES | → job_qos.id (resolved at sync time from `priority` + `queue`) |
| `name` | TEXT | NO | Job name — filterable by shell glob via `jobs_search(name=…)` / `jobhist search -N` (see note below) |
| `status` | TEXT | YES | PBS `Exit_status` — an exit **code**, not a state (see note below). Exposed as `exit_status` on every query/CLI surface |
| `submit` | DATETIME | YES | PBS `ctime` — job creation (naive UTC — see note below) |
| `queued` | DATETIME | NO | PBS `qtime` — entered *current* queue; reset by routing |
| `eligible` | DATETIME | NO | PBS `etime` — reset by queue move; by hold/release only until 2025-07-27 |
| `start` | DATETIME | YES | Start time (naive UTC) |
| `end` | DATETIME | YES | End time (naive UTC) |
| `elapsed` | INTEGER | NO | Runtime (seconds) |
| `walltime` | INTEGER | NO | Requested walltime (seconds) |
| `eligible_secs` | INTEGER | NO | PBS `eligible_time` — resource-blocked wait (seconds); see note |
| `run_count` | INTEGER | NO | PBS `run_count`; `> 1` means the job was requeued |
| `numcpus` | INTEGER | NO | CPUs allocated |
| `numgpus` | INTEGER | NO | GPUs allocated |
| `numnodes` | INTEGER | NO | Nodes allocated |
| `mpiprocs` | INTEGER | NO | MPI processes |
| `ompthreads` | INTEGER | NO | OpenMP threads |
| `reqmem` | BIGINT | NO | Requested memory (bytes) |
| `memory` | BIGINT | NO | Actual memory used (bytes) |
| `vmemory` | BIGINT | NO | Virtual memory (bytes) |
| `cputype` | TEXT | NO | CPU type (e.g., milan) |
| `gputype` | TEXT | NO | GPU type (e.g., a100) |
| `resources` | TEXT | NO | Resource specification |

**Constraints:**
- Unique: `(job_id, submit)` - prevents duplicate imports
- Foreign keys enforce referential integrity

> **Timestamp note**: All datetime columns store **naive UTC** (no `tzinfo`).
> PBS accounting logs contain Unix epoch values (`ctime`, `start`, `end`, etc.);
> `parse_pbs_timestamp()` converts these to UTC then strips the timezone before
> storing.  This is critical for PostgreSQL: if a timezone-aware datetime is
> written to a `TIMESTAMP WITHOUT TIME ZONE` column, psycopg2 converts it to
> the server's local timezone first, causing a skew (e.g., 6 hours on a
> Mountain-Time server).  Naive values are stored and compared as-is on both
> SQLite and PostgreSQL regardless of server timezone.

#### Wait time: use `Job.wait_secs`, not `start - submit`

`Job.wait_secs` is the fair queue wait, a hybrid property (Python and SQL, no
column):

> `eligible_secs` when present, else `start - eligible` when the job started
> before `ETIME_RESETS_UNTIL` (2025-07-28 UTC), else NULL.

`eligible_secs` is PBS's own `eligible_time` accrual: the cumulative wall time a
job was blocked **purely by resource scarcity**.  Time blocked by user or system
holds, unsatisfied dependencies, `qsub -a` start-time deferral, or project/user/
group run limits accrues instead to PBS's `ineligible_time`, which PBS **never**
writes to accounting logs.  `eligible_time` survives requeue and `qmove` — PBS
guarantees it only ever increases over a job's life.  `NULL` means "PBS did not
record it" and is distinct from `0`.  Availability is gated by the
`eligible_time_enable` server attribute:

| machine | `queued` / `run_count` | `eligible_secs` |
|---|---|---|
| casper | all history (2024-01 →) | all history (2024-01 →) |
| derecho | all history | **2025-01-07 17:47:50 UTC →** only |

`start - eligible` (PBS `etime`) fills Derecho's earlier history.  Until
2025-07-27 PBS reset `etime` when a hold or dependency released, so it measured
the same thing: on cpu-queue jobs ending 2025-01-08 to 2025-06-30, where both
exist, the two medians agree at every node band (p90s within 2% except 5-16
nodes).  CESM `st_archive` jobs, which always wait on a dependency, show the
change: `etime` was reset on 99% of them in 2024 and on none since 2025 Q4.  The
last reset is a job started 2025-07-27 18:48 UTC.  Since then `etime == qtime`,
so `start - eligible` is submit-to-start, holds included, and is not used.
Casper's resets stopped later (2025 Q4), so the one cutoff is safe on both
machines.  Evidence: hpc-usage-queries#128.

`start - submit` is never a substitute: it counts held, dependency-blocked and
deferred time as if the site had made the user wait.

`JobQueries.job_waits_by_resource()` filters `wait_secs IS NOT NULL`, and
`jobs_histogram('wait', …)` counts NULL rows into the response's `null_count`
(the dashboard's "N jobs unmeasured" caption), never into a wait band.  The
`min_/max_wait_secs` filters are NULL-strict.  `JobQueries.wait_history()`
reports p50/p90 of `wait_secs` by period, node band and QoS.

> **Array-parent caveat**: array-*parent* rows (job ids like `6896760[].desched1`,
> ~1% of rows) carry an `eligible_time` accrued across the whole array's
> lifetime, so it can exceed that record's own `start - submit` by hours.  Array
> subjobs and ordinary jobs are well behaved (median +2 s).

> **Do not** derive `eligible_secs` from `pbsparse`'s processed record.
> `PbsRecord.process_record()` rewrites the `eligible_time` attribute *divided by*
> `self._divisor` (qhist's display time unit: 1 = s, 60 = min, 3600 = hr).  Sync
> parses the raw `HH:MM:SS` string instead.

#### `status` holds an exit code, not a job state

`Job.status` is populated from the PBS `Exit_status` field (`sync/pbs.py`).  It is
a numeric exit code stored as text — `'0'` is success, non-zero is a failure or a
signal.  It is **not** a state letter: across all 34M rows on both production
machines there are **zero** non-numeric values (192 distinct codes on derecho,
135 on casper; the most common are `'0'`, `'1'`, `'-29'`, `'271'`, `'255'`,
`'143'`).  A filter for `'F'` matches nothing, ever.

The ORM attribute stays `Job.status` to match the DB column, but every query and
CLI surface names it `exit_status` — `jobs_search(exit_status=…)`,
`jobhist search --exit-status`, the `exit_status` column key, and the
`exit_status` facet dimension.

#### Job-name glob filtering

`jobs_search` / `jobs_count` / `jobs_facets` accept `name=` — one shell-glob
pattern or a sequence of them, OR'd (`jobhist search -N 'wrf_*' -N '*.restart'`).
`*` matches any run of characters and `?` exactly one; `ignore_case=True`
(`-i`) switches to case-insensitive matching.

Dialect-aware, in `job_history/queries/builders.py`: SQLite compiles to `GLOB`,
PostgreSQL to an anchored POSIX regex (`~`), and the case-insensitive path to
portable `ILIKE`/`lower() LIKE` with literal `%` and `_` escaped.  One asymmetry
to know: SQLite `GLOB` honours `[abc]` character classes while the PostgreSQL
regex path treats `[` as a literal — stick to `*` and `?` for identical results
on both backends.  Rows with a NULL `name` never match any pattern.

`jobs.name` is **not indexed**, so a glob is evaluated across whatever slice
`start`/`end` leave behind via `ix_jobs_end`.  Measured on 13M rows: a
one-month window costs ~107 ms with the glob versus ~100 ms without (i.e. free),
while an unbounded full-history glob costs 1.5-2.4 s.  Always bound the date
window.  A B-tree would not help — under a non-`C` collation PostgreSQL cannot
use one for `LIKE 'foo%'` at all without `text_pattern_ops`, and even then only
for left-anchored patterns; the substring case would need a `pg_trgm` GIN index.

### users, accounts, queues

Normalized lookup tables for efficient joins.

**users:**
- `id` (INTEGER, PK, AUTO)
- `username` (TEXT, UNIQUE, INDEXED)

**accounts:**
- `id` (INTEGER, PK, AUTO)
- `account_name` (TEXT, UNIQUE, INDEXED)

**queues:**
- `id` (INTEGER, PK, AUTO)
- `queue_name` (TEXT, UNIQUE, INDEXED)

### job_qos

Canonical QoS / priority-class lookup — single source of truth for the
priority → factor mapping.  Seeded by `_ensure_qos_seed_rows()` (called
from `init_db()` and `bin/update_jobs_db.py`).

| Column | Type | Description |
|--------|------|-------------|
| `id` | INTEGER | PK, AUTO |
| `name` | TEXT | UNIQUE, INDEXED — canonical class name |
| `factor` | FLOAT | Charging multiplier |
| `active` | BOOLEAN | When `false`, no new jobs should be assigned this row (existing FKs preserved) |

**Seed rows:** `premium=1.5`, `regular=1.0`, `economy=0.7`, `uncharged=0.0`,
`special=1.0`, `preempt=0.2`.

QoS resolution happens at sync time in `_insert_batch()` via
`SystemCharging.resolve_qos_name(machine, record)`.  Queue takes precedence
over priority: a queue whose rule in the machine's `QUEUE_RULES` carries a
`qos` short-circuits the priority check.  Every machine inherits
`jhublogin → uncharged`; Derecho adds `pcpu`/`pgpu → preempt`.  Route another
queue with a one-line `QueueRule(qos=...)` entry, then re-run
`bin/update_jobs_db.py <machine>` (seeds a new QoS row before backfilling
`jobs.qos_id` from the same rules) and `jobhist sync --recalculate` over the
affected window.  Add a new QoS name to `JOB_QOS_SEED`: `get_or_create_qos`
creates a row at its seed factor (sync can meet the name before `init_db`
seeds it), but an unseeded name at 1.0, and the seed never overwrites a row.

The resolved `JobQoS.factor` is copied into `job_charges.qos_factor` at
sync time as a materialized cache so `daily_summary` SQL can compute
weighted charges via a simple column multiply without a join.

### job_charges

Materialized charging calculations — **1:1 with jobs**, enforced by DB trigger.

| Column | Type | Description |
|--------|------|-------------|
| `job_id` | INTEGER | PK, FK → jobs.id (CASCADE DELETE) |
| `cpu_hours` | FLOAT | CPU-hours charged |
| `gpu_hours` | FLOAT | GPU-hours charged |
| `memory_hours` | FLOAT | Memory GB-hours charged |
| `charge_version` | INTEGER | 0 = trigger placeholder, 1 = calculated value |
| `qos_factor` | FLOAT | QoS multiplier applied to hours for charge totals |

**`charge_version` semantics:**
- `0`: inserted by `trg_ensure_job_charge` trigger immediately on job INSERT;
  all charge values are zero. Indicates charges have not yet been calculated.
- `1`: real calculated charges written by the sync code.

**QoS factors** (applied in `daily_summary` as `cpu_hours × qos_factor`):

| Priority / Queue | `qos_factor` |
|---|---|
| `premium` | 1.5 |
| regular / unset | 1.0 |
| `special` | 1.0 |
| `economy` | 0.7 |
| `preempt` (Derecho `pcpu`/`pgpu` queues) | 0.2 |
| `uncharged` (e.g. `jhublogin` queue) | 0.0 (free) |

These values are seeded into the `job_qos` lookup table by `init_db()`;
`job_charges.qos_factor` is a materialized copy refreshed at sync time
from the resolved `JobQoS.factor` for each job's `qos_id`.

**Charging rules** (implemented in `sync/charging.py`):

*Derecho (`DerechoCharging`):*
- Exclusive-node queues (every queue without a `shared` rule, reservations
  included) charge whole nodes of the requested type. A select chunk
  (`jobs.resources`, split on `+`) with `ngpus > 0` is a GPU node; with no
  select, `numgpus > 0` makes every node a GPU node.
  - CPU node: 128 cores, 0 GPUs. GPU node: 64 cores, 4 GPUs.
  - CPU-h = `elapsed_hours × (cpu_nodes × 128 + gpu_nodes × 64)`;
    GPU-h = `elapsed_hours × gpu_nodes × 4`.
- Shared queues (`cpudev`, `gpudev`): actual `numcpus` / `numgpus`

*Casper (`CasperCharging`):*
- CPU: `numcpus × elapsed_hours`
- GPU: `numgpus × elapsed_hours`
- Memory: `memory_gb × elapsed_hours` (all machines)

**DB trigger (PostgreSQL and SQLite):**
```sql
-- PostgreSQL
CREATE TRIGGER trg_ensure_job_charge
AFTER INSERT ON jobs FOR EACH ROW
EXECUTE FUNCTION fn_ensure_job_charge();
-- fn_ensure_job_charge inserts (job_id, 0.0, 0.0, 0.0, 1.0, charge_version=0)
-- ON CONFLICT (job_id) DO NOTHING

-- SQLite equivalent
CREATE TRIGGER IF NOT EXISTS trg_ensure_job_charge
AFTER INSERT ON jobs
BEGIN
    INSERT OR IGNORE INTO job_charges (...) VALUES (NEW.id, 0.0, 0.0, 0.0, 1.0, 0);
END;
```

Created by `_ensure_db_triggers()` in `database/session.py`, called from `init_db()`.
Safe to re-run (uses `CREATE OR REPLACE` / `IF NOT EXISTS`).

### daily_summary

Pre-aggregated usage by date/user/account/queue for fast historical queries.
All three dimensions (user, account, queue) are fully supported for aggregation and filtering.

| Column | Type | Index | Description |
|--------|------|-------|-------------|
| `id` | INTEGER | PK, AUTO | Primary key |
| `date` | DATE | YES | Summary date (Mountain Time day) |
| `user` | HYBRID | - | Username (hybrid property, 'NO_JOBS' if NULL FK) |
| `account` | HYBRID | - | Account name (hybrid property, 'NO_JOBS' if NULL FK) |
| `queue` | HYBRID | - | Queue name (hybrid property, 'NO_JOBS' if NULL FK) |
| `user_id` | INTEGER | FK, YES | → users.id (NULL for empty day markers) |
| `account_id` | INTEGER | FK, YES | → accounts.id (NULL for empty day markers) |
| `queue_id` | INTEGER | FK, YES | → queues.id (NULL for empty day markers) |
| `job_count` | INTEGER | NO | Number of jobs |
| `cpu_hours` | FLOAT | NO | Total raw CPU-hours |
| `gpu_hours` | FLOAT | NO | Total raw GPU-hours |
| `memory_hours` | FLOAT | NO | Total raw memory GB-hours |
| `cpu_charges` | FLOAT | NO | `SUM(cpu_hours × qos_factor)` |
| `gpu_charges` | FLOAT | NO | `SUM(gpu_hours × qos_factor)` |
| `memory_charges` | FLOAT | NO | `SUM(memory_hours × qos_factor)` |

**Constraints:**
- Unique: `(date, user_id, account_id, queue_id)`

**Day boundaries:** `generate_daily_summary()` uses Mountain Time midnight as the day
boundary, computing a naive UTC range (`America/Denver` midnight → next midnight, converted
to naive UTC) for the `WHERE j.end >= :start_utc AND j.end < :end_utc` filter.  Both the
stored `end` values and the boundary parameters are naive UTC, so comparisons are consistent
regardless of PostgreSQL server timezone.

**Marker rows:** When a date has no jobs, a row with `user_id=NULL`, `account_id=NULL`,
`queue_id=NULL`, and `job_count=0` is inserted to prevent the summarizer from repeatedly
re-scanning the same empty day.

## Composite Indexes

Optimized for common query patterns:

| Index Name | Columns | Purpose |
|------------|---------|---------|
| `uq_jobs_job_id_submit` | `(job_id, submit)` | Duplicate detection |
| `ix_jobs_user_account` | `(user_id, account_id)` | User/account combinations |
| `ix_jobs_submit_end` | `(submit, end)` | Time range queries |
| `ix_jobs_user_submit` | `(user_id, submit)` | User activity over time |
| `ix_jobs_account_submit` | `(account_id, submit)` | Account activity over time |
| `ix_jobs_queue_submit` | `(queue_id, submit)` | Queue activity over time |
| `ix_jobs_account_end` | `(account_id, end)` | Account-scoped top-N by end time |
| `ix_jobs_user_end` | `(user_id, end)` | User-scoped top-N by end time |
| `ix_daily_summary_user_account` | `(user_id, account_id)` | Summary lookups |

Single-column indexes also on: `job_id`, `short_id`, `user_id`, `account_id`, `queue_id`, `status`, `submit`, `start`, `end`

## SQLite Optimizations

Applied via event listener on every connection:

```sql
PRAGMA journal_mode=WAL;           -- Concurrent reads during writes
PRAGMA synchronous=NORMAL;         -- Faster writes, acceptable durability
PRAGMA cache_size=-64000;          -- 64MB cache
PRAGMA temp_store=MEMORY;          -- Temp tables in RAM
PRAGMA mmap_size=268435456;        -- 256MB memory-mapped I/O
PRAGMA foreign_keys=ON;            -- Enforce referential integrity
```

## Query Examples

### Top CPU Users (Optimized)

Uses composite index `ix_jobs_queue_end`:

```sql
SELECT u.username,
       COUNT(*) as jobs,
       SUM(jc.cpu_hours) as cpu_hours
FROM jobs j
JOIN users u ON j.user_id = u.id
JOIN job_charges jc ON j.id = jc.job_id
WHERE j.queue_id IN (142, 143)  -- CPU queue IDs
  AND j.end >= '2025-01-01'
  AND j.end < '2025-02-01'
GROUP BY u.username
ORDER BY cpu_hours DESC
LIMIT 10;
```

### Daily Usage from Summary Table

Instant lookup (no jobs table scan):

```sql
SELECT s.date, u.username, a.account_name,
       s.job_count, s.cpu_hours, s.gpu_hours
FROM daily_summary s
JOIN users u ON s.user_id = u.id
JOIN accounts a ON s.account_id = a.id
WHERE s.date >= '2025-01-01'
  AND u.username = 'jdoe'
ORDER BY s.date;
```

### Queue Wait Times

From Python, use `Job.wait_secs`, which compiles the rule per dialect.  In raw
SQL (PostgreSQL shown), spell out the rule:

```sql
SELECT q.queue_name,
       COUNT(*) as jobs,
       AVG(w.wait)/60.0 as avg_wait_min,
       MAX(w.wait)/60.0 as max_wait_min
FROM (SELECT j.queue_id,
             CASE WHEN j.eligible_secs IS NOT NULL THEN j.eligible_secs
                  WHEN j.start < '2025-07-28' THEN EXTRACT(EPOCH FROM j.start - j.eligible)
             END AS wait
      FROM jobs j
      WHERE j.end >= '2024-01-01') w
JOIN queues q ON w.queue_id = q.id
WHERE w.wait IS NOT NULL
GROUP BY q.queue_name
ORDER BY avg_wait_min DESC;
```

The `IS NOT NULL` guard matters: without it `COUNT(*)` counts jobs that `AVG`
skipped.  See *Wait time: use `Job.wait_secs`* above for why `start - submit` is
not a valid substitute.

## Performance Characteristics

**Derecho database:**
- Size: 11.1 GB (10.7M jobs)
- Growth: +24% from denormalized (due to indexes + materialized charges)

**Query performance (full year, 2024):**
- CPU by user (2,156 users): 3.4s
- GPU by user (189 users): 0.2s
- Complex 3-way JOIN: 0.13s
- Daily summary lookup: <0.01s

**Sync performance:**
- Import with FK resolution: ~10k jobs/sec
- Charge calculation: ~40k jobs/sec
- Composite index creation: ~10s for 10M rows

## Data Flow

1. **Import** (`sync/base.py`, `sync/pbs.py`)
   - Parse local PBS accounting logs; `parse_pbs_timestamp()` converts Unix epoch → naive UTC
   - Resolve FKs (get-or-create users/accounts/queues via `LookupCache`)
   - Bulk-insert new jobs (`ON CONFLICT DO NOTHING` on `uq_jobs_job_id_submit`)
   - DB trigger fires → zero-value `job_charges` placeholder inserted for each new job
   - Calculate real charges via `_compute_charges_for_jobs()` + `_upsert_charges()`
     (overwrites placeholder `charge_version=0` with `charge_version=1`)
   - For existing records encountered during plain/incremental sync, `_fill_missing_charges()`
     backfills any still at `charge_version=0`
   - Compress-pickle raw `PbsRecord` into `job_records`

2. **Aggregation** (`sync/summary.py`)
   - `generate_daily_summary()` uses naive UTC Mountain-Time day boundaries
   - `JOIN jobs j ON j.id = jc.job_id` — relies on 1:1 invariant being satisfied
   - Inserts both raw hours and QoS-weighted charges per `(date, user, account, queue)`

3. **Query** (`queries/jobs.py`)
   - High-level API uses composite indexes automatically
   - `daily_summary_report()` reads from `daily_summary` (fast path)
   - Other queries join `jobs` + `job_charges` directly

## Schema Evolution

The schema has evolved through several optimization phases:

**Phase 1**: Denormalized schema with text columns
**Phase 2**: Added foreign keys alongside text columns (dual columns)
**Phase 3**: Replaced text columns with hybrid properties (current)

### Hybrid Property Implementation

The current schema uses SQLAlchemy `@hybrid_property` decorators for user/account/queue fields:

```python
@hybrid_property
def user(self):
    """Username from normalized users table."""
    return self.user_obj.username if self.user_obj else None

@user.setter
def user(self, username):
    """Set user by username, creating User if necessary."""
    # Stores pending value, resolved to FK during flush

@user.expression
def user(cls):
    """Query expression for filtering by username."""
    return select(User.username).where(User.id == cls.user_id).scalar_subquery()
```

This approach provides:
- ✅ 100% backward compatibility with existing code
- ✅ Automatic FK resolution via event listeners
- ✅ Transparent query filtering (`Job.user == "alice"` works)
- ✅ Reduced storage (integer FKs vs repeated text)
- ✅ Faster queries (integer comparisons vs text)

### Migration Notes

Both casper and derecho databases have been migrated to the hybrid property schema:
- Migration time: ~25-50 minutes per database
- Timestamped backups created automatically
- All verification checks passed (FK integrity, charge accuracy, index usage)
- Text columns removed, hybrid properties maintain API compatibility
