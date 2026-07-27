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

Six composite indexes optimize common query patterns:
- `(queue_id, end)` - Primary: filter by queue + date range
- `(queue_id, user_id, end)` - User usage within queue
- `(queue_id, account_id, end)` - Account usage within queue
- `(user_id, date)`, `(account_id, date)`, `(queue_id, date)` - Daily summary lookups

Query planner verified using these indexes: `SEARCH jobs USING INDEX ix_jobs_queue_end`

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
| `eligible` | DATETIME | NO | PBS `etime` — reset by queue move **and** hold/release |
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

#### Wait time: use `eligible_secs`, not `start - submit`

`eligible_secs` is PBS's own `eligible_time` accrual: the cumulative wall time a
job was blocked **purely by resource scarcity**.  Time blocked by user or system
holds, unsatisfied dependencies, `qsub -a` start-time deferral, or project/user/
group run limits accrues instead to PBS's `ineligible_time`, which PBS **never**
writes to accounting logs.  `eligible_time` survives requeue and `qmove` — PBS
guarantees it only ever increases over a job's life.

Neither `submit` nor `eligible` is a substitute:

- `start - submit` counts held, dependency-blocked, and deferred time as if the
  site had made the user wait.
- `start - eligible` looks better but is *the same measurement in practice*:
  `qtime == etime` on 77,152 of 77,154 sampled derecho E records, and
  `eligible == submit` on 98.7% of rows in the live DB.  Measured against
  `eligible_time` on 77k records, the medians are 141 s vs 30 s and the p90s are
  23,437 s vs 11,721 s; 27% of non-array jobs disagree by more than 60 s.  For
  unconstrained jobs the two agree within ~2 s — the entire divergence is a
  negative tail, e.g. a job showing a 35-day `start - submit` wait has 17 s of
  eligible time.

`NULL` means "PBS did not record it" and is distinct from `0`.  Availability is
gated by the `eligible_time_enable` server attribute, which was enabled at
different times per machine:

| machine | `queued` / `run_count` | `eligible_secs` |
|---|---|---|
| casper | all history (2024-01 →) | all history (2024-01 →) |
| derecho | all history | **2025-01-07 17:47:50 UTC →** only |

`JobQueries.job_waits_by_resource()` filters `eligible_secs IS NOT NULL`, so
derecho reports covering dates before 2025-01-07 17:47:50 UTC exclude those jobs rather than
silently mixing two different wait definitions.

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
`special=1.0`.

QoS resolution happens at sync time in `_insert_batch()` via
`SystemCharging._resolve_qos_name(record)`.  Queue takes precedence over
priority: any queue listed in `_QUEUE_TO_QOS_NAME` (currently
`jhublogin → uncharged`) short-circuits the priority check.  Add another
non-chargeable or specially-rated queue with a one-line edit to that
dict — no schema change required.

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
| `uncharged` (e.g. `jhublogin` queue) | 0.0 (free) |

These values are seeded into the `job_qos` lookup table by `init_db()`;
`job_charges.qos_factor` is a materialized copy refreshed at sync time
from the resolved `JobQoS.factor` for each job's `qos_id`.

**Charging rules** (implemented in `sync/charging.py`):

*Derecho (`DerechoCharging`):*
- Production CPU: `numnodes × 128 cores/node × elapsed_hours`
- Production GPU: `numnodes × 4 GPUs/node × elapsed_hours`
- Dev queues (queue name contains `dev`): actual `numcpus` / `numgpus`
- GPU hours only charged for queues with `gpu` in the name

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

Portable across SQLite and PostgreSQL — `eligible_secs` is already an integer
count of seconds, so no dialect-specific date arithmetic is needed.

```sql
SELECT q.queue_name,
       COUNT(*) as jobs,
       AVG(j.eligible_secs)/60.0 as avg_wait_min,
       MAX(j.eligible_secs)/60.0 as max_wait_min
FROM jobs j
JOIN queues q ON j.queue_id = q.id
WHERE j.eligible_secs IS NOT NULL
  AND j.end >= '2025-01-01'
GROUP BY q.queue_name
ORDER BY avg_wait_min DESC;
```

The `IS NOT NULL` guard matters: without it `COUNT(*)` counts jobs that `AVG`
skipped.  See *Wait time: use `eligible_secs`* above for why `start - submit` is
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
