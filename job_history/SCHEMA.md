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

The `user`, `account`, and `queue` attributes are implemented as SQLAlchemy `@hybrid_property` decorators that:
- Return text values from relationships (e.g., `job.user` → `"alice"`)
- Accept text assignments via setters (e.g., `job.user = "alice"`)
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
| `priority` | TEXT | NO | Job priority (e.g., premium, regular, economy) |
| `user` | HYBRID | - | Username (hybrid property → user_obj.username) |
| `account` | HYBRID | - | Account name (hybrid property → account_obj.account_name) |
| `queue` | HYBRID | - | Queue name (hybrid property → queue_obj.queue_name) |
| `user_id` | INTEGER | FK, YES | → users.id |
| `account_id` | INTEGER | FK, YES | → accounts.id |
| `queue_id` | INTEGER | FK, YES | → queues.id |
| `name` | TEXT | NO | Job name |
| `status` | TEXT | YES | Completion status |
| `submit` | DATETIME | YES | Submission time (naive UTC — see note below) |
| `eligible` | DATETIME | NO | Eligible time (naive UTC) |
| `start` | DATETIME | YES | Start time (naive UTC) |
| `end` | DATETIME | YES | End time (naive UTC) |
| `elapsed` | INTEGER | NO | Runtime (seconds) |
| `walltime` | INTEGER | NO | Requested walltime (seconds) |
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
| `economy` | 0.7 |
| `jhublogin` | 0.0 (free) |

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

```sql
SELECT q.queue_name,
       COUNT(*) as jobs,
       AVG(strftime('%s', j.start) - strftime('%s', j.submit))/60.0 as avg_wait_min,
       MEDIAN(strftime('%s', j.start) - strftime('%s', j.submit))/60.0 as median_wait_min
FROM jobs j
JOIN queues q ON j.queue_id = q.id
WHERE j.start IS NOT NULL
  AND j.submit IS NOT NULL
  AND j.end >= '2025-01-01'
GROUP BY q.queue_name
ORDER BY avg_wait_min DESC;
```

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
