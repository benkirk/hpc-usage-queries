# QHist Database Schema

This document describes the optimized database schema for NCAR's Casper and Derecho HPC job history.

## Overview

- **Databases**: Separate SQLite files per machine (`data/casper.db`, `data/derecho.db`)
- **Schema**: Normalized with foreign keys and composite indexes
- **Timestamps**: UTC
- **Performance**: 5-10x query speedup via optimization

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
| `user` | HYBRID | - | Username (hybrid property → user_obj.username) |
| `account` | HYBRID | - | Account name (hybrid property → account_obj.account_name) |
| `queue` | HYBRID | - | Queue name (hybrid property → queue_obj.queue_name) |
| `user_id` | INTEGER | FK, YES | → users.id |
| `account_id` | INTEGER | FK, YES | → accounts.id |
| `queue_id` | INTEGER | FK, YES | → queues.id |
| `name` | TEXT | NO | Job name |
| `status` | TEXT | YES | Completion status |
| `submit` | DATETIME | YES | Submission time (UTC) |
| `eligible` | DATETIME | NO | Eligible time (UTC) |
| `start` | DATETIME | YES | Start time (UTC) |
| `end` | DATETIME | YES | End time (UTC) |
| `elapsed` | INTEGER | NO | Runtime (seconds) |
| `walltime` | INTEGER | NO | Requested walltime (seconds) |
| `cputime` | INTEGER | NO | CPU time used (seconds) |
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
| `ptargets` | TEXT | NO | Placement targets |
| `cpupercent` | REAL | NO | CPU utilization % |
| `avgcpu` | REAL | NO | Average CPU usage |
| `count` | INTEGER | NO | Job array count |

**Constraints:**
- Unique: `(job_id, submit)` - prevents duplicate imports
- Foreign keys enforce referential integrity

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

Materialized charging calculations for instant lookups.

| Column | Type | Description |
|--------|------|-------------|
| `job_id` | INTEGER | PK, FK → jobs.id |
| `cpu_hours` | FLOAT | CPU-hours charged |
| `gpu_hours` | FLOAT | GPU-hours charged |
| `memory_hours` | FLOAT | Memory GB-hours charged |
| `charge_version` | INTEGER | Algorithm version (for future changes) |

**Charging rules:**

*Derecho:*
- Production CPU: `numnodes × 128 cores/node × elapsed_hours`
- Production GPU: `numnodes × 4 GPUs/node × elapsed_hours`
- Dev queues: actual resources (not full-node allocation)

*Casper:*
- CPU: `numcpus × elapsed_hours`
- GPU: `numgpus × elapsed_hours`
- Memory: `memory_gb × elapsed_hours`

### daily_summary

Pre-aggregated usage by date/user/account/queue for fast historical queries.

| Column | Type | Index | Description |
|--------|------|-------|-------------|
| `id` | INTEGER | PK, AUTO | Primary key |
| `date` | DATE | YES | Summary date |
| `user` | HYBRID | - | Username (hybrid property, 'NO_JOBS' if NULL FK) |
| `account` | HYBRID | - | Account name (hybrid property, 'NO_JOBS' if NULL FK) |
| `queue` | HYBRID | - | Queue name (hybrid property, 'NO_JOBS' if NULL FK) |
| `user_id` | INTEGER | FK, YES | → users.id (NULL for empty day markers) |
| `account_id` | INTEGER | FK, YES | → accounts.id (NULL for empty day markers) |
| `queue_id` | INTEGER | FK, YES | → queues.id (NULL for empty day markers) |
| `job_count` | INTEGER | NO | Number of jobs |
| `cpu_hours` | FLOAT | NO | Total CPU-hours |
| `gpu_hours` | FLOAT | NO | Total GPU-hours |
| `memory_hours` | FLOAT | NO | Total memory GB-hours |

**Constraints:**
- Unique: `(date, user_id, account_id, queue_id)`

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

1. **Import** (sync.py)
   - Fetch via SSH + qhist
   - Resolve FKs (create new users/accounts/queues as needed)
   - Insert jobs with duplicate detection
   - Calculate and insert charges

2. **Aggregation** (summary.py)
   - Generate daily_summary from jobs + job_charges
   - Uses 4-way JOIN: jobs → job_charges → users/accounts/queues

3. **Query** (queries.py)
   - High-level API uses composite indexes automatically
   - Prefers job_charges table over on-the-fly calculation
   - Falls back to daily_summary for historical queries

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
