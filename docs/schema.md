# QHist Database Schema

This document describes the database schema for storing historical job data from NCAR's Casper and Derecho supercomputing resources.

## Overview

- **Databases**: Separate SQLite files per machine
  - `data/casper.db` - Casper cluster jobs
  - `data/derecho.db` - Derecho cluster jobs
- **Tables**: `jobs`, `daily_summary`
- **Views**: `v_jobs_charged` (machine-specific charging calculations)
- **Primary Keys**: Auto-incrementing integers (handles job ID wrap-around)
- **Timestamps**: Stored in UTC

## Design Decisions

### Separate Database Files per Machine

Casper and Derecho have independent job numbering systems. Using separate database files:

- Enables independent backup/archive per machine
- Keeps individual databases smaller and more manageable
- Allows parallel sync operations without conflicts
- Easy to drop old data or archive by machine

### Primary Key Choice

`id` (INTEGER AUTO-INCREMENT) is used as the primary key because:

- Job IDs from the scheduler can wrap around (Derecho 2025 started from low IDs that overlapped with 2024)
- Using auto-increment avoids collisions across years
- `job_id` (TEXT) stores the full scheduler ID (e.g., "2712367.desched1")
- `short_id` (INTEGER) stores just the numeric part for efficient queries
- Unique constraint on `(job_id, submit)` prevents duplicate imports

### Timestamp Handling

All timestamps are converted to UTC before storage for consistency, regardless of the timezone reported by qhist.

## Schema Definition

### jobs table

| Column | Type | Nullable | Index | Description |
|--------|------|----------|-------|-------------|
| `id` | INTEGER | NO | PK | Auto-incrementing primary key |
| `job_id` | TEXT | NO | YES | Full job ID from scheduler (e.g., "2712367.desched1") |
| `short_id` | INTEGER | YES | YES | Base job number (array index stripped) |
| `name` | TEXT | YES | NO | Job name |
| `user` | TEXT | YES | YES | Username who submitted the job |
| `account` | TEXT | YES | YES | Account/allocation charged |
| `queue` | TEXT | YES | YES | Queue/partition used |
| `status` | TEXT | YES | YES | Job completion status |
| `submit` | DATETIME | YES | YES | When job was submitted (UTC) |
| `eligible` | DATETIME | YES | NO | When job became eligible to run (UTC) |
| `start` | DATETIME | YES | YES | When job started executing (UTC) |
| `end` | DATETIME | YES | YES | When job finished (UTC) |
| `elapsed` | INTEGER | YES | NO | Actual runtime in seconds |
| `walltime` | INTEGER | YES | NO | Requested walltime in seconds |
| `cputime` | INTEGER | YES | NO | Total CPU time used in seconds |
| `numcpus` | INTEGER | YES | NO | Number of CPUs allocated |
| `numgpus` | INTEGER | YES | NO | Number of GPUs allocated |
| `numnodes` | INTEGER | YES | NO | Number of nodes allocated |
| `mpiprocs` | INTEGER | YES | NO | Number of MPI processes |
| `ompthreads` | INTEGER | YES | NO | Number of OpenMP threads |
| `reqmem` | BIGINT | YES | NO | Requested memory in bytes |
| `memory` | BIGINT | YES | NO | Actual memory used in bytes |
| `vmemory` | BIGINT | YES | NO | Virtual memory used in bytes |
| `cputype` | TEXT | YES | NO | CPU type (e.g., milan) |
| `gputype` | TEXT | YES | NO | GPU type (e.g., a100) |
| `resources` | TEXT | YES | NO | Resource specification string |
| `ptargets` | TEXT | YES | NO | Placement targets |
| `cpupercent` | REAL | YES | NO | CPU utilization percentage |
| `avgcpu` | REAL | YES | NO | Average CPU usage |
| `count` | INTEGER | YES | NO | Job array count |

### daily_summary table

Aggregated charging data for fast usage queries.

| Column | Type | Nullable | Index | Description |
|--------|------|----------|-------|-------------|
| `id` | INTEGER | NO | PK | Auto-incrementing primary key |
| `date` | DATE | NO | YES | Summary date |
| `user` | TEXT | NO | YES | Username |
| `account` | TEXT | NO | YES | Account charged |
| `queue` | TEXT | NO | NO | Queue/partition |
| `job_count` | INTEGER | YES | NO | Number of jobs |
| `charge_hours` | REAL | YES | NO | Derecho: core-hours or GPU-hours |
| `cpu_hours` | REAL | YES | NO | Casper: CPU-hours |
| `gpu_hours` | REAL | YES | NO | Casper: GPU-hours |
| `memory_hours` | REAL | YES | NO | Casper: Memory GB-hours |

**Unique constraint**: `(date, user, account, queue)`

### v_jobs_charged view

Machine-specific view that adds computed charging columns to the jobs table.

**Derecho** (`charge_hours` column):
- GPU dev queues: `elapsed * numgpus / 3600`
- CPU dev queues: `elapsed * numcpus / 3600`
- GPU production: `elapsed * numnodes * 4 / 3600` (4 GPUs per node)
- CPU production: `elapsed * numnodes * 128 / 3600` (128 cores per node)

**Casper** (`cpu_hours`, `gpu_hours`, and `memory_hours` columns):
- `cpu_hours = elapsed * numcpus / 3600`
- `gpu_hours = elapsed * numgpus / 3600`
- `memory_hours = elapsed * memory_gb / 3600`

## Indexes

**Single-column indexes:** `job_id`, `short_id`, `user`, `account`, `queue`, `status`, `submit`, `start`, `end`

**Composite indexes for common query patterns:**

| Index | Columns | Use Case |
|-------|---------|----------|
| `uq_jobs_job_id_submit` | `(job_id, submit)` | Duplicate detection |
| `ix_jobs_user_account` | `(user, account)` | Filter by user within account |
| `ix_jobs_submit_end` | `(submit, end)` | Date range queries |
| `ix_jobs_user_submit` | `(user, submit)` | User's jobs in date range |
| `ix_jobs_account_submit` | `(account, submit)` | Account usage over time |
| `ix_jobs_queue_submit` | `(queue, submit)` | Queue analysis by date |

## Example Queries

### Jobs by User
```sql
SELECT user, COUNT(*) as job_count, SUM(elapsed) as total_runtime
FROM jobs
GROUP BY user
ORDER BY total_runtime DESC;
```

### Charging Summary by Account
```sql
-- Derecho
SELECT account, SUM(charge_hours) as total_hours
FROM v_jobs_charged
WHERE date(end) BETWEEN '2025-01-01' AND '2025-01-31'
GROUP BY account;

-- Casper
SELECT account, SUM(cpu_hours) as cpu_hours, SUM(memory_hours) as mem_hours
FROM v_jobs_charged
WHERE date(end) BETWEEN '2025-01-01' AND '2025-01-31'
GROUP BY account;
```

### Daily Usage from Summary Table
```sql
SELECT date, user, account, job_count, charge_hours
FROM daily_summary
WHERE date >= '2025-01-01'
ORDER BY date, charge_hours DESC;
```

### Average Wait Time by Queue
```sql
SELECT queue,
       AVG(strftime('%s', start) - strftime('%s', submit)) as avg_wait_seconds
FROM jobs
WHERE start IS NOT NULL AND submit IS NOT NULL
GROUP BY queue;
```

## Data Sources

Data is fetched from the `qhist` command available on Casper and Derecho:

```bash
ssh derecho qhist -p 20251121 -J -f="id,short_id,..."
```

The sync process:
1. Connects via SSH to the target machine
2. Runs qhist with JSON output for specified date
3. Parses JSON and converts timestamps to UTC
4. Inserts new records (skips duplicates via unique constraint)
5. Generates daily summary for the synced day
6. Skips days that have already been summarized (use `--force` to override)
