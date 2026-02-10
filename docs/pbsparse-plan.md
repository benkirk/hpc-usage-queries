# Implementation Plan: qhist-parse-logs

## Context

Currently, the `qhist-sync` tool retrieves HPC job data by SSH'ing to remote machines (Casper/Derecho) and running `qhist` commands to query the PBS scheduler's historical job database. This requires network access and authentication.

**The Goal**: Create an alternative tool `qhist-parse-logs` that builds the same database by directly parsing PBS accounting log files from the local filesystem. This is useful when:
- Log files have been copied locally
- SSH access is unavailable
- We want to process historical logs in bulk
- We need an offline sync capability

The `pbsparse` library already exists to parse PBS accounting log files and extract job End records. We can leverage this to populate the database with the same information that `qhist-sync` provides.

## CPU/GPU Type Handling

PBS logs include `cpu_type` and `gpu_type` in the select string (e.g., `cpu_type=genoa`, `gpu_type=a100`). These should be extracted and stored in the database `cputype` and `gputype` fields.

**Extraction Strategy:**
1. **Primary**: Parse from select string: `cpu_type=X` → cputype, `gpu_type=X` → gputype
2. **Fallback**: Infer from queue name when not in select string
   - GPU queue names often indicate type: `a100`, `h100`, `v100`, `nvgpu`
   - CPU queues may need machine-specific defaults

**Known Queue → Type Mappings** (from `qhist_db/queries.py`):
- Casper GPU queues: `nvgpu`, `gpgpu`, `a100`, `h100`, `l40`, `amdgpu`
- Casper CPU queues: `htc`, `gdex`, `largemem`, `vis`, `rda`
- Derecho GPU queues: (check data)
- Derecho CPU queues: `cpu`, `cpudev`

## Field Availability Analysis

PBS End records provide all critical fields needed for the database:

**Direct Mappings Available:**
- Job identity: `id`, `short_id`, `user`, `account`, `queue`, `jobname`
- Timestamps: `ctime`/`qtime`→submit, `etime`→eligible, `start`, `end`
- Resources: `Resource_List['ncpus']`, `Resource_List['ngpus']`, `Resource_List['nodect']`, `Resource_List['mpiprocs']`, `Resource_List['mem']`
- Memory usage: `resources_used['mem']`, `resources_used['vmem']`
- Time metrics: `Resource_List['walltime']`, `resources_used['walltime']`, `resources_used['cput']`
- Performance: `resources_used['cpupercent']`
- **ptargets**: Available as `Resource_List['preempt_targets']` ✓

**Fields with Special Handling:**
- `cputype`, `gputype` - Parse from select string (`cpu_type=genoa`, `gpu_type=a100`), fallback to queue name inference
- `avgcpu` - Use same value as `cpupercent` from `resources_used`
- `status` - Can infer from `Exit_status` (0="COMPLETED", >0="FAILED") or set NULL

**Unit Conversions Required:**
- Timestamps: Unix epoch → UTC datetime
- Memory: "235gb"→bytes, "172600kb"→bytes
- Time: "HH:MM:SS"→seconds
- Parse select string: Extract `ompthreads` from `Resource_List['select']`

## Implementation Strategy

**Code Reuse Philosophy**: Maximize reuse of existing components by creating a new parser that outputs the exact same dictionary format as `qhist_db/parsers.py::parse_job_record()`. This allows all downstream code (JobImporter, bulk insert, charge calculation, summaries) to be reused without modification.

## Critical Files

### New Files to Create

1. **qhist_db/pbs_parsers.py** - Transform PBS records to database format
   - Contains all unit conversion functions
   - Main function: `parse_pbs_record(pbs_record: pbsparse.PbsRecord) -> dict`
   - Output matches format from `qhist_db/parsers.py::parse_job_record()`

2. **qhist_db/pbs_local.py** - PBS log scanning and streaming
   - `get_log_file_path(log_dir: Path, date_str: str) -> Path`
   - `fetch_jobs_from_pbs_logs(log_dir, date=None, start_date=None, end_date=None) -> Iterator[dict]`
   - Yields parsed job dictionaries ready for database insertion

3. **scripts/parse_pbs_logs.py** - CLI entry point
   - Command-line argument parsing
   - Database initialization
   - Calls sync functions
   - Prints statistics

### Files to Modify

4. **qhist_db/sync.py** - Add PBS log sync functions
   - Add `sync_jobs_from_pbs()` - Generic sync from record iterator
   - Add `sync_pbs_logs_bulk()` - High-level PBS log sync (mirrors `sync_jobs_bulk`)

## Implementation Steps

### Step 1: Create PBS Parser Module

**File**: `qhist_db/pbs_parsers.py`

**Key Functions**:
```python
def parse_pbs_time(time_str: str) -> int | None:
    """Convert HH:MM:SS to seconds."""

def parse_pbs_memory_kb(mem_str: str) -> int | None:
    """Convert '172600kb' to bytes (int * 1024)."""

def parse_pbs_memory_gb(mem_str: str) -> int | None:
    """Convert '235gb' to bytes (handle with/without GB suffix)."""

def parse_pbs_timestamp(unix_time: int) -> datetime | None:
    """Convert Unix timestamp to UTC datetime."""

def parse_select_string(select_str: str) -> dict:
    """Parse 'select' to extract mpiprocs, ompthreads, cpu_type, gpu_type.

    Returns dict with keys: mpiprocs, ompthreads, cpu_type, gpu_type (all optional)
    Example: '1:ncpus=1:cpu_type=genoa:mem=800GB' → {'cpu_type': 'genoa', ...}
    """

def infer_types_from_queue(queue_name: str, machine: str) -> dict:
    """Infer CPU/GPU types from queue name when not in select string.

    Args:
        queue_name: Queue name (e.g., 'a100', 'h100', 'gpu', 'cpu')
        machine: Machine name ('casper', 'derecho')

    Returns:
        dict with 'cputype' and/or 'gputype' keys

    Examples:
        - queue='a100' → {'gputype': 'a100'}
        - queue='h100' → {'gputype': 'h100'}
        - queue='cpu' on derecho → {'cputype': 'milan'}
    """

def parse_pbs_record(pbs_record, machine: str) -> dict:
    """Transform pbsparse.PbsRecord to database dict.

    Args:
        pbs_record: pbsparse.PbsRecord object
        machine: Machine name ('casper', 'derecho') for type inference fallback

    Returns dict matching qhist_db/parsers.py::parse_job_record() format.
    """
```

**Field Extraction Logic**:
- `job_id`: `pbs_record.id`
- `short_id`: `pbs_record.short_id`
- `user`: `pbs_record.user`
- `account`: `pbs_record.account.strip('"')` (remove quotes)
- `queue`: `pbs_record.queue`
- `name`: `pbs_record.jobname`
- `submit`: `parse_pbs_timestamp(pbs_record.qtime)` (or ctime if qtime unavailable)
- `eligible`: `parse_pbs_timestamp(pbs_record.etime)`
- `start`: `parse_pbs_timestamp(pbs_record.start)`
- `end`: `parse_pbs_timestamp(pbs_record.end)`
- `walltime`: `parse_pbs_time(pbs_record.Resource_List.get('walltime'))`
- `elapsed`: `parse_pbs_time(pbs_record.resources_used.get('walltime'))`
- `cputime`: `parse_pbs_time(pbs_record.resources_used.get('cput'))`
- `numcpus`: `int(pbs_record.Resource_List.get('ncpus'))`
- `numgpus`: `int(pbs_record.Resource_List.get('ngpus'))`
- `numnodes`: `int(pbs_record.Resource_List.get('nodect'))`
- `reqmem`: `parse_pbs_memory_gb(pbs_record.Resource_List.get('mem'))`
- `memory`: `parse_pbs_memory_kb(pbs_record.resources_used.get('mem'))`
- `vmemory`: `parse_pbs_memory_kb(pbs_record.resources_used.get('vmem'))`
- `cpupercent`: `float(pbs_record.resources_used.get('cpupercent'))`
- `avgcpu`: `float(pbs_record.resources_used.get('cpupercent'))` (same as cpupercent)
- `resources`: `pbs_record.Resource_List.get('select')`
- `ptargets`: `pbs_record.Resource_List.get('preempt_targets')`
- `count`: `pbs_record.run_count`
- `mpiprocs`, `ompthreads`, `cputype`, `gputype`: Extract from `parse_select_string(select)`
  - `cputype`/`gputype` from `cpu_type=X`/`gpu_type=X` in select string
  - If not present, fallback to `infer_types_from_queue(queue, machine)`
- `status`: Infer from `Exit_status` or set to `None`

### Step 2: Create PBS Log Scanner

**File**: `qhist_db/pbs_local.py`

```python
from pathlib import Path
from typing import Iterator
from pbsparse import get_pbs_records
from .pbs_parsers import parse_pbs_record
from .parsers import date_range

def get_log_file_path(log_dir: Path, date_str: str) -> Path:
    """Get PBS log file path for given date (YYYYMMDD format)."""

def fetch_jobs_from_pbs_logs(
    log_dir: str | Path,
    date: str | None = None,
    start_date: str | None = None,
    end_date: str | None = None,
) -> Iterator[dict]:
    """Scan PBS log files and yield parsed job dictionaries.

    Yields only End records, transformed to database format.
    """
```

**Logic**:
1. Handle date/date_range parameters (same as `qhist_db/remote.py`)
2. For each date, construct log file path
3. Use `pbsparse.get_pbs_records(path, type_filter='E')` to get End records
4. Transform each record via `parse_pbs_record()`
5. Validate records (skip if missing job_id or invalid timestamps)
6. Yield transformed dictionary

### Step 3: Extend Sync Module

**File**: `qhist_db/sync.py`

**Add Functions**:

```python
def sync_jobs_from_pbs(
    session: Session,
    machine: str,
    record_iterator: Iterator[dict],
    dry_run: bool = False,
    batch_size: int = 1000,
    verbose: bool = False,
) -> dict:
    """Sync jobs from any record iterator (generic version).

    This function can work with records from SSH/qhist or PBS logs
    since both provide the same dictionary format.

    Returns:
        Stats dict: {fetched, inserted, errors, duplicates}
    """
    # Reuse existing _insert_batch() logic with JobImporter
    # Collect batches and call _insert_batch()

def sync_pbs_logs_bulk(
    session: Session,
    machine: str,
    log_dir: str | Path,
    period: str | None = None,
    start_date: str | None = None,
    end_date: str | None = None,
    dry_run: bool = False,
    batch_size: int = 1000,
    verbose: bool = False,
    force: bool = False,
    generate_summary: bool = True,
) -> dict:
    """High-level PBS log sync (mirrors sync_jobs_bulk interface).

    Returns:
        Stats dict with fetched, inserted, errors, days_summarized
    """
    # Use fetch_jobs_from_pbs_logs() to get records
    # Call sync_jobs_from_pbs() or reuse _insert_batch() logic
    # Generate summaries if requested
```

### Step 4: Create CLI Script

**File**: `scripts/parse_pbs_logs.py`

```python
#!/usr/bin/env python3
"""Import job data from PBS accounting log files."""

import argparse
from pathlib import Path
from qhist_db.database import get_session, init_database
from qhist_db.sync import sync_pbs_logs_bulk

def parse_args():
    parser = argparse.ArgumentParser(description="Import PBS log files")
    parser.add_argument('-m', '--machine', required=True,
                       choices=['casper', 'derecho'],
                       help='Machine name')
    parser.add_argument('-l', '--log-path', required=True,
                       type=Path, help='PBS log directory')
    parser.add_argument('-d', '--date', help='Single date (YYYY-MM-DD)')
    parser.add_argument('--start', help='Start date (YYYY-MM-DD)')
    parser.add_argument('--end', help='End date (YYYY-MM-DD)')
    parser.add_argument('--batch-size', type=int, default=1000)
    parser.add_argument('--dry-run', action='store_true')
    parser.add_argument('-v', '--verbose', action='store_true')
    parser.add_argument('--force', action='store_true')
    parser.add_argument('--no-summary', action='store_true')
    return parser.parse_args()

def main():
    args = parse_args()

    # Validate log path exists
    if not args.log_path.exists():
        print(f"Error: Log path does not exist: {args.log_path}")
        return 1

    # Initialize database
    init_database(args.machine)
    session = get_session(args.machine)

    # Sync from PBS logs
    stats = sync_pbs_logs_bulk(
        session=session,
        machine=args.machine,
        log_dir=args.log_path,
        period=args.date,
        start_date=args.start,
        end_date=args.end,
        dry_run=args.dry_run,
        batch_size=args.batch_size,
        verbose=args.verbose,
        force=args.force,
        generate_summary=not args.no_summary,
    )

    # Print statistics
    print(f"\nSync Statistics for {args.machine}:")
    print(f"  Fetched:     {stats['fetched']}")
    print(f"  Inserted:    {stats['inserted']}")
    print(f"  Errors:      {stats['errors']}")
    if 'duplicates' in stats:
        print(f"  Duplicates:  {stats['duplicates']}")
    if 'days_summarized' in stats:
        print(f"  Summarized:  {stats['days_summarized']} days")

    return 0

if __name__ == '__main__':
    exit(main())
```

### Step 5: Add Console Script Entry Point

**File**: `pyproject.toml` (or `setup.py`)

Add to `[project.scripts]` section:
```toml
qhist-parse-logs = "qhist_db.scripts.parse_pbs_logs:main"
```

Or create wrapper script in `conda-env/bin/qhist-parse-logs`:
```bash
#!/bin/bash
exec python -m qhist_db.scripts.parse_pbs_logs "$@"
```

## Error Handling

**Record-level validation**:
- Skip records missing `job_id`
- Skip records with invalid timestamp ordering (submit > start > end)
- Set NULL for unparseable numeric fields
- Log validation failures but continue processing

**File-level handling**:
- Warn if log file doesn't exist for a date
- Handle corrupted/malformed log files gracefully
- Continue with next date on error

**Database-level**:
- Reuse existing duplicate detection via `(job_id, submit)` unique constraint
- Bulk insert with `session.bulk_insert_mappings(..., render_nulls=True)`
- Transaction per batch for error isolation

## Verification Strategy

### Unit Tests
Create `tests/test_pbs_parsers.py`:
- Test time parsing: "00:14:18" → 858 seconds
- Test memory parsing: "235gb" → 252348030976 bytes, "172600kb" → 176742400 bytes
- Test timestamp parsing: 1769670016 → datetime(2026, 1, 29, 0, 0, 16, tzinfo=UTC)
- Test select string parsing:
  - Extract mpiprocs, ompthreads
  - Extract `cpu_type=genoa` → cputype='genoa'
  - Extract `gpu_type=a100` → gputype='a100'
- Test queue name type inference:
  - queue='a100' → gputype='a100'
  - queue='cpu' + machine='derecho' → cputype='milan'
  - queue='largemem' + machine='casper' → cputype='cascade' or 'genoa'
- Test full record transformation

### Integration Tests
1. Parse sample PBS logs: `data/sample_pbs_logs/derecho/20260129`
2. Verify record count matches `pbsparse` End record count
3. Check database insertion (count, FK resolution, charge calculation)
4. Verify daily summaries generated correctly

### Manual Testing
```bash
# Dry run to see what would be imported
./scripts/parse_pbs_logs.py -m derecho \
  -l ./data/sample_pbs_logs/derecho \
  -d 2026-01-29 --dry-run -v

# Actual import
./scripts/parse_pbs_logs.py -m derecho \
  -l ./data/sample_pbs_logs/derecho \
  -d 2026-01-29 -v

# Verify in database
sqlite3 data/derecho.db "SELECT COUNT(*) FROM jobs WHERE date(submit)='2026-01-29'"
sqlite3 data/derecho.db "SELECT * FROM daily_summary WHERE date='2026-01-29'"

# Check FK resolution
sqlite3 data/derecho.db "SELECT COUNT(*) FROM jobs WHERE user_id IS NULL AND user IS NOT NULL"
```

### Validation Queries
```sql
-- Count jobs by date
SELECT date(submit) as day, COUNT(*) FROM jobs GROUP BY day;

-- Verify ptargets field populated
SELECT COUNT(*) FROM jobs WHERE ptargets IS NOT NULL;

-- Verify cputype/gputype populated (should be more than qhist-sync!)
SELECT cputype, gputype, COUNT(*) FROM jobs GROUP BY cputype, gputype;
SELECT COUNT(*) FROM jobs WHERE cputype IS NOT NULL OR gputype IS NOT NULL;

-- Check charge calculations
SELECT COUNT(*) FROM jobs j
LEFT JOIN job_charges jc ON j.id = jc.job_id
WHERE jc.job_id IS NULL;

-- Verify FK consistency
SELECT COUNT(*) FROM jobs WHERE user IS NOT NULL AND user_id IS NULL;
```

## Expected Outcomes

1. **New tool**: `qhist-parse-logs` available as console script
2. **Code reuse**: 95%+ of existing sync/charge/summary logic reused
3. **Feature parity**: All fields populated (except cputype/gputype which are NULL in qhist too)
4. **Compatibility**: Works with same database schema, generates same summaries
5. **Performance**: Comparable to qhist-sync for local log files
6. **Maintainability**: Single PBS parser module encapsulates all transformations

## Future Enhancements (Not in Scope)

- Parallel log file processing for date ranges
- Progress bars for large imports
- Incremental sync (track last processed position in logs)
- Support for other PBS record types (Q, S, R) if needed
