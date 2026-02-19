# Plan: DB-backed qhist frontend (bin/qhist-db)

## Context

`qhist` scans raw PBS accounting log files day-by-day to answer queries. Our `qhist_db` project
already parses those same logs into a SQLite/SQLAlchemy ORM — and, critically, **stores the
original `PbsRecord` (or `DerechoRecord`) object compressed in `JobRecord.compressed_data`**.

The goal: a new `bin/qhist-db` wrapper that accepts identical CLI arguments as `qhist`, but
replaces the `while keep_going / get_pbs_records()` scanning loop with efficient DB queries that
stream the stored, real `PbsRecord` objects — falling back to log scanning when the DB is unavailable.

Key insight: since we decompress the original `PbsRecord`, qhist's output functions receive the exact
same object type they always expect, with no adapter class needed.

Machine selection: `QHIST_MACHINE=derecho|casper` environment variable (consistent with
`QHIST_DERECHO_DB` / `QHIST_CASPER_DB` already in use).

---

## Two New Files

### 1. `qhist_db/qhist_compat.py`  — adapter module

No `DbRecord` class needed — we yield real `PbsRecord`/`DerechoRecord` objects.

---

#### `db_available(machine) -> bool`

```python
from qhist_db import get_db_path, VALID_MACHINES
def db_available(machine):
    return machine in VALID_MACHINES and get_db_path(machine).exists()
```

---

#### `db_get_records(machine, start_dt, end_dt, ...) -> Iterator[PbsRecord]`

Query strategy:
- Filter on `Job` columns + JOIN to `User`/`Account`/`Queue` aliases for WHERE clauses
- Eager-load `Job.job_record_obj` (1:1 relationship) to fetch `compressed_data` in-band
- Stream with `.yield_per(chunk_size)` for memory-bounded output
- Per record: decompress → set `_divisor` → call `process_record()` → yield

```python
from sqlalchemy.orm import joinedload, aliased
from qhist_db.models import Job, JobRecord, User, Account, Queue
from qhist_db.database import get_session

def db_get_records(machine, start_dt, end_dt, time_divisor=3600.0,
                   id_filter=None, host_filter=None, data_filters=None,
                   time_filter=None, reverse=False, chunk_size=500):

    session = get_session(machine)
    try:
        u = aliased(User); a = aliased(Account); q = aliased(Queue)

        query = (
            session.query(Job)
            .options(joinedload(Job.job_record_obj))   # eager: avoids N+1 for compressed_data
            .outerjoin(u, Job.user_id == u.id)         # for filter access only
            .outerjoin(a, Job.account_id == a.id)
            .outerjoin(q, Job.queue_id == q.id)
            .filter(Job.end >= start_dt, Job.end <= end_dt)
        )

        query = _apply_filters(query, u, a, q, id_filter, host_filter,
                               data_filters, time_filter)

        order = Job.end.desc() if reverse else Job.end.asc()
        query = query.order_by(order).yield_per(chunk_size)

        for job in query:
            if job.job_record_obj is None:
                continue  # skip jobs imported without storing raw record
            record = job.job_record_obj.to_pbs_record()  # decompress + unpickle
            record._divisor = time_divisor               # apply requested time units
            record.process_record()                      # type-convert fields
            yield record
    finally:
        session.close()
```

The stored `PbsRecord` is raw/unprocessed (`process=False` at import time in `pbs_read_logs.py`),
so calling `process_record()` here is safe and correct.

---

#### Two-phase filtering

**Phase 1 — SQL** (`_apply_sql_filters()`): applied before decompression, narrows the result set efficiently.

| qhist param | SQL translation |
|---|---|
| date bounds | `Job.end >= start_dt`, `Job.end <= end_dt` |
| `id_filter` (job IDs) | `Job.short_id.in_([int(i) for i in ids])` |
| `data_filters user` | `u.username == / != / contains value` |
| `data_filters account` | `a.account_name == / != / contains value` |
| `data_filters queue` | `q.queue_name == / !=` |
| `data_filters jobname` | `Job.name == / contains value` |
| `data_filters Exit_status` | `Job.status == value` |
| `data_filters waittime` | `(Job.start - Job.eligible) > timedelta(...)` |
| `time_filter` (intra-day) | `func.strftime('%H:%M:%S', Job.end)` range |

**Phase 2 — Python** (applied after `to_pbs_record()` + `process_record()`): handles filters that
require the fully-typed, parsed record object.

```python
for job in query:
    if job.job_record_obj is None:
        continue
    record = job.job_record_obj.to_pbs_record()
    record._divisor = time_divisor
    record.process_record()

    # host_filter: requires exec_vnode → get_nodes() parsing
    if host_filter:
        nodes = record.get_nodes()
        if not all(h in nodes for h in host_filter):
            continue

    # Python-deferred data_filters: exotic freeform --filter fields
    # (same operator logic as pbsparse internally uses)
    if python_data_filters:
        if not _passes_python_filters(record, python_data_filters):
            continue

    yield record
```

`_passes_python_filters()` applies `(negation, op, field, value)` tuples using `getattr(record, field)`
(and `getattr(record, dict_field)[key]` for nested fields like `Resource_List[ncpus]`).

**Filter classification at call time:**
- Known DB-translatable fields → SQL phase
- `host_filter` → always Python phase
- Unknown/complex `--filter` fields → Python phase (no warning needed; handled transparently)

---

### 2. `bin/qhist-db`  — wrapper entrypoint

Reuses qhist's arg parser, config loading, and all output functions.
Replaces only the scanning loop with a single DB query (or falls back to log scanning).

**Structure:**

```python
#!/usr/bin/env python3
"""
qhist-db: qhist frontend backed by qhist-db SQLite when available.
Set QHIST_MACHINE=derecho|casper to enable DB mode; falls back to log scanning.
"""
import os, sys, datetime, operator
from collections import OrderedDict
from qhist.qhist import (get_parser, QhistConfig, get_time_bounds, keep_going,
                          tabular_output, list_output, csv_output, json_output,
                          FillFormatter, ONE_DAY)
from pbsparse import get_pbs_records
from qhist_db.qhist_compat import db_available, db_get_records

def _output_jobs(jobs, args, table_format, fields, labels, list_format):
    """Dispatch a stream of job objects to the requested output format."""
    if args.list:
        for job in jobs:
            list_output(job, fields, labels, list_format, nodes=args.nodes)
    elif args.csv:
        for job in jobs:
            csv_output(job, fields)
    elif args.json:
        ...  # same as qhist
    else:
        for job in jobs:
            print(tabular_output(vars(job), table_format))

def main():
    args = get_parser().parse_args()
    config = QhistConfig(time_format=args.time)
    # load server config (same as qhist) ...
    # build time_divisor, id_filter, host_filter, data_filters, time_filters (same as qhist) ...
    # set up output format type and table_format (same as qhist) ...

    bounds = get_time_bounds(config.pbs_log_start, config.pbs_date_format,
                             period=args.period, days=args.days)

    machine = os.environ.get("QHIST_MACHINE", "").lower()

    if machine and db_available(machine):
        # DB path: single streaming query over full date range
        jobs_iter = db_get_records(
            machine, bounds[0], bounds[1],
            time_divisor=time_divisor,
            id_filter=id_filter, host_filter=host_filter,
            data_filters=data_filters, time_filter=time_filters,
            reverse=args.reverse,
        )
        _output_jobs(jobs_iter, args, table_format, fields, labels, list_format)

    else:
        # Fallback: original qhist day-by-day log scanning loop
        log_date = bounds[1] if args.reverse else bounds[0]
        while keep_going(bounds, log_date, args.reverse):
            data_file = os.path.join(config.pbs_log_path,
                         datetime.datetime.strftime(log_date, config.pbs_date_format))
            jobs = get_pbs_records(data_file, CustomRecord, True, args.events,
                                   id_filter, host_filter, data_filters, time_filters,
                                   args.reverse, time_divisor)
            _output_jobs(jobs, args, table_format, fields, labels, list_format)
            log_date += -ONE_DAY if args.reverse else ONE_DAY
```

---

## Critical Files

| File | Role |
|---|---|
| `qhist_db/qhist_compat.py` | New: `db_available()` + `db_get_records()` generator |
| `bin/qhist-db` | New: wrapper entrypoint |
| `qhist_db/models.py` | `Job`, `JobRecord`, `User`, `Account`, `Queue` — read-only |
| `qhist_db/database.py` | `get_db_path()`, `get_session()` — read-only |
| `conda-env/.../qhist/qhist.py` | Read-only reference; import its functions |

No existing files need modification.

## Key Reused Functions / Classes

- `qhist.qhist.get_parser()`, `QhistConfig`, `get_time_bounds()`, `keep_going()` — qhist plumbing
- `qhist.qhist.tabular_output()`, `list_output()`, `csv_output()`, `json_output()` — output
- `qhist_db.database.get_session()`, `get_db_path()` — DB access
- `qhist_db.models.JobRecord.to_pbs_record()` — decompress + unpickle stored record
- `qhist_db.models.Job.job_record_obj` — relationship to `JobRecord`

## Verification

```bash
# Ensure DB exists for derecho
ls data/derecho.db

# Basic smoke test: compare DB and log results for a single date
QHIST_MACHINE=derecho bin/qhist-db -p 20260115 | head -20
qhist -p 20260115 | head -20   # should match

# Confirm fallback works when QHIST_MACHINE is unset
bin/qhist-db -p 20260115 | head -5   # should log-scan (no DB warning)

# Test common filters via DB
QHIST_MACHINE=derecho bin/qhist-db -p 20260101-20260131 -A <account> -q cpu --csv

# Test reverse order
QHIST_MACHINE=derecho bin/qhist-db -p 20260101-20260115 -r | head -5

# Test host_filter warning (should warn to stderr, not crash)
QHIST_MACHINE=derecho bin/qhist-db -H dec0001 -p 20260115 2>&1 | grep -i "warn"

# Confirm memory-bounded streaming on large range (no OOM)
QHIST_MACHINE=derecho bin/qhist-db -p 20250101-20260101 --csv | wc -l

# Verify DerechoRecord power fields appear when applicable
QHIST_MACHINE=derecho bin/qhist-db -p 20260115 --list | grep power
```
